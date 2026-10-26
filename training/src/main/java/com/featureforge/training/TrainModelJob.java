package com.featureforge.training;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

public final class TrainModelJob {

    public record TrainingRow(long itemId, float[] features, int label) {}
    public record MetricsReport(double xgbLogLoss, double xgbAuc, double baselineLogLoss, double priorLogLoss, double optimalM) {}

    public static MetricsReport trainAndPublish(
            List<TrainingRow> trainRows,
            List<TrainingRow> valRows,
            List<Double> candidateMs,
            Path outputDir,
            String modelVersion) throws Exception {

        Files.createDirectories(outputDir);

        // 1. Prepare XGBoost DMatrices
        DMatrix trainMat = buildDMatrix(trainRows);
        DMatrix valMat = buildDMatrix(valRows);

        Map<String, Object> params = new HashMap<>();
        params.put("eta", 0.1);
        params.put("max_depth", 6);
        params.put("objective", "binary:logistic");
        params.put("eval_metric", "logloss");
        params.put("nthread", 4);
        params.put("seed", 42);

        Booster booster = XGBoost.train(trainMat, params, 50, Map.of("train", trainMat, "val", valMat), null, null);

        // 2. Evaluate XGBoost on validation split
        float[][] xgbPreds = booster.predict(valMat);
        double[] xgbProbs = new double[valRows.size()];
        int[] valLabels = new int[valRows.size()];
        for (int i = 0; i < valRows.size(); i++) {
            xgbProbs[i] = xgbPreds[i][0];
            valLabels[i] = valRows.get(i).label();
        }
        double xgbLogLoss = computeLogLoss(xgbProbs, valLabels);
        double xgbAuc = computeRocAuc(xgbProbs, valLabels);

        // 3. Train simple SGD Logistic Regression Baseline for comparison
        double[] weights = trainLogisticBaseline(trainRows, 5, 0.01, 20);
        double[] baseProbs = new double[valRows.size()];
        for (int i = 0; i < valRows.size(); i++) {
            baseProbs[i] = predictLogistic(weights, valRows.get(i).features());
        }
        double baseLogLoss = computeLogLoss(baseProbs, valLabels);

        // 4. Compute Item Priors on TRAIN split only and sweep for optimal m (Fix #3)
        Map<Long, ItemPriorTuner.ItemStats> itemStats = new HashMap<>();
        long totalPos = 0;
        for (TrainingRow r : trainRows) {
            totalPos += r.label();
            ItemPriorTuner.ItemStats prev = itemStats.getOrDefault(r.itemId(), new ItemPriorTuner.ItemStats(0, 0));
            itemStats.put(r.itemId(), new ItemPriorTuner.ItemStats(prev.views() + 1, prev.positives() + r.label()));
        }
        double globalPrior = trainRows.isEmpty() ? 0.05 : (double) totalPos / trainRows.size();
        List<ItemPriorTuner.ValidationRow> priorVal = valRows.stream()
                .map(r -> new ItemPriorTuner.ValidationRow(r.itemId(), r.label()))
                .toList();
        double optimalM = ItemPriorTuner.selectOptimalM(itemStats, globalPrior, priorVal, candidateMs);

        double[] priorProbs = new double[valRows.size()];
        for (int i = 0; i < valRows.size(); i++) {
            ItemPriorTuner.ItemStats st = itemStats.get(valRows.get(i).itemId());
            priorProbs[i] = (st == null) ? globalPrior : (st.positives() + optimalM * globalPrior) / (st.views() + optimalM);
        }
        double priorLogLoss = computeLogLoss(priorProbs, valLabels);

        // 5. Publish model and manifest atomically
        Path tempModel = outputDir.resolve("model_" + modelVersion + ".xgb.tmp");
        Path finalModel = outputDir.resolve("model_" + modelVersion + ".xgb");
        booster.saveModel(tempModel.toString());
        Files.move(tempModel, finalModel, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        String sha256 = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(finalModel))
        );

        // Golden validation sample score for hot-swap verification
        float[] goldenFeatures = new float[]{2.0f, 5.0f, 1.0f, 10.0f, 2.0f};
        DMatrix goldenMat = new DMatrix(goldenFeatures, 1, 5, Float.NaN);
        double goldenExpectedScore = booster.predict(goldenMat)[0][0];

        String manifestJson = String.format(Locale.US,
                "{\n  \"modelVersion\": \"%s\",\n  \"modelFile\": \"%s\",\n  \"sha256\": \"%s\",\n  \"featureSchemaVersion\": 1,\n  \"optimalM\": %.2f,\n  \"globalPrior\": %.6f,\n  \"goldenExpectedScore\": %.6f\n}",
                modelVersion, finalModel.getFileName().toString(), sha256, optimalM, globalPrior, goldenExpectedScore
        );

        Path tempManifest = outputDir.resolve("manifest.json.tmp");
        Path finalManifest = outputDir.resolve("manifest.json");
        Files.writeString(tempManifest, manifestJson, StandardCharsets.UTF_8);
        Files.move(tempManifest, finalManifest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        goldenMat.dispose();
        trainMat.dispose();
        valMat.dispose();
        booster.dispose();

        return new MetricsReport(xgbLogLoss, xgbAuc, baseLogLoss, priorLogLoss, optimalM);
    }

    private static DMatrix buildDMatrix(List<TrainingRow> rows) throws Exception {
        int n = rows.size();
        float[] flat = new float[n * 5];
        float[] labels = new float[n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(rows.get(i).features(), 0, flat, i * 5, 5);
            labels[i] = rows.get(i).label();
        }
        DMatrix mat = new DMatrix(flat, n, 5, Float.NaN);
        mat.setLabel(labels);
        return mat;
    }

    private static double[] trainLogisticBaseline(List<TrainingRow> rows, int dim, double lr, int epochs) {
        double[] w = new double[dim + 1]; // w[dim] is bias
        for (int ep = 0; ep < epochs; ep++) {
            for (TrainingRow r : rows) {
                double p = predictLogistic(w, r.features());
                double err = p - r.label();
                for (int j = 0; j < dim; j++) {
                    w[j] -= lr * err * r.features()[j];
                }
                w[dim] -= lr * err;
            }
        }
        return w;
    }

    private static double predictLogistic(double[] w, float[] x) {
        double z = w[x.length];
        for (int j = 0; j < x.length; j++) z += w[j] * x[j];
        return 1.0 / (1.0 + Math.exp(-z));
    }

    public static double computeLogLoss(double[] probs, int[] labels) {
        double sum = 0.0;
        for (int i = 0; i < probs.length; i++) {
            double p = Math.max(1e-6, Math.min(1.0 - 1e-6, probs[i]));
            sum += -(labels[i] * Math.log(p) + (1 - labels[i]) * Math.log(1.0 - p));
        }
        return probs.length == 0 ? 0.0 : sum / probs.length;
    }

    public static double computeRocAuc(double[] probs, int[] labels) {
        Integer[] idx = new Integer[probs.length];
        for (int i = 0; i < probs.length; i++) idx[i] = i;
        Arrays.sort(idx, Comparator.comparingDouble(i -> probs[i]));

        long nPos = 0, nNeg = 0;
        double rankSumPos = 0.0;
        for (int rank = 0; rank < idx.length; rank++) {
            if (labels[idx[rank]] == 1) {
                nPos++;
                rankSumPos += (rank + 1);
            } else {
                nNeg++;
            }
        }
        if (nPos == 0 || nNeg == 0) return 0.5;
        return (rankSumPos - (nPos * (nPos + 1) / 2.0)) / (nPos * nNeg);
    }
}