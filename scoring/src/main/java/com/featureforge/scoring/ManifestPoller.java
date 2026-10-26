package com.featureforge.scoring;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class ManifestPoller {
    private final Path modelDir;
    private final ScoringService scoringService;
    private final int expectedSchemaVersion;
    private final AtomicLong modelLoadFailuresTotal = new AtomicLong(0);
    private String currentLoadedVersion = "";

    public ManifestPoller(Path modelDir, ScoringService scoringService, int expectedSchemaVersion) {
        this.modelDir = modelDir;
        this.scoringService = scoringService;
        this.expectedSchemaVersion = expectedSchemaVersion;
    }

    public boolean pollAndReloadIfChanged(
            String version,
            Path modelFile,
            String expectedSha256,
            int schemaVersion,
            double expectedGoldenScore,
            Map<Long, Double> itemPriors,
            double globalPrior) {

        if (version.equals(currentLoadedVersion)) {
            return false;
        }

        try {
            // 1. Schema version gate
            if (schemaVersion != expectedSchemaVersion) {
                throw new IllegalArgumentException("Incompatible featureSchemaVersion: " + schemaVersion);
            }

            // 2. Cryptographic SHA-256 verification
            byte[] fileBytes = Files.readAllBytes(modelFile);
            String actualSha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(fileBytes));
            if (!actualSha.equalsIgnoreCase(expectedSha256)) {
                throw new SecurityException("Checksum mismatch! Expected " + expectedSha256 + " but got " + actualSha);
            }

            // 3. Load candidate off request path & validate golden batch score
            Booster candidateBooster = XGBoost.loadModel(modelFile.toString());
            float[] goldenFeatures = new float[]{2.0f, 5.0f, 1.0f, 10.0f, 2.0f};
            DMatrix goldenMat = new DMatrix(goldenFeatures, 1, 5, Float.NaN);
            double actualScore;
            try {
                actualScore = candidateBooster.predict(goldenMat)[0][0];
            } finally {
                goldenMat.dispose();
            }

            if (Math.abs(actualScore - expectedGoldenScore) > 1e-4) {
                candidateBooster.dispose();
                throw new IllegalStateException("Golden batch score verification failed!");
            }

            // 4. Atomic hot-swap
            ScoringService.LoadedModel candidateSnapshot = new ScoringService.LoadedModel(
                    candidateBooster, version, schemaVersion, itemPriors, globalPrior
            );
            scoringService.swapModelAtomically(candidateSnapshot, expectedSchemaVersion);
            currentLoadedVersion = version;
            return true;

        } catch (Exception e) {
            // Corrupt file, bad checksum, or schema mismatch leaves current model serving
            modelLoadFailuresTotal.incrementAndGet();
            return false;
        }
    }

    public long getModelLoadFailuresTotal() {
        return modelLoadFailuresTotal.get();
    }
}