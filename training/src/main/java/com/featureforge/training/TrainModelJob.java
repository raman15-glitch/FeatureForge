package com.featureforge.training;

import java.util.List;
import java.util.Map;

public final class ItemPriorTuner {

    public record ItemStats(long views, long positives) {}
    public record ValidationRow(long itemId, int label) {}

    public static double selectOptimalM(
            Map<Long, ItemStats> trainItemStats,
            double globalPrior,
            List<ValidationRow> validationSet,
            List<Double> candidateMs) {

        double bestM = candidateMs.getFirst();
        double bestLogLoss = Double.POSITIVE_INFINITY;

        for (double m : candidateMs) {
            double logLossSum = 0.0;
            for (ValidationRow row : validationSet) {
                ItemStats stats = trainItemStats.get(row.itemId());
                double p = (stats == null)
                        ? globalPrior
                        : (stats.positives() + m * globalPrior) / (stats.views() + m);

                // Clamp probability to avoid log(0)
                double clamped = Math.max(1e-6, Math.min(1.0 - 1e-6, p));
                logLossSum += -(row.label() * Math.log(clamped) + (1 - row.label()) * Math.log(1.0 - clamped));
            }
            double avgLogLoss = logLossSum / validationSet.size();
            if (avgLogLoss < bestLogLoss) {
                bestLogLoss = avgLogLoss;
                bestM = m;
            }
        }
        return bestM;
    }
}