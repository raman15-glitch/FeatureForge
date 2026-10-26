package com.featureforge.batch;

import com.featureforge.lib.FeatureDefinitions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public final class TrainingSetJoinerJob {

    public static Dataset<Row> joinAndSplitWithEmbargo(
            Dataset<Row> labels,
            Dataset<Row> features,
            long censoredViewCount,
            double trainFraction) {

        long labelCount = labels.count();
        long distinctLabels = labels.select("view_id").distinct().count();
        long featureCount = features.count();
        long distinctFeatures = features.select("view_id").distinct().count();

        if (labelCount != distinctLabels || featureCount != distinctFeatures) {
            throw new IllegalStateException("Duplicate view_id detected in labels or features!");
        }

        // Assert no label row is missing a feature row (when evaluated on accepted views)
        long missingFeatures = labels.join(features, "view_id", "left_anti").count();
        if (missingFeatures > 0) {
            throw new IllegalStateException("Orphaned label rows without features: " + missingFeatures);
        }

        // Assert unlabelled feature rows equal the exact 30m right-censored count
        long unlabelledFeatures = features.join(labels, "view_id", "left_anti").count();
        if (unlabelledFeatures != censoredViewCount) {
            throw new IllegalStateException(
                    String.format("Unlabelled features (%d) != right-censored view count (%d)",
                            unlabelledFeatures, censoredViewCount));
        }

        Dataset<Row> joined = features.join(labels, "view_id", "inner");

        // Temporal split with a strict 30-minute embargo between train and validation
        long[] quantiles = joined.stat().approxQuantile("query_time_ms", new double[]{trainFraction}, 0.001);
        long splitTimestampMs = quantiles[0];
        long validationStartMs = splitTimestampMs + FeatureDefinitions.LABEL_WINDOW_MS;

        return joined.withColumn(
                "split",
                when(col("query_time_ms").leq(splitTimestampMs), lit("TRAIN"))
                        .when(col("query_time_ms").gt(validationStartMs), lit("VAL"))
                        .otherwise(lit("EMBARGO_DROPPED"))
        ).filter(col("split").notEqual("EMBARGO_DROPPED"))
         .orderBy("view_id");
    }
}