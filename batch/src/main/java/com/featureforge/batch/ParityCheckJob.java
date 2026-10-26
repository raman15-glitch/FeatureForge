package com.featureforge.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public final class ParityCheckJob {

    public record ParityReport(long totalCompared, long conformanceMismatches, double policyImpactShare, double policyMeanAbsDiff) {}

    public static ParityReport evaluateParity(
            Dataset<Row> streamFeaturesSnapshot,
            Dataset<Row> batchAcceptedFeatures,
            Dataset<Row> batchAllEventsFeatures) {

        long total = batchAcceptedFeatures.count();

        // 1. Conformance: batch over accepted events MUST match stream values with 0 deviation
        Dataset<Row> confJoin = streamFeaturesSnapshot.as("s")
                .join(batchAcceptedFeatures.as("b"), "view_id", "inner");

        long mismatches = confJoin.filter(
                col("s.visitor_views_1h").notEqual(col("b.visitor_views_1h"))
                        .or(col("s.visitor_views_24h").notEqual(col("b.visitor_views_24h")))
                        .or(col("s.visitor_carts_24h").notEqual(col("b.visitor_carts_24h")))
                        .or(col("s.item_views_1h").notEqual(col("b.item_views_1h")))
                        .or(col("s.item_carts_24h").notEqual(col("b.item_carts_24h")))
        ).count();

        if (mismatches > 0) {
            throw new IllegalStateException("Parity Conformance Failed! Mismatched rows: " + mismatches);
        }

        // 2. Policy Impact: compare stream values against batch computed over ALL events (including late)
        Dataset<Row> impactJoin = streamFeaturesSnapshot.as("s")
                .join(batchAllEventsFeatures.as("a"), "view_id", "inner")
                .withColumn("abs_diff_v1h", abs(col("s.visitor_views_1h").minus(col("a.visitor_views_1h"))));

        long changedRows = impactJoin.filter(col("abs_diff_v1h").gt(0)).count();
        Row meanRow = impactJoin.agg(avg("abs_diff_v1h")).head();
        double mad = meanRow.isNullAt(0) ? 0.0 : meanRow.getDouble(0);

        return new ParityReport(total, mismatches, total == 0 ? 0.0 : (double) changedRows / total, mad);
    }
}