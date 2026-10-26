package com.featureforge.batch;

import com.featureforge.lib.FeatureDefinitions;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public final class PointInTimeFeatureJob {

    public static Dataset<Row> computeFeatures(Dataset<Row> acceptedEvents) {
        Dataset<Row> eventsWithMinute = acceptedEvents.withColumn(
                "event_minute",
                floor(col("event_time_ms").divide(FeatureDefinitions.ONE_MINUTE_MS))
        );

        Dataset<Row> queryViews = eventsWithMinute
                .filter(col("event_type").equalTo("VIEW"))
                .select(
                        col("event_id").as("view_id"),
                        col("visitor_id"),
                        col("item_id"),
                        col("event_time_ms").as("query_time_ms"),
                        col("event_minute").as("query_minute")
                ).as("q");

        Dataset<Row> history = eventsWithMinute.as("h");

        // Strict "Two Clocks" + Minute-Bucket Join Condition for Visitor History
        Column visitorPitCondition = col("q.visitor_id").equalTo(col("h.visitor_id"))
                .and(col("h.available_at_ms").lt(col("q.query_time_ms")))
                .and(col("h.event_id").notEqual(col("q.view_id")))
                .and(col("h.event_minute").geq(col("q.query_minute").minus(FeatureDefinitions.TWENTY_FOUR_HOURS_MINUTES)))
                .and(col("h.event_minute").leq(col("q.query_minute").minus(1)));

        Dataset<Row> visitorAgg = queryViews.join(history, visitorPitCondition, "left_outer")
                .groupBy("q.view_id", "q.visitor_id", "q.item_id", "q.query_time_ms", "q.query_minute")
                .agg(
                        sum(when(col("h.event_type").equalTo("VIEW")
                                .and(col("h.event_minute").geq(col("q.query_minute").minus(FeatureDefinitions.ONE_HOUR_MINUTES))), 1)
                                .otherwise(0)).as("visitor_views_1h"),
                        sum(when(col("h.event_type").equalTo("VIEW"), 1).otherwise(0)).as("visitor_views_24h"),
                        sum(when(col("h.event_type").equalTo("ADD_TO_CART"), 1).otherwise(0)).as("visitor_carts_24h"),
                        max("h.event_time_ms").as("max_contributing_event_time_ms")
                );

        // Item History Join
        Column itemPitCondition = col("v.item_id").equalTo(col("ih.item_id"))
                .and(col("ih.available_at_ms").lt(col("v.query_time_ms")))
                .and(col("ih.event_id").notEqual(col("v.view_id")))
                .and(col("ih.event_minute").geq(col("v.query_minute").minus(FeatureDefinitions.TWENTY_FOUR_HOURS_MINUTES)))
                .and(col("ih.event_minute").leq(col("v.query_minute").minus(1)));

        return visitorAgg.as("v").join(eventsWithMinute.as("ih"), itemPitCondition, "left_outer")
                .groupBy(
                        col("v.view_id"),
                        col("v.visitor_views_1h"),
                        col("v.visitor_views_24h"),
                        col("v.visitor_carts_24h"),
                        col("v.max_contributing_event_time_ms")
                )
                .agg(
                        sum(when(col("ih.event_type").equalTo("VIEW")
                                .and(col("ih.event_minute").geq(col("v.query_minute").minus(FeatureDefinitions.ONE_HOUR_MINUTES))), 1)
                                .otherwise(0)).as("item_views_1h"),
                        sum(when(col("ih.event_type").equalTo("ADD_TO_CART"), 1).otherwise(0)).as("item_carts_24h")
                )
                .orderBy("view_id");
    }
}