package com.featureforge.batch;

import com.featureforge.proto.EventType;
import com.featureforge.proto.RetailEvent;
import com.featureforge.testkit.GoldenFixture;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LeakageMetamorphicTest {
    private SparkSession spark;

    @BeforeAll
    void setUpSpark() {
        spark = SparkSession.builder()
                .appName("FeatureForge-LeakageTests")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
    }

    @AfterAll
    void tearDownSpark() {
        if (spark != null) spark.stop();
    }

    @Test
    void goldenFixtureTrapsAreHandledCorrectly() {
        Dataset<Row> df = toDataFrame(GoldenFixture.buildTrapEvents()).dropDuplicates("event_id");
        Dataset<Row> features = PointInTimeFeatureJob.computeFeatures(df);

        Row targetRow = features.filter(col("view_id").equalTo("ev-query-target")).head();
        // Only ev-valid-1 and ev-valid-2 are strictly before T=30m and available before T=30m
        assertEquals(2L, targetRow.getLong(targetRow.fieldIndex("visitor_views_1h")));
    }

    @Test
    void labelWindowAndLeftSemiJoinAgreeExactly() {
        Dataset<Row> df = toDataFrame(GoldenFixture.buildTrapEvents()).dropDuplicates("event_id");
        List<Row> windowRows = LabelGenerationJob.computeLabelsWindow(df).collectAsList();
        List<Row> semiJoinRows = LabelGenerationJob.computeLabelsSemiJoin(df).collectAsList();
        assertEquals(windowRows, semiJoinRows);
    }

    @Test
    void futurePerturbationAndTruncationLeaveFeaturesIdentical() {
        List<RetailEvent> baseLog = new ArrayList<>(GoldenFixture.buildTrapEvents());
        long cutoffT = GoldenFixture.T0_MS + 30 * GoldenFixture.MIN_MS;

        // 1. Truncated log cut at T
        List<RetailEvent> truncatedLog = baseLog.stream()
                .filter(e -> e.getEventTimeMs() <= cutoffT && e.getAvailableAtMs() <= cutoffT)
                .toList();

        // 2. Perturbed log with 50 injected future clicks and carts after T
        List<RetailEvent> perturbedLog = new ArrayList<>(baseLog);
        for (int i = 1; i <= 50; i++) {
            long futureTs = cutoffT + i * 10_000L;
            perturbedLog.add(RetailEvent.newBuilder()
                    .setEventId("perturbed-future-" + i)
                    .setVisitorId(101L)
                    .setItemId(501L)
                    .setEventType(EventType.VIEW)
                    .setEventTimeMs(futureTs)
                    .setAvailableAtMs(futureTs)
                    .setSchemaVersion(1)
                    .build());
        }

        Row truncatedRow = PointInTimeFeatureJob.computeFeatures(toDataFrame(truncatedLog).dropDuplicates("event_id"))
                .filter(col("view_id").equalTo("ev-query-target")).head();
        Row perturbedRow = PointInTimeFeatureJob.computeFeatures(toDataFrame(perturbedLog).dropDuplicates("event_id"))
                .filter(col("view_id").equalTo("ev-query-target")).head();

        assertEquals(
                truncatedRow.getLong(truncatedRow.fieldIndex("visitor_views_1h")),
                perturbedRow.getLong(perturbedRow.fieldIndex("visitor_views_1h")),
                "Future perturbation leaked into historical feature row!"
        );
    }

    @Test
    void negativeControlDeliberatelyLeakyJoinFailsPerturbationTest() {
        List<RetailEvent> baseLog = GoldenFixture.buildTrapEvents();
        Dataset<Row> df = toDataFrame(baseLog).dropDuplicates("event_id");

        // Deliberately leaky join: aggregates all views per visitor ignoring time
        long leakyCount = df.filter(col("visitor_id").equalTo(101L).and(col("event_type").equalTo("VIEW"))).count();
        assertNotEquals(2L, leakyCount, "Negative control must fail the golden expectation of 2!");
    }

    private Dataset<Row> toDataFrame(List<RetailEvent> events) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("event_id", DataTypes.StringType, false),
                DataTypes.createStructField("visitor_id", DataTypes.LongType, false),
                DataTypes.createStructField("item_id", DataTypes.LongType, false),
                DataTypes.createStructField("event_type", DataTypes.StringType, false),
                DataTypes.createStructField("event_time_ms", DataTypes.LongType, false),
                DataTypes.createStructField("available_at_ms", DataTypes.LongType, false)
        });
        List<Row> rows = events.stream().map(e -> RowFactory.create(
                e.getEventId(), e.getVisitorId(), e.getItemId(),
                e.getEventType().name(), e.getEventTimeMs(), e.getAvailableAtMs()
        )).toList();
        return spark.createDataFrame(rows, schema);
    }
}