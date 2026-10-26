package com.featureforge.lib;

public final class FeatureDefinitions {
    public static final long ONE_MINUTE_MS = 60_000L;
    public static final long ONE_HOUR_MINUTES = 60L;
    public static final long TWENTY_FOUR_HOURS_MINUTES = 1440L;
    public static final long LABEL_WINDOW_MS = 30 * ONE_MINUTE_MS; // 1,800,000 ms

    private FeatureDefinitions() {}

    /**
     * Floors a millisecond timestamp to its exact minute bucket ID.
     */
    public static long toMinuteBucket(long timestampMs) {
        return Math.floorDiv(timestampMs, ONE_MINUTE_MS);
    }

    /**
     * Shared rule: The feature value at query time T is the count of events whose
     * event minute falls in the N minutes strictly before the minute containing T.
     * The current partial minute containing T is excluded.
     */
    public static boolean isInFeatureWindow(
            long eventTimeMs,
            long eventAvailableAtMs,
            long queryTimeMs,
            long windowMinutes) {

        // Rule 1: Event must have been known to the system strictly before queryTimeMs ("Two Clocks")
        if (eventAvailableAtMs >= queryTimeMs) {
            return false;
        }

        // Rule 2: Event minute bucket must fall in [minute(T) - N, minute(T) - 1]
        long queryMinute = toMinuteBucket(queryTimeMs);
        long eventMinute = toMinuteBucket(eventTimeMs);

        long windowStartMinuteInclusive = queryMinute - windowMinutes;
        long windowEndMinuteInclusive = queryMinute - 1;

        return eventMinute >= windowStartMinuteInclusive && eventMinute <= windowEndMinuteInclusive;
    }

    /**
     * Redis key naming contract shared by Stream writer and Scoring reader.
     */
    public static String visitorMinuteKey(long visitorId, String metric, long minuteBucket) {
        return "v:" + visitorId + ":" + metric + ":" + minuteBucket;
    }

    public static String itemMinuteKey(long itemId, String metric, long minuteBucket) {
        return "i:" + itemId + ":" + metric + ":" + minuteBucket;
    }
}