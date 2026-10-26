package com.featureforge.testkit;

import com.featureforge.proto.EventType;
import com.featureforge.proto.RetailEvent;
import java.util.List;

public final class GoldenFixture {
    // Base timestamp aligned to an exact hour boundary: 1_800_000_000_000L
    public static final long T0_MS = 1_800_000_000_000L;
    public static final long MIN_MS = 60_000L;

    /**
     * Generates the Golden Fixture containing all 6 leakage traps for visitor 101, item 501.
     * Query View Event is at T = T0 + 30 mins.
     * Expected visitor_views_1h at T = 2.
     */
    public static List<RetailEvent> buildTrapEvents() {
        return List.of(
            // 1. Valid view at t = 10m (Counted -> 1)
            event("ev-valid-1", 101L, 501L, EventType.VIEW, T0_MS + 10 * MIN_MS, T0_MS + 10 * MIN_MS),
            // 2. Duplicate of ev-valid-1 (Ignored by deduplicator -> still 1)
            event("ev-valid-1", 101L, 501L, EventType.VIEW, T0_MS + 10 * MIN_MS, T0_MS + 10 * MIN_MS + 500),
            // 3. Valid view at t = 20m (Counted -> 2)
            event("ev-valid-2", 101L, 501L, EventType.VIEW, T0_MS + 20 * MIN_MS, T0_MS + 20 * MIN_MS),
            // 4. Late arrival trap: event_time = 15m, but available_at = 50m (Excluded at T=30m!)
            event("ev-late-1", 101L, 501L, EventType.VIEW, T0_MS + 15 * MIN_MS, T0_MS + 50 * MIN_MS),
            // 5. Same-timestamp tie & Label target view itself at t = 30m (Excluded from own row!)
            event("ev-query-target", 101L, 501L, EventType.VIEW, T0_MS + 30 * MIN_MS, T0_MS + 30 * MIN_MS),
            // 6. Future click at t = 40m (Excluded from T=30m!)
            event("ev-future-1", 101L, 501L, EventType.VIEW, T0_MS + 40 * MIN_MS, T0_MS + 40 * MIN_MS),
            // 7. Positive Add-To-Cart within 30m after T=30m (at t = 45m -> Sets Label = 1, never in features)
            event("ev-cart-label", 101L, 501L, EventType.ADD_TO_CART, T0_MS + 45 * MIN_MS, T0_MS + 45 * MIN_MS)
        );
    }

    private static RetailEvent event(String id, long visitor, long item, EventType type, long evTime, long availTime) {
        return RetailEvent.newBuilder()
                .setEventId(id)
                .setVisitorId(visitor)
                .setItemId(item)
                .setEventType(type)
                .setEventTimeMs(evTime)
                .setAvailableAtMs(availTime)
                .setSchemaVersion(1)
                .build();
    }
}