package com.featureforge.stream;

import com.featureforge.lib.FeatureDefinitions;
import com.featureforge.proto.EventType;
import com.featureforge.proto.RetailEvent;
import redis.clients.jedis.JedisPooled;

import java.util.List;

public final class LuaRedisFeatureWriter implements StreamFeaturesTopology.RedisFeatureWriter {
    /**
     * Atomic Lua script: checks if event_id was already applied to this bucket key;
     * if not, increments the bucket counter and refreshes the event-time-scaled TTL.
     */
    private static final String IDEMPOTENT_INCR_LUA =
            "local bucketKey = KEYS[1]\n" +
            "local dedupSetKey = KEYS[2]\n" +
            "local eventId = ARGV[1]\n" +
            "local ttlSeconds = tonumber(ARGV[2])\n" +
            "if redis.call('SISMEMBER', dedupSetKey, eventId) == 1 then\n" +
            "    return tonumber(redis.call('GET', bucketKey) or '0')\n" +
            "end\n" +
            "redis.call('SADD', dedupSetKey, eventId)\n" +
            "redis.call('EXPIRE', dedupSetKey, ttlSeconds)\n" +
            "local val = redis.call('INCR', bucketKey)\n" +
            "redis.call('EXPIRE', bucketKey, ttlSeconds)\n" +
            "return val\n";

    private final JedisPooled jedis;
    private final long wallTtlSeconds;

    public LuaRedisFeatureWriter(JedisPooled jedis, long allowedLatenessMinutes, double replaySpeedupFactor) {
        this.jedis = jedis;
        // Event-time TTL = 24h + L + 1h slack, converted to wall-clock seconds via replaySpeedupFactor
        long eventTtlSeconds = (24 * 60 + allowedLatenessMinutes + 60) * 60L;
        this.wallTtlSeconds = Math.max(1L, Math.round(eventTtlSeconds / replaySpeedupFactor));
    }

    @Override
    public void incrementFeatureBuckets(RetailEvent event, long minuteBucket) {
        String metric = switch (event.getEventType()) {
            case VIEW -> "view";
            case ADD_TO_CART -> "cart";
            default -> null;
        };
        if (metric == null) return;

        String vKey = FeatureDefinitions.visitorMinuteKey(event.getVisitorId(), metric, minuteBucket);
        String iKey = FeatureDefinitions.itemMinuteKey(event.getItemId(), metric, minuteBucket);

        evalIdempotentIncr(vKey, event.getEventId());
        evalIdempotentIncr(iKey, event.getEventId());
    }

    private void evalIdempotentIncr(String bucketKey, String eventId) {
        String dedupKey = "dedup:" + bucketKey;
        // Under Redis 'noeviction', if maxmemory is reached, this throws JedisDataException (OOM) loudly
        jedis.eval(
                IDEMPOTENT_INCR_LUA,
                List.of(bucketKey, dedupKey),
                List.of(eventId, String.valueOf(wallTtlSeconds))
        );
    }
}