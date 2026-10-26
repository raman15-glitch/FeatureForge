package com.featureforge.scoring;

import com.featureforge.lib.FeatureDefinitions;
import redis.clients.jedis.JedisPooled;

import java.util.ArrayList;
import java.util.List;

public final class PipelinedRedisFeatureClient implements ScoringService.BatchedRedisFeatureClient {
    private final JedisPooled jedis;

    public PipelinedRedisFeatureClient(JedisPooled jedis) {
        this.jedis = jedis;
    }

    @Override
    public float[] fetchFiveFeatures(long visitorId, long itemId, long requestTimeMs) {
        long queryMinute = FeatureDefinitions.toMinuteBucket(requestTimeMs);
        List<String> keysToFetch = new ArrayList<>(1440 * 3);

        // Build all minute-bucket keys for [queryMinute - 1440, queryMinute - 1] in one MGET call
        for (long m = queryMinute - 1440; m <= queryMinute - 1; m++) {
            keysToFetch.add(FeatureDefinitions.visitorMinuteKey(visitorId, "view", m));
            keysToFetch.add(FeatureDefinitions.visitorMinuteKey(visitorId, "cart", m));
            keysToFetch.add(FeatureDefinitions.itemMinuteKey(itemId, "view", m));
            keysToFetch.add(FeatureDefinitions.itemMinuteKey(itemId, "cart", m));
        }

        List<String> values = jedis.mget(keysToFetch.toArray(new String[0]));

        float vViews1h = 0, vViews24h = 0, vCarts24h = 0, iViews1h = 0, iCarts24h = 0;
        int idx = 0;
        for (long m = queryMinute - 1440; m <= queryMinute - 1; m++) {
            boolean inLastHour = (m >= queryMinute - 60);
            float vView = parseOrZero(values.get(idx++));
            float vCart = parseOrZero(values.get(idx++));
            float iView = parseOrZero(values.get(idx++));
            float iCart = parseOrZero(values.get(idx++));

            if (inLastHour) {
                vViews1h += vView;
                iViews1h += iView;
            }
            vViews24h += vView;
            vCarts24h += vCart;
            iCarts24h += iCart;
        }

        return new float[]{vViews1h, vViews24h, vCarts24h, iViews1h, iCarts24h};
    }

    private static float parseOrZero(String val) {
        return (val == null || val.isEmpty()) ? 0.0f : Float.parseFloat(val);
    }
}