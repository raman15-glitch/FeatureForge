package com.featureforge.scoring;

import com.featureforge.proto.ScoreRequest;
import com.featureforge.proto.ScoreResponse;
import com.featureforge.proto.ScoringMode;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class ScoringService {

    public static final class LoadedModel {
        private final Booster booster;
        private final String modelVersion;
        private final int featureSchemaVersion;
        private final Map<Long, Double> itemPriorMap;
        private final double globalPrior;
        private final AtomicInteger activeReaders = new AtomicInteger(0);
        private volatile boolean retired = false;

        public LoadedModel(
                Booster booster,
                String modelVersion,
                int featureSchemaVersion,
                Map<Long, Double> itemPriorMap,
                double globalPrior) {
            this.booster = booster;
            this.modelVersion = modelVersion;
            this.featureSchemaVersion = featureSchemaVersion;
            this.itemPriorMap = Map.copyOf(itemPriorMap);
            this.globalPrior = globalPrior;
        }

        public boolean tryAcquire() {
            while (true) {
                if (retired) return false;
                int current = activeReaders.get();
                if (activeReaders.compareAndSet(current, current + 1)) {
                    if (retired) {
                        release();
                        return false;
                    }
                    return true;
                }
            }
        }

        public void release() {
            if (activeReaders.decrementAndGet() == 0 && retired) {
                booster.dispose(); // Free XGBoost native C++ heap memory safely
            }
        }

        public void markRetired() {
            retired = true;
            if (activeReaders.get() == 0) {
                booster.dispose();
            }
        }

        public double itemPrior(long itemId) {
            return itemPriorMap.getOrDefault(itemId, globalPrior);
        }

        /**
         * Synchronized or thread-local prediction call depending on XGBoost4J native thread safety.
         */
        public synchronized double predict(float[] features) throws Exception {
            DMatrix matrix = new DMatrix(features, 1, features.length, Float.NaN);
            try {
                return booster.predict(matrix)[0][0];
            } finally {
                matrix.dispose();
            }
        }

        public String modelVersion() { return modelVersion; }
        public int featureSchemaVersion() { return featureSchemaVersion; }
    }

    public interface BatchedRedisFeatureClient {
        float[] fetchFiveFeatures(long visitorId, long itemId, long requestTimeMs) throws Exception;
    }

    private final AtomicReference<LoadedModel> activeModelRef;
    private final BatchedRedisFeatureClient redisClient;
    private final CircuitBreaker circuitBreaker;

    public ScoringService(LoadedModel initialModel, BatchedRedisFeatureClient redisClient) {
        this.activeModelRef = new AtomicReference<>(initialModel);
        this.redisClient = redisClient;

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(20)
                .minimumNumberOfCalls(10)
                .slowCallDurationThreshold(Duration.ofMillis(25))
                .slowCallRateThreshold(50.0f)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(3)
                .build();

        this.circuitBreaker = CircuitBreaker.of("redis-feature-store", config);
    }

    /**
     * Atomic hot-swap: validates candidate off the request path, swaps reference,
     * and safely retires the previous native Booster handle once in-flight requests drain.
     */
    public void swapModelAtomically(LoadedModel candidate, int expectedSchemaVersion) {
        if (candidate.featureSchemaVersion() != expectedSchemaVersion) {
            throw new IllegalArgumentException("Schema mismatch on model swap!");
        }
        LoadedModel old = activeModelRef.getAndSet(candidate);
        if (old != null && old != candidate) {
            old.markRetired();
        }
    }

    public ScoreResponse score(ScoreRequest req) {
        LoadedModel snapshot = activeModelRef.get();
        while (!snapshot.tryAcquire()) {
            snapshot = activeModelRef.get();
        }

        try {
            Callable<float[]> decoratedRedisCall = CircuitBreaker.decorateCallable(
                    circuitBreaker,
                    () -> redisClient.fetchFiveFeatures(req.getVisitorId(), req.getItemId(), req.getRequestTimeMs())
            );

            float[] features = decoratedRedisCall.call();
            double prediction = snapshot.predict(features);

            return ScoreResponse.newBuilder()
                    .setScore(prediction)
                    .setMode(ScoringMode.FULL)
                    .setReason("OK")
                    .setModelVersion(snapshot.modelVersion())
                    .build();

        } catch (CallNotPermittedException openBreakerEx) {
            return buildDegradedResponse(snapshot, req.getItemId(), "CIRCUIT_OPEN");
        } catch (Exception redisOrTimeoutEx) {
            return buildDegradedResponse(snapshot, req.getItemId(), "REDIS_TIMEOUT_OR_ERROR");
        } finally {
            snapshot.release();
        }
    }

    private ScoreResponse buildDegradedResponse(LoadedModel snapshot, long itemId, String reason) {
        return ScoreResponse.newBuilder()
                .setScore(snapshot.itemPrior(itemId))
                .setMode(ScoringMode.DEGRADED)
                .setReason(reason)
                .setModelVersion(snapshot.modelVersion())
                .build();
    }
}