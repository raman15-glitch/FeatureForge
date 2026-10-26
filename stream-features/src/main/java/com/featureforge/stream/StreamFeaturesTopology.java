package com.featureforge.stream;

import com.featureforge.lib.FeatureDefinitions;
import com.featureforge.proto.RetailEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.concurrent.atomic.AtomicLong;

public final class StreamFeaturesTopology {
    public static final String DEDUP_STORE = "event-id-dedup-store";
    public static final String INPUT_TOPIC = "events.raw";
    public static final String ACCEPTED_TOPIC = "events.accepted";
    public static final String LATE_TOPIC = "events.late";

    public static final class ConservationMetrics {
        public final AtomicLong eventsIn = new AtomicLong();
        public final AtomicLong accepted = new AtomicLong();
        public final AtomicLong late = new AtomicLong();
        public final AtomicLong duplicates = new AtomicLong();

        public void assertConservation() {
            long in = eventsIn.get();
            long sum = accepted.get() + late.get() + duplicates.get();
            if (in != sum) {
                throw new IllegalStateException(
                        String.format("Conservation invariant violated! in=%d != accepted(%d)+late(%d)+dup(%d)",
                                in, accepted.get(), late.get(), duplicates.get()));
            }
        }
    }

    public static Topology build(
            long allowedLatenessMs,
            ConservationMetrics metrics,
            RedisFeatureWriter redisWriter) {

        StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DEDUP_STORE),
                Serdes.String(),
                Serdes.Long()
        ));

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), new RetailEventSerde()))
                .process(() -> new LatenessAndDedupProcessor(allowedLatenessMs, metrics, redisWriter), DEDUP_STORE)
                .to(
                        (key, value, recordContext) -> key.startsWith("LATE:") ? LATE_TOPIC : ACCEPTED_TOPIC,
                        Produced.with(Serdes.String(), new RetailEventSerde())
                );

        return builder.build();
    }

    private static final class LatenessAndDedupProcessor
            extends ContextualProcessor<String, RetailEvent, String, RetailEvent> {

        private final long allowedLatenessMs;
        private final ConservationMetrics metrics;
        private final RedisFeatureWriter redisWriter;
        private KeyValueStore<String, Long> dedupStore;
        private long partitionStreamTimeMs = Long.MIN_VALUE;

        LatenessAndDedupProcessor(
                long allowedLatenessMs,
                ConservationMetrics metrics,
                RedisFeatureWriter redisWriter) {
            this.allowedLatenessMs = allowedLatenessMs;
            this.metrics = metrics;
            this.redisWriter = redisWriter;
        }

        @Override
        public void init(ProcessorContext<String, RetailEvent> context) {
            super.init(context);
            this.dedupStore = context.getStateStore(DEDUP_STORE);
        }

        @Override
        public void process(Record<String, RetailEvent> record) {
            metrics.eventsIn.incrementAndGet();
            RetailEvent ev = record.value();
            long eventTime = ev.getEventTimeMs();

            // 1. Deduplication check first
            if (dedupStore.get(ev.getEventId()) != null) {
                metrics.duplicates.incrementAndGet();
                return;
            }
            dedupStore.put(ev.getEventId(), eventTime);

            // 2. Advance stream time
            if (eventTime > partitionStreamTimeMs) {
                partitionStreamTimeMs = eventTime;
            }

            // 3. Lateness rule: strictly less than streamTime - L is late; boundary is accepted
            if (eventTime < partitionStreamTimeMs - allowedLatenessMs) {
                metrics.late.incrementAndGet();
                context().forward(record.withKey("LATE:" + ev.getEventId()));
                return;
            }

            // 4. Accepted: Increment minute bucket in Redis with idempotent Lua script and forward
            metrics.accepted.incrementAndGet();
            long minuteBucket = FeatureDefinitions.toMinuteBucket(eventTime);
            redisWriter.incrementFeatureBuckets(ev, minuteBucket);
            context().forward(record.withKey(String.valueOf(ev.getVisitorId())));
        }
    }

    public interface RedisFeatureWriter {
        void incrementFeatureBuckets(RetailEvent event, long minuteBucket);
    }
}