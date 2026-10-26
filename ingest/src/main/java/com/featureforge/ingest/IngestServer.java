package com.featureforge.ingest;

import com.featureforge.proto.*;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.util.Properties;

public final class IngestServer extends IngestServiceGrpc.IngestServiceImplBase {
    private static final int SUPPORTED_SCHEMA_VERSION = 1;
    private final Producer<String, byte[]> producer;
    private final String rawTopic;

    public IngestServer(Producer<String, byte[]> producer, String rawTopic) {
        this.producer = producer;
        this.rawTopic = rawTopic;
    }

    public static Properties buildMtlsProducerProps(String bootstrapServers, String keystorePath, String truststorePath, String password) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Idempotent producer guarantees exactly-once delivery per partition session
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        if (keystorePath != null && !keystorePath.isEmpty()) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorePath);
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, password);
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password);
        }
        return props;
    }

    @Override
    public void publishEvents(IngestRequest request, StreamObserver<IngestResponse> responseObserver) {
        int accepted = 0;
        try {
            for (RetailEvent ev : request.getEventsList()) {
                if (ev.getSchemaVersion() != SUPPORTED_SCHEMA_VERSION || ev.getEventId().isEmpty()) {
                    throw new IllegalArgumentException("Invalid schema version or missing event_id");
                }
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        rawTopic,
                        null,
                        ev.getEventTimeMs(),
                        String.valueOf(ev.getVisitorId()),
                        ev.toByteArray()
                );
                producer.send(record).get();
                accepted++;
            }
            responseObserver.onNext(IngestResponse.newBuilder().setAcceptedCount(accepted).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}