package com.tkm.kafkastreem.config.kafka;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaStreamsConfig {

    /**
     * @value boostrap server url
     */
    @Value("${CONFLUENT.URL}")
    private String bootstrapServer;

    /**
     * @value confluent cloud cluster username
     */
    @Value("${CONFLUENT.USERNAME}")
    private String userName;

    /**
     * @value confluent cloud cluster password
     */
    @Value("${CONFLUENT.PASSWORD}")
    private String password;

    /**
     * @value confluent cloud schema registry URL
     */
    @Value("${CONFLUENT.SCHEMA_REG_URL}")
    private String schemaRegistryUrl;

    /**
     * @value confluent cloud schema registry user info for authentication
     */
    @Value("${CONFLUENT.SCHEMA_REG_USER}")
    private String schemaRegistryUser;


    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "one-click-to-deliver-stream");
        configProps.put("bootstrap.servers", bootstrapServer);
        configProps.put("ssl.endpoint.identification.algorithm", "https");
        configProps.put("security.protocol", "SASL_SSL");
        configProps.put("sasl.mechanism", "PLAIN");
        configProps.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", userName, password).toString());
        //avro
        configProps.put("basic.auth.credentials.source", "USER_INFO");
        configProps.put("schema.registry.basic.auth.user.info", schemaRegistryUser);
        configProps.put("schema.registry.url", schemaRegistryUrl);

        configProps.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        //configProps.put(StreamsConfig.STATE_DIR_CONFIG, "/etc/stream-state/one-click-integration-module");
        configProps.put("errors.tolerance", "all");
        configProps.put("auto.offset.reset", "earliest");
        configProps.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO");

        configProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new SpecificAvroSerde<>().getClass().getName());
        configProps.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        configProps.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);

        return new KafkaStreamsConfiguration(configProps);
    }

    @Bean
    public StreamsBuilderFactoryBeanCustomizer customizer() {
        return fb -> fb.setStateListener((newState, oldState) -> {
            System.out.println("State transition from " + oldState + " to " + newState);
        });
    }
}
