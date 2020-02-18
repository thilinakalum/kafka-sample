package com.tkm.kafkastreem.config.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
public class KafkaProducerConfig {


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


    @Bean
    public ProducerFactory<?, ?> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put("bootstrap.servers", bootstrapServer);
        configProps.put("ssl.endpoint.identification.algorithm", "https");
        configProps.put("security.protocol", "SASL_SSL");
        configProps.put("sasl.mechanism", "PLAIN");
        configProps.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", userName, password).toString());
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        //avro
        configProps.put("basic.auth.credentials.source", "USER_INFO");
        configProps.put("schema.registry.basic.auth.user.info", schemaRegistryUser);
        configProps.put("schema.registry.url", schemaRegistryUrl);
        configProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        configProps.put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

        DefaultKafkaProducerFactory factory = new DefaultKafkaProducerFactory<>(configProps);
        factory.transactionCapable();
        String transactionId = UUID.randomUUID().toString();
        factory.setTransactionIdPrefix("one-click-module-" + transactionId);
        return factory;
    }

    @Bean(name = "orderStartedBean")
    public KafkaTemplate<?, ?> kafkaTemplate() {
        return new KafkaTemplate(producerFactory());
    }


    @Bean(name = "kafkaTransactionManager")
    public KafkaTransactionManager transactionManager(ProducerFactory producerFactory) {
        KafkaTransactionManager manager = new KafkaTransactionManager(producerFactory);
        return manager;
    }


}