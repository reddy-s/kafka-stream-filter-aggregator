package io.dataglitter.kafka.aggregator.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.dataglitter.kafka.commons.KafkaAvroSerializerWithSchemaName;
import io.dataglitter.kafka.commons.serializer.GenericAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by reddys on 06/04/2018.
 */
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    private static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id}")
    private String kStreamsConsumerGroup;

    private final KafkaProperties properties;

    @Autowired
    public KafkaConfig( KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    public ProducerFactory<?, ?> kafkaProducerFactory() {
        Map<String, Object> producerProperties = properties.buildProducerProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializerWithSchemaName.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializerWithSchemaName.class);
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
        return new DefaultKafkaProducerFactory<Object, Object>(producerProperties);
    }

    @Bean
    public ConsumerFactory<?, ?> kafkaConsumerFactory() {
        Map<String, Object> consumerProperties = properties.buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
        return new DefaultKafkaConsumerFactory<Object, Object>(consumerProperties);
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfig(){
        Map<String, Object> kStreamProperties = new HashMap<>();
        kStreamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, kStreamsConsumerGroup);
        kStreamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kStreamProperties.put(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
        kStreamProperties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        kStreamProperties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        kStreamProperties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        kStreamProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        return new StreamsConfig(kStreamProperties);
    }

}
