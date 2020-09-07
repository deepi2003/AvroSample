package com.deepti.kafka.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@EnableKafkaStreams
@Configuration
public class KafkaStreamBuilder {
    private final Configurations configuration;
    private final TopologyBuilder topologyBuilder;


    public KafkaStreamBuilder(Configurations configuration, TopologyBuilder topologyBuilder) {
        this.configuration = configuration;
        this.topologyBuilder = topologyBuilder;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, configuration.getApplicationID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServer());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AvroSerializer.class);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, configuration.getClientID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder kStreamBuilder) {
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder.getTopology(kStreamBuilder),
                kStreamsConfigs().asProperties());
        return kafkaStreams;
    }


}
