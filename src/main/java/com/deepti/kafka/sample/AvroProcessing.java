package com.deepti.kafka.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AvroProcessing {
    static final String USER_TOPIC = "user-avro";


    private static final String APPLICATION_ID = "kafka-stream-sample-avro";
    private static final String CLIENT_ID = "kafka-stream-sample-avro";

    public static void startProcessing() {

        System.out.println(" Processing started");
        final String bootstrapServers = "http://localhost:9092";
        final KafkaStreams streams = new KafkaStreams(getTopology(), streamsConfig(bootstrapServers, "/tmp/kafka-streams"));
        streams.cleanUp();
        streams.start();

        userMessageProducer(bootstrapServers);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));
    }

    private static void userMessageProducer(String bootstrapServers) {
        Producer<String, User> producer = createUserProducer(bootstrapServers);
        produceRecord(producer, User.newBuilder().setName("John").setFavoriteColor("Red").build());
        produceRecord(producer, User.newBuilder().setName("Jay").setFavoriteColor("Red").setFavoriteNumber(15).build());
        produceRecord(producer, User.newBuilder().setName("Mary").build());
        produceRecord(producer, User.newBuilder().setName("Phil").setFavoriteNumber(5).setFavoriteColor("Red").build());
        produceRecord(producer, User.newBuilder().setName("Jack").setFavoriteNumber(5).setFavoriteColor("Red").build());
    }


    private static void produceRecord(Producer<String, User> producer, User user)  {
        ProducerRecord<String, User> record = new ProducerRecord<>(USER_TOPIC,
                user.getName().toString(), user);

        RecordMetadata metadata = null;
        try {
            metadata = producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("Record sent with key " + " to partition " + metadata.partition()
                + " with offset " + metadata.offset() +  " with key size of " + metadata.serializedValueSize() + " with value size of " +metadata.serializedValueSize());
    }


    static Properties streamsConfig(final String bootstrapServers,final String stateDir) {
        final Properties streamsConfiguration = new Properties();


        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        return streamsConfiguration;
    }

    static Topology getTopology() {

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, User> baselineStoreOrders = builder.stream(USER_TOPIC);
        baselineStoreOrders.print(Printed.toSysOut());
        return builder.build();
    }


    public static Producer<String, User> createUserProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers );
        props.put(ProducerConfig.CLIENT_ID_CONFIG, APPLICATION_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
