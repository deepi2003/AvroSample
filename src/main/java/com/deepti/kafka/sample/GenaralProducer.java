package com.deepti.kafka.sample;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class GenaralProducer {

    public static Producer<String, User> createUserProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9200" );
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "APPLICATION_ID");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void produceRecord(Producer<String, User> producer, User user)  {
        ProducerRecord<String, User> record = new ProducerRecord<>("finalCROrder",
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


    private static void userMessageProducer(String bootstrapServers) {
        Producer<String, User> producer = createUserProducer(bootstrapServers);
        produceRecord(producer, User.newBuilder().setName("John").setFavoriteColor("Red").build());
        produceRecord(producer, User.newBuilder().setName("Jay").setFavoriteColor("Red").setFavoriteNumber(15).build());
        produceRecord(producer, User.newBuilder().setName("Mary").setFavoriteNumber(5).setFavoriteColor("Red").build());
        produceRecord(producer, User.newBuilder().setName("Jack").setFavoriteNumber(5).setFavoriteColor("Red").build());
        produceRecord(producer, User.newBuilder().setName("Phill").setFavoriteNumber(5).setFavoriteColor("Red").build());
    }
}
