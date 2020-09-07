package com.deepti.kafka.sample;

import com.deepti.kafka.sample.avro.User;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Component
public class GeneralProducer {

   private final Configurations configurations;
   private final Producer<String, User> producer;

    public GeneralProducer(Configurations configurations) {
        this.configurations = configurations;
         producer = createUserProducer();
         sendUserRecords();
    }


    private Producer<String, User> createUserProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,configurations.getBootstrapServer() );
        props.put(ProducerConfig.CLIENT_ID_CONFIG, configurations.getApplicationID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private void produceRecord(User user)  {
        ProducerRecord<String, User> record = new ProducerRecord<>(configurations.getTopic(),
                user.getName(), user);

        RecordMetadata metadata = null;
        try {
            metadata = producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        assert metadata != null;
        System.out.println("Record sent with key " + " to partition " + metadata.partition()
                + " with offset " + metadata.offset() +  " with key size of "
                + metadata.serializedKeySize() + " with value size of "
                +metadata.serializedValueSize());
    }


    public void sendUserRecords() {
        produceRecord(User.newBuilder().setName("John").setFavoriteColor("Red").build());
        produceRecord(User.newBuilder().setName("Jay").setFavoriteColor("Red").setFavoriteNumber(15).build());
        produceRecord(User.newBuilder().setName("Mary").setFavoriteNumber(5).setFavoriteColor("Red").build());
        produceRecord(User.newBuilder().setName("Jack").setFavoriteNumber(5).setFavoriteColor("Red").build());
        produceRecord(User.newBuilder().setName("Phill").setFavoriteNumber(5).setFavoriteColor("Red").build());
    }
}
