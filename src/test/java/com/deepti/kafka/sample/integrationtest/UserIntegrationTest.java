package com.deepti.kafka.sample.integrationtest;

import com.deepti.kafka.sample.AvroSerde;
import com.deepti.kafka.sample.AvroSerializer;
import com.deepti.kafka.sample.KafkaStreamBuilder;
import com.deepti.kafka.sample.avro.User;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(
        brokerProperties = {"listeners=PLAINTEXT://${kafka.connection.bootstrapServer}"})
@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@RunWith(SpringRunner.class)
public class UserIntegrationTest {

    @Value("${kafka.topic}")
    private String userTopic;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    private Consumer<String, User> consumer;
    private Producer<Object, Object> producer;

    @Autowired
    KafkaStreamBuilder kafkaStreamBuilder;


    Serde<String> stringSerde = Serdes.String();
    AvroSerde userSerdes = new AvroSerde();

    @BeforeAll
    public void setup() {
        embeddedKafkaBroker.addTopics("user-avro");
        initializeConsumer();
        initializeProducer();
    }

    @AfterAll
    public void teardown() {
        consumer.unsubscribe();
        producer.close();
        consumer.close();
        embeddedKafkaBroker.destroy();
    }

    @Test
    public void testUser() {
        User user = new User("John", 10, "4");
        producer.send(new ProducerRecord(userTopic, user.getName(), user ));
        producer.flush();

        ConsumerRecord<String, User> userRecord = KafkaTestUtils.getSingleRecord(consumer, userTopic);
        assertThat(userRecord).isNotNull();

    }

    private void initializeProducer() {
        Map<String, Object> producerConfigs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producerConfigs.put("key.serializer", stringSerde.serializer());
        producerConfigs.put("value.serializer", userSerdes.serializer());
        producerConfigs.put("default.value.serde", userSerdes);
        producer = new DefaultKafkaProducerFactory<>(producerConfigs).createProducer();
    }

    private void initializeConsumer() {
        Map<String, Object> consumerConfigs = new HashMap<>(KafkaTestUtils.consumerProps("consumer1", "false", embeddedKafkaBroker));
        consumerConfigs.put("key.deserializer", stringSerde.deserializer());
        consumerConfigs.put("value.deserializer", userSerdes.deserializer());
        consumer = new DefaultKafkaConsumerFactory<>(consumerConfigs, stringSerde.deserializer(), userSerdes.deserializer()).createConsumer();
        consumer.subscribe(singleton(userTopic));
    }
}
