package com.deepti.kafka.sample;

import com.deepti.kafka.sample.avro.User;
import org.apache.kafka.common.serialization.Serdes;

public final class AvroSerde extends Serdes.WrapperSerde<User> {
    public AvroSerde () {
        super(new AvroSerializer<>(), new AvroDeserializer<>(User.class));
    }
}