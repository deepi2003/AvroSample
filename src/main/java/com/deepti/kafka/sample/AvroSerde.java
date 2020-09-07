package com.deepti.kafka.sample;

import com.deepti.kafka.sample.avro.User;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

public final class AvroSerde extends Serdes.WrapperSerde<User> {
    public AvroSerde () {
        super(new AvroSerializer<>(), new AvroDeserializer<>(User.class));
    }
}