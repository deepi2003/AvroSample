package com.deepti.kafka.sample;

import com.deepti.kafka.sample.avro.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

@Component
public class TopologyBuilder {

    Configurations configurations;

    public  TopologyBuilder(Configurations configurations) {
        this.configurations = configurations;
    }
    public Topology getTopology(StreamsBuilder kStreamBuilder) {
        final KStream<String, User> userList = kStreamBuilder.stream(configurations.getTopic());
        userList.map( (k, v) -> KeyValue.pair(k, new User(v.getName(),
                v.getFavoriteNumber() +1, v.getFavoriteColor()))
           ).to(configurations.getTopic(), Produced.with(Serdes.String(), new AvroSerde()));
        System.out.println(" USers from topics -------");
        userList.print(Printed.toSysOut());
        return kStreamBuilder.build();
    }
}
