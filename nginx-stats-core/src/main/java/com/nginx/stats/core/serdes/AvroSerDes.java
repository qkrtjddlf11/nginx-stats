package com.nginx.stats.core.serdes;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;

public class AvroSerDes {

    private AvroSerDes() {
    }

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(String schemaRegistryUrl) {
        SpecificAvroSerde<T> avroSerde = new SpecificAvroSerde<>();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        avroSerde.configure(config, false);
        return avroSerde;
    }
}