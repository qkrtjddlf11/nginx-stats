package com.nginx.stats.httpcode.streams.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nginx.stats.core.define.NginxDefineKeyword;
import com.nginx.stats.core.statestore.ChangeLog;
import com.nginx.stats.core.time.TimeConverter;
import com.nginx.stats.httpcode.streams.Aggregation;
import com.nginx.stats.httpcode.streams.DefineKeyword;
import com.nginx.stats.httpcode.streams.config.KafkaStreamsProperties;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

@RequiredArgsConstructor
@Slf4j
public class OneMinNginxStatsHttpcodeProcessorSupplier extends Aggregation implements
    ProcessorSupplier<String, JsonNode, String, JsonNode> {

    private final KafkaStreamsProperties properties;
    private final Serde<JsonNode> jsonNodeSerde;

    @Override
    public Processor<String, JsonNode, String, JsonNode> get() {
        return new Processor<>() {

            private ProcessorContext<String, JsonNode> context;
            private KeyValueStore<String, JsonNode> store;

            @Override
            public void init(ProcessorContext<String, JsonNode> context) {
                this.context = context;
                this.context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, this::punctuator);
                this.store = context.getStateStore(DefineKeyword.ONE_MIN_NGINX_STATS_HTTPCODE_STORE_NAME);
            }

            private void punctuator(final long timestamp) {
                try (KeyValueIterator<String, JsonNode> iterator = this.store.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, JsonNode> entry = iterator.next();
                        final Record<String, JsonNode> msg = new Record<>(entry.key, entry.value, timestamp);

                        this.context.forward(msg);
                    }
                }
            }

            @Override
            public void process(Record<String, JsonNode> record) {
                final JsonNode v = record.value();
                final String statTime = TimeConverter.convertUtcToKst(v.get(NginxDefineKeyword.TIMESTAMP).asText());
                final String host = v.get(NginxDefineKeyword.HOST).asText();
                final int status = v.get(NginxDefineKeyword.STATUS).asInt();

                JsonNode aggregating = this.store.get("");

                if (aggregating == null) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    ObjectNode objectNode = objectMapper.createObjectNode();

                    objectNode.put("stat_date", statTime);
                    objectNode.put("hostname", host);
                    objectNode.put("status", status);
                    objectNode.put("count", 0);

                    aggregating = objectNode;
                } else {
                    int count = aggregating.get("count").asInt() + 1;
                }


                this.store.put("key", aggregating);
                // Key: 202412121123_hostname_httpcode
                /* Value:
                    {
                        stat_date: 202412121123
                        hostname: nginx-server-1
                        httpcode: 200
                        count: 1234
                    }
                 */

            }

            @Override
            public void close() {
                // Cleanup somehting.
            }

        };
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<KeyValueStore<String, JsonNode>> keyValueStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(DefineKeyword.ONE_MIN_NGINX_STATS_HTTPCODE_STORE_NAME),
                    Serdes.String(), jsonNodeSerde)
                .withCachingEnabled()
                .withLoggingEnabled(ChangeLog.getChangelogConfig(properties.getMinInSyncReplicas(),
                    TimeConverter.convertDaysToMillSec(properties.getRetentionDays())));

        return Collections.synchronizedSet(Set.of(keyValueStoreBuilder));
    }

}