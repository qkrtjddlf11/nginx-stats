package com.nginx.stats.httpcode.streams.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nginx.nginx.stats.httpcode.avro.NginxStatsHttpcode;
import com.nginx.stats.core.define.NginxDefineKeyword;
import com.nginx.stats.core.generate.StoreKey;
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
    ProcessorSupplier<String, JsonNode, String, NginxStatsHttpcode> {

    private final KafkaStreamsProperties properties;
    private final Serde<NginxStatsHttpcode> nginxStatsHttpcodeSerde;

    @Override
    public Processor<String, JsonNode, String, NginxStatsHttpcode> get() {
        return new Processor<>() {

            private ProcessorContext<String, NginxStatsHttpcode> context;
            private KeyValueStore<String, NginxStatsHttpcode> store;

            @Override
            public void init(ProcessorContext<String, NginxStatsHttpcode> context) {
                this.context = context;
                this.context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, this::punctuator);
                this.store = context.getStateStore(DefineKeyword.ONE_MIN_NGINX_STATS_HTTPCODE_STORE_NAME);
            }

            private void punctuator(final long timestamp) {
                try (KeyValueIterator<String, NginxStatsHttpcode> iterator = this.store.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, NginxStatsHttpcode> entry = iterator.next();
                        final Record<String, NginxStatsHttpcode> msg = new Record<>(entry.key, entry.value, timestamp);

                        this.context.forward(msg);
                    }
                }
            }

            @Override
            public void process(Record<String, JsonNode> r) {
                final JsonNode v = r.value();
                final String statTime = TimeConverter.convertUtcToKst(v.get(NginxDefineKeyword.TIMESTAMP).asText());
                final String host = v.get(NginxDefineKeyword.HOST).asText();
                final String status = v.get(NginxDefineKeyword.STATUS).asText();
                final String storeKey = StoreKey.generateStateStoreKey(statTime, host, status);

                NginxStatsHttpcode aggregating = this.store.get(storeKey);

                if (aggregating == null) {
                    aggregating = NginxStatsHttpcode.newBuilder().setStatDate(statTime).setHostname(host)
                        .setHttpcode(Integer.parseInt(status)).setCount(1).build();
                } else {
                    aggregating.setCount(aggregating.getCount() + 1);
                }

                this.store.put(storeKey, aggregating);
            }

            @Override
            public void close() {
                // Cleanup somehting.
            }

        };
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<KeyValueStore<String, NginxStatsHttpcode>> keyValueStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(DefineKeyword.ONE_MIN_NGINX_STATS_HTTPCODE_STORE_NAME),
                    Serdes.String(), nginxStatsHttpcodeSerde)
                .withCachingEnabled()
                .withLoggingEnabled(ChangeLog.getChangelogConfig(properties.getMinInSyncReplicas(),
                    TimeConverter.convertDaysToMillSec(properties.getRetentionDays())));

        return Collections.synchronizedSet(Set.of(keyValueStoreBuilder));
    }

}