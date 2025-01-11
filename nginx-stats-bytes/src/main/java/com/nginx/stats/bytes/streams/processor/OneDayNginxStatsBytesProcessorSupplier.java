package com.nginx.stats.bytes.streams.processor;

import com.nginx.nginx.stats.bytes.avro.NginxStatsBytes;
import com.nginx.stats.bytes.streams.DefineKeyword;
import com.nginx.stats.bytes.streams.config.KafkaStreamsProperties;
import com.nginx.stats.bytes.streams.impl.NginxStatsBytesAggregator;
import com.nginx.stats.core.generate.StateStoreKey;
import com.nginx.stats.core.statestore.ChangeLog;
import com.nginx.stats.core.time.TimeGroup;
import com.nginx.stats.core.time.TimeYMDHM;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
public class OneDayNginxStatsBytesProcessorSupplier extends NginxStatsBytesAggregator implements
    ProcessorSupplier<String, NginxStatsBytes, String, NginxStatsBytes> {

    private final KafkaStreamsProperties properties;
    private final Serde<NginxStatsBytes> nginxStatsBytesSerde;

    @Override
    public Processor<String, NginxStatsBytes, String, NginxStatsBytes> get() {
        return new Processor<>() {

            private ProcessorContext<String, NginxStatsBytes> context;
            private KeyValueStore<String, NginxStatsBytes> store;
            private KeyValueStore<String, String> keyStore;
            private StateStoreKey stateStoreKey;
            private TimeGroup timeGroup;

            @Override
            public void init(ProcessorContext<String, NginxStatsBytes> context) {
                this.context = context;
                this.context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
                this.store = context.getStateStore(DefineKeyword.ONE_DAY_NGINX_STATS_BYTES_STORE_NAME);
                this.keyStore = context.getStateStore(DefineKeyword.ONE_DAY_NGINX_STATS_BYTES_KEY_STORE_NAME);
                this.stateStoreKey = new StateStoreKey();
                this.timeGroup = new TimeGroup(TimeGroup.ONE_DAY_SEC, 2, new TimeYMDHM());
            }

            private void punctuate(long timestamp) {
                try (KeyValueIterator<String, String> iterator = this.keyStore.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, String> entry = iterator.next();
                        final NginxStatsBytes v = this.store.get(entry.key);
                        final Record<String, NginxStatsBytes> msg = new Record<>(entry.key, v, timestamp);

                        this.context.forward(msg);
                        this.keyStore.delete(entry.key);
                    }
                }
            }

            @Override
            public void process(Record<String, NginxStatsBytes> r) {
                NginxStatsBytes v = r.value();
                final String statTime = this.timeGroup.generateStatTime(v.getStatDate());
                final String host = v.getHostname();
                final long bytes = v.getBytes();
                final String storeKey = this.stateStoreKey.generateStateStoreKey(statTime, host);

                NginxStatsBytes aggregating = this.store.get(storeKey);

                if (aggregating == null) {
                    aggregating = aggregateByStoreKey(statTime, host, "", bytes);
                } else {
                    aggregating.setBytes(aggregating.getBytes() + v.getBytes());
                }

                this.keyStore.putIfAbsent(storeKey, "");
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
        StoreBuilder<KeyValueStore<String, NginxStatsBytes>> keyValueStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(DefineKeyword.ONE_DAY_NGINX_STATS_BYTES_STORE_NAME),
                    Serdes.String(), nginxStatsBytesSerde)
                .withCachingEnabled()
                .withLoggingEnabled(ChangeLog.getChangelogConfig(properties.getMinInSyncReplicas(),
                    TimeUnit.DAYS.toMillis(properties.getRetentionDays())));

        StoreBuilder<KeyValueStore<String, String>> keyStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(DefineKeyword.ONE_DAY_NGINX_STATS_BYTES_KEY_STORE_NAME),
                    Serdes.String(), Serdes.String())
                .withCachingEnabled()
                .withLoggingEnabled(ChangeLog.getChangelogConfig(properties.getMinInSyncReplicas(),
                    TimeUnit.DAYS.toMillis(properties.getRetentionDays())));

        return Collections.synchronizedSet(Set.of(keyValueStoreBuilder, keyStoreBuilder));
    }

}