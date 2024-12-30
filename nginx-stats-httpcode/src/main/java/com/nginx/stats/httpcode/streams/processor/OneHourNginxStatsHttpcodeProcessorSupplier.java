package com.nginx.stats.httpcode.streams.processor;

import com.nginx.nginx.stats.httpcode.avro.NginxStatsHttpcode;
import com.nginx.stats.core.generate.StateStoreKey;
import com.nginx.stats.core.statestore.ChangeLog;
import com.nginx.stats.core.time.TimeGroup;
import com.nginx.stats.core.time.TimeYMDHM;
import com.nginx.stats.httpcode.streams.Aggregation;
import com.nginx.stats.httpcode.streams.DefineKeyword;
import com.nginx.stats.httpcode.streams.config.KafkaStreamsProperties;
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
public class OneHourNginxStatsHttpcodeProcessorSupplier extends Aggregation implements
    ProcessorSupplier<String, NginxStatsHttpcode, String, NginxStatsHttpcode> {

    private final KafkaStreamsProperties properties;
    private final Serde<NginxStatsHttpcode> nginxStatsHttpcodeSerde;

    @Override
    public Processor<String, NginxStatsHttpcode, String, NginxStatsHttpcode> get() {
        return new Processor<>() {

            private ProcessorContext<String, NginxStatsHttpcode> context;
            private KeyValueStore<String, NginxStatsHttpcode> store;
            private KeyValueStore<String, String> keyStore;
            private StateStoreKey stateStoreKey;
            private TimeGroup timeGroup;

            @Override
            public void init(ProcessorContext<String, NginxStatsHttpcode> context) {
                this.context = context;
                this.context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
                this.store = context.getStateStore(DefineKeyword.ONE_HOUR_NGINX_STATS_HTTPCODE_STORE_NAME);
                this.keyStore = context.getStateStore(DefineKeyword.ONE_HOUR_NGINX_STATS_HTTPCODE_KEY_STORE_NAME);
                this.stateStoreKey = new StateStoreKey();
                this.timeGroup = new TimeGroup(TimeGroup.ONE_HOUR_SEC, 24, new TimeYMDHM());
            }

            private void punctuate(long timestamp) {
                try (KeyValueIterator<String, String> iterator = this.keyStore.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, String> entry = iterator.next();
                        final NginxStatsHttpcode v = this.store.get(entry.key);
                        final Record<String, NginxStatsHttpcode> msg = new Record<>(entry.key, v, timestamp);

                        this.context.forward(msg);
                        this.keyStore.delete(entry.key);
                    }
                }
            }

            @Override
            public void process(Record<String, NginxStatsHttpcode> r) {
                NginxStatsHttpcode v = r.value();
                final String statTime = this.timeGroup.generateStatTime(v.getStatDate());
                final String host = v.getHostname();
                final String status = String.valueOf(v.getHttpcode());
                final String storeKey = this.stateStoreKey.generateStateStoreKey(statTime, host, status);

                NginxStatsHttpcode aggregating = this.store.get(storeKey);

                if (aggregating == null) {
                    aggregating = NginxStatsHttpcode.newBuilder().setStatDate(statTime).setHostname(host)
                        .setHttpcode(Integer.parseInt(status)).setCount(v.getCount()).build();
                } else {
                    aggregating.setCount(aggregating.getCount() + v.getCount());
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
        StoreBuilder<KeyValueStore<String, NginxStatsHttpcode>> keyValueStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(DefineKeyword.ONE_HOUR_NGINX_STATS_HTTPCODE_STORE_NAME),
                    Serdes.String(), nginxStatsHttpcodeSerde)
                .withCachingEnabled()
                .withLoggingEnabled(ChangeLog.getChangelogConfig(properties.getMinInSyncReplicas(),
                    TimeUnit.DAYS.toMillis(properties.getRetentionDays())));

        StoreBuilder<KeyValueStore<String, String>> keyStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(DefineKeyword.ONE_HOUR_NGINX_STATS_HTTPCODE_KEY_STORE_NAME),
                    Serdes.String(), Serdes.String())
                .withCachingEnabled()
                .withLoggingEnabled(ChangeLog.getChangelogConfig(properties.getMinInSyncReplicas(),
                    TimeUnit.DAYS.toMillis(properties.getRetentionDays())));

        return Collections.synchronizedSet(Set.of(keyValueStoreBuilder, keyStoreBuilder));
    }

}