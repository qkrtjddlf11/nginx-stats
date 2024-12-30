package com.nginx.stats.httpcode.streams.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.nginx.nginx.stats.httpcode.avro.NginxStatsHttpcode;
import com.nginx.stats.core.define.NginxDefineKeyword;
import com.nginx.stats.core.generate.StateStoreKey;
import com.nginx.stats.core.metric.MetricCode;
import com.nginx.stats.core.metric.MetricLogger;
import com.nginx.stats.core.statestore.ChangeLog;
import com.nginx.stats.core.time.TimeGroup;
import com.nginx.stats.core.time.TimeISO8601;
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
public class OneMinNginxStatsHttpcodeProcessorSupplier extends Aggregation implements
    ProcessorSupplier<String, JsonNode, String, NginxStatsHttpcode> {

    private final KafkaStreamsProperties properties;
    private final Serde<NginxStatsHttpcode> nginxStatsHttpcodeSerde;

    private void printPerfInfo(long cnt) {
        MetricLogger.printMetricInfoLog(log, MetricCode.PERF_I_0001_FMT, MetricCode.PERF_I_0001,
            MetricCode.PERF_I_0001_DOC, cnt, cnt / 5);
    }

    @Override
    public Processor<String, JsonNode, String, NginxStatsHttpcode> get() {
        return new Processor<>() {

            private ProcessorContext<String, NginxStatsHttpcode> context;
            private KeyValueStore<String, NginxStatsHttpcode> store;
            private StateStoreKey stateStoreKey;
            private TimeGroup timeGroup;
            private long cnt = 0;

            @Override
            public void init(ProcessorContext<String, NginxStatsHttpcode> context) {
                this.context = context;
                this.context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
                this.store = context.getStateStore(DefineKeyword.ONE_MIN_NGINX_STATS_HTTPCODE_STORE_NAME);
                this.stateStoreKey = new StateStoreKey();
                this.timeGroup = new TimeGroup(TimeGroup.ONE_MIN_SEC, 600, new TimeISO8601());
            }

            private void punctuate(long timestamp) {
                try (KeyValueIterator<String, NginxStatsHttpcode> iterator = this.store.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, NginxStatsHttpcode> entry = iterator.next();
                        final Record<String, NginxStatsHttpcode> msg = new Record<>(entry.key, entry.value, timestamp);

                        this.context.forward(msg);
                        this.store.delete(entry.key);
                    }
                }

                printPerfInfo(cnt);
                cnt = 0;
            }

            @Override
            public void process(Record<String, JsonNode> r) {
                final JsonNode v = r.value();
                final String statTime = this.timeGroup.generateStatTime(v.get(NginxDefineKeyword.TIMESTAMP).asText());
                final String host = v.get(NginxDefineKeyword.HOST).asText();
                final String status = v.get(NginxDefineKeyword.STATUS).asText();
                final String storeKey = this.stateStoreKey.generateStateStoreKey(statTime, host, status);

                NginxStatsHttpcode aggregating = this.store.get(storeKey);

                if (aggregating == null) {
                    aggregating = NginxStatsHttpcode.newBuilder().setStatDate(statTime).setHostname(host)
                        .setHttpcode(Integer.parseInt(status)).setCount(1).build();
                } else {
                    aggregating.setCount(aggregating.getCount() + 1);
                }

                this.store.put(storeKey, aggregating);

                cnt++;
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
                    TimeUnit.DAYS.toMillis(properties.getRetentionDays())));

        return Collections.synchronizedSet(Set.of(keyValueStoreBuilder));
    }

}