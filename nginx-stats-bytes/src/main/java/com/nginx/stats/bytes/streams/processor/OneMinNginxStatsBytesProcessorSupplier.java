package com.nginx.stats.bytes.streams.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.nginx.nginx.stats.bytes.avro.NginxStatsBytes;
import com.nginx.stats.bytes.streams.DefineKeyword;
import com.nginx.stats.bytes.streams.config.KafkaStreamsProperties;
import com.nginx.stats.bytes.streams.impl.NginxStatsBytesAggregator;
import com.nginx.stats.core.define.NginxDefineKeyword;
import com.nginx.stats.core.generate.StateStoreKey;
import com.nginx.stats.core.metric.MetricCode;
import com.nginx.stats.core.metric.MetricLogger;
import com.nginx.stats.core.statestore.ChangeLog;
import com.nginx.stats.core.time.TimeGroup;
import com.nginx.stats.core.time.TimeISO8601;
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
public class OneMinNginxStatsBytesProcessorSupplier extends NginxStatsBytesAggregator implements
    ProcessorSupplier<String, JsonNode, String, NginxStatsBytes> {

    private final KafkaStreamsProperties properties;
    private final Serde<NginxStatsBytes> nginxStatsBytesSerde;

    private void printPerfInfo(long cnt) {
        MetricLogger.printMetricInfoLog(log, MetricCode.PERF_I_0001_FMT, MetricCode.PERF_I_0001,
            MetricCode.PERF_I_0001_DOC, cnt, cnt / 5);
    }

    @Override
    public Processor<String, JsonNode, String, NginxStatsBytes> get() {
        return new Processor<>() {

            private ProcessorContext<String, NginxStatsBytes> context;
            private KeyValueStore<String, NginxStatsBytes> store;
            private StateStoreKey stateStoreKey;
            private TimeGroup timeGroup;
            private long cnt = 0;

            @Override
            public void init(ProcessorContext<String, NginxStatsBytes> context) {
                this.context = context;
                this.context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
                this.store = context.getStateStore(DefineKeyword.ONE_MIN_NGINX_STATS_BYTES_STORE_NAME);
                this.stateStoreKey = new StateStoreKey();
                this.timeGroup = new TimeGroup(TimeGroup.ONE_MIN_SEC, 600, new TimeISO8601());
            }

            private void punctuate(long timestamp) {
                try (KeyValueIterator<String, NginxStatsBytes> iterator = this.store.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, NginxStatsBytes> entry = iterator.next();
                        final Record<String, NginxStatsBytes> msg = new Record<>(entry.key, entry.value, timestamp);

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
                final long bytes = v.get(NginxDefineKeyword.BYTES).asLong();
                final String storeKey = this.stateStoreKey.generateStateStoreKey(statTime, host);

                // TODO
                // Http Status Code에 따라서 합산 처리가 필요하다면 Filtering 추가

                NginxStatsBytes aggregating = this.store.get(storeKey);

                if (aggregating == null) {
                    aggregating = aggregateByStoreKey(statTime, host, "", bytes);
                } else {
                    aggregating.setBytes(aggregating.getBytes() + 1);
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
        StoreBuilder<KeyValueStore<String, NginxStatsBytes>> keyValueStoreBuilder =
            Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(DefineKeyword.ONE_MIN_NGINX_STATS_BYTES_STORE_NAME),
                    Serdes.String(), nginxStatsBytesSerde)
                .withCachingEnabled()
                .withLoggingEnabled(ChangeLog.getChangelogConfig(properties.getMinInSyncReplicas(),
                    TimeUnit.DAYS.toMillis(properties.getRetentionDays())));

        return Collections.synchronizedSet(Set.of(keyValueStoreBuilder));
    }

}