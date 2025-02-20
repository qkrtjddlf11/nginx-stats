package com.nginx.stats.bytes.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.nginx.nginx.stats.bytes.avro.NginxStatsBytes;
import com.nginx.stats.bytes.streams.config.KafkaStreamsProperties;
import com.nginx.stats.bytes.streams.processor.FiveMinNginxStatsBytesProcessorSupplier;
import com.nginx.stats.bytes.streams.processor.OneDayNginxStatsBytesProcessorSupplier;
import com.nginx.stats.bytes.streams.processor.OneHourNginxStatsBytesProcessorSupplier;
import com.nginx.stats.bytes.streams.processor.OneMinNginxStatsBytesProcessorSupplier;
import com.nginx.stats.core.metric.MetricCode;
import com.nginx.stats.core.metric.MetricLogger;
import com.nginx.stats.core.predicate.NginxValidator;
import com.nginx.stats.core.serdes.AvroSerDes;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties(KafkaStreamsProperties.class)
@Slf4j
public class NginxStatsBytesRunner implements ApplicationRunner {

    @Autowired
    private KafkaStreamsProperties kafkaStreamsProperties;

    private Properties getStreamsConfig() {
        final Properties streamsConfig = new Properties();
        // Streams config
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamsProperties.getApplicationName());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStreamsProperties.getBootstrapServers());
        streamsConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            kafkaStreamsProperties.getSchemaRegistryUrl());
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafkaStreamsProperties.getNumStreamThreads());
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        streamsConfig.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");

        // Producer config
        streamsConfig.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "gzip");
        streamsConfig.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG), 5);
        streamsConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        streamsConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);
        streamsConfig.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), Integer.MAX_VALUE);
        streamsConfig.put(StreamsConfig.producerPrefix(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), 100);

        return streamsConfig;
    }

    private void buildTopology(StreamsBuilder builder) {
        try (final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
            final Serde<NginxStatsBytes> nginxStatsBytesSerde = AvroSerDes.getSpecificAvroSerde(
                kafkaStreamsProperties.getSchemaRegistryUrl())) {

            // Nginx log 유효성 검사 후 SUCCESS_BRANCH OR FAILED_BRANCH로 분기 처리.
            Map<String, KStream<String, JsonNode>> branchMap = builder.stream(kafkaStreamsProperties.getInputTopic(),
                    Consumed.with(Serdes.String(), jsonNodeSerde))
                .split(Named.as(DefineKeyword.VALIDATOR_SPLIT_PREFIX_NAME))
                .branch(new NginxValidator(), Branched.as(DefineKeyword.VALIDATOR_SUCCCESS_BRANCH_NAME))
                .defaultBranch(Branched.as(DefineKeyword.VALIDATOR_FAILED_BRANCH_NAME));

            // 유효성 검사를 통과하지 못한 경우 Error 토픽으로 전송.
            branchMap.get(DefineKeyword.VALIDATOR_SPLIT_PREFIX_NAME + DefineKeyword.VALIDATOR_FAILED_BRANCH_NAME)
                .to(DefineKeyword.VALIDATOR_FAILED_TOPIC_NAME);

            KStream<String, JsonNode> successBranch = branchMap.get(
                DefineKeyword.VALIDATOR_SPLIT_PREFIX_NAME + DefineKeyword.VALIDATOR_SUCCCESS_BRANCH_NAME);

            final OneMinNginxStatsBytesProcessorSupplier oneMinProcessorSupplier = new OneMinNginxStatsBytesProcessorSupplier(
                kafkaStreamsProperties, nginxStatsBytesSerde);

            Repartitioned<String, NginxStatsBytes> repartitioner = Repartitioned.with(Serdes.String(),
                    nginxStatsBytesSerde).withNumberOfPartitions(2)
                .withName(DefineKeyword.NGINX_STATS_BYTES_REPARTITION_TOPIC_NAME);

            KStream<String, NginxStatsBytes> oneMinBYTESStream = successBranch.process(oneMinProcessorSupplier,
                    Named.as(DefineKeyword.ONE_MIN_NGINX_STATS_BYTES_PROCESSOR_NAME),
                    DefineKeyword.ONE_MIN_NGINX_STATS_BYTES_STORE_NAME)
                .selectKey((k, v) -> v.getHostname())
                .repartition(repartitioner);

            final FiveMinNginxStatsBytesProcessorSupplier fiveMinProcessorSupplier = new FiveMinNginxStatsBytesProcessorSupplier(
                kafkaStreamsProperties, nginxStatsBytesSerde);

            KStream<String, NginxStatsBytes> fiveMinBYTESStream = oneMinBYTESStream.process(
                fiveMinProcessorSupplier, Named.as(DefineKeyword.FIVE_MIN_NGINX_STATS_BYTES_PROCESSOR_NAME),
                DefineKeyword.FIVE_MIN_NGINX_STATS_BYTES_STORE_NAME,
                DefineKeyword.FIVE_MIN_NGINX_STATS_BYTES_KEY_STORE_NAME);

            fiveMinBYTESStream.to(DefineKeyword.FIVE_MIN_NGINX_STATS_BYTES_TOPIC_NAME,
                Produced.with(Serdes.String(), nginxStatsBytesSerde));

            final OneHourNginxStatsBytesProcessorSupplier oneHourProcessorSupplier = new OneHourNginxStatsBytesProcessorSupplier(
                kafkaStreamsProperties, nginxStatsBytesSerde);

            KStream<String, NginxStatsBytes> oneHourBYTESStream = fiveMinBYTESStream.process(
                oneHourProcessorSupplier, Named.as(DefineKeyword.ONE_HOUR_NGINX_STATS_BYTES_PROCESSOR_NAME),
                DefineKeyword.ONE_HOUR_NGINX_STATS_BYTES_STORE_NAME,
                DefineKeyword.ONE_HOUR_NGINX_STATS_BYTES_KEY_STORE_NAME);

            oneHourBYTESStream.to(DefineKeyword.ONE_HOUR_NGINX_STATS_BYTES_TOPIC_NAME,
                Produced.with(Serdes.String(), nginxStatsBytesSerde));

            final OneDayNginxStatsBytesProcessorSupplier oneDayProcessorSupplier = new OneDayNginxStatsBytesProcessorSupplier(
                kafkaStreamsProperties, nginxStatsBytesSerde);

            KStream<String, NginxStatsBytes> oneDayBYTESStream = oneHourBYTESStream.process(
                oneDayProcessorSupplier, Named.as(DefineKeyword.ONE_DAY_NGINX_STATS_BYTES_PROCESSOR_NAME),
                DefineKeyword.ONE_DAY_NGINX_STATS_BYTES_STORE_NAME,
                DefineKeyword.ONE_DAY_NGINX_STATS_BYTES_KEY_STORE_NAME);

            oneDayBYTESStream.to(DefineKeyword.ONE_DAY_NGINX_STATS_BYTES_TOPIC_NAME,
                Produced.with(Serdes.String(), nginxStatsBytesSerde));

        } catch (Exception e) {
            MetricLogger.printMetricErrorLog(log, MetricCode.APPL_E_0001_FMT, MetricCode.APPL_E_0001,
                MetricCode.APPL_E_0001_DOC, e.getMessage());
        }

    }

    @Override
    public void run(ApplicationArguments args) {
        final Properties config = getStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();

        buildTopology(builder);

        Topology topology = builder.build();
        TopologyDescription description = topology.describe();

        MetricLogger.printMetricInfoLog(log, MetricCode.APPL_I_0001_FMT, MetricCode.APPL_I_0001,
            MetricCode.APPL_I_0001_DOC, description);

        try (final KafkaStreams streams = new KafkaStreams(topology, config)) {
            final CountDownLatch latch = new CountDownLatch(1);

            streams.setStateListener((newState, oldState) -> {
                MetricLogger.printMetricInfoLog(log, MetricCode.APPL_I_0004_FMT, MetricCode.APPL_I_0004,
                    MetricCode.APPL_I_0004_DOC, oldState, newState);

                if (newState == KafkaStreams.State.ERROR || newState == KafkaStreams.State.NOT_RUNNING) {
                    latch.countDown();
                }
            });

            streams.setUncaughtExceptionHandler(ex -> {
                MetricLogger.printMetricErrorLog(log, MetricCode.APPL_E_0001_FMT, MetricCode.APPL_E_0001,
                    MetricCode.APPL_E_0001_DOC, ex.getMessage());
                if (ex instanceof InvalidPidMappingException) {
                    return StreamThreadExceptionResponse.REPLACE_THREAD;
                }
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });

            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreamsProperties.getApplicationName()) {
                @Override
                public void run() {
                    MetricLogger.printMetricInfoLog(log, MetricCode.APPL_I_0003_FMT, MetricCode.APPL_I_0003,
                        MetricCode.APPL_I_0003_DOC);
                    streams.close(Duration.ofSeconds(300));
                    latch.countDown();
                }
            });

            streams.cleanUp();
            streams.start();

            MetricLogger.printMetricInfoLog(log, MetricCode.APPL_I_0002_FMT, MetricCode.APPL_I_0002,
                MetricCode.APPL_I_0002_DOC);

            latch.await();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
