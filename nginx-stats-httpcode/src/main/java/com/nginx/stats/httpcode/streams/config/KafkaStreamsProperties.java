package com.nginx.stats.httpcode.streams.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.validator.constraints.Length;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@NoArgsConstructor
@Getter
@Setter
@ToString
@Configuration
@Validated
@ConfigurationProperties(prefix = "kafkastreams")
public class KafkaStreamsProperties {

    @Length(min = 3, max = 24)
    @NotNull
    private String applicationName;

    @NotNull
    private String bootstrapServers;

    @NotNull
    private String stateDirPath;

    @NotNull
    private String inputTopic;

    @Max(128)
    @Min(1)
    @NotNull
    private Integer numStreamThreads;

    @Max(3)
    @Min(1)
    @NotNull
    private Integer minInSyncReplicas;

    @Max(3)
    @Min(1)
    @NotNull
    private Integer replicationFactor;

    @Max(365)
    @Min(1)
    @NotNull
    private Integer retentionDays;

}
