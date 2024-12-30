package com.nginx.stats.core.time;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TimeYMDHM implements ParseStrTimeStrategy {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    @Override
    public long parse(String timestamp) {
        return LocalDateTime.parse(timestamp, formatter).atOffset(ZoneOffset.ofHours(9))
            .withOffsetSameInstant(ZoneOffset.UTC).toEpochSecond();
    }
}
