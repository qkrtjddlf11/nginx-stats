package com.nginx.stats.core.time;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeISO8601 implements ParseStrTimeStrategy {

    @Override
    public long parse(String timestamp) {
        return ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toEpochSecond();
    }
}
