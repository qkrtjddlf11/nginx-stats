package com.nginx.stats.core.time;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class TimeConverter {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private TimeConverter() {
    }

    public static Long convertDaysToMillSec(int days) {
        return TimeUnit.DAYS.toMillis(days);
    }

    public static String convertUtcToKst(String utcTime) {
        ZonedDateTime utcDateTime = ZonedDateTime.parse(utcTime);
        ZonedDateTime kstDateTime = utcDateTime.withZoneSameInstant(ZoneId.of("Asia/Seoul"));
        return kstDateTime.format(formatter);
    }
}
