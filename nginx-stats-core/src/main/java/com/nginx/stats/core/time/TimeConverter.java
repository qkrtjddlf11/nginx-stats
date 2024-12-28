package com.nginx.stats.core.time;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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

    public static String convertStattimeByTimeGroup(String statTime, long timeGroup) {
        long epochTime = toUnixTime(statTime);
        return toFormattedTime(epochTime - (epochTime % timeGroup));
    }

    private static String toFormattedTime(long epochTime) {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochTime, 0, ZoneOffset.ofHours(9));
        return dateTime.format(formatter);
    }

    private static long toUnixTime(String formattedTime) {
        LocalDateTime dateTime = LocalDateTime.parse(formattedTime, formatter);
        return dateTime.toEpochSecond(ZoneOffset.ofHours(9));
    }
}
