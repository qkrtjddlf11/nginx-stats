package com.nginx.stats.core.time;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;

public class TimeGroup {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    private final ConcurrentHashMap<String, String> cacheMap = new ConcurrentHashMap<>();
    private final int truncateSec;
    private final int cacheSize;
    private final ParseStrTimeStrategy parseStrTimeStrategy;

    public static final int ONE_MIN_SEC = 60;
    public static final int FIVE_MIN_SEC = 300;
    public static final int ONE_HOUR_SEC = 3600;
    public static final int ONE_DAY_SEC = 86400;

    public TimeGroup(int truncateSec, int cacheSize, ParseStrTimeStrategy parseStrTimeStrategy) {
        this.truncateSec = truncateSec;
        this.cacheSize = cacheSize;
        this.parseStrTimeStrategy = parseStrTimeStrategy;
    }

    public String generateStatTime(String timestamp) {
        if (this.cacheMap.size() > this.cacheSize) {
            this.cacheMap.clear();
        }

        return this.cacheMap.computeIfAbsent(timestamp, k -> {
            long epoch = this.parseStrTimeStrategy.parse(k);

            if (this.truncateSec == TimeGroup.ONE_DAY_SEC) {
                epoch = toKstMidnight(epoch);
            } else {
                epoch -= epoch % this.truncateSec;
            }

            return this.toFormattedStatTime(epoch);
        });
    }

    private long toKstMidnight(long epoch) {
        epoch -= epoch % TimeGroup.ONE_DAY_SEC;
        return epoch - ZoneOffset.ofHours(9).getTotalSeconds();
    }

    private String toFormattedStatTime(long epoch) {
        System.out.println("toFormattedStatTime: " + epoch);
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.ofHours(9));
        return dateTime.format(formatter);
    }
}
