package com.nginx.stats.core.aggregation;

public interface Aggregator <T> {
    T aggregateByStoreKey(String statTime, String host, String key, long value);
}
