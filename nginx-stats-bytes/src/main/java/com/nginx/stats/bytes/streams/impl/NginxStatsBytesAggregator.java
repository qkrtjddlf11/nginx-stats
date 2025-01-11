package com.nginx.stats.bytes.streams.impl;

import com.nginx.nginx.stats.bytes.avro.NginxStatsBytes;
import com.nginx.stats.core.aggregation.Aggregator;

public class NginxStatsBytesAggregator implements Aggregator <NginxStatsBytes> {

    @Override
    public NginxStatsBytes aggregateByStoreKey(String statTime, String host, String notUsed, long bytes) {
        return NginxStatsBytes.newBuilder().setStatDate(statTime).setHostname(host)
            .setBytes(bytes).build();
    }
}
