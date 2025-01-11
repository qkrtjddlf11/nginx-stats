package com.nginx.stats.httpcode.streams.impl;

import com.nginx.nginx.stats.httpcode.avro.NginxStatsHttpcode;
import com.nginx.stats.core.aggregation.Aggregator;

public class NginxStatsHttpcodeAggregator implements Aggregator<NginxStatsHttpcode> {

    @Override
    public NginxStatsHttpcode aggregateByStoreKey(String statTime, String host, String status, long count) {
        return NginxStatsHttpcode.newBuilder().setStatDate(statTime).setHostname(host)
            .setHttpcode(Integer.parseInt(status)).setCount(count).build();
    }
}
