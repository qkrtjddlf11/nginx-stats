package com.nginx.stats.httpcode.streams;

import com.nginx.nginx.stats.httpcode.avro.NginxStatsHttpcode;

public class Aggregation {

    public NginxStatsHttpcode aggregateByKey(String statTime, String host, String status, int count) {
        return NginxStatsHttpcode.newBuilder().setStatDate(statTime).setHostname(host)
            .setHttpcode(Integer.parseInt(status)).setCount(count).build();
    }
}
