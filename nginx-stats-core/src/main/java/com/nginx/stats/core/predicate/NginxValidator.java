package com.nginx.stats.core.predicate;

import com.fasterxml.jackson.databind.JsonNode;
import com.nginx.stats.core.define.NginxDefineKeyword;
import com.nginx.stats.core.metric.MetricCode;
import com.nginx.stats.core.metric.MetricLogger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Predicate;

@Slf4j
public class NginxValidator implements Predicate<String, JsonNode> {

    private static final String[] nginxFields = new String[]{NginxDefineKeyword.IP, NginxDefineKeyword.BYTES,
        NginxDefineKeyword.METHOD, NginxDefineKeyword.PATH, NginxDefineKeyword.STATUS, NginxDefineKeyword.TIMESTAMP,
        NginxDefineKeyword.USER_AGENT};

    @Override
    public boolean test(String s, JsonNode jsonNode) {
        return this.hasRequiredKeys(nginxFields, jsonNode) && this.isValidValues(jsonNode);
    }

    private boolean hasRequiredKeys(String[] keys, JsonNode jsonNode) {
        for (String key : keys) {
            if (!jsonNode.hasNonNull(key)) {
                MetricLogger.printMetricErrorLog(log, MetricCode.JSON_E_0001_FMT, MetricCode.JSON_E_0001,
                    MetricCode.JSON_E_0001_DOC, key, jsonNode);
                return false;
            }
        }
        return true;
    }

    private boolean isValidValues(JsonNode v) {
        final String ip = v.get(NginxDefineKeyword.IP).asText();
        final long bytes = v.get(NginxDefineKeyword.BYTES).asLong(-1);
        final long status = v.get(NginxDefineKeyword.STATUS).asLong(-1);

        if (!isInvalidIPv4(ip)) {
            printInvalidValueErrorLog(NginxDefineKeyword.IP, ip);
            return false;
        } else if (isNumOutOfRange(status, 0, 999)) {
            printInvalidValueErrorLog(NginxDefineKeyword.STATUS, status);
            return false;
        } else if (isNumOutOfRange(bytes, 0, Integer.MAX_VALUE)) {
            printInvalidValueErrorLog(NginxDefineKeyword.BYTES, bytes);
            return false;
        }
        return true;
    }

    public boolean isInvalidIPv4(String ip) {
        try {
            InetAddress inetAddress = InetAddress.getByName(ip);
            return inetAddress.getHostAddress().equals(ip) && inetAddress instanceof java.net.Inet4Address;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    private boolean isNumOutOfRange(long value, long min, long max) {
        return value < min || value > max;
    }

    private void printInvalidValueErrorLog(String field, Object v) {
        MetricLogger.printMetricErrorLog(log, MetricCode.JSON_E_0002_FMT, MetricCode.JSON_E_0002,
            MetricCode.JSON_E_0002_DOC, field, v);
    }
}
