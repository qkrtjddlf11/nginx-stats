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
        return this.hasRequiredKeys(jsonNode) && this.hasRequiredValues(jsonNode) && this.isValidValues(jsonNode);
    }

    private boolean hasRequiredKeys(JsonNode v) {
        for (String key : NginxValidator.nginxFields) {
            if (!v.hasNonNull(key)) {
                MetricLogger.printMetricErrorLog(log, MetricCode.JSON_E_0001_FMT, MetricCode.JSON_E_0001,
                    MetricCode.JSON_E_0001_DOC, key, v);
                return false;
            }
        }
        return true;
    }

    private boolean hasRequiredValues(JsonNode v) {
        final JsonNode ip = v.get(NginxDefineKeyword.IP);
        final JsonNode bytes = v.get(NginxDefineKeyword.BYTES);
        final JsonNode status = v.get(NginxDefineKeyword.STATUS);

        if (ip.isNull()) {
            return printEmptyValueErrorLog(NginxDefineKeyword.IP, ip);
        } else if (bytes.isNull()) {
            return printEmptyValueErrorLog(NginxDefineKeyword.BYTES, bytes);
        } else if (status.isNull()) {
            return printEmptyValueErrorLog(NginxDefineKeyword.STATUS, status);
        }

        return true;
    }

    private boolean isValidValues(JsonNode v) {
        final String ip = v.get(NginxDefineKeyword.IP).asText();
        final long bytes = v.get(NginxDefineKeyword.BYTES).asLong(-1);
        final long status = v.get(NginxDefineKeyword.STATUS).asLong(-1);

        if (!isInvalidIPv4(ip)) {
            return printInvalidValueErrorLog(NginxDefineKeyword.IP, ip);
        } else if (isNumOutOfRange(status, 0, 999)) {
            return printInvalidValueErrorLog(NginxDefineKeyword.STATUS, status);
        } else if (isNumOutOfRange(bytes, 0, Integer.MAX_VALUE)) {
            return printInvalidValueErrorLog(NginxDefineKeyword.BYTES, bytes);
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

    private boolean printEmptyValueErrorLog(String field, Object v) {
        MetricLogger.printMetricErrorLog(log, MetricCode.JSON_E_0003_FMT, MetricCode.JSON_E_0003,
            MetricCode.JSON_E_0003_DOC, field, v);
        return false;
    }

    private boolean printInvalidValueErrorLog(String field, Object v) {
        MetricLogger.printMetricErrorLog(log, MetricCode.JSON_E_0002_FMT, MetricCode.JSON_E_0002,
            MetricCode.JSON_E_0002_DOC, field, v);
        return false;
    }
}
