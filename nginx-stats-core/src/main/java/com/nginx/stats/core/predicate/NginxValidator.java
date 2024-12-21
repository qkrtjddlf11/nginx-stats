package com.nginx.stats.core.predicate;

import com.fasterxml.jackson.databind.JsonNode;
import com.nginx.stats.core.define.NginxDefineKeyword;
import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Predicate;

@Slf4j
public class NginxValidator implements Predicate<String, JsonNode> {

    private final String[] nginxFields = new String[]{NginxDefineKeyword.IP, NginxDefineKeyword.BYTES,
        NginxDefineKeyword.METHOD, NginxDefineKeyword.PATH, NginxDefineKeyword.STATUS, NginxDefineKeyword.TIMESTAMP,
        NginxDefineKeyword.USER_AGENT};

    private boolean isPresentRequiredKeys(String[] keys, JsonNode jsonNode) {
        for (String key : keys) {
            if (!jsonNode.hasNonNull(key)) {
                return false;
            }
        }
        return true;
    }

    private boolean isNumOutOfRange(long v, long min, long max) {
        return v < min && v > max;
    }

    public boolean isInvalidIPv4(String ip) {
        try {
            InetAddress inetAddress = InetAddress.getByName(ip);
            return inetAddress.getHostAddress().equals(ip) && inetAddress instanceof java.net.Inet4Address;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    @SuppressWarnings("all")
    private boolean isValidValues(JsonNode v) {
        final String ip = v.get(NginxDefineKeyword.IP).asText();
        final long bytes = v.get(NginxDefineKeyword.BYTES).asLong(-1);
        final long status = v.get(NginxDefineKeyword.STATUS).asLong(-1);

        if (!isInvalidIPv4(ip)) {
            log.warn("isInvalidIPv4");
            return false;
        } else if (isNumOutOfRange(status, 0, Integer.MAX_VALUE)) {
            log.warn("isNumOutOfRange");
            return false;
        } else if (isNumOutOfRange(bytes, 0, Integer.MAX_VALUE)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean test(String s, JsonNode jsonNode) {
        return this.isPresentRequiredKeys(nginxFields, jsonNode) && this.isValidValues(jsonNode);
    }
}
