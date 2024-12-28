package com.nginx.stats.core.generate;

public class StateStoreKey {

    public static final String SEPARATOR = "_";
    private final StringBuilder sb;

    public StateStoreKey() {
        sb = new StringBuilder();
    }

    public String generateStateStoreKey(String statTime, String... keys) {
        sb.setLength(0);
        sb.append(statTime);

        for (String key: keys) {
            sb.append(SEPARATOR);
            sb.append(key);
        }

        return sb.toString();
    }
}
