package com.nginx.stats.core.generate;

public class StoreKey {

    private StoreKey() {
    }

    public static String generateStateStoreKey(String... args) {
        StringBuilder sb = new StringBuilder();

        for (String arg : args) {
            sb.append(arg);
            sb.append("_");
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }
}
