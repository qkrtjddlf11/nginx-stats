package com.nginx.stats.core.statestore;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;

public class ChangeLog {

    private ChangeLog() {
    }

    public static Map<String, String> getChangelogConfig(int minInSyncReplicas, long retentionMs) {
        Map<String, String> config = new HashMap<>();
        config.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minInSyncReplicas));
        config.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionMs));
        config.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, Long.toString(retentionMs));
        config.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        config.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
        return config;
    }
}