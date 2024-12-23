package com.nginx.stats.httpcode.streams;

public class DefineKeyword {

    public static final String VALIDATOR_SPLIT_PREFIX_NAME = "VALIDATOR_";
    public static final String VALIDATOR_SUCCCESS_BRANCH_NAME = "SUCCESS_BRANCH";
    public static final String VALIDATOR_FAILED_BRANCH_NAME = "FAILED_BRANCH";

    public static final String VALIDATOR_FAILED_TOPIC_NAME = "HTTPCODE_VALIDATOR_FAILED";

    public static final String ONE_MIN_NGINX_STATS_HTTPCODE_PROCESSOR_NAME = "ONE_MIN_NGINX_STATS_HTTPCODE_PROCESSOR";
    public static final String ONE_MIN_NGINX_STATS_HTTPCODE_STORE_NAME = "ONE_MIN_NGINX_STATS_HTTPCODE_STORE";

    // Output Topic
    public static final String ONE_MIN_NGINX_STATS_HTTPCODE_TOPIC_NAME = "T_JOB_1M_HTTPCODE";
    public static final String FIVE_MIN_NGINX_STATS_HTTPCODE_TOPIC_NAME = "T_JOB_5M_HTTPCODE";

    private DefineKeyword() {
    }
}
