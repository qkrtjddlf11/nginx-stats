package com.nginx.stats.core.metric;

public class MetricCode {

    public static final String APPL_I_0001 = "APPL_I_0001";
    public static final String APPL_I_0001_DOC = "Topology description.";
    public static final String APPL_I_0001_FMT = "{} - {} {}";

    public static final String APPL_I_0002 = "APPL_I_0002";
    public static final String APPL_I_0002_DOC = "Kafka Streams Application starts.";
    public static final String APPL_I_0002_FMT = "{} - {}";

    public static final String APPL_I_0003 = "APPL_I_0003";
    public static final String APPL_I_0003_DOC = "Kafka Streams Application is stopped.";
    public static final String APPL_I_0003_FMT = "{} - {}";

    public static final String APPL_I_0004 = "APPL_I_0004";
    public static final String APPL_I_0004_DOC = "Kafka Streams state is changed.";
    public static final String APPL_I_0004_FMT = "{} - {} State from {} to {}.";

    public static final String APPL_E_0001 = "APPL_E_0001";
    public static final String APPL_E_0001_DOC = "UncaughtException ocurred.";
    public static final String APPL_E_0001_FMT = "{} - {} Reason: {}";

    // Json
    public static final String JSON_E_0001 = "JSON_E_0001";
    public static final String JSON_E_0001_DOC = "Match failed";
    public static final String JSON_E_0001_FMT = "{} - {}. Value: {}";

    public static final String JSON_E_0002 = "JSON_E_0002";
    public static final String JSON_E_0002_DOC = "Invalid value";
    public static final String JSON_E_0002_FMT = "{} - {}: {}, Value: {}";

    public static final String JSON_E_0003 = "JSON_E_0003";
    public static final String JSON_E_0003_DOC = "Empty value";
    public static final String JSON_E_0003_FMT = "{} - {}: {}";

    public static final String PERF_I_0001 = "PERF_I_0001";
    public static final String PERF_I_0001_DOC = "Performance.";
    public static final String PERF_I_0001_FMT = "{} - {} Cnt: {}, Rps: {}";

    private MetricCode() {
    }
}
