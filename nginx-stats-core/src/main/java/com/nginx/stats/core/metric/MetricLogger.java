package com.nginx.stats.core.metric;


import org.slf4j.Logger;

public class MetricLogger {

    private MetricLogger() {
    }

    public static void printMetricInfoLog(final Logger logger, final String format, final Object... arguments) {
        logger.info(format, arguments);
    }

    public static void printMetricWarnLog(final Logger logger, final String format, final Object... arguments) {
        logger.warn(format, arguments);
    }

    public static void printMetricErrorLog(final Logger logger, final String format, final Object... arguments) {
        logger.error(format, arguments);
    }
}
