package com.nginx.stats.core.time;

public interface ParseStrTimeStrategy {

    long parse(String timestamp);
}
