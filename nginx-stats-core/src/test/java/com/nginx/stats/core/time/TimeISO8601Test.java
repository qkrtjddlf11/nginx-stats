package com.nginx.stats.core.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TimeISO8601Test {

    private static final Map<String, Long> expected = Map.ofEntries(
        Map.entry("2024-12-30T06:19:01Z", 1735539541L),
        Map.entry("2024-12-31T00:00:00Z", 1735603200L),
        Map.entry("2024-12-31T23:59:00Z", 1735689540L));

    private void testParse(long statTime, long expected) {
        assertThat(statTime).isEqualTo(expected);
    }

    @Test
    @DisplayName("Test parse.")
    void testParse() {
        TimeISO8601 timeIso8601 = new TimeISO8601();
        expected.forEach((statTime, epoch) -> testParse(timeIso8601.parse(statTime), epoch));
    }
}
