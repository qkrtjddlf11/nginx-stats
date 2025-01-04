package com.nginx.stats.core.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TimeYMDHMTest {

    private static final Map<String, Long> expected = Map.ofEntries(
        Map.entry("202412300619", 1735507140L),
        Map.entry("202412310000", 1735570800L),
        Map.entry("202412312359", 1735657140L));

    private void testParse(long statTime, long expected) {
        assertThat(statTime).isEqualTo(expected);
    }

    @Test
    @DisplayName("Test parse.")
    void testParse() {
        TimeYMDHM timeYMDHM = new TimeYMDHM();
        expected.forEach((statTime, epoch) -> testParse(timeYMDHM.parse(statTime), epoch));
    }
}
