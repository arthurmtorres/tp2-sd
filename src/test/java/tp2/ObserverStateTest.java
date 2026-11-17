package tp2;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;

class ObserverStateTest {
    @Test
    void validSerialAccessesFinish() {
        ObserverState s = new ObserverState(2, 2, false, Map.of("c1", 1, "c2", 1));
        s.accept("BEGIN n1 c1-1 1 10");
        s.accept("END n1 c1-1 1");
        s.accept("BEGIN n2 c2-1 1 20");
        s.accept("END n2 c2-1 1");
        s.accept("DONE n1");
        s.accept("DONE n2");
        assertEquals(0, s.exitCode());
        assertEquals(15, Double.parseDouble(s.summary("test", "1").get("median_wait_ms")));
    }

    @Test
    void overlapIsRecordedInsteadOfPrevented() {
        ObserverState s = new ObserverState(2, 2, false, Map.of());
        s.accept("BEGIN n1 c1-1 1 0");
        s.accept("BEGIN n2 c2-1 1 0");
        assertEquals("2", s.summary("test", "1").get("active"));
        s.accept("END n1 c1-1 1");
        s.accept("END n2 c2-1 1");
        s.accept("DONE n1");
        s.accept("DONE n2");
        assertEquals(2, s.exitCode());
    }

    @Test
    void duplicateEndAndDoneCannotFakeCompletion() {
        ObserverState s = new ObserverState(2, 2, false, Map.of());
        s.accept("END n1 c1-1 1");
        s.accept("END n1 c1-1 1");
        s.accept("DONE n1");
        s.accept("DONE n1");
        assertFalse(s.allDone());
        s.accept("DONE n2");
        assertEquals(4, s.exitCode());
        assertEquals("0", s.summary("test", "1").get("completed"));
    }

    @Test
    void activeRequestPreventsSuccess() {
        ObserverState s = new ObserverState(1, 1, false, Map.of());
        s.accept("BEGIN n1 c1-1 1 1");
        s.accept("DONE n1");
        assertEquals(3, s.exitCode());
    }

    @Test
    void wrongOwnerCannotCloseAnotherNodesRequest() {
        ObserverState s = new ObserverState(2, 1, false, Map.of());
        s.accept("BEGIN n1 c1-1 1 1");
        s.accept("END n2 c1-1 1");
        assertEquals("1", s.summary("test", "1").get("active"));
        assertEquals(4, s.exitCode());
    }

    @Test
    void duplicateRequestCannotBeReusedOnAnotherNode() {
        ObserverState s = new ObserverState(2, 2, false, Map.of());
        s.accept("BEGIN n1 c1-1 1 1");
        s.accept("END n1 c1-1 1");
        s.accept("BEGIN n2 c1-1 1 1");
        assertEquals(4, s.exitCode());
    }

    @Test
    void duplicateArrivalDoesNotReplaceAnotherParticipant() {
        ObserverState s = new ObserverState(2, 2, true, Map.of());
        s.accept("ARRIVE n1 1");
        s.accept("ARRIVE n1 1");
        s.accept("PASS n1 1");
        assertEquals(4, s.exitCode());
        assertEquals("1", s.summary("test", "1").get("barrier_violations"));
    }

    @Test
    void barrierSeparatesTwoRounds() {
        ObserverState s = new ObserverState(2, 4, true, Map.of("c1", 2, "c2", 2));
        for (int round = 1; round <= 2; round++) {
            s.accept("ARRIVE n1 " + round);
            s.accept("ARRIVE n2 " + round);
            s.accept("PASS n1 " + round);
            s.accept("PASS n2 " + round);
            for (int n = 1; n <= 2; n++) {
                s.accept("BEGIN n" + n + " c" + n + "-" + round + " " + round + " 10");
                s.accept("END n" + n + " c" + n + "-" + round + " " + round);
            }
            s.accept("LEAVE n1 " + round);
            s.accept("LEAVE n2 " + round);
        }
        s.accept("DONE n1");
        s.accept("DONE n2");
        assertEquals(0, s.exitCode());
    }

    @Test
    void earlyNextRoundIsDetected() {
        ObserverState s = new ObserverState(2, 4, true, Map.of());
        s.accept("ARRIVE n1 1");
        s.accept("ARRIVE n2 1");
        s.accept("PASS n1 1");
        s.accept("PASS n2 1");
        s.accept("BEGIN n1 c1-1 1 0");
        s.accept("END n1 c1-1 1");
        s.accept("ARRIVE n1 2");
        assertEquals("1", s.summary("test", "1").get("barrier_violations"));
    }

    @Test
    void perClientCountsRequireEveryExpectedClient() {
        ObserverState s = new ObserverState(2, 2, false, Map.of("c1", 1, "c2", 1));
        s.accept("BEGIN n1 c1-1 1 10");
        s.accept("END n1 c1-1 1");
        s.accept("BEGIN n1 c1-2 2 10");
        assertEquals(4, s.exitCode());
    }

    @Test
    void medianAndNearestRankPercentile() {
        assertEquals(0, Metrics.median(List.of()));
        assertEquals(15, Metrics.median(List.of(20d, 10d)));
        assertEquals(20, Metrics.median(List.of(30d, 10d, 20d)));
        assertEquals(20, Metrics.percentile(List.of(10d, 20d), .95));
    }

    @Test
    void csvUsesDecimalPointInBrazilianLocale(@TempDir Path directory) throws Exception {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("pt-BR"));
            Path path = directory.resolve("metrics.csv");
            Metrics.writeCsv(path, Map.of("time", Metrics.number(1.25), "label", "a,b"));
            String text = Files.readString(path);
            assertTrue(text.contains("1.250000"));
            assertTrue(text.contains("\"a,b\""));
            assertFalse(text.contains("1,250000"));
        } finally {
            Locale.setDefault(previous);
        }
    }
}
