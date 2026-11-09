package br.ufop.bcc362.tp2.logging;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class EventLog {
    private EventLog() {
    }

    public static synchronized void info(String component, String event, Object... fields) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index + 1 < fields.length; index += 2) {
            values.put(String.valueOf(fields[index]), fields[index + 1]);
        }
        String details = values.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + sanitize(entry.getValue()))
                .collect(Collectors.joining(" "));
        System.out.printf("timestamp=%s component=%s event=%s%s%n",
                Instant.now(), sanitize(component), sanitize(event), details.isEmpty() ? "" : " " + details);
    }

    private static String sanitize(Object value) {
        return String.valueOf(value).replaceAll("[\\r\\n\\t ]+", "_");
    }
}

