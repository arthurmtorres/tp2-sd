package br.ufop.bcc362.tp2;

import java.time.Instant;

final class EventLog {
    private EventLog() {
    }

    static synchronized void info(String component, String event, Object... fields) {
        StringBuilder line = new StringBuilder()
                .append("timestamp=").append(Instant.now())
                .append(" component=").append(clean(component))
                .append(" event=").append(clean(event));
        for (int index = 0; index + 1 < fields.length; index += 2) {
            line.append(' ').append(fields[index]).append('=').append(clean(fields[index + 1]));
        }
        System.out.println(line);
    }

    private static String clean(Object value) {
        return String.valueOf(value).replaceAll("[\\r\\n\\t ]+", "_");
    }
}

