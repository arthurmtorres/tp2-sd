package br.ufop.bcc362.tp2.observer;

import br.ufop.bcc362.tp2.model.AuditEvent;

import java.util.HashMap;
import java.util.Map;

public final class AuditorState {
    private final Map<String, String> activeRequests = new HashMap<>();
    private int maximumConcurrentAccesses;
    private long beginEvents;
    private long completedAccesses;
    private long overlapViolations;
    private long invalidEvents;

    public synchronized Result register(AuditEvent event) {
        return switch (event.type()) {
            case BEGIN -> begin(event);
            case END -> end(event);
        };
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(
                activeRequests.size(), maximumConcurrentAccesses, beginEvents,
                completedAccesses, overlapViolations, invalidEvents
        );
    }

    private Result begin(AuditEvent event) {
        if (activeRequests.containsKey(event.requestId())) {
            invalidEvents++;
            return new Result(false, false, "BEGIN duplicado para " + event.requestId(), snapshot());
        }

        boolean overlap = !activeRequests.isEmpty();
        if (overlap) {
            overlapViolations++;
        }
        activeRequests.put(event.requestId(), event.processId());
        beginEvents++;
        maximumConcurrentAccesses = Math.max(maximumConcurrentAccesses, activeRequests.size());
        return new Result(true, overlap, overlap ? "sobreposição detectada" : "BEGIN registrado", snapshot());
    }

    private Result end(AuditEvent event) {
        String processId = activeRequests.get(event.requestId());
        if (processId == null || !processId.equals(event.processId())) {
            invalidEvents++;
            return new Result(false, false, "END sem BEGIN correspondente", snapshot());
        }
        activeRequests.remove(event.requestId());
        completedAccesses++;
        return new Result(true, false, "END registrado", snapshot());
    }

    public record Result(boolean accepted, boolean overlap, String message, Snapshot snapshot) {
    }

    public record Snapshot(
            int activeAccesses,
            int maximumConcurrentAccesses,
            long beginEvents,
            long completedAccesses,
            long overlapViolations,
            long invalidEvents
    ) {
    }
}

