package br.ufop.bcc362.tp2.model;

import java.util.Objects;

public record AuditEvent(Type type, String processId, String requestId, int round) {
    public AuditEvent {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(processId, "processId");
        Objects.requireNonNull(requestId, "requestId");
        if (round < 1) {
            throw new IllegalArgumentException("round deve ser positivo");
        }
    }

    public enum Type {
        BEGIN,
        END
    }
}

