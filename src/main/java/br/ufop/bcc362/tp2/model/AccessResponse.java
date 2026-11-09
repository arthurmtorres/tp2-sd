package br.ufop.bcc362.tp2.model;

import java.util.Objects;

public record AccessResponse(Status status, String requestId, long waitNanos, String message) {
    public AccessResponse {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(message, "message");
    }

    public static AccessResponse committed(String requestId, long waitNanos) {
        return new AccessResponse(Status.COMMITTED, requestId, waitNanos, "ok");
    }

    public static AccessResponse error(String requestId, String message) {
        return new AccessResponse(Status.ERROR, requestId, 0, message);
    }

    public enum Status {
        COMMITTED,
        ERROR
    }
}

