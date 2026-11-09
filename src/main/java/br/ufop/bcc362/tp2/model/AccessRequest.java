package br.ufop.bcc362.tp2.model;

import java.util.Objects;

public record AccessRequest(String clientId, int sequence, long criticalSectionMillis) {
    public AccessRequest {
        Objects.requireNonNull(clientId, "clientId");
        if (!clientId.matches("[A-Za-z0-9_-]{1,40}")) {
            throw new IllegalArgumentException("clientId inválido: " + clientId);
        }
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence deve ser positivo");
        }
        if (criticalSectionMillis < 0 || criticalSectionMillis > 60_000) {
            throw new IllegalArgumentException("duração da seção crítica fora do intervalo permitido");
        }
    }

    public String requestId() {
        return clientId + "-" + sequence;
    }
}

