package br.ufop.bcc362.tp2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

final class WireProtocol {
    private static final int VERSION = 1;

    private WireProtocol() {
    }

    static void writeRequest(DataOutputStream output, AccessRequest request) throws IOException {
        output.writeInt(VERSION);
        output.writeUTF(request.clientId());
        output.writeInt(request.sequence());
        output.writeLong(request.criticalSectionMillis());
        output.flush();
    }

    static AccessRequest readRequest(DataInputStream input) throws IOException {
        checkVersion(input.readInt());
        return new AccessRequest(input.readUTF(), input.readInt(), input.readLong());
    }

    static void writeResponse(DataOutputStream output, AccessResponse response) throws IOException {
        output.writeInt(VERSION);
        output.writeUTF(response.status().name());
        output.writeUTF(response.requestId());
        output.writeLong(response.waitNanos());
        output.writeUTF(response.message());
        output.flush();
    }

    static AccessResponse readResponse(DataInputStream input) throws IOException {
        checkVersion(input.readInt());
        return new AccessResponse(
                AccessResponse.Status.valueOf(input.readUTF()),
                input.readUTF(), input.readLong(), input.readUTF());
    }

    static void writeAuditEvent(DataOutputStream output, AuditEvent event) throws IOException {
        output.writeInt(VERSION);
        output.writeUTF(event.type().name());
        output.writeUTF(event.processId());
        output.writeUTF(event.requestId());
        output.writeInt(event.round());
        output.flush();
    }

    static AuditEvent readAuditEvent(DataInputStream input) throws IOException {
        checkVersion(input.readInt());
        return new AuditEvent(AuditEvent.Type.valueOf(input.readUTF()),
                input.readUTF(), input.readUTF(), input.readInt());
    }

    static void writeAcknowledgement(DataOutputStream output, boolean accepted, String message)
            throws IOException {
        output.writeInt(VERSION);
        output.writeBoolean(accepted);
        output.writeUTF(message);
        output.flush();
    }

    static Acknowledgement readAcknowledgement(DataInputStream input) throws IOException {
        checkVersion(input.readInt());
        return new Acknowledgement(input.readBoolean(), input.readUTF());
    }

    private static void checkVersion(int version) throws IOException {
        if (version != VERSION) {
            throw new IOException("versão de protocolo incompatível: " + version);
        }
    }

    record AccessRequest(String clientId, int sequence, long criticalSectionMillis) {
        AccessRequest {
            Objects.requireNonNull(clientId);
            if (!clientId.matches("[A-Za-z0-9_-]{1,40}") || sequence < 1) {
                throw new IllegalArgumentException("pedido inválido");
            }
            if (criticalSectionMillis < 0 || criticalSectionMillis > 60_000) {
                throw new IllegalArgumentException("duração da seção crítica inválida");
            }
        }

        String requestId() {
            return clientId + "-" + sequence;
        }
    }

    record AccessResponse(Status status, String requestId, long waitNanos, String message) {
        static AccessResponse committed(String requestId, long waitNanos) {
            return new AccessResponse(Status.COMMITTED, requestId, waitNanos, "ok");
        }

        static AccessResponse error(String requestId, String message) {
            return new AccessResponse(Status.ERROR, requestId, 0, message);
        }

        enum Status { COMMITTED, ERROR }
    }

    record AuditEvent(Type type, String processId, String requestId, int round) {
        enum Type { BEGIN, END }
    }

    record Acknowledgement(boolean accepted, String message) {
    }
}

