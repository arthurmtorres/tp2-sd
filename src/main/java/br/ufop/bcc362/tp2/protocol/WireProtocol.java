package br.ufop.bcc362.tp2.protocol;

import br.ufop.bcc362.tp2.model.AccessRequest;
import br.ufop.bcc362.tp2.model.AccessResponse;
import br.ufop.bcc362.tp2.model.AuditEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class WireProtocol {
    private static final int VERSION = 1;

    private WireProtocol() {
    }

    public static void writeRequest(DataOutputStream output, AccessRequest request) throws IOException {
        output.writeInt(VERSION);
        output.writeUTF(request.clientId());
        output.writeInt(request.sequence());
        output.writeLong(request.criticalSectionMillis());
        output.flush();
    }

    public static AccessRequest readRequest(DataInputStream input) throws IOException {
        requireVersion(input.readInt());
        return new AccessRequest(input.readUTF(), input.readInt(), input.readLong());
    }

    public static void writeResponse(DataOutputStream output, AccessResponse response) throws IOException {
        output.writeInt(VERSION);
        output.writeUTF(response.status().name());
        output.writeUTF(response.requestId());
        output.writeLong(response.waitNanos());
        output.writeUTF(response.message());
        output.flush();
    }

    public static AccessResponse readResponse(DataInputStream input) throws IOException {
        requireVersion(input.readInt());
        return new AccessResponse(
                AccessResponse.Status.valueOf(input.readUTF()),
                input.readUTF(),
                input.readLong(),
                input.readUTF()
        );
    }

    public static void writeAuditEvent(DataOutputStream output, AuditEvent event) throws IOException {
        output.writeInt(VERSION);
        output.writeUTF(event.type().name());
        output.writeUTF(event.processId());
        output.writeUTF(event.requestId());
        output.writeInt(event.round());
        output.flush();
    }

    public static AuditEvent readAuditEvent(DataInputStream input) throws IOException {
        requireVersion(input.readInt());
        return new AuditEvent(
                AuditEvent.Type.valueOf(input.readUTF()),
                input.readUTF(),
                input.readUTF(),
                input.readInt()
        );
    }

    public static void writeAcknowledgement(DataOutputStream output, boolean accepted, String message)
            throws IOException {
        output.writeInt(VERSION);
        output.writeBoolean(accepted);
        output.writeUTF(message);
        output.flush();
    }

    public static Acknowledgement readAcknowledgement(DataInputStream input) throws IOException {
        requireVersion(input.readInt());
        return new Acknowledgement(input.readBoolean(), input.readUTF());
    }

    private static void requireVersion(int version) throws IOException {
        if (version != VERSION) {
            throw new IOException("versão de protocolo incompatível: " + version);
        }
    }

    public record Acknowledgement(boolean accepted, String message) {
    }
}
