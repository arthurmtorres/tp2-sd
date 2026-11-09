package br.ufop.bcc362.tp2;

import br.ufop.bcc362.tp2.WireProtocol.AuditEvent;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ObserverServer {
    private final AuditorState state = new AuditorState();
    private final ExecutorService handlers = Executors.newCachedThreadPool();

    public static void main(String[] args) throws IOException {
        new ObserverServer().run(Environment.integer("OBSERVER_PORT", 7000));
    }

    private void run(int port) throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Snapshot snapshot = state.snapshot();
            EventLog.info("observer", "SUMMARY", "completed", snapshot.completed(),
                    "max_concurrent", snapshot.maxConcurrent(), "overlaps", snapshot.overlaps(),
                    "invalid_events", snapshot.invalid());
            handlers.shutdownNow();
        }));

        try (ServerSocket server = new ServerSocket(port)) {
            EventLog.info("observer", "READY", "port", port);
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = server.accept();
                handlers.execute(() -> handle(socket));
            }
        }
    }

    private void handle(Socket socket) {
        try (socket;
             DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            AuditEvent event = WireProtocol.readAuditEvent(input);
            Result result = state.register(event);
            Snapshot snapshot = result.snapshot();
            EventLog.info("observer", event.type().name(), "sequence", result.sequence(),
                    "process_id", event.processId(), "request_id", event.requestId(),
                    "round", event.round(), "active", snapshot.active(),
                    "max_concurrent", snapshot.maxConcurrent(), "overlap", result.overlap(),
                    "overlap_count", snapshot.overlaps());
            WireProtocol.writeAcknowledgement(output, result.accepted(), result.message());
        } catch (Exception exception) {
            EventLog.info("observer", "ERROR", "message", exception.getMessage());
        }
    }

    static final class AuditorState {
        private final Map<String, String> active = new HashMap<>();
        private int maxConcurrent;
        private long completed;
        private long overlaps;
        private long invalid;
        private long sequence;

        synchronized Result register(AuditEvent event) {
            return event.type() == AuditEvent.Type.BEGIN ? begin(event) : end(event);
        }

        synchronized Snapshot snapshot() {
            return new Snapshot(active.size(), maxConcurrent, completed, overlaps, invalid);
        }

        private Result begin(AuditEvent event) {
            if (active.containsKey(event.requestId())) {
                invalid++;
                return result(false, false, "BEGIN duplicado");
            }
            boolean overlap = !active.isEmpty();
            active.put(event.requestId(), event.processId());
            sequence++;
            maxConcurrent = Math.max(maxConcurrent, active.size());
            if (overlap) {
                overlaps++;
            }
            return result(true, overlap, overlap ? "sobreposição detectada" : "BEGIN registrado");
        }

        private Result end(AuditEvent event) {
            if (!event.processId().equals(active.get(event.requestId()))) {
                invalid++;
                return result(false, false, "END sem BEGIN correspondente");
            }
            active.remove(event.requestId());
            completed++;
            sequence++;
            return result(true, false, "END registrado");
        }

        private Result result(boolean accepted, boolean overlap, String message) {
            return new Result(accepted, overlap, sequence, message, snapshot());
        }
    }

    record Result(boolean accepted, boolean overlap, long sequence, String message, Snapshot snapshot) {
    }

    record Snapshot(int active, int maxConcurrent, long completed, long overlaps, long invalid) {
    }
}
