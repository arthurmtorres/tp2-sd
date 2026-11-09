package br.ufop.bcc362.tp2.observer;

import br.ufop.bcc362.tp2.config.Environment;
import br.ufop.bcc362.tp2.logging.EventLog;
import br.ufop.bcc362.tp2.model.AuditEvent;
import br.ufop.bcc362.tp2.protocol.WireProtocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ObserverServer {
    private final AuditorState state = new AuditorState();
    private final ExecutorService handlers = Executors.newCachedThreadPool();

    public static void main(String[] args) throws IOException {
        int port = Environment.integer("OBSERVER_PORT", 7000);
        new ObserverServer().run(port);
    }

    public void run(int port) throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            AuditorState.Snapshot snapshot = state.snapshot();
            EventLog.info("observer", "SUMMARY",
                    "completed", snapshot.completedAccesses(),
                    "max_concurrent", snapshot.maximumConcurrentAccesses(),
                    "overlaps", snapshot.overlapViolations(),
                    "invalid_events", snapshot.invalidEvents());
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
            AuditorState.Result result = state.register(event);
            AuditorState.Snapshot snapshot = result.snapshot();
            EventLog.info("observer", event.type().name(),
                    "sequence", result.sequence(),
                    "process_id", event.processId(),
                    "request_id", event.requestId(),
                    "round", event.round(),
                    "active", snapshot.activeAccesses(),
                    "max_concurrent", snapshot.maximumConcurrentAccesses(),
                    "overlap", result.overlap(),
                    "overlap_count", snapshot.overlapViolations());
            WireProtocol.writeAcknowledgement(output, result.accepted(), result.message());
        } catch (Exception exception) {
            EventLog.info("observer", "ERROR", "message", exception.getMessage());
        }
    }
}
