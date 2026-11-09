package br.ufop.bcc362.tp2.observer;

import br.ufop.bcc362.tp2.model.AuditEvent;
import br.ufop.bcc362.tp2.protocol.WireProtocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

public final class ObserverClient {
    private final String host;
    private final int port;
    private final int timeoutMillis;

    public ObserverClient(String host, int port, Duration timeout) {
        this.host = host;
        this.port = port;
        this.timeoutMillis = Math.toIntExact(timeout.toMillis());
    }

    public void register(AuditEvent event) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            WireProtocol.writeAuditEvent(output, event);
            WireProtocol.Acknowledgement acknowledgement = WireProtocol.readAcknowledgement(input);
            if (!acknowledgement.accepted()) {
                throw new IOException("observador rejeitou evento: " + acknowledgement.message());
            }
        }
    }
}

