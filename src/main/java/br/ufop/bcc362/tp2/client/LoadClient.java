package br.ufop.bcc362.tp2.client;

import br.ufop.bcc362.tp2.config.Environment;
import br.ufop.bcc362.tp2.logging.EventLog;
import br.ufop.bcc362.tp2.model.AccessRequest;
import br.ufop.bcc362.tp2.model.AccessResponse;
import br.ufop.bcc362.tp2.protocol.WireProtocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.SplittableRandom;

public final class LoadClient {
    private final Config config;
    private final SplittableRandom random;

    private LoadClient(Config config) {
        this.config = config;
        this.random = new SplittableRandom(config.seed());
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnvironment();
        new LoadClient(config).run();
    }

    private void run() throws Exception {
        long experimentStarted = System.nanoTime();
        long totalWaitNanos = 0;

        EventLog.info(config.clientId(), "START",
                "requests", config.requestCount(),
                "seed", config.seed(),
                "cluster", config.clusterHost() + ":" + config.clusterPort());

        for (int sequence = 1; sequence <= config.requestCount(); sequence++) {
            long criticalSectionMillis = randomBetween(
                    config.criticalSectionMinMillis(), config.criticalSectionMaxMillis());
            AccessRequest request = new AccessRequest(config.clientId(), sequence, criticalSectionMillis);
            AccessResponse response = sendWhenAvailable(request);
            if (response.status() != AccessResponse.Status.COMMITTED) {
                throw new IllegalStateException(response.requestId() + " falhou: " + response.message());
            }
            totalWaitNanos += response.waitNanos();
            EventLog.info(config.clientId(), "COMMITTED",
                    "request_id", response.requestId(),
                    "wait_ns", response.waitNanos(),
                    "critical_section_ms", criticalSectionMillis);

            if (sequence < config.requestCount()) {
                Thread.sleep(randomBetween(config.delayMinMillis(), config.delayMaxMillis()));
            }
        }

        EventLog.info(config.clientId(), "SUMMARY",
                "committed", config.requestCount(),
                "total_wait_ns", totalWaitNanos,
                "elapsed_ns", System.nanoTime() - experimentStarted);
    }

    private AccessResponse sendWhenAvailable(AccessRequest request) throws Exception {
        long deadline = System.nanoTime() + config.startupTimeout().toNanos();
        while (true) {
            try {
                return send(request);
            } catch (ConnectException exception) {
                if (System.nanoTime() >= deadline) {
                    throw exception;
                }
                Thread.sleep(500);
            }
        }
    }

    private AccessResponse send(AccessRequest request) throws IOException {
        int timeoutMillis = Math.toIntExact(config.requestTimeout().toMillis());
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(config.clusterHost(), config.clusterPort()), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            WireProtocol.writeRequest(output, request);
            return WireProtocol.readResponse(input);
        }
    }

    private long randomBetween(long minimum, long maximum) {
        return minimum == maximum ? minimum : random.nextLong(minimum, maximum + 1);
    }

    private record Config(
            String clientId,
            String clusterHost,
            int clusterPort,
            int requestCount,
            long delayMinMillis,
            long delayMaxMillis,
            long criticalSectionMinMillis,
            long criticalSectionMaxMillis,
            long seed,
            Duration startupTimeout,
            Duration requestTimeout
    ) {
        private static Config fromEnvironment() {
            int requestCount = Environment.integer("REQUEST_COUNT", 10);
            if (requestCount < 10 || requestCount > 50) {
                throw new IllegalArgumentException("REQUEST_COUNT deve estar entre 10 e 50");
            }
            long delayMin = Environment.longInteger("CLIENT_DELAY_MIN_MS", 1_000);
            long delayMax = Environment.longInteger("CLIENT_DELAY_MAX_MS", 5_000);
            long sectionMin = Environment.longInteger("CRITICAL_SECTION_MIN_MS", 200);
            long sectionMax = Environment.longInteger("CRITICAL_SECTION_MAX_MS", 1_000);
            requireRange("intervalo do cliente", delayMin, delayMax);
            requireRange("seção crítica", sectionMin, sectionMax);

            String clientId = Environment.text("CLIENT_ID", "client-1");
            return new Config(
                    clientId,
                    Environment.text("CLUSTER_HOST", "localhost"),
                    Environment.integer("CLUSTER_PORT", 8000),
                    requestCount,
                    delayMin,
                    delayMax,
                    sectionMin,
                    sectionMax,
                    Environment.longInteger("RANDOM_SEED", clientId.hashCode()),
                    Duration.ofSeconds(Environment.longInteger("STARTUP_TIMEOUT_SECONDS", 60)),
                    Duration.ofSeconds(Environment.longInteger("REQUEST_TIMEOUT_SECONDS", 180))
            );
        }

        private static void requireRange(String name, long minimum, long maximum) {
            if (minimum < 0 || maximum < minimum || maximum > 60_000) {
                throw new IllegalArgumentException(name + " inválido: " + minimum + ".." + maximum);
            }
        }
    }
}
