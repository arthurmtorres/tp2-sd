package br.ufop.bcc362.tp2.cluster;

import br.ufop.bcc362.tp2.config.Environment;
import br.ufop.bcc362.tp2.logging.EventLog;
import br.ufop.bcc362.tp2.model.AccessRequest;
import br.ufop.bcc362.tp2.model.AccessResponse;
import br.ufop.bcc362.tp2.model.AuditEvent;
import br.ufop.bcc362.tp2.observer.ObserverClient;
import br.ufop.bcc362.tp2.protocol.WireProtocol;
import br.ufop.bcc362.tp2.sync.CuratorMutexSynchronizer;
import br.ufop.bcc362.tp2.sync.DistributedSynchronizer;
import br.ufop.bcc362.tp2.sync.RoundBarrier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class ClusterNode {
    private static final String LOCK_PATH = "/tp2/resources/R/lock";
    private static final String BARRIER_ROOT = "/tp2/barriers";

    private final Config config;
    private final CuratorFramework curator;
    private final SimulatedResource resource;
    private final DistributedSynchronizer synchronizer;
    private final ExecutorService localQueue;

    private ClusterNode(Config config) {
        this.config = config;
        this.curator = CuratorFrameworkFactory.newClient(
                config.zookeeperConnect(),
                new ExponentialBackoffRetry(1_000, 5)
        );
        ObserverClient observer = new ObserverClient(
                config.observerHost(), config.observerPort(), Duration.ofSeconds(10));
        this.resource = new SimulatedResource(config.nodeId(), observer);
        this.synchronizer = config.lockEnabled()
                ? new CuratorMutexSynchronizer(curator, LOCK_PATH)
                : DistributedSynchronizer.disabled();
        this.localQueue = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, config.nodeId() + "-local-queue");
            thread.setDaemon(false);
            return thread;
        });
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnvironment();
        new ClusterNode(config).run();
    }

    private void run() throws Exception {
        curator.start();
        if (!curator.blockUntilConnected(
                Math.toIntExact(config.connectionTimeout().toMillis()), TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("não foi possível conectar ao ZooKeeper");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            localQueue.shutdownNow();
            curator.close();
        }));

        try (ServerSocket server = new ServerSocket(config.port())) {
            EventLog.info(config.nodeId(), "READY",
                    "port", config.port(),
                    "lock_enabled", config.lockEnabled(),
                    "barrier_enabled", config.barrierEnabled());
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = server.accept();
                localQueue.execute(() -> handle(socket));
            }
        } finally {
            localQueue.shutdownNow();
            curator.close();
        }
    }

    private void handle(Socket socket) {
        String requestId = "unknown";
        try (socket;
             DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            socket.setSoTimeout(Math.toIntExact(config.requestTimeout().toMillis()));
            AccessRequest request = WireProtocol.readRequest(input);
            requestId = request.requestId();
            WireProtocol.writeResponse(output, execute(request));
        } catch (Exception exception) {
            EventLog.info(config.nodeId(), "ERROR", "request_id", requestId, "message", exception.getMessage());
        }
    }

    private AccessResponse execute(AccessRequest request) {
        String requestId = request.requestId();
        RoundBarrier barrier = config.barrierEnabled()
                ? new RoundBarrier(curator, BARRIER_ROOT, request.sequence(), config.expectedParticipants())
                : null;
        DistributedSynchronizer.Lease lease = null;
        boolean enteredBarrier = false;
        boolean released = false;

        EventLog.info(config.nodeId(), "REQUEST", "request_id", requestId, "round", request.sequence());
        try {
            if (barrier != null) {
                EventLog.info(config.nodeId(), "BARRIER_ENTER_WAIT", "request_id", requestId, "round", request.sequence());
                barrier.enter(config.barrierTimeout());
                enteredBarrier = true;
                EventLog.info(config.nodeId(), "BARRIER_ENTERED", "request_id", requestId, "round", request.sequence());
            }

            long waitStarted = System.nanoTime();
            lease = synchronizer.acquire(requestId, config.lockTimeout());
            long waitNanos = System.nanoTime() - waitStarted;
            EventLog.info(config.nodeId(), "ACQUIRE",
                    "request_id", requestId,
                    "wait_ns", waitNanos,
                    "lock_enabled", config.lockEnabled());

            resource.write(request);

            lease.close();
            released = true;
            EventLog.info(config.nodeId(), "RELEASE", "request_id", requestId);

            if (barrier != null) {
                barrier.leave(config.barrierTimeout());
                enteredBarrier = false;
                EventLog.info(config.nodeId(), "BARRIER_LEFT", "request_id", requestId, "round", request.sequence());
            }

            EventLog.info(config.nodeId(), "COMMITTED", "request_id", requestId, "wait_ns", waitNanos);
            return AccessResponse.committed(requestId, waitNanos);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return AccessResponse.error(requestId, "processamento interrompido");
        } catch (Exception exception) {
            EventLog.info(config.nodeId(), "ERROR", "request_id", requestId, "message", exception.getMessage());
            return AccessResponse.error(requestId, exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage());
        } finally {
            if (lease != null && !released) {
                try {
                    lease.close();
                    EventLog.info(config.nodeId(), "RELEASE", "request_id", requestId, "cleanup", true);
                } catch (Exception cleanupException) {
                    EventLog.info(config.nodeId(), "CLEANUP_ERROR", "request_id", requestId,
                            "step", "release", "message", cleanupException.getMessage());
                }
            }
            if (barrier != null && enteredBarrier) {
                try {
                    barrier.leave(config.barrierTimeout());
                } catch (Exception cleanupException) {
                    EventLog.info(config.nodeId(), "CLEANUP_ERROR", "request_id", requestId,
                            "step", "barrier_leave", "message", cleanupException.getMessage());
                }
            }
        }
    }

    /** Seção crítica que representa o recurso hipotético R; não conhece o mecanismo de lock. */
    private static final class SimulatedResource {
        private final String processId;
        private final ObserverClient observer;

        private SimulatedResource(String processId, ObserverClient observer) {
            this.processId = Objects.requireNonNull(processId, "processId");
            this.observer = Objects.requireNonNull(observer, "observer");
        }

        private void write(AccessRequest request) throws IOException, InterruptedException {
            observer.register(new AuditEvent(
                    AuditEvent.Type.BEGIN, processId, request.requestId(), request.sequence()));
            EventLog.info(processId, "ENTER", "resource", "R",
                    "request_id", request.requestId(), "round", request.sequence());
            try {
                Thread.sleep(request.criticalSectionMillis());
            } finally {
                EventLog.info(processId, "EXIT", "resource", "R",
                        "request_id", request.requestId(), "round", request.sequence());
                observer.register(new AuditEvent(
                        AuditEvent.Type.END, processId, request.requestId(), request.sequence()));
            }
        }
    }

    private record Config(
            String nodeId,
            int port,
            String zookeeperConnect,
            String observerHost,
            int observerPort,
            int expectedParticipants,
            boolean lockEnabled,
            boolean barrierEnabled,
            Duration connectionTimeout,
            Duration requestTimeout,
            Duration lockTimeout,
            Duration barrierTimeout
    ) {
        private static Config fromEnvironment() {
            int participants = Environment.integer("EXPECTED_PARTICIPANTS", 5);
            if (participants < 2) {
                throw new IllegalArgumentException("EXPECTED_PARTICIPANTS deve ser no mínimo 2");
            }
            return new Config(
                    Environment.text("NODE_ID", "cluster-1"),
                    Environment.integer("NODE_PORT", 8000),
                    Environment.text("ZK_CONNECT", "localhost:2181"),
                    Environment.text("OBSERVER_HOST", "localhost"),
                    Environment.integer("OBSERVER_PORT", 7000),
                    participants,
                    Environment.bool("LOCK_ENABLED", true),
                    Environment.bool("BARRIER_ENABLED", true),
                    Duration.ofSeconds(Environment.longInteger("CONNECTION_TIMEOUT_SECONDS", 30)),
                    Duration.ofSeconds(Environment.longInteger("REQUEST_TIMEOUT_SECONDS", 180)),
                    Duration.ofSeconds(Environment.longInteger("LOCK_TIMEOUT_SECONDS", 120)),
                    Duration.ofSeconds(Environment.longInteger("BARRIER_TIMEOUT_SECONDS", 120))
            );
        }
    }
}

