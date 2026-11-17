package tp2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

/** Uma JVM do Cluster Sync. Leitores alimentam uma fila; um worker processa R. */
public final class SyncNode {
    private interface Work {}

    private record Request(String client, String id, PrintWriter out, long receivedAt) implements Work {}

    private record ClientFinished(String client) implements Work {}

    private record ClientError(Exception error) implements Work {}

    private final Map<String, String> options;
    private final String id;
    private final BlockingQueue<Work> queue = new LinkedBlockingQueue<>();
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private final Set<String> connectedClients = ConcurrentHashMap.newKeySet();

    private final Random random;
    private final int clients, participants, csMin, csMax;
    private final boolean barrier, sync;
    private int round;

    private SyncNode(Map<String, String> options) {
        this.options = options;
        id = Util.id(Util.required(options, "id"), "n");

        clients = Util.integer(options, "clients", 1, 1, 100);
        participants = Util.integer(options, "participants", 5, 1, 100);
        csMin = Util.integer(options, "cs-min", 200, 200, 1000);
        csMax = Util.integer(options, "cs-max", 1000, csMin, 1000);

        barrier = options.containsKey("barrier");
        sync = !options.containsKey("no-sync");

        random = new Random(Long.parseLong(options.getOrDefault("seed", "1")) * 1000 + id.hashCode());
    }

    public static void main(String[] args) throws Exception {
        SyncNode node = new SyncNode(Util.args(args));

        try {
            node.run();
        } catch (Exception e) {
            Util.log(node.id, "ERROR", "type=" + e.getClass().getSimpleName() + " message=" + e.getMessage());
            throw e;
        }
    }

    private void run() throws Exception {
        int port = Util.integer(options, "port", 7001, 1, 65535);
        String namespace = options.getOrDefault("ns", "tp2");

        try (CuratorFramework curator = CuratorFrameworkFactory.builder()
                        .connectString(options.getOrDefault("zk", "localhost:2181"))
                        .namespace(namespace)
                        .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                        .build()) {
            curator.getConnectionStateListenable().addListener(
                    (c, state) -> Util.log(id, "ZK_STATE", "state=" + state));

            curator.start();

            if (!curator.blockUntilConnected(30, TimeUnit.SECONDS))
                throw new IOException("ZooKeeper indisponível");

            ResourceLock lock = new ResourceLock(curator, sync);

            try (ObsClient observer = new ObsClient(options.getOrDefault("observer", "localhost:9000"));
                    ServerSocket server = new ServerSocket(port)) {
                Thread acceptor = new Thread(() -> acceptClients(server));
                acceptor.setDaemon(true);
                acceptor.start();

                Util.log(id, "READY",
                        "port=" + port + " resource=R sync=" + sync + " barrier=" + barrier
                                + " participants=" + participants + " clients=" + clients + " namespace=" + namespace
                                + " cs_min_ms=" + csMin + " cs_max_ms=" + csMax);

                Set<String> finished = new java.util.HashSet<>();

                while (finished.size() < clients) {
                    Work work = queue.take();

                    if (work instanceof Request request)
                        process(request, curator, lock, observer);
                    else if (work instanceof ClientFinished end) {
                        if (!finished.add(end.client()))
                            throw new IOException("BYE duplicado: " + end.client());

                        Util.log(id, "CLIENT_FINISHED",
                                "client=" + end.client() + " finished_clients=" + finished.size());
                    } else if (work instanceof ClientError error)
                        throw error.error();
                }

                if (!queue.isEmpty())
                    throw new IOException("Fila não esvaziou no encerramento");

                observer.send("DONE " + id);
                Util.log(id, "NODE_DONE",
                        "completed=" + round + " clients=" + finished.size() + " " + lock.counters()
                                + " observer_messages=" + observer.messages());
            } finally {
                for (Socket socket : connections)
                    socket.close();
            }
        }
    }

    // Essa função aplica a dupla barreira quando a opção --barrier está habilitada.
    // Cada rodada usa seu próprio caminho no ZooKeeper; enter() espera todos os nós chegarem.
    // Depois, cada nó adquire o mutex, simula o acesso a R e libera o mutex antes de leave().
    // COMMITTED confirma o fim do acesso individual; leave() espera todos terminarem a rodada.
    // Assim, o nó só processa o próximo pedido após a saída coletiva, sem manter R bloqueado.
    private void process(Request request, CuratorFramework curator, ResourceLock lock, ObsClient observer)
            throws Exception {
        round++;
        long started = System.nanoTime();
        String fields = "resource=R node=" + id + " client=" + request.client() + " request_id=" + request.id()
                + " round=" + round;
        Util.log(id, "REQUEST", fields + " local_queue_s=" + Util.seconds(started - request.receivedAt()));

        DistributedDoubleBarrier gate = null;

        if (barrier) {
            gate = new DistributedDoubleBarrier(curator, "/barriers/round-" + round, participants);
            observer.send("ARRIVE " + id + " " + round);
            Util.log(id, "BARRIER_WAIT", fields + " participants=" + participants);

            if (!gate.enter(120, TimeUnit.SECONDS))
                throw new IOException("Timeout na entrada da barreira");

            observer.send("PASS " + id + " " + round);
            Util.log(id, "BARRIER_PASSED", fields);
        }

        Util.log(id, sync ? "LOCK_WAIT" : "SYNC_DISABLED", fields + " path=/" + curator.getNamespace() + "/locks/R");
        ResourceLock.Acquisition acquisition = lock.acquire("R", request.id());

        try {
            String queueNodes =
                    acquisition.queue().stream().map(ResourceLock::shortNode).collect(Collectors.joining(","));
            Util.log(id, "ACQUIRE",
                    fields + " sync=" + sync + " wait_s=" + Util.seconds(acquisition.waitNanos()) + " znode="
                            + ResourceLock.shortNode(acquisition.znode()) + " queue_size=" + acquisition.queue().size()
                            + " queue=" + (queueNodes.isEmpty() ? "-" : queueNodes));
            observer.send("BEGIN " + id + " " + request.id() + " " + round + " "
                    + Metrics.number(acquisition.waitNanos() / 1e6));
            simulateCriticalSection(fields);
            observer.send("END " + id + " " + request.id() + " " + round);
        } finally {
            lock.release("R", request.id());
            Util.log(id, "RELEASE", fields + " sync=" + sync);
        }

        long total = System.nanoTime() - started;

        synchronized (request.out()) {
            request.out().println("COMMITTED " + request.id() + " node_wait_s=" + Util.seconds(acquisition.waitNanos())
                    + " node_total_s=" + Util.seconds(total));

            if (request.out().checkError())
                throw new IOException("Cliente desconectou antes de COMMITTED");
        }

        Util.log(id, "COMMITTED", fields + " total_s=" + Util.seconds(total));

        // O mutex já está livre. O próximo pedido só é processado depois de leave().
        if (gate != null) {
            if (!gate.leave(120, TimeUnit.SECONDS))
                throw new IOException("Timeout na saída da barreira");

            observer.send("LEAVE " + id + " " + round);
            Util.log(id, "BARRIER_LEFT", fields);
        }
    }

    private void simulateCriticalSection(String fields) throws InterruptedException {
        int sleep = csMin + random.nextInt(csMax - csMin + 1);
        long begin = System.nanoTime();
        Util.log(id, "ENTER", fields + " sleep_ms=" + sleep);

        Thread.sleep(sleep);

        Util.log(id, "EXIT", fields + " cs_s=" + Util.seconds(System.nanoTime() - begin));
    }

    private void acceptClients(ServerSocket server) {

        try {
            while (!server.isClosed()) {
                Socket socket = server.accept();
                socket.setTcpNoDelay(true);
                connections.add(socket);

                Thread reader = new Thread(() -> readClient(socket));
                reader.setDaemon(true);
                reader.start();
            }
        } catch (IOException e) {
            if (!server.isClosed())
                queue.offer(new ClientError(e));
        }
    }

    private void readClient(Socket socket) {
        try (socket; BufferedReader in =
                             new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8)) {
            String hello = in.readLine();
            if (hello == null || !hello.matches("HELLO c[1-9][0-9]*"))
                throw new IOException("Esperado HELLO client_id");

            String client = hello.split(" ")[1];

            synchronized (connectedClients) {
                if (connectedClients.size() >= clients || !connectedClients.add(client))
                    throw new IOException("Cliente extra ou duplicado: " + client);
            }

            out.println("READY " + id);
            String line;

            while ((line = in.readLine()) != null) {
                if (line.equals("BYE " + client)) {
                    queue.put(new ClientFinished(client));
                    return;
                }

                String[] fields = line.split(" ");
                if (fields.length != 2 || !fields[0].equals("REQUEST") || !Util.clientOf(fields[1]).equals(client))
                    throw new IOException("Pedido inválido: " + line);

                queue.put(new Request(client, fields[1], out, System.nanoTime()));
            }

            throw new IOException("Cliente desconectou sem BYE: " + client);
        } catch (Exception e) {
            queue.offer(new ClientError(e));
        } finally {
            connections.remove(socket);
        }
    }
}
