package tp2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Processo do Cluster Sync.
 *
 * Sequência por pedido (modo --barrier):
 *   barreira.enter -> mutex.acquire -> acesso a R (sleep) -> mutex.release -> COMMITTED -> barreira.leave
 * O mutex nunca é mantido enquanto se espera na barreira (evita deadlock).
 *
 * Flags: --no-sync (desabilita o mutex, para provar que o observador detecta sobreposição)
 *        --barrier (habilita a barreira dupla por rodada)
 */
public class SyncNode {
    private record Req(String id, PrintWriter out) {}

    private static final Req POISON = new Req("__BYE__", null);
    private static final long TIMEOUT_S = 120;

    public static void main(String[] args) throws Exception {
        Map<String, String> o = Util.parseArgs(args);
        final String id = o.get("id");
        final int port = Integer.parseInt(o.get("port"));
        final String zk = o.getOrDefault("zk", "localhost:2181");
        final String ns = o.getOrDefault("ns", "tp2");
        final int participants = Integer.parseInt(o.getOrDefault("participants", "5"));
        final boolean sync = !o.containsKey("no-sync");
        final boolean barrier = o.containsKey("barrier");
        final long seed = Long.parseLong(o.getOrDefault("seed", "1"));
        final Random rnd = new Random(seed * 1000 + id.hashCode());

        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(zk)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .namespace(ns) // isola cada execução no ZooKeeper
                .build();
        cf.getConnectionStateListenable().addListener((c, st) -> Util.log(id, "ZK_STATE", st.name()));
        cf.start();
        if (!cf.blockUntilConnected(30, TimeUnit.SECONDS)) throw new IllegalStateException("ZooKeeper indisponível");

        // Todos os nós usam o MESMO caminho de lock para R.
        InterProcessMutex mutex = new InterProcessMutex(cf, "/locks/R");
        ObsClient obs = new ObsClient(o.getOrDefault("observer", "localhost:9000"));

        // Fila FIFO explícita: pedidos simultâneos recebidos pelo nó são serializados.
        BlockingQueue<Req> queue = new LinkedBlockingQueue<>();
        ServerSocket server = new ServerSocket(port);
        Thread acceptor = new Thread(() -> {
            try {
                while (true) {
                    Socket s = server.accept();
                    Thread t = new Thread(() -> readClient(s, queue));
                    t.setDaemon(true);
                    t.start();
                }
            } catch (Exception ignored) { /* servidor encerrado */ }
        });
        acceptor.setDaemon(true);
        acceptor.start();
        Util.log(id, "READY", "port=" + port + " sync=" + sync + " barrier=" + barrier + " participants=" + participants);

        int round = 0;
        while (true) {
            Req r = queue.take();
            if (r == POISON) break;
            round++;
            Util.log(id, "REQUEST", "request_id=" + r.id() + " round=" + round);

            DistributedDoubleBarrier bar = null;
            if (barrier) {
                // Um caminho por rodada: identifica a fase e evita misturar rodadas.
                bar = new DistributedDoubleBarrier(cf, "/barriers/round-" + round, participants);
                obs.send("ARRIVE " + id + " " + round);
                Util.log(id, "BARRIER_ENTER_WAIT", "round=" + round);
                if (!bar.enter(TIMEOUT_S, TimeUnit.SECONDS))
                    throw new IllegalStateException("Timeout na entrada da barreira, rodada " + round);
                obs.send("PASS " + id + " " + round);
                Util.log(id, "BARRIER_PASSED", "round=" + round);
            }

            long t0 = System.nanoTime();
            boolean locked = false;
            if (sync) {
                if (!mutex.acquire(TIMEOUT_S, TimeUnit.SECONDS))
                    throw new IllegalStateException("Timeout no mutex para " + r.id());
                locked = true;
            }
            try {
                long waitMs = (System.nanoTime() - t0) / 1_000_000; // relógio monotônico local
                String why = "";
                if (sync) { // evidência de por que este nó entrou: ele é o primeiro da fila de znodes
                    Collection<String> q = new ArrayList<>(mutex.getParticipantNodes());
                    String head = q.isEmpty() ? "?" : q.iterator().next();
                    why = " holder_znode=" + head.substring(head.lastIndexOf('/') + 1) + " queue_size=" + q.size();
                }
                Util.log(id, "ACQUIRE", "request_id=" + r.id() + " wait_ms=" + waitMs + why);
                obs.send("BEGIN " + id + " " + r.id() + " " + round + " " + waitMs);
                Util.log(id, "ENTER", "request_id=" + r.id());
                Thread.sleep(200 + rnd.nextInt(801)); // seção crítica simulada: 0,2 a 1 s
                Util.log(id, "EXIT", "request_id=" + r.id());
                obs.send("END " + id + " " + r.id() + " " + round);
            } finally {
                if (locked) { // liberação garantida mesmo com exceção
                    mutex.release();
                    Util.log(id, "RELEASE", "request_id=" + r.id());
                }
            }

            synchronized (r.out()) { r.out().println("COMMITTED " + r.id()); }
            Util.log(id, "COMMITTED", "request_id=" + r.id());

            if (barrier) {
                obs.send("LEAVE_READY " + id + " " + round);
                if (!bar.leave(TIMEOUT_S, TimeUnit.SECONDS))
                    throw new IllegalStateException("Timeout na saída da barreira, rodada " + round);
                obs.send("LEFT " + id + " " + round);
                Util.log(id, "BARRIER_LEFT", "round=" + round);
            }
        }

        obs.send("DONE " + id);
        Util.log(id, "DONE", "rounds=" + round);
        obs.close();
        server.close();
        cf.close();
    }

    private static void readClient(Socket s, BlockingQueue<Req> queue) {
        try (Socket sock = s;
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] p = line.split(" ");
                if (p[0].equals("REQUEST")) queue.put(new Req(p[1], out));
                else if (p[0].equals("BYE")) { queue.put(POISON); break; }
            }
        } catch (Exception e) { Util.log("SYNC", "CLIENT_ERROR", e.toString()); }
    }
}
