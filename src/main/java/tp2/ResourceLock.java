package tp2;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/** Coordenação de R, separada da simulação. Todas as chamadas vêm do worker do nó. */
final class ResourceLock {
    record Acquisition(long waitNanos, String znode, List<String> queue) {}

    private final IdentifiedMutex mutex;
    private final boolean enabled;
    private String currentRequest;
    private long acquireCalls, releaseCalls, snapshotReads;

    ResourceLock(CuratorFramework client, boolean enabled) throws Exception {

        if (enabled) {
            // A raiz do recurso permanece durante a execução. Se fosse CONTAINER,
            // sua limpeza automática poderia reiniciar a sequência após um período ocioso.

            try {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/locks/R");
            } catch (KeeperException.NodeExistsException ignored) {
                // Outro nó já preparou o mesmo recurso.
            }
        }

        this.mutex = new IdentifiedMutex(client);
        this.enabled = enabled;
    }

    Acquisition acquire(String resource, String requestId) throws Exception {
        checkResource(resource);

        if (currentRequest != null)
            throw new IllegalStateException("Já existe um pedido ativo neste worker");

        if (!enabled) {
            currentRequest = requestId;

            return new Acquisition(0, "-", List.of());
        }

        mutex.requestId = requestId;
        long start = System.nanoTime();
        acquireCalls++;

        if (!mutex.acquire(120, TimeUnit.SECONDS))
            throw new IllegalStateException("Timeout no mutex: " + requestId);

        long wait = System.nanoTime() - start;
        currentRequest = requestId;

        try {
            snapshotReads++;
            List<String> queue = List.copyOf(mutex.getParticipantNodes());

            return new Acquisition(wait, mutex.currentPath(), queue);
        } catch (Exception e) {
            release(resource, requestId);
            throw e;
        }
    }

    void release(String resource, String requestId) throws Exception {
        checkResource(resource);

        if (!requestId.equals(currentRequest))
            throw new IllegalStateException("Release não corresponde ao acquire");

        if (enabled) {
            mutex.release();
            releaseCalls++;
        }

        currentRequest = null;
    }

    String counters() {
        return "recipe_acquire_calls=" + acquireCalls + " recipe_release_calls=" + releaseCalls
                + " evidence_zk_reads=" + snapshotReads;
    }

    static String shortNode(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private static void checkResource(String resource) {
        if (!"R".equals(resource))
            throw new IllegalArgumentException("Este cenário usa apenas o recurso R");
    }

    /** Torna a associação request_id→znode verificável, sem reimplementar a receita. */
    private static final class IdentifiedMutex extends InterProcessMutex {
        private String requestId;

        IdentifiedMutex(CuratorFramework client) {
            super(client, "/locks/R");
        }

        protected byte[] getLockNodeBytes() {
            return requestId.getBytes(StandardCharsets.UTF_8);
        }

        String currentPath() {
            return getLockPath();
        }
    }
}
