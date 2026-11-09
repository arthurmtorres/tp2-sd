package br.ufop.bcc362.tp2;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class CuratorSync implements AutoCloseable {
    private static final String LOCK_PATH = "/tp2/resources/R/lock";
    private static final String BARRIER_ROOT = "/tp2/barriers/round-";

    private final CuratorFramework client;
    private final InterProcessMutex mutex;
    private final boolean lockEnabled;

    CuratorSync(String connectString, boolean lockEnabled, Duration connectionTimeout) throws Exception {
        this.lockEnabled = lockEnabled;
        client = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1_000, 5));
        mutex = new InterProcessMutex(client, LOCK_PATH);
        client.start();
        if (!client.blockUntilConnected(
                Math.toIntExact(connectionTimeout.toMillis()), TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("não foi possível conectar ao ZooKeeper");
        }
    }

    Lease acquire(String requestId, Duration timeout) throws Exception {
        if (!lockEnabled) {
            return () -> {
            };
        }
        if (!mutex.acquire(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("timeout ao adquirir mutex para " + requestId);
        }
        return mutex::release;
    }

    RoundBarrier barrier(int round, int participants) {
        return new RoundBarrier(new DistributedDoubleBarrier(
                client, BARRIER_ROOT + round, participants));
    }

    @Override
    public void close() {
        client.close();
    }

    interface Lease extends AutoCloseable {
        @Override
        void close() throws Exception;
    }

    static final class RoundBarrier {
        private final DistributedDoubleBarrier barrier;

        private RoundBarrier(DistributedDoubleBarrier barrier) {
            this.barrier = barrier;
        }

        void enter(Duration timeout) throws Exception {
            if (!barrier.enter(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("timeout ao entrar na barreira dupla");
            }
        }

        void leave(Duration timeout) throws Exception {
            if (!barrier.leave(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("timeout ao sair da barreira dupla");
            }
        }
    }
}
