package br.ufop.bcc362.tp2;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CuratorSyncTest {
    private TestingServer server;
    private final List<CuratorSync> clients = new ArrayList<>();

    @BeforeEach
    void startZooKeeper() throws Exception {
        server = new TestingServer();
    }

    @AfterEach
    void stopZooKeeper() throws Exception {
        clients.forEach(CuratorSync::close);
        if (server != null) {
            server.close();
        }
    }

    @Test
    void mutexAllowsOnlyOneProcess() throws Exception {
        int count = 5;
        ExecutorService executor = Executors.newFixedThreadPool(count);
        CountDownLatch ready = new CountDownLatch(count);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int index = 0; index < count; index++) {
            CuratorSync sync = newClient();
            int request = index;
            futures.add(executor.submit(() -> {
                ready.countDown();
                start.await();
                try (CuratorSync.Lease ignored = sync.acquire(
                        "request-" + request, Duration.ofSeconds(10))) {
                    maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                    Thread.sleep(50);
                    active.decrementAndGet();
                }
                return null;
            }));
        }

        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        for (Future<?> future : futures) {
            future.get(15, TimeUnit.SECONDS);
        }
        executor.shutdownNow();
        assertEquals(1, maximum.get());
    }

    @Test
    void barrierSynchronizesEndOfRound() throws Exception {
        int count = 5;
        ExecutorService executor = Executors.newFixedThreadPool(count);
        AtomicInteger finished = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int index = 0; index < count; index++) {
            CuratorSync.RoundBarrier barrier = newClient().barrier(1, count);
            futures.add(executor.submit(() -> {
                barrier.enter(Duration.ofSeconds(10));
                Thread.sleep(50);
                finished.incrementAndGet();
                barrier.leave(Duration.ofSeconds(10));
                assertEquals(count, finished.get());
                return null;
            }));
        }

        for (Future<?> future : futures) {
            future.get(15, TimeUnit.SECONDS);
        }
        executor.shutdownNow();
    }

    private CuratorSync newClient() throws Exception {
        CuratorSync client = new CuratorSync(server.getConnectString(), true, Duration.ofSeconds(5));
        clients.add(client);
        return client;
    }
}

