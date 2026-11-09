package br.ufop.bcc362.tp2.sync;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
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

class CuratorRecipesIntegrationTest {
    private TestingServer server;
    private final List<CuratorFramework> clients = new ArrayList<>();

    @BeforeEach
    void startZooKeeper() throws Exception {
        server = new TestingServer();
    }

    @AfterEach
    void stopZooKeeper() throws Exception {
        clients.forEach(CuratorFramework::close);
        if (server != null) {
            server.close();
        }
    }

    @Test
    void mutexAllowsOnlyOneActiveProcess() throws Exception {
        int participantCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(participantCount);
        CountDownLatch ready = new CountDownLatch(participantCount);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int index = 0; index < participantCount; index++) {
            CuratorMutexSynchronizer mutex = new CuratorMutexSynchronizer(newClient(), "/tests/mutex");
            int request = index;
            futures.add(executor.submit(() -> {
                ready.countDown();
                start.await();
                try (DistributedSynchronizer.Lease ignored =
                             mutex.acquire("request-" + request, Duration.ofSeconds(10))) {
                    int current = active.incrementAndGet();
                    maximumActive.accumulateAndGet(current, Math::max);
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

        assertEquals(1, maximumActive.get());
        assertEquals(0, active.get());
    }

    @Test
    void doubleBarrierSynchronizesStartAndEndOfRound() throws Exception {
        int participantCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(participantCount);
        AtomicInteger entered = new AtomicInteger();
        AtomicInteger activityFinished = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int index = 0; index < participantCount; index++) {
            RoundBarrier barrier = new RoundBarrier(newClient(), "/tests/barriers", 1, participantCount);
            futures.add(executor.submit(() -> {
                barrier.enter(Duration.ofSeconds(10));
                entered.incrementAndGet();
                Thread.sleep(50);
                activityFinished.incrementAndGet();
                barrier.leave(Duration.ofSeconds(10));
                assertEquals(participantCount, activityFinished.get());
                return null;
            }));
        }

        for (Future<?> future : futures) {
            future.get(15, TimeUnit.SECONDS);
        }
        executor.shutdownNow();

        assertEquals(participantCount, entered.get());
        assertEquals(participantCount, activityFinished.get());
    }

    private CuratorFramework newClient() throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        clients.add(client);
        return client;
    }
}
