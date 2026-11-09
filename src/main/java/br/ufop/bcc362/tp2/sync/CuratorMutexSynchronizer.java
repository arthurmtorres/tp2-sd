package br.ufop.bcc362.tp2.sync;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class CuratorMutexSynchronizer implements DistributedSynchronizer {
    private final InterProcessMutex mutex;

    public CuratorMutexSynchronizer(CuratorFramework curator, String lockPath) {
        this.mutex = new InterProcessMutex(
                Objects.requireNonNull(curator, "curator"),
                Objects.requireNonNull(lockPath, "lockPath")
        );
    }

    @Override
    public Lease acquire(String requestId, Duration timeout) throws Exception {
        boolean acquired = mutex.acquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!acquired) {
            throw new TimeoutException("timeout ao adquirir mutex para " + requestId);
        }
        return mutex::release;
    }
}

