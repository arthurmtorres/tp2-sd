package br.ufop.bcc362.tp2.sync;

import java.time.Duration;

public final class NoOpSynchronizer implements DistributedSynchronizer {
    @Override
    public Lease acquire(String requestId, Duration timeout) {
        return () -> {
        };
    }
}

