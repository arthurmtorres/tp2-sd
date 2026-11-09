package br.ufop.bcc362.tp2.sync;

import java.time.Duration;

public interface DistributedSynchronizer {
    Lease acquire(String requestId, Duration timeout) throws Exception;

    static DistributedSynchronizer disabled() {
        return (requestId, timeout) -> () -> {
        };
    }

    interface Lease extends AutoCloseable {
        @Override
        void close() throws Exception;
    }
}
