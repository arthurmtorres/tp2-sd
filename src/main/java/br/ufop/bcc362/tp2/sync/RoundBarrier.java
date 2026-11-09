package br.ufop.bcc362.tp2.sync;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class RoundBarrier {
    private final DistributedDoubleBarrier barrier;

    public RoundBarrier(CuratorFramework curator, String rootPath, int round, int participants) {
        if (round < 1 || participants < 2) {
            throw new IllegalArgumentException("rodada e participantes inválidos");
        }
        String path = Objects.requireNonNull(rootPath, "rootPath") + "/round-" + round;
        this.barrier = new DistributedDoubleBarrier(curator, path, participants);
    }

    public void enter(Duration timeout) throws Exception {
        if (!barrier.enter(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("timeout ao entrar na barreira dupla");
        }
    }

    public void leave(Duration timeout) throws Exception {
        if (!barrier.leave(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("timeout ao sair da barreira dupla");
        }
    }
}

