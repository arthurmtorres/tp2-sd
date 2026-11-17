package tp2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Estado do verificador. Contagens medem ocupação; nenhuma delas autoriza acesso. */
final class ObserverState {
    private record Access(String node, String client, int round, double waitMs) {}

    private static final class Round {
        final Set<String> arrived = new HashSet<>(), passed = new HashSet<>();
        final Set<String> finished = new HashSet<>(), left = new HashSet<>();
    }

    private final int participants, expected;
    private final boolean barrier;
    private final Map<String, Integer> clientRequests;
    private final Map<String, Access> requests = new HashMap<>(), active = new HashMap<>();
    private final Set<String> ended = new HashSet<>(), done = new HashSet<>();
    private final Map<String, Integer> lastRound = new HashMap<>(), perClient = new TreeMap<>();
    private final Map<Integer, Round> rounds = new HashMap<>();
    private final List<Double> waits = new ArrayList<>();

    private long seq, incoming, first = -1, last = -1;
    private int maxActive, overlaps, barrierViolations, invalidEvents;

    ObserverState(int participants, int expected, boolean barrier, Map<String, Integer> clientRequests) {
        this.participants = participants;
        this.expected = expected;
        this.barrier = barrier;
        this.clientRequests = Map.copyOf(clientRequests);
    }

    /** Mesmo uma sobreposição recebe OK: registrar um evento não concede um lock. */
    synchronized void accept(String line) {
        incoming++;
        seq++;

        try {
            String[] p = line.trim().split("\\s+");
            if (p.length < 2)
                throw new IllegalArgumentException("evento_incompleto");

            String node = p[1];
            Util.id(node, "n");
            if (Integer.parseInt(node.substring(1)) > participants)
                throw new IllegalArgumentException("no_desconhecido");
            if (done.contains(node))
                throw new IllegalArgumentException("evento_apos_DONE");

            switch (p[0]) {
                case "BEGIN" -> begin(p, node);
                case "END" -> end(p, node);
                case "ARRIVE", "PASS", "LEAVE" -> barrierEvent(p, node);
                case "DONE" -> {
                    length(p, 2);
                    done.add(node);
                    emit("DONE", "node=" + node + " done_nodes=" + done.size() + " active=" + active.size());
                }

                default -> throw new IllegalArgumentException("evento_desconhecido");
            }
        } catch (IllegalArgumentException e) {
            invalidEvents++;
            emit("INVALID_EVENT", "reason=" + e.getMessage().replace(' ', '_') + " input=" + line.replace(' ', '|'));
        }
    }

    private void begin(String[] p, String node) {
        length(p, 5);
        String request = p[2], client = Util.clientOf(request);
        int round = positive(p[3]);
        double waitMs = Double.parseDouble(p[4]);

        if (!Double.isFinite(waitMs) || waitMs < 0)
            throw new IllegalArgumentException("espera_invalida");
        if (requests.containsKey(request))
            throw new IllegalArgumentException("BEGIN_duplicado");
        if (round != lastRound.getOrDefault(node, 0) + 1)
            throw new IllegalArgumentException("rodada_do_no_invalida");
        if (active.values().stream().anyMatch(a -> a.node().equals(node)))
            throw new IllegalArgumentException("dois_pedidos_no_mesmo_worker");

        if (!clientRequests.isEmpty()) {
            int number = Integer.parseInt(request.substring(request.lastIndexOf('-') + 1));
            if (number > clientRequests.getOrDefault(client, 0))
                throw new IllegalArgumentException("pedido_nao_previsto");
        }

        if (barrier) {
            Round state = round(round);
            if (state.arrived.size() != participants || !state.passed.contains(node))
                barrierViolation("BEGIN_antes_da_barreira", node, round);
        }

        Access access = new Access(node, client, round, waitMs);
        requests.put(request, access);
        active.put(request, access);
        lastRound.put(node, round);
        waits.add(waitMs);

        if (first < 0)
            first = System.nanoTime();

        maxActive = Math.max(maxActive, active.size());
        boolean overlap = active.size() > 1;

        if (overlap)
            overlaps++;

        emit("BEGIN",
                fields(node, request, round) + " active=" + active.size() + " overlap=" + overlap
                        + " wait_ms=" + Metrics.number(waitMs));

        if (overlap)
            emit("VIOLATION_OVERLAP", fields(node, request, round) + " active=" + active.size());
    }

    private void end(String[] p, String node) {
        length(p, 4);
        String request = p[2];
        int round = positive(p[3]);
        Access access = active.get(request);

        if (access == null)
            throw new IllegalArgumentException("END_sem_BEGIN_ativo");
        if (!access.node().equals(node) || access.round() != round)
            throw new IllegalArgumentException("END_no_ou_rodada_incorretos");

        active.remove(request);
        ended.add(request);
        perClient.merge(access.client(), 1, Integer::sum);
        last = System.nanoTime();

        if (barrier)
            round(round).finished.add(node);

        emit("END", fields(node, request, round) + " active=" + active.size() + " completed=" + ended.size());
    }

    private void barrierEvent(String[] p, String node) {
        length(p, 3);
        if (!barrier)
            throw new IllegalArgumentException("barreira_desabilitada");

        int number = positive(p[2]);
        Round current = round(number);

        switch (p[0]) {
            case "ARRIVE" -> {
                if (current.arrived.contains(node))
                    throw new IllegalArgumentException("ARRIVE_duplicado");
                if (number != lastRound.getOrDefault(node, 0) + 1)
                    throw new IllegalArgumentException("ARRIVE_rodada_invalida");
                if (number > 1
                        && (round(number - 1).finished.size() != participants
                                || !round(number - 1).left.contains(node)))
                    barrierViolation("proxima_rodada_antecipada", node, number);

                current.arrived.add(node);
            }

            case "PASS" -> {
                if (!current.arrived.contains(node) || !current.passed.add(node))
                    throw new IllegalArgumentException("PASS_sem_ARRIVE_ou_duplicado");
                if (current.arrived.size() != participants)
                    barrierViolation("PASS_antes_de_todos", node, number);
            }

            case "LEAVE" -> {
                if (!current.finished.contains(node) || !current.left.add(node))
                    throw new IllegalArgumentException("LEAVE_sem_END_ou_duplicado");
                if (current.finished.size() != participants)
                    barrierViolation("LEAVE_antes_de_todos", node, number);
            }

            default -> throw new IllegalArgumentException("evento_de_barreira_invalido");
        }

        emit(p[0],
                "node=" + node + " round=" + number + " arrived=" + current.arrived.size()
                        + " finished=" + current.finished.size() + " left=" + current.left.size());
    }

    private void barrierViolation(String reason, String node, int round) {
        barrierViolations++;
        emit("VIOLATION_BARRIER", "node=" + node + " round=" + round + " reason=" + reason);
    }

    private Round round(int number) {
        return rounds.computeIfAbsent(number, ignored -> new Round());
    }

    private void emit(String event, String fields) {
        Util.log("observer", event, "seq=" + seq + " resource=R " + fields);
    }

    private static String fields(String node, String request, int round) {
        return "node=" + node + " client=" + Util.clientOf(request) + " request_id=" + request + " round=" + round;
    }

    private static int positive(String value) {
        int number = Integer.parseInt(value);
        if (number < 1)
            throw new IllegalArgumentException("rodada_invalida");

        return number;
    }

    private static void length(String[] p, int expected) {
        if (p.length != expected)
            throw new IllegalArgumentException("numero_de_campos_invalido");
    }

    synchronized boolean allDone() {
        return done.size() == participants;
    }

    synchronized int exitCode() {
        if (invalidEvents > 0)
            return 4;

        boolean incomplete = !allDone() || !active.isEmpty() || requests.size() != ended.size()
                || expected >= 0 && ended.size() != expected;

        if (!clientRequests.isEmpty() && !perClient.equals(clientRequests))
            incomplete = true;
        if (barrier
                && rounds.values().stream().anyMatch(r
                        -> r.arrived.size() != participants || r.passed.size() != participants
                                || r.finished.size() != participants || r.left.size() != participants))
            incomplete = true;

        if (incomplete)
            return 3;

        return overlaps + barrierViolations > 0 ? 2 : 0;
    }

    synchronized Map<String, String> summary(String label, String seed) {
        Map<String, String> result = new LinkedHashMap<>();
        int code = exitCode();
        double total = first < 0 || last < 0 ? 0 : (last - first) / 1e9;

        String status = switch (code) {
            case 0 -> "OK";
            case 2 -> "VIOLACAO";
            case 3 -> "INCOMPLETO";
            default -> "INVALIDO";
        };

        result.put("scenario", label);
        result.put("seed", seed);
        result.put("nodes", "" + participants);
        result.put("expected", "" + expected);
        result.put("completed", "" + ended.size());
        result.put("active", "" + active.size());

        result.put("total_s", Metrics.number(total));
        result.put("median_wait_ms", Metrics.number(Metrics.median(waits)));
        result.put("p95_wait_ms", Metrics.number(Metrics.percentile(waits, .95)));
        result.put("throughput_per_s", Metrics.number(total > 0 ? ended.size() / total : 0));

        result.put("max_active", "" + maxActive);
        result.put("overlaps", "" + overlaps);
        result.put("barrier_violations", "" + barrierViolations);
        result.put("invalid_events", "" + invalidEvents);
        result.put("done_nodes", "" + done.size());
        result.put("observer_messages", "" + (2 * incoming));
        result.put("status", status);

        result.put("per_client",
                perClient.entrySet()
                        .stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(";")));

        return result;
    }
}
