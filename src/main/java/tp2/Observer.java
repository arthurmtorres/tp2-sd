package tp2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Observador/verificador. Apenas REGISTRA e ACUSA violações; não concede permissão
 * e não bloqueia ninguém (responde "OK" imediatamente após registrar).
 *
 * Eventos: BEGIN node req round waitMs | END node req round | ARRIVE node round
 *          PASS node round | DONE node
 *
 * Propriedades verificadas:
 *  - exclusão mútua: nunca mais de 1 acesso ativo a R (contador só mede);
 *  - barreira de entrada: ninguém passa/inicia a rodada r antes de todos chegarem;
 *  - barreira de saída: ninguém chega à rodada r+1 antes de todos terminarem a rodada r;
 *  - conclusão: nº de acessos concluídos == esperado.
 * Código de saída: 0 ok | 2 violação | 3 incompleto.
 */
public class Observer {
    private static final Object L = new Object();
    private static int n, expected, active, maxActive, overlaps, barrierViol, ends, doneNodes;
    private static boolean barrierMode;
    private static long first = -1, last = -1;
    private static final Map<Integer, Integer> arrive = new HashMap<>(), roundEnds = new HashMap<>();
    private static final Map<String, Integer> perClient = new TreeMap<>();
    private static final List<Long> waits = new ArrayList<>();
    private static String label, csv, seed;

    public static void main(String[] args) throws Exception {
        Map<String, String> o = Util.parseArgs(args);
        int port = Integer.parseInt(o.getOrDefault("port", "9000"));
        n = Integer.parseInt(o.get("nodes"));
        expected = Integer.parseInt(o.getOrDefault("expected", "-1"));
        barrierMode = o.containsKey("barrier");
        label = o.getOrDefault("label", "run");
        seed = o.getOrDefault("seed", "0");
        csv = o.getOrDefault("csv", "results/summary.csv");

        try (ServerSocket ss = new ServerSocket(port)) {
            Util.log("OBS", "LISTENING", "port=" + port + " nodes=" + n + " barrier=" + barrierMode);
            while (true) {
                Socket s = ss.accept();
                Thread t = new Thread(() -> handle(s));
                t.setDaemon(true);
                t.start();
            }
        }
    }

    private static void handle(Socket s) {
        try (Socket sock = s;
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
             PrintWriter out = new PrintWriter(sock.getOutputStream(), true)) {
            String line;
            while ((line = in.readLine()) != null) {
                boolean all = process(line);
                out.println("OK");
                if (all) finish();
            }
        } catch (Exception ignored) { /* conexão encerrada */ }
    }

    private static boolean process(String line) {
        String[] p = line.split(" ");
        synchronized (L) {
            long now = System.nanoTime();
            switch (p[0]) {
                case "BEGIN" -> {
                    String req = p[2];
                    int round = Integer.parseInt(p[3]);
                    if (first < 0) first = now;
                    active++;
                    maxActive = Math.max(maxActive, active);
                    if (active > 1) {
                        overlaps++;
                        Util.log("OBS", "VIOLATION_OVERLAP", "request_id=" + req + " active=" + active);
                    }
                    if (barrierMode && arrive.getOrDefault(round, 0) < n) {
                        barrierViol++;
                        Util.log("OBS", "VIOLATION_BARRIER_ENTRY", "BEGIN antes de todos chegarem, round=" + round);
                    }
                    waits.add(Long.parseLong(p[4]));
                    perClient.merge(req.substring(0, req.lastIndexOf('-')), 1, Integer::sum);
                    Util.log("OBS", "BEGIN", "node=" + p[1] + " request_id=" + req + " active=" + active);
                }
                case "END" -> {
                    active--;
                    ends++;
                    last = now;
                    roundEnds.merge(Integer.parseInt(p[3]), 1, Integer::sum);
                    Util.log("OBS", "END", "node=" + p[1] + " request_id=" + p[2] + " active=" + active);
                }
                case "ARRIVE" -> {
                    int r = Integer.parseInt(p[2]);
                    if (barrierMode && r > 1 && roundEnds.getOrDefault(r - 1, 0) < n) {
                        barrierViol++;
                        Util.log("OBS", "VIOLATION_BARRIER_EXIT", "node=" + p[1] + " avançou para round=" + r
                                + " com rodada anterior em execução");
                    }
                    arrive.merge(r, 1, Integer::sum);
                }
                case "PASS" -> {
                    int r = Integer.parseInt(p[2]);
                    if (arrive.getOrDefault(r, 0) < n) {
                        barrierViol++;
                        Util.log("OBS", "VIOLATION_BARRIER_ENTRY", "node=" + p[1] + " passou antes de todos, round=" + r);
                    }
                }
                case "DONE" -> doneNodes++;
                default -> Util.log("OBS", "UNKNOWN", line);
            }
            return p[0].equals("DONE") && doneNodes == n;
        }
    }

    private static void finish() {
        int code;
        String summary;
        synchronized (L) {
            Collections.sort(waits);
            double median = waits.isEmpty() ? 0 : waits.get(waits.size() / 2);
            double p95 = waits.isEmpty() ? 0 : waits.get((int) Math.min(waits.size() - 1, Math.floor(waits.size() * 0.95)));
            double total = first < 0 ? 0 : (last - first) / 1e9;
            StringBuilder pc = new StringBuilder();
            perClient.forEach((k, v) -> pc.append(pc.length() == 0 ? "" : ";").append(k).append('=').append(v));
            boolean incomplete = expected >= 0 && ends != expected;
            code = (overlaps + barrierViol > 0) ? 2 : (incomplete ? 3 : 0);
            String status = code == 0 ? "OK" : code == 2 ? "VIOLACAO" : "INCOMPLETO";

            Util.log("OBS", "SUMMARY", String.format(java.util.Locale.ROOT, 
                    "status=%s completed=%d expected=%d total_s=%.1f median_wait_ms=%.0f p95_wait_ms=%.0f "
                            + "max_active=%d overlaps=%d barrier_violations=%d per_client=%s",
                    status, ends, expected, total, median, p95, maxActive, overlaps, barrierViol, pc));
            summary = String.format(java.util.Locale.ROOT, "%s,%s,%d,%d,%d,%.1f,%.0f,%.0f,%.2f,%d,%d,%d,%s,%s",
                    label, seed, n, expected, ends, total, median, p95,
                    total > 0 ? ends / total : 0, maxActive, overlaps, barrierViol, status, pc);
        }
        try {
            File f = new File(csv);
            if (f.getParentFile() != null) f.getParentFile().mkdirs();
            boolean header = !f.exists() || f.length() == 0;
            try (FileWriter w = new FileWriter(f, true)) {
                if (header) w.write("scenario,seed,nodes,expected,completed,total_s,median_wait_ms,p95_wait_ms,"
                        + "throughput_per_s,max_active,overlaps,barrier_violations,status,per_client\n");
                w.write(summary + "\n");
            }
        } catch (Exception e) {
            Util.log("OBS", "CSV_ERROR", e.toString());
        }
        System.exit(code);
    }
}
