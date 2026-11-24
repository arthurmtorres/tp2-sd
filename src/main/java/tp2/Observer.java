package tp2;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
 *
 * Painel web (somente leitura): http://localhost:<web-port> (padrão 8080; 0 desliga).
 *  --web-port N   ou variável OBS_WEB_PORT
 *  --hold S       ou variável OBS_HOLD_S  (segundos que o painel continua no ar após o fim)
 *  --zk h:p,h:p   ou variável ZK          (servidores do ensemble a exibir no painel)
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

    // --- estado exibido no painel (protegido por L) ---
    private static final DateTimeFormatter TF = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final ArrayDeque<String> events = new ArrayDeque<>();
    private static final Map<String, String> nodeState = new TreeMap<>();
    private static final LinkedHashSet<String> holders = new LinkedHashSet<>();
    private static long evSeq;
    private static boolean finished;
    private static String finalStatus = "";
    private static int holdS;
    private static volatile String zkJson = "[]";

    public static void main(String[] args) throws Exception {
        Map<String, String> o = Util.parseArgs(args);
        int port = Integer.parseInt(o.getOrDefault("port", "9000"));
        n = Integer.parseInt(o.get("nodes"));
        expected = Integer.parseInt(o.getOrDefault("expected", "-1"));
        barrierMode = o.containsKey("barrier");
        label = o.getOrDefault("label", "run");
        seed = o.getOrDefault("seed", "0");
        csv = o.getOrDefault("csv", "results/summary.csv");

        int webPort = Integer.parseInt(o.getOrDefault("web-port", env("OBS_WEB_PORT", "8080")));
        holdS = Integer.parseInt(o.getOrDefault("hold", env("OBS_HOLD_S", "0")));
        String zkAddrs = o.getOrDefault("zk", env("ZK", "localhost:2181"));
        for (int i = 1; i <= n; i++) nodeState.put("n" + i, "aguardando"); // ids n1..nN (ver scripts)
        if (webPort > 0) {
            startZkPoller(zkAddrs);
            startWeb(webPort);
        }

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

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
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

    /** Escreve no log e guarda para o painel. Chamar sempre dentro de synchronized (L). */
    private static void ev(String type, String text) {
        Util.log("OBS", type, text);
        events.addLast("{\"i\":" + (++evSeq) + ",\"t\":\"" + LocalTime.now().format(TF)
                + "\",\"type\":\"" + esc(type) + "\",\"text\":\"" + esc(text) + "\"}");
        while (events.size() > 150) events.removeFirst();
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
                        ev("VIOLATION_OVERLAP", "request_id=" + req + " active=" + active);
                    }
                    if (barrierMode && arrive.getOrDefault(round, 0) < n) {
                        barrierViol++;
                        ev("VIOLATION_BARRIER_ENTRY", "BEGIN antes de todos chegarem, round=" + round);
                    }
                    waits.add(Long.parseLong(p[4]));
                    perClient.merge(req.substring(0, req.lastIndexOf('-')), 1, Integer::sum);
                    holders.add(p[1] + " " + req);
                    nodeState.put(p[1], "usando R (" + req + ")");
                    ev("BEGIN", "node=" + p[1] + " request_id=" + req + " wait_ms=" + p[4] + " active=" + active);
                }
                case "END" -> {
                    active--;
                    ends++;
                    last = now;
                    roundEnds.merge(Integer.parseInt(p[3]), 1, Integer::sum);
                    holders.remove(p[1] + " " + p[2]);
                    nodeState.put(p[1], "livre (concluiu " + p[2] + ")");
                    ev("END", "node=" + p[1] + " request_id=" + p[2] + " active=" + active);
                }
                case "ARRIVE" -> {
                    int r = Integer.parseInt(p[2]);
                    if (barrierMode && r > 1 && roundEnds.getOrDefault(r - 1, 0) < n) {
                        barrierViol++;
                        ev("VIOLATION_BARRIER_EXIT", "node=" + p[1] + " avançou para round=" + r
                                + " com rodada anterior em execução");
                    }
                    arrive.merge(r, 1, Integer::sum);
                    nodeState.put(p[1], "esperando na barreira (rodada " + r + ")");
                    ev("ARRIVE", "node=" + p[1] + " round=" + r);
                }
                case "PASS" -> {
                    int r = Integer.parseInt(p[2]);
                    if (arrive.getOrDefault(r, 0) < n) {
                        barrierViol++;
                        ev("VIOLATION_BARRIER_ENTRY", "node=" + p[1] + " passou antes de todos, round=" + r);
                    }
                    nodeState.put(p[1], "passou a barreira (rodada " + r + ")");
                    ev("PASS", "node=" + p[1] + " round=" + r);
                }
                case "DONE" -> {
                    doneNodes++;
                    nodeState.put(p[1], "finalizado");
                    ev("DONE", "node=" + p[1]);
                }
                default -> ev("UNKNOWN", line);
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

            ev("SUMMARY", String.format(java.util.Locale.ROOT,
                    "status=%s completed=%d expected=%d total_s=%.1f median_wait_ms=%.0f p95_wait_ms=%.0f "
                            + "max_active=%d overlaps=%d barrier_violations=%d per_client=%s",
                    status, ends, expected, total, median, p95, maxActive, overlaps, barrierViol, pc));
            summary = String.format(java.util.Locale.ROOT, "%s,%s,%d,%d,%d,%.1f,%.0f,%.0f,%.2f,%d,%d,%d,%s,%s",
                    label, seed, n, expected, ends, total, median, p95,
                    total > 0 ? ends / total : 0, maxActive, overlaps, barrierViol, status, pc);
            finalStatus = status;
            finished = true;
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
        if (holdS > 0) { // mantém o painel no ar para a apresentação
            Util.log("OBS", "HOLD", "painel no ar por " + holdS + "s (Ctrl+C encerra antes)");
            try { Thread.sleep(holdS * 1000L); } catch (InterruptedException ignored) { }
        }
        System.exit(code);
    }

    // ------------------------------------------------------------------ painel web

    private static String esc(String s) {
        StringBuilder b = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') b.append('\\').append(c);
            else if (c < 0x20) b.append(' ');
            else b.append(c);
        }
        return b.toString();
    }

    private static String stateJson() {
        synchronized (L) {
            StringBuilder b = new StringBuilder("{");
            b.append("\"label\":\"").append(esc(label)).append("\",\"seed\":\"").append(esc(seed)).append('"');
            b.append(",\"nodes\":").append(n).append(",\"expected\":").append(expected);
            b.append(",\"completed\":").append(ends).append(",\"active\":").append(active);
            b.append(",\"maxActive\":").append(maxActive).append(",\"overlaps\":").append(overlaps);
            b.append(",\"barrierViol\":").append(barrierViol).append(",\"barrier\":").append(barrierMode);
            b.append(",\"finished\":").append(finished).append(",\"status\":\"").append(finalStatus).append('"');
            b.append(",\"holders\":[");
            boolean f = true;
            for (String h : holders) { b.append(f ? "" : ",").append('"').append(esc(h)).append('"'); f = false; }
            b.append("],\"nodeState\":{");
            f = true;
            for (var e : nodeState.entrySet()) {
                b.append(f ? "" : ",").append('"').append(esc(e.getKey())).append("\":\"").append(esc(e.getValue())).append('"');
                f = false;
            }
            b.append("},\"perClient\":{");
            f = true;
            for (var e : perClient.entrySet()) {
                b.append(f ? "" : ",").append('"').append(esc(e.getKey())).append("\":").append(e.getValue());
                f = false;
            }
            b.append("},\"zk\":").append(zkJson);
            b.append(",\"events\":[").append(String.join(",", events)).append("]}");
            return b.toString();
        }
    }

    private static void startWeb(int port) {
        try {
            HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);
            http.createContext("/api/state", ex -> reply(ex, 200, "application/json; charset=utf-8", stateJson()));
            http.createContext("/", ex -> {
                if (ex.getRequestURI().getPath().equals("/")) reply(ex, 200, "text/html; charset=utf-8", Dashboard.HTML);
                else reply(ex, 404, "text/plain; charset=utf-8", "não encontrado");
            });
            http.start();
            Util.log("OBS", "DASHBOARD", "http://localhost:" + port);
        } catch (Exception e) { // porta ocupada etc.: o teste continua, só sem painel
            Util.log("OBS", "DASHBOARD_ERROR", e.toString());
        }
    }

    private static void reply(HttpExchange ex, int status, String type, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(status, b.length);
        try (var os = ex.getResponseBody()) { os.write(b); }
    }

    /** A cada segundo pergunta a cada servidor do ensemble (comando de quatro letras "mntr"). */
    private static void startZkPoller(String addrs) {
        Thread t = new Thread(() -> {
            while (true) {
                StringBuilder b = new StringBuilder("[");
                for (String a : addrs.split(",")) {
                    a = a.trim();
                    if (a.isEmpty()) continue;
                    if (b.length() > 1) b.append(',');
                    b.append(queryZk(a));
                }
                zkJson = b.append(']').toString();
                try { Thread.sleep(1000); } catch (InterruptedException e) { return; }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private static String queryZk(String addr) {
        int c = addr.lastIndexOf(':');
        String host = c < 0 ? addr : addr.substring(0, c);
        int port = c < 0 ? 2181 : Integer.parseInt(addr.substring(c + 1));
        Map<String, String> m = new HashMap<>();
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), 400);
            s.setSoTimeout(800);
            s.getOutputStream().write("mntr".getBytes(StandardCharsets.US_ASCII));
            s.getOutputStream().flush();
            BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            String l;
            while ((l = r.readLine()) != null) {
                int i = l.indexOf('\t');
                if (i > 0) m.put(l.substring(0, i), l.substring(i + 1));
            }
        } catch (Exception ignored) { /* servidor fora do ar */ }
        String mode = m.get("zk_server_state");
        return "{\"addr\":\"" + esc(addr) + "\",\"up\":" + (mode != null)
                + ",\"mode\":\"" + (mode == null ? "" : esc(mode))
                + "\",\"znodes\":\"" + esc(m.getOrDefault("zk_znode_count", "-"))
                + "\",\"conns\":\"" + esc(m.getOrDefault("zk_num_alive_connections", "-")) + "\"}";
    }
}
