package tp2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** Servidor TCP do verificador; jamais espera active=0 para responder a BEGIN. */
public final class Observer {
    public static void main(String[] args) throws Exception {
        Map<String, String> options = Util.args(args);
        int nodes = Util.integer(options, "nodes", 5, 1, 100);
        int expected = Util.integer(options, "expected", -1, -1, 100000);
        int port = Util.integer(options, "port", 9000, 1, 65535);
        String label = options.getOrDefault("label", "manual");
        String seed = options.getOrDefault("seed", "1");
        Path csv = Path.of(options.getOrDefault("csv", "results/observer.csv"));

        ObserverState state = new ObserverState(nodes, expected, options.containsKey("barrier"),
                clientRequests(options.getOrDefault("client-requests", "")));

        AtomicBoolean finishing = new AtomicBoolean();

        try (ServerSocket server = new ServerSocket(port)) {
            Util.log("observer", "READY", "port=" + port + " nodes=" + nodes + " expected=" + expected);

            while (true) {
                Socket socket = server.accept();
                socket.setTcpNoDelay(true);

                Thread thread = new Thread(() -> handle(socket, state, finishing, label, seed, csv));
                thread.setDaemon(true);
                thread.start();
            }
        }
    }

    private static void handle(
            Socket socket, ObserverState state, AtomicBoolean finishing, String label, String seed, Path csv) {
        try (socket; BufferedReader in =
                             new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8)) {
            String line;

            while ((line = in.readLine()) != null) {
                boolean finish;

                // Registro e OK ficam na mesma ordem causal para todas as conexões.
                synchronized (state) {
                    state.accept(line);
                    out.println("OK");
                    finish = state.allDone() && finishing.compareAndSet(false, true);
                }

                if (finish) {
                    Map<String, String> summary = state.summary(label, seed);
                    Metrics.writeCsv(csv, summary);
                    Util.log("observer", "SUMMARY",
                            summary.entrySet()
                                    .stream()
                                    .map(e -> e.getKey() + "=" + e.getValue())
                                    .collect(Collectors.joining(" ")));
                    System.exit(state.exitCode());
                }
            }
        } catch (Exception e) {
            Util.log("observer", "ERROR", "type=" + e.getClass().getSimpleName() + " message=" + e.getMessage());
            System.exit(1);
        }
    }

    private static Map<String, Integer> clientRequests(String text) {
        Map<String, Integer> counts = new TreeMap<>();

        if (text.isBlank())

            return counts;

        for (String field : text.split(",")) {
            String[] pair = field.split(":");
            if (pair.length != 2)
                throw new IllegalArgumentException("Use --client-requests c1:10,c2:10");
            Util.id(pair[0], "c");

            int count = Integer.parseInt(pair[1]);
            if (count < 1 || counts.putIfAbsent(pair[0], count) != null)
                throw new IllegalArgumentException("Contagem de cliente inválida");
        }

        return counts;
    }
}
