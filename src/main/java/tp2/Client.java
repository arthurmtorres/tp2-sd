package tp2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;
import java.util.Random;

/**
 * Cliente: conhece apenas UM nó do Cluster Sync. Envia pedidos de escrita em R
 * (request_id = clientId-seq), espera COMMITTED e aguarda um intervalo aleatório.
 */
public class Client {
    public static void main(String[] args) throws Exception {
        Map<String, String> o = Util.parseArgs(args);
        String id = o.get("id");
        String node = o.get("node");
        int requests = Integer.parseInt(o.getOrDefault("requests", "10"));
        long thinkMin = Long.parseLong(o.getOrDefault("think-min", "1000"));
        long thinkMax = Long.parseLong(o.getOrDefault("think-max", "5000"));
        long seed = Long.parseLong(o.getOrDefault("seed", "1"));
        long startAt = Long.parseLong(o.getOrDefault("start-at", "0"));
        Random rnd = new Random(seed * 7919 + id.hashCode());

        try (Socket s = Util.connect(node, 60);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {

            long wait = startAt - System.currentTimeMillis(); // disparo conjunto dos clientes
            if (wait > 0) Thread.sleep(wait);

            for (int k = 1; k <= requests; k++) {
                String rid = id + "-" + k;
                long t0 = System.nanoTime();
                Util.log(id, "REQUEST", "request_id=" + rid + " node=" + node);
                out.println("REQUEST " + rid);
                String resp = in.readLine();
                if (resp == null || !resp.equals("COMMITTED " + rid))
                    throw new IOException("Resposta inesperada para " + rid + ": " + resp);
                long ms = (System.nanoTime() - t0) / 1_000_000;
                Util.log(id, "COMMITTED", "request_id=" + rid + " latency_ms=" + ms);
                if (k < requests && thinkMax > 0) {
                    Thread.sleep(thinkMin + rnd.nextInt((int) (thinkMax - thinkMin + 1)));
                }
            }
            out.println("BYE");
        }
    }
}
