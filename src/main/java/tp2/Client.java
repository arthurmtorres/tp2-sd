package tp2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

/** Um cliente conhece apenas seu SyncNode e aguarda COMMITTED a cada pedido. */
public final class Client {
    public static void main(String[] args) throws Exception {
        Map<String, String> options = Util.args(args);
        String id = Util.id(Util.required(options, "id"), "c");
        String node = Util.required(options, "node");

        int count = Util.integer(options, "requests", 10, options.containsKey("demo") ? 1 : 10, 50);
        int thinkMin = Util.integer(options, "think-min", 1000, 0, 5000);
        int thinkMax = Util.integer(options, "think-max", 5000, thinkMin, 5000);

        long seed = Long.parseLong(options.getOrDefault("seed", "1"));
        long startAt = Long.parseLong(options.getOrDefault("start-at", "0"));

        Random random = new Random(seed * 7919 + id.hashCode());
        long started = System.nanoTime();

        try (Socket socket = Util.connect(node); BufferedReader in = new BufferedReader(new InputStreamReader(
                        socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8)) {
            out.println("HELLO " + id);
            String ready = in.readLine();

            if (ready == null || !ready.startsWith("READY n"))
                throw new IOException("Handshake inválido: " + ready);

            String nodeId = ready.split(" ")[1];
            Util.log(id, "CONNECTED", "node=" + nodeId + " address=" + node + " requests=" + count);

            long delay = startAt - System.currentTimeMillis();

            if (delay > 0)
                Thread.sleep(delay);

            for (int number = 1; number <= count; number++) {
                String request = id + "-" + number;
                long begin = System.nanoTime();
                Util.log(id, "REQUEST", "resource=R node=" + nodeId + " client=" + id + " request_id=" + request);
                out.println("REQUEST " + request);

                if (out.checkError())
                    throw new IOException("Não foi possível enviar " + request);

                String response = in.readLine();

                if (response == null || !response.startsWith("COMMITTED " + request + " "))
                    throw new IOException("Resposta inválida para " + request + ": " + response);

                Util.log(id, "RECEIVED_COMMITTED",
                        "resource=R node=" + nodeId + " client=" + id + " request_id=" + request
                                + " latency_s=" + Util.seconds(System.nanoTime() - begin) + " "
                                + response.substring(("COMMITTED " + request + " ").length()));

                if (number < count && thinkMax > 0)
                    Thread.sleep(thinkMin + random.nextInt(thinkMax - thinkMin + 1));
            }

            out.println("BYE " + id);

            if (out.checkError())
                throw new IOException("Não foi possível encerrar o cliente");

            Util.log(id, "CLIENT_DONE",
                    "client=" + id + " completed=" + count + " total_s=" + Util.seconds(System.nanoTime() - started));
        }
    }
}
