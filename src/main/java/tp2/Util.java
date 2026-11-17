package tp2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Funções pequenas compartilhadas; não realizam sincronização distribuída. */
final class Util {
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Util() {
    }

    static synchronized void log(String actor, String event, String fields) {
        System.out.printf(Locale.ROOT, "%s %-9s %-20s %s%n", LocalTime.now().format(CLOCK), actor, event, fields);
        System.out.flush();
    }

    static String seconds(long nanos) {
        return String.format(Locale.ROOT, "%.6f", nanos / 1e9);
    }

    static Map<String, String> args(String[] args) {
        Map<String, String> result = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--"))
                throw new IllegalArgumentException("Argumento inválido: " + args[i]);

            String key = args[i].substring(2);
            String value = i + 1 < args.length && !args[i + 1].startsWith("--") ? args[++i] : "true";

            if (result.putIfAbsent(key, value) != null)
                throw new IllegalArgumentException("Opção repetida: " + key);
        }

        return result;
    }

    static String required(Map<String, String> args, String key) {
        String value = args.get(key);

        if (value == null || value.isBlank())
            throw new IllegalArgumentException("Falta --" + key);

        return value;
    }

    static int integer(Map<String, String> args, String key, int fallback, int min, int max) {
        int value = Integer.parseInt(args.getOrDefault(key, Integer.toString(fallback)));
        if (value < min || value > max)
            throw new IllegalArgumentException("--" + key + " deve estar entre " + min + " e " + max);

        return value;
    }

    static String id(String value, String prefix) {
        if (!value.matches(prefix + "[1-9][0-9]*"))
            throw new IllegalArgumentException("ID inválido: " + value);

        return value;
    }

    static String clientOf(String requestId) {
        if (!requestId.matches("c[1-9][0-9]*-[1-9][0-9]*"))
            throw new IllegalArgumentException("request_id inválido: " + requestId);

        return requestId.substring(0, requestId.lastIndexOf('-'));
    }

    static Socket connect(String address) throws IOException, InterruptedException {
        int colon = address.lastIndexOf(':');
        if (colon <= 0)
            throw new IllegalArgumentException("Use host:porta: " + address);

        String host = address.substring(0, colon);
        int port = Integer.parseInt(address.substring(colon + 1));
        IOException last = null;

        for (int attempt = 0; attempt < 60; attempt++) {
            Socket socket = new Socket();

            try {
                socket.connect(new InetSocketAddress(host, port), 1000);
                socket.setTcpNoDelay(true);

                return socket;
            } catch (IOException e) {
                last = e;
                socket.close();
                Thread.sleep(250);
            }
        }

        throw last;
    }
}
