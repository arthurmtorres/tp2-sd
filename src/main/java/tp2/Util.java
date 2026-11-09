package tp2;

import java.io.IOException;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/** Utilitários: log padronizado, parse de argumentos e conexão com retentativas. */
final class Util {
    private static final DateTimeFormatter F = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Util() {}

    static synchronized void log(String who, String event, String details) {
        System.out.println(LocalTime.now().format(F) + " " + who + " " + event
                + (details.isEmpty() ? "" : " " + details));
        System.out.flush();
    }

    /** "--chave valor" ou "--flag". */
    static Map<String, String> parseArgs(String[] a) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < a.length; i++) {
            if (!a[i].startsWith("--")) continue;
            String k = a[i].substring(2);
            if (i + 1 < a.length && !a[i + 1].startsWith("--")) m.put(k, a[++i]);
            else m.put(k, "true");
        }
        return m;
    }

    static Socket connect(String hostPort, int tries) throws IOException, InterruptedException {
        int c = hostPort.lastIndexOf(':');
        String host = hostPort.substring(0, c);
        int port = Integer.parseInt(hostPort.substring(c + 1));
        IOException last = null;
        for (int i = 0; i < tries; i++) {
            try {
                return new Socket(host, port);
            } catch (IOException e) {
                last = e;
                Thread.sleep(500);
            }
        }
        throw last;
    }
}
