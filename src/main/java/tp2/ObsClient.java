package tp2;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Canal síncrono com o observador: send() só retorna depois que o observador
 * registrou o evento (resposta "OK"). Isso garante a ordem causal BEGIN/END/ARRIVE/PASS
 * sem comparar relógios físicos. O observador nunca bloqueia nem autoriza ninguém.
 */
final class ObsClient implements Closeable {
    private final Socket socket;
    private final PrintWriter out;
    private final BufferedReader in;

    ObsClient(String hostPort) throws IOException, InterruptedException {
        socket = Util.connect(hostPort, 60);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    synchronized void send(String line) throws IOException {
        out.println(line);
        String r = in.readLine();
        if (!"OK".equals(r)) throw new IOException("Observador respondeu: " + r);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
