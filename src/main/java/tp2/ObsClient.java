package tp2;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/** OK confirma somente o registro. O observador nunca concede autorização para R. */
final class ObsClient implements Closeable {
    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;
    private long messages;

    ObsClient(String address) throws IOException, InterruptedException {
        socket = Util.connect(address);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        out = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
    }

    synchronized void send(String line) throws IOException {
        out.println(line);

        if (out.checkError() || !"OK".equals(in.readLine()))
            throw new IOException("Observador não confirmou " + line);

        messages += 2; // evento + confirmação; não são mensagens da receita ZooKeeper
    }

    long messages() {
        return messages;
    }

    public void close() throws IOException {
        socket.close();
    }
}
