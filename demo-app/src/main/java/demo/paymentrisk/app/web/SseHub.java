package demo.paymentrisk.app.web;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * Server-Sent Events fan-out. Every message is a full snapshot of server state; a reconnecting
 * client simply receives the current snapshot. Nothing is replayed and no run is started.
 */
public final class SseHub {
    public static final int MAX_CLIENTS = 32;

    private static final class Client {
        final HttpExchange exchange;
        final OutputStream out;

        Client(HttpExchange exchange) {
            this.exchange = exchange;
            this.out = exchange.getResponseBody();
        }

        synchronized void send(byte[] bytes) throws IOException {
            out.write(bytes);
            out.flush();
        }

        void close() {
            try {
                exchange.close();
            } catch (Exception ignored) {
                // already closed
            }
        }
    }

    private final List<Client> clients = new CopyOnWriteArrayList<>();
    private final Supplier<String> currentSnapshot;

    public SseHub(Supplier<String> currentSnapshot) {
        this.currentSnapshot = currentSnapshot;
    }

    public int clientCount() {
        return clients.size();
    }

    void register(HttpExchange ex) throws IOException {
        if (clients.size() >= MAX_CLIENTS) {
            byte[] b = "too many open dashboards".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(503, b.length);
            ex.getResponseBody().write(b);
            ex.close();
            return;
        }
        ex.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.getResponseHeaders().set("X-Accel-Buffering", "no");
        ex.sendResponseHeaders(200, 0);
        Client c = new Client(ex);
        try {
            c.send(("retry: 2000\n" + frame(currentSnapshot.get())).getBytes(StandardCharsets.UTF_8));
            clients.add(c);
        } catch (IOException e) {
            c.close();
        }
    }

    public void broadcast(String snapshotJson) {
        send(frame(snapshotJson).getBytes(StandardCharsets.UTF_8));
    }

    public void heartbeat() {
        send(": keep-alive\n\n".getBytes(StandardCharsets.UTF_8));
    }

    private void send(byte[] bytes) {
        for (Client c : clients) {
            try {
                c.send(bytes);
            } catch (IOException e) {
                clients.remove(c);
                c.close();
            }
        }
    }

    private static String frame(String json) {
        return "event: snapshot\ndata: " + json + "\n\n";
    }
}
