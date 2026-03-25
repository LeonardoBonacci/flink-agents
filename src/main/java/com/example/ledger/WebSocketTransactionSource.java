package com.example.ledger;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.types.Row;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;

/**
 * Flink source that runs a WebSocket server on port 8765.
 * Each incoming message is expected to be a pipe-delimited line:
 * {@code txId|fromAccountId|toAccountId|amount|merchantCountry}
 *
 * <p>The source stays open until cancelled by Flink or the client sends "DONE".
 */
public class WebSocketTransactionSource implements SourceFunction<Row> {

    private final int port;
    private volatile boolean running = true;
    private transient WebSocketServer server;

    public WebSocketTransactionSource(int port) {
        this.port = port;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        server = new WebSocketServer(new InetSocketAddress(port)) {
            @Override
            public void onOpen(WebSocket conn, ClientHandshake handshake) {
                System.out.println("Client connected: " + conn.getRemoteSocketAddress());
            }

            @Override
            public void onMessage(WebSocket conn, String message) {
                String trimmed = message.trim();
                if ("DONE".equalsIgnoreCase(trimmed)) {
                    running = false;
                    return;
                }
                // Expected format: txId|fromAccountId|toAccountId|amount|merchantCountry
                String[] parts = trimmed.split("\\|");
                if (parts.length == 5) {
                    Row row = Row.of(parts[0], parts[1], parts[2], parts[3], parts[4]);
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(row);
                    }
                }
            }

            @Override
            public void onClose(WebSocket conn, int code, String reason, boolean remote) {
                System.out.println("Client disconnected.");
            }

            @Override
            public void onError(WebSocket conn, Exception ex) {
                ex.printStackTrace();
            }

            @Override
            public void onStart() {
                System.out.println("WebSocket server listening on ws://localhost:" + port);
            }
        };

        server.setReuseAddr(true);
        server.start();

        // Block until cancelled or client sends DONE
        while (running) {
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (server != null) {
            try {
                server.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
