package com.example.ledger;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Flink Source (FLIP-27) that runs a WebSocket server.
 * Each incoming message is expected as pipe-delimited:
 * {@code txId|fromAccountId|toAccountId|amount|merchantCountry}
 *
 * <p>Sends "DONE" to stop the source.
 */
public class WebSocketTransactionSource implements Source<Row, WebSocketTransactionSource.WsSplit, Void> {

    private final int port;

    public WebSocketTransactionSource(int port) {
        this.port = port;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Row, WsSplit> createReader(SourceReaderContext ctx) {
        return new WsReader();
    }

    @Override
    public SplitEnumerator<WsSplit, Void> createEnumerator(SplitEnumeratorContext<WsSplit> ctx) {
        return new WsEnumerator(ctx, port);
    }

    @Override
    public SplitEnumerator<WsSplit, Void> restoreEnumerator(SplitEnumeratorContext<WsSplit> ctx, Void checkpoint) {
        return new WsEnumerator(ctx, port);
    }

    @Override
    public SimpleVersionedSerializer<WsSplit> getSplitSerializer() {
        return new WsSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new VoidSerializer();
    }

    // ── Split ─────────────────────────────────────────────────────────────

    public static class WsSplit implements SourceSplit, Serializable {
        private final int port;

        public WsSplit(int port) {
            this.port = port;
        }

        @Override
        public String splitId() {
            return "ws-" + port;
        }
    }

    // ── Reader ────────────────────────────────────────────────────────────

    private static class WsReader implements SourceReader<Row, WsSplit> {

        private final LinkedBlockingQueue<Row> queue = new LinkedBlockingQueue<>();
        private volatile CompletableFuture<Void> available = new CompletableFuture<>();
        private volatile boolean done = false;
        private WebSocketServer server;

        @Override
        public void start() {
            // server starts when the split is assigned
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Row> output) {
            Row row = queue.poll();
            if (row != null) {
                output.collect(row);
                return queue.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
            }
            if (done) {
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            if (!queue.isEmpty() || done) {
                return CompletableFuture.completedFuture(null);
            }
            // Reset future — Flink will wait on this until we signal data
            available = new CompletableFuture<>();
            // Double-check after resetting (avoid race)
            if (!queue.isEmpty() || done) {
                available.complete(null);
            }
            return available;
        }

        @Override
        public void addSplits(List<WsSplit> splits) {
            if (splits.isEmpty()) return;
            int port = splits.get(0).port;

            server = new WebSocketServer(new InetSocketAddress(port)) {
                @Override
                public void onOpen(WebSocket conn, ClientHandshake hs) {
                    System.out.println("Client connected: " + conn.getRemoteSocketAddress());
                }

                @Override
                public void onMessage(WebSocket conn, String message) {
                    String trimmed = message.trim();
                    if ("DONE".equalsIgnoreCase(trimmed)) {
                        done = true;
                        available.complete(null);
                        return;
                    }
                    String[] parts = trimmed.split("\\|");
                    if (parts.length == 5) {
                        queue.add(Row.of(parts[0], parts[1], parts[2], parts[3], parts[4]));
                        available.complete(null);
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
        }

        @Override
        public void notifyNoMoreSplits() {
            // single-split source — nothing to do
        }

        @Override
        public List<WsSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            if (server != null) {
                try { server.stop(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
    }

    // ── Enumerator ────────────────────────────────────────────────────────

    private static class WsEnumerator implements SplitEnumerator<WsSplit, Void> {

        private final SplitEnumeratorContext<WsSplit> ctx;
        private final int port;
        private boolean assigned = false;

        WsEnumerator(SplitEnumeratorContext<WsSplit> ctx, int port) {
            this.ctx = ctx;
            this.port = port;
        }

        @Override
        public void start() {
            // Assign immediately if a reader is already registered
            if (!assigned && !ctx.registeredReaders().isEmpty()) {
                int readerId = ctx.registeredReaders().keySet().iterator().next();
                ctx.assignSplit(new WsSplit(port), readerId);
                assigned = true;
            }
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            // not used
        }

        @Override
        public void addSplitsBack(List<WsSplit> splits, int subtaskId) {
            // single-shot source — no reassignment
        }

        @Override
        public void addReader(int subtaskId) {
            if (!assigned) {
                ctx.assignSplit(new WsSplit(port), subtaskId);
                assigned = true;
            }
        }

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() {
        }
    }

    // ── Serializers ───────────────────────────────────────────────────────

    private static class WsSplitSerializer implements SimpleVersionedSerializer<WsSplit> {
        @Override
        public int getVersion() { return 1; }

        @Override
        public byte[] serialize(WsSplit split) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            new DataOutputStream(baos).writeInt(split.port);
            return baos.toByteArray();
        }

        @Override
        public WsSplit deserialize(int version, byte[] serialized) throws IOException {
            return new WsSplit(new DataInputStream(new ByteArrayInputStream(serialized)).readInt());
        }
    }

    private static class VoidSerializer implements SimpleVersionedSerializer<Void> {
        @Override
        public int getVersion() { return 1; }

        @Override
        public byte[] serialize(Void obj) { return new byte[0]; }

        @Override
        public Void deserialize(int version, byte[] serialized) { return null; }
    }
}
