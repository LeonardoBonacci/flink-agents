package com.example.ledger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Flink Source (FLIP-27) that runs an HTTP server with SSE support.
 *
 * <ul>
 *   <li>{@code POST /transactions} — pipe-delimited: txId|fromAccountId|toAccountId|amount|merchantCountry</li>
 *   <li>{@code GET  /events}       — SSE stream for verdict results (used by {@link SseVerdictSink})</li>
 * </ul>
 *
 * <p>Send "DONE" as POST body to stop the source.
 */
public class SseTransactionSource implements Source<Row, SseTransactionSource.SseSplit, Void> {

    /** Shared SSE output streams — used by SseVerdictSink to push results back to clients. */
    static final CopyOnWriteArrayList<OutputStream> sseClients = new CopyOnWriteArrayList<>();

    private final int port;

    public SseTransactionSource(int port) {
        this.port = port;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Row, SseSplit> createReader(SourceReaderContext ctx) {
        return new SseReader();
    }

    @Override
    public SplitEnumerator<SseSplit, Void> createEnumerator(SplitEnumeratorContext<SseSplit> ctx) {
        return new SseEnumerator(ctx, port);
    }

    @Override
    public SplitEnumerator<SseSplit, Void> restoreEnumerator(SplitEnumeratorContext<SseSplit> ctx, Void checkpoint) {
        return new SseEnumerator(ctx, port);
    }

    @Override
    public SimpleVersionedSerializer<SseSplit> getSplitSerializer() {
        return new SseSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new VoidSerializer();
    }

    // ── Split ─────────────────────────────────────────────────────────────

    public static class SseSplit implements SourceSplit, Serializable {
        private final int port;

        public SseSplit(int port) {
            this.port = port;
        }

        @Override
        public String splitId() {
            return "sse-" + port;
        }
    }

    // ── Reader ────────────────────────────────────────────────────────────

    private static class SseReader implements SourceReader<Row, SseSplit> {

        private final LinkedBlockingQueue<Row> queue = new LinkedBlockingQueue<>();
        private volatile CompletableFuture<Void> available = new CompletableFuture<>();
        private volatile boolean done = false;
        private HttpServer server;

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
            available = new CompletableFuture<>();
            if (!queue.isEmpty() || done) {
                available.complete(null);
            }
            return available;
        }

        @Override
        public void addSplits(List<SseSplit> splits) {
            if (splits.isEmpty()) return;
            int port = splits.get(0).port;

            try {
                server = HttpServer.create(new InetSocketAddress(port), 0);

                // POST /transactions — receive pipe-delimited transactions
                server.createContext("/transactions", exchange -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        respond(exchange, 405, "Method Not Allowed");
                        return;
                    }
                    String body = new String(
                            exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
                    System.out.println("[HTTP] POST /transactions: " + body);

                    if ("DONE".equalsIgnoreCase(body)) {
                        done = true;
                        available.complete(null);
                        respond(exchange, 200, "OK");
                        return;
                    }

                    String[] parts = body.split("\\|");
                    if (parts.length == 5) {
                        Row row = Row.withNames();
                        row.setField("txId", parts[0]);
                        row.setField("fromAccountId", parts[1]);
                        row.setField("toAccountId", parts[2]);
                        row.setField("amount", parts[3]);
                        row.setField("merchantCountry", parts[4]);
                        queue.add(row);
                        available.complete(null);
                        respond(exchange, 200, "OK");
                    } else {
                        respond(exchange, 400,
                                "Expected: txId|fromAccountId|toAccountId|amount|merchantCountry");
                    }
                });

                // GET /events — SSE endpoint for verdict results
                server.createContext("/events", this::handleSseConnection);

                server.setExecutor(Executors.newCachedThreadPool());
                server.start();
                System.out.println("HTTP/SSE server listening on http://localhost:" + port);
                System.out.println("  POST /transactions  — submit transactions");
                System.out.println("  GET  /events        — receive verdicts (SSE)");

            } catch (IOException e) {
                throw new RuntimeException("Failed to start HTTP server on port " + port, e);
            }
        }

        private void handleSseConnection(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.getResponseHeaders().set("Connection", "keep-alive");
            exchange.sendResponseHeaders(200, 0);

            OutputStream os = exchange.getResponseBody();
            sseClients.add(os);
            System.out.println("SSE client connected: " + exchange.getRemoteAddress());

            // Send initial comment so the client knows the connection is live
            os.write(": connected\n\n".getBytes(StandardCharsets.UTF_8));
            os.flush();

            // Block handler thread to keep the SSE connection alive.
            // The thread is interrupted when server.stop() is called.
            try {
                while (!done) {
                    Thread.sleep(15_000);
                    os.write(": keepalive\n\n".getBytes(StandardCharsets.UTF_8));
                    os.flush();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                // client disconnected
            } finally {
                sseClients.remove(os);
                exchange.close();
            }
        }

        private void respond(HttpExchange exchange, int code, String body) throws IOException {
            byte[] bytes = (body + "\n").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(code, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }

        @Override
        public void notifyNoMoreSplits() {
            // single-split source — nothing to do
        }

        @Override
        public List<SseSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            done = true;
            if (server != null) {
                server.stop(0);
            }
            sseClients.clear();
        }
    }

    // ── Enumerator ────────────────────────────────────────────────────────

    private static class SseEnumerator implements SplitEnumerator<SseSplit, Void> {

        private final SplitEnumeratorContext<SseSplit> ctx;
        private final int port;
        private boolean assigned = false;

        SseEnumerator(SplitEnumeratorContext<SseSplit> ctx, int port) {
            this.ctx = ctx;
            this.port = port;
        }

        @Override
        public void start() {
            if (!assigned && !ctx.registeredReaders().isEmpty()) {
                int readerId = ctx.registeredReaders().keySet().iterator().next();
                ctx.assignSplit(new SseSplit(port), readerId);
                assigned = true;
            }
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) { }

        @Override
        public void addSplitsBack(List<SseSplit> splits, int subtaskId) { }

        @Override
        public void addReader(int subtaskId) {
            if (!assigned) {
                ctx.assignSplit(new SseSplit(port), subtaskId);
                assigned = true;
            }
        }

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() { }
    }

    // ── Serializers ───────────────────────────────────────────────────────

    private static class SseSplitSerializer implements SimpleVersionedSerializer<SseSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(SseSplit split) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            new DataOutputStream(baos).writeInt(split.port);
            return baos.toByteArray();
        }

        @Override
        public SseSplit deserialize(int version, byte[] serialized) throws IOException {
            return new SseSplit(new DataInputStream(new ByteArrayInputStream(serialized)).readInt());
        }
    }

    private static class VoidSerializer implements SimpleVersionedSerializer<Void> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Void obj) {
            return new byte[0];
        }

        @Override
        public Void deserialize(int version, byte[] serialized) {
            return null;
        }
    }
}
