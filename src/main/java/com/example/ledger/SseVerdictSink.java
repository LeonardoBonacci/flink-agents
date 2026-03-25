package com.example.ledger;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Flink Sink that pushes verdict rows to all connected SSE clients.
 * Uses the shared client list from {@link SseTransactionSource}.
 */
public class SseVerdictSink implements Sink<Row> {

    private final String prefix;

    public SseVerdictSink(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public SinkWriter<Row> createWriter(WriterInitContext context) {
        return new SseWriter(prefix);
    }

    private static class SseWriter implements SinkWriter<Row> {

        private final String prefix;

        SseWriter(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public void write(Row row, Context context) {
            String message = String.format("%s %s | %s → %s | %s EUR | %s",
                    prefix,
                    row.getField("txId"), row.getField("fromAccountId"),
                    row.getField("toAccountId"), row.getField("amount"),
                    row.getField("reason"));

            byte[] event = ("data: " + message + "\n\n").getBytes(StandardCharsets.UTF_8);

            for (OutputStream os : SseTransactionSource.sseClients) {
                try {
                    os.write(event);
                    os.flush();
                } catch (IOException e) {
                    // Client disconnected — remove dead stream
                    SseTransactionSource.sseClients.remove(os);
                }
            }
        }

        @Override
        public void flush(boolean endOfInput) { }

        @Override
        public void close() { }
    }
}
