package com.example.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * HTTP/SSE client that sends transactions via POST and receives
 * verdict results (approved/rejected) back via Server-Sent Events.
 *
 * <ul>
 *   <li>POST /transactions — send pipe-delimited transactions</li>
 *   <li>GET  /events       — receive verdicts as SSE data events</li>
 * </ul>
 */
public class ClientApp {

    private static final String BASE_URL = "http://localhost:8765";

    // Transactions: txId|fromAccountId|toAccountId|amount|merchantCountry
    private static final List<String> TRANSACTIONS = List.of(
            "TX-001|ACC-001|ACC-003|400.00|NL",   // approve
            "TX-002|ACC-001|ACC-002|500.00|RU"    // REJECT geo
    );

    public static void main(String[] args) throws Exception {
        int expectedVerdicts = TRANSACTIONS.size();
        CountDownLatch latch = new CountDownLatch(expectedVerdicts);

        // 1. Start SSE listener in a background thread
        Thread sseThread = new Thread(() -> {
            try {
                HttpURLConnection conn = (HttpURLConnection)
                        URI.create(BASE_URL + "/events").toURL().openConnection();
                conn.setRequestProperty("Accept", "text/event-stream");
                conn.setRequestProperty("Cache-Control", "no-cache");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(0); // no read timeout — SSE stream stays open

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("data: ")) {
                        System.out.println("  ← " + line.substring(6));
                        latch.countDown();
                    }
                }
            } catch (Exception e) {
                if (latch.getCount() > 0) {
                    e.printStackTrace();
                }
            }
        }, "sse-listener");
        sseThread.setDaemon(true);
        sseThread.start();

        // Give SSE connection time to establish
        Thread.sleep(500);

        System.out.println("Connected to Flink agent pipeline.\n");
        System.out.println("Sending " + TRANSACTIONS.size() + " transactions:");

        // 2. POST each transaction
        for (String tx : TRANSACTIONS) {
            System.out.println("  → " + tx);
            post(BASE_URL + "/transactions", tx);
        }

        System.out.println("\nWaiting for verdicts...\n");

        // 3. Wait for all verdicts via SSE
        latch.await();
        System.out.println("\nAll verdicts received.");

        // 4. Signal DONE
        post(BASE_URL + "/transactions", "DONE");
        Thread.sleep(500);
    }

    private static void post(String url, String body) throws Exception {
        HttpURLConnection conn = (HttpURLConnection)
                URI.create(url).toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "text/plain");
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        conn.getResponseCode(); // wait for response
        conn.disconnect();
    }
}
