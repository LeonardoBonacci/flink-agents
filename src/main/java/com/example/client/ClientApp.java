package com.example.client;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.List;

/**
 * WebSocket client that sends transactions to the Flink agent pipeline.
 *
 * <p>Start the Flink job first ({@code mvn compile exec:exec -q}),
 * then run this client in a second terminal:
 * <pre>
 *   mvn compile exec:exec -Dexec.arguments="-classpath %classpath com.example.client.ClientApp" -q
 * </pre>
 */
public class ClientApp {

    // Transactions: txId|fromAccountId|toAccountId|amount|merchantCountry
    private static final List<String> TRANSACTIONS = List.of(
            "TX-001|ACC-001|ACC-003|400.00|NL",   // approve
            "TX-002|ACC-001|ACC-002|500.00|RU"    // REJECT geo
    );

    public static void main(String[] args) throws Exception {
        URI serverUri = new URI("ws://localhost:8765");

        WebSocketClient client = new WebSocketClient(serverUri) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                System.out.println("Connected to Flink agent pipeline.");
                for (String tx : TRANSACTIONS) {
                    System.out.println("  → " + tx);
                    send(tx);
                }
                System.out.println("All transactions sent. Sending DONE.");
                send("DONE");
            }

            @Override
            public void onMessage(String message) {
                System.out.println("← " + message);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                System.out.println("Connection closed.");
            }

            @Override
            public void onError(Exception ex) {
                ex.printStackTrace();
            }
        };

        client.connectBlocking();
        // Give the server a moment to process the DONE signal
        Thread.sleep(1000);
        client.closeBlocking();
    }
}
