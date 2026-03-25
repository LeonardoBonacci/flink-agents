# Flink Agents POC

A proof-of-concept: a ReAct agent pipeline that validates bank transactions for fraud using balance tracking and geo-risk scoring, powered by [Apache Flink](https://flink.apache.org/) and [Flink Agents](https://nightlies.apache.org/flink/flink-agents-docs-release-0.2/).

A WebSocket client sends transactions, the Flink agent investigates each one (balance check, geo fraud score, ledger updates via tools), and streams verdicts back to the client.

## Prerequisites

- Java 21+
- Maven 3+
- Ollama running locally: `ollama serve`
- Model pulled: `ollama pull qwen3:8b`

## Project Structure

```
src/main/java/com/example/
├── ledger/
│   ├── SimpleDataStreamJob.java         # Flink agent pipeline (main class)
│   ├── WebSocketTransactionSource.java  # FLIP-27 Source: WS server on port 8765
│   └── WebSocketVerdictSink.java        # Sink: broadcasts verdicts back via WS
└── client/
    └── ClientApp.java                   # WS client: sends transactions, receives verdicts
```

## Running

### Terminal 1 — Start the Flink agent pipeline

```bash
mvn compile exec:exec -q
```

Starts the WebSocket server on `ws://localhost:8765` and waits for transactions.

### Terminal 2 — Run the client

```bash
java -cp "target/classes:$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout)" com.example.client.ClientApp
```

Sends 2 transactions, waits for verdicts, then exits.