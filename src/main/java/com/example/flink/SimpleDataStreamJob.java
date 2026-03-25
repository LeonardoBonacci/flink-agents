package com.example.flink;

import org.apache.flink.agents.api.AgentsExecutionEnvironment;
import org.apache.flink.agents.api.agents.ReActAgent;
import org.apache.flink.agents.api.annotation.ToolParam;
import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.prompt.Prompt;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceName;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.api.tools.Tool;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transaction Fraud Validator — ReAct Agent with investigation tools.
 *
 * <p>Each incoming transaction is investigated by a ReAct agent that can call
 * tools to gather evidence (geo fraud score, account balance). The agent
 * outputs a structured verdict (approved/rejected + reason), and downstream
 * Flink operators split the stream deterministically.
 *
 * <p>Balances are tracked in a shared ledger. Approved transactions deduct
 * from the ledger, so subsequent balance checks reflect prior spending.
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>Ollama running locally: {@code ollama serve}</li>
 *   <li>Model pulled: {@code ollama pull qwen3:8b}</li>
 * </ul>
 *
 * <p>Run with: {@code mvn compile exec:exec -q}
 */
public class SimpleDataStreamJob {

    // ── Balance Ledger ────────────────────────────────────────────────────

    /** Shared ledger — simulates an external balance service. All accounts start at 1000 EUR. */
    private static final Map<String, Double> LEDGER = new ConcurrentHashMap<>(Map.of(
            "ACC-001", 1000.0,
            "ACC-002", 1000.0,
            "ACC-003", 1000.0,
            "ACC-004", 1000.0
    ));

    // ── Prompt ────────────────────────────────────────────────────────────

    private static final String SYSTEM_PROMPT =
            "You are a bank transaction fraud validator. For each transaction, "
            + "investigate whether it should be approved or rejected.\n\n"
            + "You have two tools:\n"
            + "- getGeoFraudScore: returns a fraud risk percentage (0-100) for a country. "
            + "Scores above 70 are high risk.\n"
            + "- checkAccountBalance: returns the current account balance in EUR.\n\n"
            + "Use these tools to gather evidence, then decide:\n"
            + "- APPROVE if the geo fraud score is acceptable AND the account has sufficient funds.\n"
            + "- REJECT if the geo fraud score is too high OR insufficient funds, and explain why.\n\n"
            + "IMPORTANT: You must echo back the accountId and amount exactly as given in the input.\n\n"
            + "Ensure your response can be parsed as JSON, using this exact format:\n"
            + "{\"txId\": \"TX-001\", \"accountId\": \"ACC-001\", \"amount\": \"250.00\", "
            + "\"approved\": true, \"reason\": \"low risk, sufficient funds\"}";

    // ── Tools ─────────────────────────────────────────────────────────────

    /** Returns a geo-location fraud risk score (0-100) for the given country. */
    @org.apache.flink.agents.api.annotation.Tool
    public static String getGeoFraudScore(
            @ToolParam(description = "The country code to check fraud risk for (e.g. NL, US, NG)")
            String countryCode) {
        int score = switch (countryCode.toUpperCase()) {
            case "NL" -> 3;
            case "DE" -> 5;
            case "US" -> 12;
            case "RU" -> 78;
            case "NG" -> 85;
            case "KP" -> 97;
            default   -> 50;
        };
        return "{\"countryCode\":\"" + countryCode.toUpperCase()
                + "\",\"fraudScore\":" + score + "}";
    }

    /** Returns the current account balance from the shared ledger. */
    @org.apache.flink.agents.api.annotation.Tool
    public static String checkAccountBalance(
            @ToolParam(description = "The account ID to check the balance for")
            String accountId) {
        double balance = LEDGER.getOrDefault(accountId, 0.0);
        return "{\"accountId\":\"" + accountId
                + "\",\"balance\":" + String.format("%.2f", balance) + "}";
    }

    // ── Main ──────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {

        // 1. Create Flink + Agents execution environments
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // sequential processing so balance updates are visible
        final AgentsExecutionEnvironment agentsEnv =
                AgentsExecutionEnvironment.getExecutionEnvironment(env);

        // 2. Register resources: Ollama connection + investigation tools
        agentsEnv
                .addResource(
                        "ollamaConnection",
                        ResourceType.CHAT_MODEL_CONNECTION,
                        ResourceDescriptor.Builder
                                .newBuilder(ResourceName.ChatModel.OLLAMA_CONNECTION)
                                .addInitialArgument("endpoint", "http://localhost:11434")
                                .addInitialArgument("requestTimeout", 120)
                                .build())
                .addResource(
                        "getGeoFraudScore",
                        ResourceType.TOOL,
                        Tool.fromMethod(
                                SimpleDataStreamJob.class.getMethod(
                                        "getGeoFraudScore", String.class)))
                .addResource(
                        "checkAccountBalance",
                        ResourceType.TOOL,
                        Tool.fromMethod(
                                SimpleDataStreamJob.class.getMethod(
                                        "checkAccountBalance", String.class)));

        // 3. Build prompt
        Prompt prompt = Prompt.fromMessages(Arrays.asList(
                new ChatMessage(MessageRole.SYSTEM, SYSTEM_PROMPT),
                new ChatMessage(MessageRole.USER,
                        "Transaction {txId}: account {accountId} wants to "
                        + "transfer {amount} EUR to {merchantCountry}.")
        ));

        // 4. Output schema — verdict with accountId + amount for downstream balance tracking
        RowTypeInfo outputSchema = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.BOOLEAN_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                },
                new String[]{"txId", "accountId", "amount", "approved", "reason"}
        );

        // 5. Create ReAct Agent
        ReActAgent agent = new ReActAgent(
                ResourceDescriptor.Builder
                        .newBuilder(ResourceName.ChatModel.OLLAMA_SETUP)
                        .addInitialArgument("connection", "ollamaConnection")
                        .addInitialArgument("model", "qwen3:8b")
                        .addInitialArgument("tools",
                                Arrays.asList("getGeoFraudScore", "checkAccountBalance"))
                        .addInitialArgument("extractReasoning", true)
                        .build(),
                prompt,
                outputSchema
        );

        // 6. Source — sample transactions that will drain account balances
        //    All accounts start at 1000 EUR
        RowTypeInfo inputSchema = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                },
                new String[]{"txId", "accountId", "amount", "merchantCountry"}
        );

        DataStream<Row> transactions = env.fromData(
                Row.of("TX-001", "ACC-001", "400.00",  "NL"),  // approve → bal 600
                Row.of("TX-002", "ACC-002", "500.00",  "DE"),  // approve → bal 500
                Row.of("TX-003", "ACC-001", "350.00",  "NL"),  // approve → bal 250
                Row.of("TX-004", "ACC-003", "200.00",  "US"),  // approve → bal 800
                Row.of("TX-005", "ACC-001", "500.00",  "NL"),  // REJECT — only 250 left
                Row.of("TX-006", "ACC-002", "600.00",  "NL"),  // REJECT — only 500 left
                Row.of("TX-007", "ACC-004", "150.00",  "RU"),  // REJECT — geo 78 (high risk)
                Row.of("TX-008", "ACC-003", "900.00",  "KP")   // REJECT — geo 97 + only 800 left
        ).returns(inputSchema);

        // 7. Apply agent to stream
        @SuppressWarnings("unchecked")
        DataStream<Row> verdicts = (DataStream<Row>) (DataStream<?>)
                agentsEnv
                        .fromDataStream(transactions)
                        .apply(agent)
                        .toDataStream();

        // 8. Split into approved / rejected streams
        DataStream<Row> approved = verdicts.filter(row -> (Boolean) row.getField("approved"));
        DataStream<Row> rejected = verdicts.filter(row -> !(Boolean) row.getField("approved"));

        // 9. Approved → deduct from ledger, print new balance
        approved.map(row -> {
            String accountId = (String) row.getField("accountId");
            double amount = Double.parseDouble((String) row.getField("amount"));
            double newBalance = LEDGER.compute(accountId, (k, bal) -> bal - amount);
            return String.format("✓ %s | %s | -%.2f EUR | new balance: %.2f EUR",
                    row.getField("txId"), accountId, amount, newBalance);
        }).returns(BasicTypeInfo.STRING_TYPE_INFO).print("PROCESSED");

        rejected.map(row -> String.format("✗ %s | %s | %s EUR | %s",
                row.getField("txId"), row.getField("accountId"),
                row.getField("amount"), row.getField("reason"))
        ).returns(BasicTypeInfo.STRING_TYPE_INFO).print("REJECTED ");

        // 10. Execute
        agentsEnv.execute();
    }
}
