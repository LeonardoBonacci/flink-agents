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

/**
 * Transaction Fraud Validator — ReAct Agent with investigation tools.
 *
 * <p>Each incoming transaction is investigated by a ReAct agent that can call
 * tools to gather evidence (geo fraud score, account balance). The agent
 * outputs a structured verdict (approved/rejected + reason), and downstream
 * Flink operators split the stream deterministically.
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

    // ── Prompt ────────────────────────────────────────────────────────────

    private static final String SYSTEM_PROMPT =
            "You are a bank transaction fraud validator. For each transaction, "
            + "investigate whether it should be approved or rejected.\n\n"
            + "You have two tools:\n"
            + "- getGeoFraudScore: returns a fraud risk percentage (0-100) for a country. "
            + "Scores above 70 are high risk.\n"
            + "- checkAccountBalance: returns the current account balance.\n\n"
            + "Use these tools to gather evidence, then decide:\n"
            + "- APPROVE if the geo fraud score is acceptable AND the account has sufficient funds.\n"
            + "- REJECT if the geo fraud score is too high OR insufficient funds, and explain why.\n\n"
            + "Ensure your response can be parsed as JSON, using this exact format:\n"
            + "{\"txId\": \"TX001\", \"approved\": true, \"reason\": \"low risk, sufficient funds\"}";

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

    /** Returns the current account balance for the given account ID. */
    @org.apache.flink.agents.api.annotation.Tool
    public static String checkAccountBalance(
            @ToolParam(description = "The account ID to check the balance for")
            String accountId) {
        double balance = switch (accountId) {
            case "ACC-001" -> 15_240.50;
            case "ACC-002" -> 342.10;
            case "ACC-003" -> 89_500.00;
            case "ACC-004" -> 1_200.75;
            default        -> 0.0;
        };
        return "{\"accountId\":\"" + accountId
                + "\",\"balance\":" + balance + "}";
    }

    // ── Main ──────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {

        // 1. Create Flink + Agents execution environments
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
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

        // 4. Output schema — the agent's verdict
        RowTypeInfo outputSchema = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.BOOLEAN_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                },
                new String[]{"txId", "approved", "reason"}
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

        // 6. Source — sample transactions: Row(txId, accountId, amount, merchantCountry)
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
                Row.of("TX-001", "ACC-001", "250.00",    "NL"),   // low risk, plenty of funds
                Row.of("TX-002", "ACC-002", "8000.00",   "NG"),   // high risk country + insufficient funds
                Row.of("TX-003", "ACC-003", "12000.00",  "DE"),   // low risk, sufficient funds
                Row.of("TX-004", "ACC-004", "5000.00",   "KP")    // very high risk country
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

        approved.print("APPROVED");
        rejected.print("REJECTED");

        // 9. Execute
        agentsEnv.execute();
    }
}
