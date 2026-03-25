package com.example.ledger;

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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Transaction Fraud Validator — ReAct Agent with tool-based balance tracking + fraud investigation.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>All transactions flow to the ReAct agent</li>
 *   <li>Agent checks balance (getBalance), geo fraud (getGeoFraudScore), updates ledger (updateBalance)</li>
 *   <li>Downstream Flink operators route approved/rejected verdicts</li>
 * </ol>
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


    // ── Balance ledger (agent's "long-term memory" via tools) ─────────────

    private static final Map<String, Double> balances = new HashMap<>();

    // ── Prompt ────────────────────────────────────────────────────────────

    private static final String SYSTEM_PROMPT =
            "You are a bank transaction fraud investigator with memory. "
            + "You have three tools:\n"
            + "- getBalance(accountId): returns the current balance for an account (all start at 0 EUR).\n"
            + "- getGeoFraudScore(countryCode): returns a fraud risk score 0-100. Above 70 = high risk.\n"
            + "- updateBalance(accountId, amount): adjusts an account balance (negative to debit, positive to credit).\n\n"
            + "For each transaction:\n"
            + "1. Call getBalance for the sender. If balance - amount < -1000 (overdraft limit), REJECT for insufficient funds.\n"
            + "2. Call getGeoFraudScore for the merchant country. If score > 70, REJECT for high fraud risk.\n"
            + "3. If both checks pass, call updateBalance TWICE: debit sender (-amount) and credit receiver (+amount). Then APPROVE.\n\n"
            + "IMPORTANT: Echo back fromAccountId, toAccountId, and amount exactly as given.\n\n"
            + "Return JSON in this exact format:\n"
            + "{\"txId\": \"TX-001\", \"fromAccountId\": \"ACC-001\", \"toAccountId\": \"ACC-002\", "
            + "\"amount\": \"250.00\", \"approved\": true, \"reason\": \"balance OK, low geo risk (3%)\"}";  

    // ── Tools ─────────────────────────────────────────────────────────────

    /** Returns a geo-location fraud risk score (0-100) for the given country. */
    @org.apache.flink.agents.api.annotation.Tool
    public static String getGeoFraudScore(
            @ToolParam(description = "The country code to check fraud risk for (e.g. NL, US, NG)")
            String countryCode) {
        System.out.printf("[TOOL] getGeoFraudScore(%s)%n", countryCode);
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

    /** Returns the current balance for an account (all accounts start at 0 EUR). */
    @org.apache.flink.agents.api.annotation.Tool
    public static String getBalance(
            @ToolParam(description = "The account ID to check balance for")
            String accountId) {
        double bal = balances.getOrDefault(accountId, 0.0);
        System.out.printf("[TOOL] getBalance(%s) → %.2f%n", accountId, bal);
        return "{\"accountId\":\"" + accountId + "\",\"balance\":" + bal + "}";
    }

    /** Adjusts the balance of an account (negative to debit, positive to credit). */
    @org.apache.flink.agents.api.annotation.Tool
    public static String updateBalance(
            @ToolParam(description = "The account ID to update")
            String accountId,
            @ToolParam(description = "Amount to add (negative to debit, positive to credit)")
            String amount) {
        double amt = Double.parseDouble(amount);
        double newBal = balances.merge(accountId, amt, Double::sum);
        System.out.printf("[TOOL] updateBalance(%s, %s) → new balance: %.2f%n", accountId, amount, newBal);
        return "{\"accountId\":\"" + accountId + "\",\"newBalance\":" + newBal + "}";
    }

    // ── Main ──────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {

        // 1. Create Flink + Agents execution environments
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // sequential processing so balance updates are visible
        final AgentsExecutionEnvironment agentsEnv =
                AgentsExecutionEnvironment.getExecutionEnvironment(env);

        // 2. Register resources: Ollama connection + geo fraud tool
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
                        "getBalance",
                        ResourceType.TOOL,
                        Tool.fromMethod(
                                SimpleDataStreamJob.class.getMethod(
                                        "getBalance", String.class)))
                .addResource(
                        "updateBalance",
                        ResourceType.TOOL,
                        Tool.fromMethod(
                                SimpleDataStreamJob.class.getMethod(
                                        "updateBalance", String.class, String.class)));

        // 3. Build prompt
        Prompt prompt = Prompt.fromMessages(Arrays.asList(
                new ChatMessage(MessageRole.SYSTEM, SYSTEM_PROMPT),
                new ChatMessage(MessageRole.USER,
                        "Transaction {txId}: account {fromAccountId} wants to "
                        + "transfer {amount} EUR to account {toAccountId} "
                        + "in {merchantCountry}.")
        ));

        // 4. Output schema — agent verdict
        RowTypeInfo outputSchema = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.BOOLEAN_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                },
                new String[]{"txId", "fromAccountId", "toAccountId", "amount", "approved", "reason"}
        );

        // 5. Create ReAct Agent (balance + geo fraud investigation)
        ReActAgent agent = new ReActAgent(
                ResourceDescriptor.Builder
                        .newBuilder(ResourceName.ChatModel.OLLAMA_SETUP)
                        .addInitialArgument("connection", "ollamaConnection")
                        .addInitialArgument("model", "qwen3:8b")
                        .addInitialArgument("tools",
                                Arrays.asList("getBalance", "getGeoFraudScore", "updateBalance"))
                        .addInitialArgument("extractReasoning", false)
                        .build(),
                prompt,
                outputSchema
        );

        // 6. Source — HTTP/SSE server on port 8765 (client POSTs transactions, receives verdicts via SSE)
        RowTypeInfo inputSchema = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                },
                new String[]{"txId", "fromAccountId", "toAccountId", "amount", "merchantCountry"}
        );

        DataStream<Row> transactions = env
                .fromSource(
                        new SseTransactionSource(8765),
                        WatermarkStrategy.noWatermarks(),
                        "HTTP/SSE Transactions")
                .returns(inputSchema);

        // Log incoming transactions
        DataStream<Row> loggedTransactions = transactions.map(row -> {
            System.out.printf("[IN]  %s | %s → %s | %s EUR | country=%s%n",
                    row.getField("txId"), row.getField("fromAccountId"),
                    row.getField("toAccountId"), row.getField("amount"),
                    row.getField("merchantCountry"));
            return row;
        }).returns(inputSchema);

        // 7. Send ALL transactions to the agent — it handles balance + geo checks via tools
        @SuppressWarnings("unchecked")
        DataStream<Row> verdicts = (DataStream<Row>) (DataStream<?>)
                agentsEnv
                        .fromDataStream(loggedTransactions)
                        .apply(agent)
                        .toDataStream();

        // Log agent verdicts
        DataStream<Row> loggedVerdicts = verdicts.map(row -> {
            System.out.printf("[OUT] %s | %s → %s | %s EUR | approved=%s | %s%n",
                    row.getField("txId"), row.getField("fromAccountId"),
                    row.getField("toAccountId"), row.getField("amount"),
                    row.getField("approved"), row.getField("reason"));
            return row;
        }).returns(outputSchema);

        // 8. Split agent verdicts into approved / rejected
        DataStream<Row> approved = loggedVerdicts.filter(row -> (Boolean) row.getField("approved"));
        DataStream<Row> rejected = loggedVerdicts.filter(row -> !(Boolean) row.getField("approved"));

        // 9. Send results back to client via SSE
        approved.sinkTo(new SseVerdictSink("✓ APPROVED"));
        rejected.sinkTo(new SseVerdictSink("✗ REJECTED"));

        // 12. Execute
        agentsEnv.execute();
    }
}
