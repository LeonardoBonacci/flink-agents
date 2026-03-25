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
import java.util.Collections;

/**
 * ReAct Agent example — Review Analysis.
 *
 * <p>Mirrors the Python quickstart from the Flink Agents 0.2 docs, translated
 * to the Java API. Reads a bounded stream of product reviews, sends each to a
 * ReAct agent backed by Ollama (qwen3:8b), and prints the analysis result.
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
            "Analyze the user review and product information to determine a "
            + "satisfaction score (1-5) and potential reasons for dissatisfaction.\n\n"
            + "If the review mentions shipping damage, call the notifyShippingManager "
            + "tool with the product id and review text.\n\n"
            + "Example input format:\n"
            + "{\"id\": \"12345\", \"review\": \"The headphones broke after one week.\"}\n\n"
            + "Ensure your response can be parsed as JSON, using this exact format:\n"
            + "{\"id\": \"12345\", \"score\": 1, \"reasons\": [\"poor quality\"]}";

    // ── Tool ──────────────────────────────────────────────────────────────

    /**
     * Notify the shipping manager when a product received a negative review
     * due to shipping damage.
     */
    @org.apache.flink.agents.api.annotation.Tool
    public static String notifyShippingManager(
            @ToolParam(description = "The id of the product that received a negative review due to shipping damage")
            String id,
            @ToolParam(description = "The negative review content")
            String review) {
        System.out.println("[NOTIFICATION] Shipping manager notified for product " + id + ": " + review);
        return "Shipping manager has been notified for product " + id;
    }

    // ── Main ──────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {

        // 1. Create Flink + Agents execution environments
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        final AgentsExecutionEnvironment agentsEnv =
                AgentsExecutionEnvironment.getExecutionEnvironment(env);

        // 2. Register resources: Ollama connection + tool
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
                        "notifyShippingManager",
                        ResourceType.TOOL,
                        Tool.fromMethod(
                                SimpleDataStreamJob.class.getMethod(
                                        "notifyShippingManager",
                                        String.class, String.class)));

        // 3. Build prompt (system instructions + user input template)
        Prompt prompt = Prompt.fromMessages(Arrays.asList(
                new ChatMessage(MessageRole.SYSTEM, SYSTEM_PROMPT),
                new ChatMessage(MessageRole.USER,
                        "\"id\": \"{id}\",\n\"review\": \"{review}\"")
        ));

        // 4. Define output schema
        RowTypeInfo outputSchema = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                },
                new String[]{"id", "score", "reasons"}
        );

        // 5. Create ReAct Agent
        ReActAgent agent = new ReActAgent(
                ResourceDescriptor.Builder
                        .newBuilder(ResourceName.ChatModel.OLLAMA_SETUP)
                        .addInitialArgument("connection", "ollamaConnection")
                        .addInitialArgument("model", "llama3.1")
                        .addInitialArgument("tools",
                                Collections.singletonList("notifyShippingManager"))
                        .addInitialArgument("extractReasoning", true)
                        .build(),
                prompt,
                outputSchema
        );

        // 6. Source — sample product reviews as Row(id, review)
        RowTypeInfo inputSchema = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                },
                new String[]{"id", "review"}
        );

        DataStream<Row> reviews = env.fromData(
                Row.of("1", "Great product, exactly what I needed!"),
                Row.of("2", "Product arrived damaged due to poor packaging during shipping."),
                Row.of("3", "Battery life is terrible, barely lasts 2 hours."),
                Row.of("4", "Excellent quality and fast delivery, highly recommended.")
        ).returns(inputSchema);

        // 7. Apply agent to stream
        DataStream<?> results = agentsEnv
                .fromDataStream(reviews)
                .apply(agent)
                .toDataStream();

        results.print();

        // 8. Execute
        agentsEnv.execute();
    }
}
