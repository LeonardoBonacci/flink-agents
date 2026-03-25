---
description: "Use when working with Apache Flink, Flink Agents, DataStream API, agent pipelines, or this flink-agents-poc project. Covers project setup, dependency versions, embedded execution, and the Flink Agents programming model."
applyTo: "**/*.java"
---

# Flink Agents POC — Project Context

## Stack & Versions

- **Flink**: 2.2.0 (required by Flink Agents 0.2)
- **Flink Agents**: 0.2.0 (`org.apache.flink:flink-agents-api`)
- **Java**: 11+ (21+ recommended for async agent execution)
- **Build**: Maven, single-module project
- **Execution**: Embedded (mini-cluster) — no external Flink runtime needed

## Maven Dependencies

Flink core deps (`flink-streaming-java`, `flink-clients`) are at **compile** scope for embedded execution.

When running on a real Flink cluster, these should be **provided** scope instead. The agents deps in `pom.xml` are commented out and also use provided scope:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-agents-api</artifactId>        <!-- core agents API -->
    <version>0.2.0</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-agents-ide-support</artifactId> <!-- bundles runtime deps for local execution -->
    <version>0.2.0</version>
</dependency>
```

## Running the App

```bash
mvn compile exec:exec -q     # quick dev iteration (exec:exec, NOT exec:java — Flink classloading requires a forked process)
mvn package -q && java -jar target/flink-agents-poc-1.0-SNAPSHOT.jar   # fat jar via maven-shade-plugin
```

**Important**: `mvn exec:java` does NOT work with Flink because it runs in the Maven JVM with a classloader that breaks Flink's operator serialization. Always use `exec:exec` (forked process) instead.

## Flink Agents Programming Model

A Flink Agents pipeline follows this pattern:

```java
// 1. Create environments
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
AgentsExecutionEnvironment agentsEnv = AgentsExecutionEnvironment.getExecutionEnvironment(env);

// 2. Register resources (chat models, tools)
agentsEnv
    .addResource("modelConnection", ResourceType.CHAT_MODEL_CONNECTION, descriptor)
    .addResource("toolName", ResourceType.TOOL, Tool.fromMethod(...));

// 3. Build source DataStream as usual
DataStream<Row> source = env.fromSource(...);

// 4. Apply agent to stream
DataStream<Object> result = agentsEnv
    .fromDataStream(source)
    .apply(agent)
    .toDataStream();

result.print();
agentsEnv.execute();
```

### Key Agent Types

- **ReActAgent** — Autonomous reasoning + action loop. Takes a chat model, prompt, and output schema. Can call registered tools.
- **WorkflowSingleAgent / WorkflowMultipleAgent** — Structured multi-step agent workflows.

### Agent Construction (ReActAgent example)

```java
ReActAgent agent = new ReActAgent(
    ResourceDescriptor.Builder.newBuilder(ResourceName.ChatModel.OLLAMA_SETUP)
        .addInitialArgument("connection", "modelConnection")
        .addInitialArgument("model", "qwen3:8b")
        .addInitialArgument("tools", Collections.singletonList("toolName"))
        .build(),
    prompt,
    OutputSchema.class
);
```

### Tools & Prompts

- Tools are annotated with `@Tool` and `@ToolParam`, registered via `Tool.fromMethod()`
- Prompts are defined with `@Prompt` annotation or as `Prompt` objects
- Resources (models, tools) are registered on `AgentsExecutionEnvironment` by name and looked up by agents at runtime

## Project Conventions

- Package: `com.example.flink`
- Main class: `com.example.flink.SimpleDataStreamJob`
- Logging: log4j2 via `src/main/resources/log4j2.properties` (Flink internals suppressed to WARN/ERROR)
