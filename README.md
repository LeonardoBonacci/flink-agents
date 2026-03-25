# Flink Agents POC

A proof-of-concept project for building agentic AI applications with [Apache Flink](https://flink.apache.org/) and [Flink Agents](https://nightlies.apache.org/flink/flink-agents-docs-release-0.2/).

Currently runs a simple embedded DataStream word-count pipeline. The project is structured to be extended with Flink Agents 0.2 (ReAct agents, tools, LLM integration).

## Prerequisites

- Java 11+
- Maven 3+

## Project Structure

```
flink-agents/
├── pom.xml                                          # Maven config (Flink 2.2.0, agents deps ready)
└── src/main/
    ├── java/com/example/flink/
    │   └── SimpleDataStreamJob.java                 # Embedded DataStream word-count pipeline
    └── resources/
        └── log4j2.properties                        # Logging config
```

## Running

No external Flink cluster needed — the app runs embedded using Flink's mini-cluster.

### Quick iteration

```bash
mvn compile exec:exec -q
```

### Fat jar

```bash
mvn package -q
java -jar target/flink-agents-poc-1.0-SNAPSHOT.jar
```

## Next Steps

The Flink Agents dependencies (`flink-agents-api`, `flink-agents-ide-support`) are pre-configured in `pom.xml` (commented out). To start building agents:

1. Uncomment the agents dependencies in `pom.xml`
2. Wrap `StreamExecutionEnvironment` with `AgentsExecutionEnvironment`
3. Add chat model connections and tools to the agents environment
4. Replace map/flatMap operators with agent `.apply()` calls