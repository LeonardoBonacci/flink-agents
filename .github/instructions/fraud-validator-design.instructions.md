---
description: "Design guidance for the bank account fraud/transaction validator agent pipeline. Covers agent type selection (Workflow vs ReAct), pipeline topology, and routing strategy."
applyTo: "**/*.java"
---

# Fraud Validator Agent — Design Decisions

## Agent Type: ReAct Agent (with MCP tools)

This project uses a **ReAct Agent** for transaction fraud validation.

### Pipeline Topology

```
transaction_requests → [ReAct agent + MCP tools] → filter/side-output → transactions (approved)
                                                                       → rejected
```

The ReAct agent autonomously investigates each transaction using MCP tools, outputs a structured verdict, and downstream Flink DataStream operators handle the deterministic routing into approved/rejected sinks.

### Why ReAct

1. **Dynamic tool selection** — The LLM must reason about *which* MCP tools to call based on each transaction's characteristics. A large international wire needs different investigation (account history, recipient verification, geolocation) than a small domestic purchase (spending patterns). This dynamic reasoning loop is exactly what ReAct is built for.
2. **MCP tools integration** — ReAct natively supports autonomous tool calling. The agent reasons, picks a tool, observes the result, and decides whether to call more tools or produce a verdict.
3. **Structured output** — ReAct Agent supports `OutputSchema`, so the agent outputs a typed verdict (approved/rejected + reasoning) that downstream Flink operators can route deterministically.

### Why NOT Workflow Agent

Workflow Agent is for pipelines where every step is predetermined and the control flow is fixed. With MCP tools, the investigation path varies per transaction — the LLM needs autonomy to decide which tools to call and in what order. Hardcoding every possible tool-calling sequence as explicit Workflow actions would be brittle and defeat the purpose of LLM-driven investigation.

### Routing Strategy

The ReAct agent outputs a structured schema containing the verdict. The split into two output streams happens **outside** the agent, in plain Flink DataStream code:

- Use `OutputTag` + `ProcessFunction` side outputs, or
- Use `.filter()` on the agent's output stream

This keeps the deterministic routing logic (approved vs. rejected) in explicit, auditable Flink code while the LLM handles the non-deterministic investigation.
