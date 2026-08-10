# AsyncResponse documentation

Task-oriented index of every documentation page. Start with the
[main README](../README.md) for the elevator pitch, the channel/transport matrix, and quick-start
recipes; come back here to go deeper.

## Getting started

| I want to… | Read |
|---|---|
| Understand what the library does and whether it fits my problem | [Main README](../README.md) — [the problem](../README.md#the-problem-it-solves), [when to use it](../README.md#when-to-use-it--and-when-not) |
| Wire up a channel + transport + flow store and await my first response | [Install and run](../README.md#install-and-run), then [provider-examples.md](provider-examples.md) |
| Copy a setup for a specific channel, transport, or durable-flow store | [provider-examples.md](provider-examples.md) |
| Run a complete example app and poke every scenario over HTTP | [sample.md](sample.md) |

## Guides

| I want to… | Read |
|---|---|
| Orchestrate a multi-step flow that survives crashes and redeploys | [durable-flows.md](durable-flows.md) |
| Compose flows from child flows, and know every failure mode | [Child flows](durable-flows.md#child-flows) and [What happens when things die](durable-flows.md#what-happens-when-things-die) |
| Sleep inside a flow for days, schedule delayed jobs, or run flows on cron | [timers-and-scheduling.md](timers-and-scheduling.md) |
| Test flows and waits deterministically — virtual clock, scripted replies, crash injection, simulated restarts | [testing.md](testing.md) |
| Route late responses after a redeploy (resume vs. fail) | [recovery.md](recovery.md) |
| Keep flow ledgers in my own database (or write a custom store) | [durable-flow-state-stores.md](durable-flow-state-stores.md), with one example per provider |
| Lock down persisted callbacks, stack traces, and type resolution | [security.md](security.md) |

## Reference

| I want to… | Read |
|---|---|
| Look up any option: engine, flow-store, channel, or transport | [configuration.md](configuration.md) |
| Compare and copy every provider registration | [provider-examples.md](provider-examples.md) |
| Understand a transport's ACK, redelivery, and dead-letter semantics | [Transport options](configuration.md#transport-options) |
| Run the Redis pair on Valkey / Dragonfly / Garnet or managed Redis | [Redis-compatible servers](configuration.md#redis-compatible-servers) |
| Connect traces and metrics (span names, instruments, tags) | [observability.md](observability.md) |
| Ship a trimmed / Native AOT app | [aot.md](aot.md) |
| Understand the PostgreSQL channel/transport internals and tuning | [postgresql.md](postgresql.md) |
| Understand the SQL Server channel/transport internals and tuning | [sqlserver.md](sqlserver.md) |

## Operations

| I want to… | Read |
|---|---|
| Apply the production best practices | [Best practices](operations.md#best-practices) |
| Build the solution and run the unit/integration suites | [Building and testing](operations.md#building-and-testing) |
| Benchmark, stress-test, or load-test the library | [Benchmarking and load testing](operations.md#benchmarking-and-load-testing) |
| Diagnose a symptom — stuck flow, lock-lost redeliveries, slow MongoDB wakes, analyzer errors | [troubleshooting.md](troubleshooting.md) |
| See what's planned next — capabilities and backends — and what was rejected, and why | [roadmap.md](roadmap.md) |
