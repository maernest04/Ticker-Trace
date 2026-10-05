# Product Specification

## Recorded-market extension

The private local workflow now supports saving an observed stock interval and comparing two independent simulated orders with exactly one controlled configuration change. Its central question is: which source event first explains a difference in execution? Quantity, latency, limit-price, and market/limit changes reuse the existing top-of-book rules; these are model explanations, not predictions of broker fills.

The intended user task is to select an interval, vary a parameter, inspect the responsible quote, and explain the outcome without terminal access. Actual user usefulness and a reason to return are not validated yet. Retain execution simulation rather than adding a reconciliation ledger; do not claim originality merely because recording or replay exists.

## Status

Approved for MVP implementation.

## Product Summary

Market Execution Lab is a web application for technically curious traders and quant students who want to understand how simulated stock orders behave under changing quotes, visible liquidity, and processing delay. It streams live or replayed market events through a distributed execution pipeline and explains the resulting fills, spread cost, slippage, and time to fill.

## Target User

- **Primary user:** A technically curious retail trader, finance student, or aspiring quant.
- **Current workflow:** Uses charts or basic paper trading, which reports an outcome without explaining the event sequence that produced it.
- **Main frustration:** Cannot easily compare how order type, quantity, timing, and latency alter an execution.
- **Technical experience:** Understands basic stock orders but does not require distributed-systems knowledge.
- **Return reason:** Tests new symbols, order sizes, latency settings, and historical scenarios.

## Problem Statement

Paper-trading products commonly report that an order filled, but they do not make the execution model or contributing market events easy to inspect. Users need a transparent environment where they can submit a simulated order, observe when it becomes executable, and understand how the visible bid/ask, quote size, spread, and delay affected the result.

## Originality Test

- It explains execution outcomes instead of only displaying prices or reporting a fill.
- It exposes the exact normalized events and deterministic rules used by the simulator.
- It compares order behavior under configurable delay and replay speed.
- It demonstrates recovery, idempotency, event ordering, and worker scaling using a user-visible financial workflow.

## Primary User Workflow

1. The user chooses live-private or public-replay mode and selects a supported stock.
2. The application displays the current best bid, ask, recent trades, data freshness, and selected scenario time.
3. The user submits a simulated buy or sell market/limit order with quantity and optional artificial latency.
4. The distributed engine activates the order at the correct event time and evaluates it against subsequent quote updates.
5. The application displays fills, remaining quantity, weighted average price, spread cost, slippage, time to fill, and the market events that caused each state transition.
6. In replay mode, the user reruns the same order with different parameters and compares results.

## Execution Model

The MVP uses top-of-book trades and quotes rather than full order-book depth.

- A submitted order receives a deterministic activation time equal to submission event time plus configured latency.
- A market buy consumes visible ask size; a market sell consumes visible bid size.
- A limit buy becomes eligible when the ask is at or below its limit; a limit sell becomes eligible when the bid is at or above its limit.
- Orders may fill partially as eligible quote updates arrive.
- Every fill records the triggering event and simulation rule.
- The same normalized input and configuration must produce the same output.

The simulator does not model queue priority, hidden liquidity, market impact, exchange routing, fees, or guaranteed real-world fill probability. The UI must state these limitations anywhere execution results are shown.

## MVP Features

- Private live mode using real IEX quotes and trades for an operator-selected list of up to 10 liquid symbols
- Public demo mode using generated or explicitly redistribution-safe replay events
- Buy and sell market orders
- Buy and sell limit orders
- Partial fills against visible top-of-book size
- Configurable artificial order latency
- Current quote, recent-trade, order-state, and execution-explanation views
- Replay at 1x, 10x, and maximum safe speed
- Deterministic results for identical inputs
- Redis-backed current state and independently runnable event workers
- Metrics for throughput, latency, lag, retries, duplicate suppression, and recovery

## Non-Goals

- Real-money trade execution
- Investment advice or price prediction
- Guaranteed investment recommendations
- Full Level 2 reconstruction or exchange-accurate fill simulation
- Queue position, hidden liquidity, routing, market impact, or transaction fees
- Options, crypto, futures, and other asset classes
- Multiple market-data providers
- User accounts, portfolios, social features, and payments
- Kafka, Kubernetes, TimescaleDB, or features added only to increase the technology count
- Production-scale compliance or exchange connectivity

## Success Criteria

- [ ] A first-time user can explain the product after viewing the landing state for one minute.
- [ ] A user can submit an order and understand its outcome without terminal access.
- [ ] Identical replay input and configuration produce identical fills and metrics.
- [ ] One million generated events complete with zero duplicate fills.
- [ ] The pipeline sustains twice the observed peak rate of the selected live-symbol set for ten minutes without continuously increasing consumer lag.
- [ ] A killed worker resumes unfinished events without losing acknowledged data.
- [ ] Public mode exposes no vendor credentials or restricted live market data.
- [ ] Every published performance claim includes its dataset, duration, hardware, worker count, and configuration.

## Remaining Non-Blocking Decisions

No product decision remains open that blocks implementation.

Deployment-provider selection and final benchmark thresholds may be completed during their implementation phases because they do not change the MVP domain model.
