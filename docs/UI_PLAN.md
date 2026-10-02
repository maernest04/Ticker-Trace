# UI Plan

## Current implementation boundary

Public mode selects nine generated datasets, submits independent replay orders, compares configured latency against the same fixture at zero latency, and animates completed recorded traces at 1×/5×/20×. These controls do not change backend processing speed. Longer datasets show 300 events with dataset names distinguishing experiments for the same stock.

Private mode uses a separate terminal component: live subscription controls (up to ten symbols), real quote/provider freshness enforced server-side, buy/sell market/limit ticket, continuous session snapshots, and durable fill-event explanations. Quotes older than 15 seconds disable the ticket; connection/backend errors are visible. No zero-latency counterfactual is offered against changing live conditions. Private mode has no account authentication and must be loopback/network protected.

The `/system` route and richer monitoring below are historical proposed scope; they are not implemented. There are no portfolio, candlestick indicators, real-money orders, or brokerage connections. Snapshot event history is bounded to the latest 500 persisted events; older triggering quotes are fetched separately for fill explanations and need not appear in the recent trace.

## Original UI proposal

## UI Objective

The interface lets a user submit a simulated stock order, watch its state change against a live or replayed quote stream, and understand exactly why it filled or remained open. It should not become a generic collection of financial charts.

## Primary User Journey

1. The user enters the execution lab and sees whether the environment is public replay or private live mode.
2. The user selects a supported stock and, in replay mode, a dataset and playback speed.
3. The application displays the latest bid, ask, visible size, recent trades, event time, and freshness.
4. The user configures buy/sell, market/limit, quantity, optional limit price, and artificial latency.
5. The user submits the simulated order.
6. The interface displays submission, activation, partial fills, completion or expiration, and the triggering market events.
7. The result summarizes weighted average price, fill rate, spread cost, slippage, time to fill, and latency impact.
8. In replay mode, the user modifies one parameter and reruns the scenario for comparison.

## Routes and Views

### `/` Execution Lab

The primary and initially only product route.

- Mode badge: public replay or private live
- Symbol and replay-dataset selectors
- Current bid, ask, spread, visible size, last trade, and freshness
- Compact quote/trade timeline
- Simulated-order form
- Order-state timeline with triggering-event explanations
- Execution summary and zero-latency comparison when replaying
- Persistent execution-model limitation notice

### `/system` System Health

Added after the primary workflow works.

- Events received and processed per second
- End-to-end p50, p95, and p99 latency
- Queue depth, pending age, and consumer lag by partition
- Worker status and last heartbeat
- Duplicate suppression, retries, dead-letter events, and reconnects

No portfolio, news, social, account, or general market-dashboard pages are included in the MVP.

## Order Form

- Symbol: selected from the supported server-provided list
- Side: buy or sell
- Type: market or limit
- Quantity: positive whole-share amount within configured demo limits
- Limit price: required only for limit orders
- Artificial latency: predefined safe values plus zero
- Submit button disabled when data is stale, replay is stopped, or the service is unhealthy

The server remains authoritative for validation and execution.

## Execution Explanation

Every transition displays:

- State and event timestamp
- Human-readable reason
- Triggering market-event identifier
- Bid, ask, and visible size used by the rule
- Filled and remaining quantity
- Whether configured latency changed the outcome

## Real-Time Update Strategy

- WebSockets for live updates that users need immediately
- REST for initial page state and historical queries
- Backend coalescing and client-side batching so the browser does not render every market event
- Explicit reconnect and stale-data indicators
- Full state refresh after reconnect before incremental updates resume

## Required UI States

- Loading
- No data
- Disconnected
- Reconnecting
- Partial dependency failure
- Invalid user input
- Rate limited
- Replay complete
- Order partially filled
- Order remains open at the end of replay
- Unsupported live mode in the public deployment

## Design Principles

- Emphasize the unique workflow over decorative dashboards.
- Show timestamps and data freshness clearly.
- Do not imply that simulated results are real executions.
- Keep the top-of-book model and missing Level 2 behavior visible.
- Keep system metrics understandable to non-experts where possible.
- Avoid displaying more streaming data than a browser can usefully render.
- Prefer an execution timeline and explanation over decorative market charts.

## UI Acceptance Criteria

- [ ] A first-time user understands the product's purpose.
- [ ] The primary workflow requires no terminal commands.
- [ ] Live data visibly updates without manual refresh.
- [ ] Connection loss and stale data are unmistakable.
- [ ] The interface remains responsive during peak backend event rates.
- [ ] Simulated, historical, and live data are clearly distinguished.
- [ ] Every fill can be traced to the quote/event that triggered it.
- [ ] The UI never exposes Alpaca credentials or restricted live data in public mode.
- [ ] A user can compare one replayed order against its zero-latency baseline.
