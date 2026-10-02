"use client";

import { type FormEvent, type ReactNode, useEffect, useMemo, useState } from "react";

type Mode = "public_replay" | "private_live";
type OrderType = "market" | "limit";
type Side = "buy" | "sell";

type Scenario = {
  name: string;
  symbol: string;
  default_side: Side;
  default_order_type: OrderType;
  default_quantity: number;
  default_limit_price: string | null;
  default_latency_ms: number;
  replay_change_percent: string | null;
  market: {
    event_time: string;
    bid_price: string | null;
    bid_size: number | null;
    ask_price: string | null;
    ask_size: number | null;
    last_trade_price: string | null;
  };
};

type QueueResult = {
  run_id: string;
  order_id: string;
  partition: number;
  status: string;
};

type MarketEvent = {
  event_id: string;
  event_type: string;
  symbol: string;
  event_time: string;
  sequence: number;
  bid_price: string | null;
  bid_size: number | null;
  ask_price: string | null;
  ask_size: number | null;
  price: string | null;
  size: number | null;
};

type ExecutionMetrics = {
  average_fill_price: string | null;
  fill_rate: string;
  spread_cost: string | null;
  time_to_first_fill_ms: number | null;
  time_to_completion_ms: number | null;
  latency_impact: string | null;
};

type SessionOrder = {
  order_id: string;
  final_state: string | null;
  remaining_quantity: number | null;
  metrics: ExecutionMetrics | null;
  fills: { fill_id: string; triggering_event_id: string; quantity: number; price: string; filled_at: string }[];
  transitions: { state: string; changed_at: string; triggering_event_id: string | null }[];
};

type SessionSnapshot = {
  events: MarketEvent[];
  market: Record<string, Scenario["market"]>;
  orders: SessionOrder[];
};

type PipelineHealth = {
  queue_depth: number;
  engine_lag: number;
  engine_pending: number;
  persistence_lag: number;
  persistence_pending: number;
  throughput_events_per_second: number;
  processing_latency_ms: number | null;
};

const scenarioTitle = (name: string) => name.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());

const formatPrice = (price: string | null) => price ? `$${Number(price).toFixed(2)}` : "—";

function useReplaySession(queued: QueueResult | undefined) {
  const [session, setSession] = useState<SessionSnapshot>();
  const [connectionState, setConnectionState] = useState("Replay ready");

  useEffect(() => {
    if (!queued) {
      setSession(undefined);
      setConnectionState("Replay ready");
      return;
    }
    let socket: WebSocket | undefined;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let closed = false;
    let attempts = 0;
    const deadline = Date.now() + 90_000;
    const origin = process.env.NEXT_PUBLIC_API_ORIGIN ?? "http://localhost:8000";
    const socketUrl = `${origin.replace(/^http/, "ws")}/ws/v1/sessions/${queued.run_id}`;
    const connect = () => {
      attempts += 1;
      setConnectionState("Connecting to workers");
      socket = new WebSocket(socketUrl);
      socket.onopen = () => setConnectionState("Streaming updates");
      socket.onmessage = (message) => {
        const snapshot = JSON.parse(message.data) as SessionSnapshot;
        setSession(snapshot);
        setConnectionState(snapshot.orders[0]?.final_state ? "Replay complete" : "Streaming updates");
        if (snapshot.orders[0]?.final_state) {
          closed = true;
          socket?.close();
        }
      };
      socket.onerror = () => socket?.close();
      socket.onclose = () => {
        if (!closed && attempts < 10 && Date.now() < deadline) {
          setConnectionState("Waiting for workers");
          retry = setTimeout(connect, 2_000);
        } else if (!closed) {
          setConnectionState("Connection unavailable; run again");
        }
      };
    };
    connect();
    return () => {
      closed = true;
      socket?.close();
      if (retry) {
        clearTimeout(retry);
      }
    };
  }, [queued]);

  return { connectionState, session };
}

export default function ExecutionLab() {
  const [mode, setMode] = useState<Mode>();
  const [scenarios, setScenarios] = useState<Scenario[]>([]);
  const [scenarioName, setScenarioName] = useState("");
  const [side, setSide] = useState<Side>("buy");
  const [orderType, setOrderType] = useState<OrderType>("market");
  const [quantity, setQuantity] = useState("1");
  const [limitPrice, setLimitPrice] = useState("");
  const [latency, setLatency] = useState("0");
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [queued, setQueued] = useState<QueueResult>();
  const [baselineQueued, setBaselineQueued] = useState<QueueResult>();
  const [pipelineHealth, setPipelineHealth] = useState<PipelineHealth>();
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [visibleEventCount, setVisibleEventCount] = useState(0);
  const { connectionState, session } = useReplaySession(queued);
  const { session: baselineSession } = useReplaySession(baselineQueued);

  const scenario = useMemo(() => scenarios.find((item) => item.name === scenarioName), [scenarioName, scenarios]);
  const market = scenario && session?.market[scenario.symbol] ? session.market[scenario.symbol] : scenario?.market;
  const spread = market?.bid_price && market.ask_price
    ? (Number(market.ask_price) - Number(market.bid_price)).toFixed(2)
    : null;
  const activeOrder = session?.orders[0];
  const baselineOrder = baselineSession?.orders[0];
  const visibleEvents = session?.events.slice(0, visibleEventCount) ?? [];
  const replayComplete = Boolean(activeOrder?.final_state);

  useEffect(() => {
    Promise.all([fetch("/backend/configuration"), fetch("/backend/scenarios")])
      .then(async ([configuration, availableScenarios]) => {
        if (!configuration.ok || !availableScenarios.ok) {
          throw new Error("Unable to load the execution terminal.");
        }
        const nextMode = (await configuration.json()).mode as Mode;
        const nextScenarios = (await availableScenarios.json()) as Scenario[];
        setMode(nextMode);
        setScenarios(nextScenarios);
        setScenarioName(nextScenarios[0]?.name ?? "");
      })
      .catch((reason: unknown) => setError(reason instanceof Error ? reason.message : "Unable to load the execution terminal."))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!scenario) {
      return;
    }
    setSide(scenario.default_side);
    setOrderType(scenario.default_order_type);
    setQuantity(String(scenario.default_quantity));
    setLimitPrice(scenario.default_limit_price ?? "");
    setLatency(String(scenario.default_latency_ms));
    setQueued(undefined);
    setBaselineQueued(undefined);
    setPipelineHealth(undefined);
    setError("");
  }, [scenario]);

  useEffect(() => {
    if (!session?.events.length) {
      setVisibleEventCount(0);
      return;
    }
    setVisibleEventCount((current) => current === 0 ? 1 : Math.min(current, session.events.length));
    const timer = setInterval(() => {
      setVisibleEventCount((current) => current >= session.events.length ? current : current + 1);
    }, 600 / playbackSpeed);
    return () => clearInterval(timer);
  }, [playbackSpeed, session?.events]);

  useEffect(() => {
    if (!queued) {
      return;
    }
    let active = true;
    const controller = new AbortController();
    const fetchHealth = async () => {
      try {
        const response = await fetch(`/backend/replays/${queued.run_id}/health?partition=${queued.partition}`, { signal: controller.signal });
        const health = response.ok ? (await response.json()) as PipelineHealth : undefined;
        if (active) {
          setPipelineHealth(health);
        }
      } catch {
        if (active) {
          setPipelineHealth(undefined);
        }
      }
    };
    void fetchHealth();
    const timer = replayComplete ? undefined : setInterval(() => void fetchHealth(), 1_000);
    const deadline = setTimeout(() => {
      active = false;
      controller.abort();
      clearInterval(timer);
    }, 90_000);
    return () => {
      active = false;
      controller.abort();
      clearInterval(timer);
      clearTimeout(deadline);
    };
  }, [queued?.run_id, queued?.partition, replayComplete]);

  async function submitOrder(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!scenario) {
      return;
    }
    setSubmitting(true);
    setError("");
    setQueued(undefined);
    setBaselineQueued(undefined);
    setPipelineHealth(undefined);
    try {
      const payload = {
        scenario_name: scenario.name,
        symbol: scenario.symbol,
        side,
        order_type: orderType,
        quantity: Number(quantity),
        limit_price: orderType === "limit" ? Number(limitPrice) : null,
      };
      const queueOrder = async (latencyMs: number) => {
        const response = await fetch("/backend/orders", {
        method: "POST",
        headers: { "content-type": "application/json" },
          body: JSON.stringify({ ...payload, latency_ms: latencyMs }),
        });
        if (!response.ok) {
          const failure = await response.json().catch(() => null);
          throw new Error(failure?.error?.message ?? "The simulated order could not be queued.");
        }
        return (await response.json()) as QueueResult;
      };
      const primary = await queueOrder(Number(latency));
      setQueued(primary);
      if (Number(latency) > 0) {
        try {
          setBaselineQueued(await queueOrder(0));
        } catch {
          setError("Primary replay queued, but the zero-latency comparison could not be queued.");
        }
      }
    } catch (reason: unknown) {
      setError(reason instanceof Error ? reason.message : "The simulated order could not be queued.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="terminal">
      <header className="terminal-header">
        <div className="brand"><span className="brand-mark"><span>M</span></span><strong>Market Execution Lab</strong><span className="subtle">Execution terminal</span></div>
        <nav className="terminal-nav" aria-label="Terminal sections"><span className="active">Execution</span><span>Replay</span><span>Pipeline</span></nav>
        <div className="terminal-controls">
          <span className="connection"><i /> {connectionState}</span>
          <div className="playback-controls" aria-label="Trace playback speed"><span>Trace</span>{[1, 5, 20].map((speed) => <button className={playbackSpeed === speed ? "active" : ""} key={speed} onClick={() => setPlaybackSpeed(speed)} type="button">{speed}×</button>)}</div>
          <span className="mode">{mode === "private_live" ? "Private live" : "Public replay"}</span>
          <label className="dataset" htmlFor="scenario">Dataset<select id="scenario" value={scenarioName} onChange={(event) => setScenarioName(event.target.value)} disabled={loading}>{scenarios.map((item) => <option key={item.name} value={item.name}>{item.symbol} · {scenarioTitle(item.name)}</option>)}</select></label>
        </div>
      </header>

      {loading ? <p className="notice">Loading replay scenarios…</p> : null}
      {error ? <p className="notice error" role="alert">{error}</p> : null}

      {scenario ? <>
        <section className="terminal-grid">
          <aside className="watchlist panel">
            <div className="panel-heading"><div><span className="eyebrow">Watchlist</span><h2>Replay universe</h2></div><span className="count">{scenarios.length}</span></div>
            <div className="watchlist-columns"><span>Symbol</span><span>Last</span><span>Replay Δ</span></div>
            <div className="watchlist-items">
              {scenarios.map((item) => {
                const latestMarket = item.symbol === scenario.symbol && session?.market[item.symbol] ? session.market[item.symbol] : item.market;
                return <button className={`watchlist-row ${item.name === scenario.name ? "selected" : ""}`} key={item.name} onClick={() => setScenarioName(item.name)} type="button"><strong>{item.symbol}</strong><span>{formatPrice(latestMarket.last_trade_price ?? latestMarket.ask_price ?? latestMarket.bid_price)}</span><Change value={item.replay_change_percent} /></button>;
              })}
            </div>
            <p className="panel-note">Replay Δ compares the first and final fixture event, not live day performance.</p>
            <div className="session-summary"><div><span>Mode</span><strong>{mode === "private_live" ? "Private live" : "Public replay"}</strong></div><div><span>Fixtures</span><strong>{scenarios.length} available</strong></div><div><span>Model</span><strong>Top of book</strong></div></div>
          </aside>

          <section className="market-workspace panel">
            <div className="market-heading">
              <div><span className="eyebrow">Selected instrument</span><div className="symbol-title"><h1>{scenario.symbol}</h1><Change value={scenario.replay_change_percent} /></div></div>
              <div className="timestamp"><span>{session ? "Last event" : "Replay timestamp"}</span><strong>{market ? new Date(market.event_time).toLocaleTimeString() : "—"}</strong></div>
            </div>
            <div className="quote-strip"><Quote label="Bid" price={market?.bid_price ?? null} size={market?.bid_size} tone="bid" /><Quote label="Ask" price={market?.ask_price ?? null} size={market?.ask_size} tone="ask" /><Quote label="Spread" price={spread} /><Quote label="Last trade" price={market?.last_trade_price ?? null} /></div>
            <div className="market-body">
              <div className="depth-panel"><div className="section-label"><span>Top of book</span><span>Visible liquidity</span></div><div className="book-row ask"><span>Ask</span><strong>{formatPrice(market?.ask_price ?? null)}</strong><span>{market?.ask_size ?? "—"} sh</span></div><div className="book-row mid"><span>Mid</span><strong>{market?.bid_price && market.ask_price ? formatPrice(((Number(market.bid_price) + Number(market.ask_price)) / 2).toFixed(2)) : "—"}</strong><span>{spread ? `$${spread} spread` : "—"}</span></div><div className="book-row bid"><span>Bid</span><strong>{formatPrice(market?.bid_price ?? null)}</strong><span>{market?.bid_size ?? "—"} sh</span></div></div>
              <div className="tape-panel"><div className="section-label"><span>Replay price trace</span><span>{session ? `${visibleEvents.length}/${session.events.length} events` : "Waiting for run"}</span></div>{visibleEvents.length ? <PriceTrace events={visibleEvents} /> : <div className="tape-empty"><strong>Waiting for worker output</strong><p>The trace opens when the engine and persistence workers process this replay.</p></div>}<div className="trace-scale"><span>Client playback · {playbackSpeed}×</span><span>Event time →</span></div></div>
            </div>
            <p className="model-note">Top-of-book simulation only. Results do not represent a brokerage order, full depth, or exchange execution.</p>
          </section>

          <form className="order-ticket panel" onSubmit={submitOrder}>
            <div className="panel-heading"><div><span className="eyebrow">Order ticket</span><h2>Simulated order</h2></div><span className="ticket-symbol">{scenario.symbol}</span></div>
            <div className="side-toggle"><button className={side === "buy" ? "buy active" : "buy"} onClick={() => setSide("buy")} type="button">Buy</button><button className={side === "sell" ? "sell active" : "sell"} onClick={() => setSide("sell")} type="button">Sell</button></div>
            <div className="ticket-fields"><Field label="Order type"><select value={orderType} onChange={(event) => setOrderType(event.target.value as OrderType)}><option value="market">Market</option><option value="limit">Limit</option></select></Field><Field label="Quantity"><input min="1" required type="number" value={quantity} onChange={(event) => setQuantity(event.target.value)} /></Field>{orderType === "limit" ? <Field label="Limit price"><input min="0.01" required step="0.01" type="number" value={limitPrice} onChange={(event) => setLimitPrice(event.target.value)} /></Field> : null}<Field label="Artificial latency"><select value={latency} onChange={(event) => setLatency(event.target.value)}><option value="0">0 ms</option><option value="25">25 ms</option><option value="50">50 ms</option><option value="100">100 ms</option><option value="250">250 ms</option></select></Field></div>
            <button className="submit-order" disabled={submitting} type="submit">{submitting ? "Queueing simulation…" : `Queue ${side} simulation`}</button>
            <p className="ticket-note">Orders enter the Redis replay pipeline and remain simulated.</p>
          </form>
        </section>

        <section className="execution-panel panel" aria-live="polite">
          <div className="panel-heading"><div><span className="eyebrow">Execution lifecycle</span><h2>{activeOrder?.final_state ? `Order ${activeOrder.final_state}` : queued ? "Simulation queued" : "No active simulation"}</h2></div><span className={activeOrder?.final_state || queued ? "status queued-status" : "status"}>{activeOrder?.final_state ?? (queued ? "Queued" : "Standing by")}</span></div>
          {activeOrder ? <ExecutionDetails order={activeOrder} events={session?.events ?? []} /> : queued ? <div className="queue-details"><span>Run ID <code>{queued.run_id}</code></span><span>Order ID <code>{queued.order_id}</code></span><span>Partition {queued.partition}</span><span>{connectionState}</span></div> : <div className="empty-lifecycle"><span>01</span><p>Configure a market or limit order in the ticket. Its queue, activation, fills, and explanation will appear here.</p></div>}
          {queued ? <div className="replay-insights"><ReplayComparison latency={Number(latency)} primary={activeOrder} baseline={baselineOrder} /><PipelineStatus health={pipelineHealth} connectionState={connectionState} /></div> : null}
        </section>
        <footer className="status-ticker"><span><i /> {mode === "private_live" ? "Private live mode" : "Generated replay fixtures"}</span><span>Symbols {scenarios.length}</span><span>Execution model: top of book</span><span>Redis Streams → workers → PostgreSQL</span><span>{connectionState}</span></footer>
      </> : null}
    </main>
  );
}

function Field({ children, label }: { children: ReactNode; label: string }) {
  return <label className="field"><span>{label}</span>{children}</label>;
}

function Quote({ label, price, size, tone }: { label: string; price: string | null; size?: number | null; tone?: string }) {
  return <div className={`quote ${tone ?? ""}`}><span>{label}</span><strong>{formatPrice(price)}</strong>{size ? <small>{size} shares</small> : null}</div>;
}

function Change({ value }: { value: string | null }) {
  if (!value) {
    return <span className="change flat">—</span>;
  }
  const amount = Number(value);
  const direction = amount > 0 ? "up" : amount < 0 ? "down" : "flat";
  return <span className={`change ${direction}`}>{amount > 0 ? "+" : ""}{amount.toFixed(2)}%</span>;
}

function PriceTrace({ events }: { events: MarketEvent[] }) {
  const pricedEvents = events.map((event) => ({ event, price: event.price ?? event.ask_price ?? event.bid_price })).filter((item): item is { event: MarketEvent; price: string } => item.price !== null);
  const prices = pricedEvents.map((item) => Number(item.price));
  const low = Math.min(...prices);
  const high = Math.max(...prices);
  const points = pricedEvents.map(({ price }, index) => {
    const x = pricedEvents.length === 1 ? 50 : (index / (pricedEvents.length - 1)) * 100;
    const y = high === low ? 50 : 88 - ((Number(price) - low) / (high - low)) * 76;
    return `${x},${y}`;
  }).join(" ");

  return <div className="trace-content"><svg aria-label="Replay price trace" viewBox="0 0 100 100" preserveAspectRatio="none"><polyline fill="none" points={points} stroke="#00d971" strokeWidth="1.2" vectorEffect="non-scaling-stroke" />{pricedEvents.map(({ event, price }, index) => { const [x, y] = points.split(" ")[index].split(","); return <circle cx={x} cy={y} fill={event.event_type === "trade" ? "#f5ca4a" : "#00d971"} key={event.event_id} r="1.6" vectorEffect="non-scaling-stroke" />; })}</svg><div className="event-tape">{events.slice(-4).reverse().map((event) => <div key={event.event_id}><span>{event.event_type}</span><strong>{formatPrice(event.price ?? event.ask_price ?? event.bid_price)}</strong><small>{new Date(event.event_time).toLocaleTimeString()}</small></div>)}</div></div>;
}

function ExecutionDetails({ order, events }: { order: SessionOrder; events: MarketEvent[] }) {
  return <div className="execution-details">
    {order.metrics ? <div className="execution-metrics"><Metric label="Fill rate" value={`${(Number(order.metrics.fill_rate) * 100).toFixed(0)}%`} /><Metric label="Average fill" value={formatPrice(order.metrics.average_fill_price)} /><Metric label="Spread cost" value={formatPrice(order.metrics.spread_cost)} /><Metric label="Latency impact" value={formatPrice(order.metrics.latency_impact)} /><Metric label="Time to fill" value={order.metrics.time_to_completion_ms === null ? "—" : `${order.metrics.time_to_completion_ms} ms`} /></div> : null}
    <div className="transition-list">{order.transitions.map((transition) => { const event = events.find((item) => item.event_id === transition.triggering_event_id); return <div key={`${transition.state}-${transition.changed_at}`}><span className="transition-state">{transition.state}</span><span>{event ? `${event.event_type} ${event.event_id}` : "order submitted"}</span><time>{new Date(transition.changed_at).toLocaleTimeString()}</time></div>; })}</div>
    {order.fills.map((fill) => <p className="fill-note" key={fill.fill_id}>Filled {fill.quantity} shares at {formatPrice(fill.price)} from event <code>{fill.triggering_event_id}</code>.</p>)}
  </div>;
}

function ReplayComparison({ latency, primary, baseline }: { latency: number; primary: SessionOrder | undefined; baseline: SessionOrder | undefined }) {
  if (latency === 0) {
    return <section className="replay-comparison"><div className="insight-heading"><span className="eyebrow">Parameter comparison</span><strong>Zero-latency baseline</strong></div><p>Choose artificial latency to run this order against the same 0 ms replay baseline.</p></section>;
  }
  if (!primary?.metrics || !baseline?.metrics) {
    return <section className="replay-comparison"><div className="insight-heading"><span className="eyebrow">Parameter comparison</span><strong>{latency} ms vs 0 ms baseline</strong></div><p>Running the same order against a zero-latency fixture baseline…</p></section>;
  }
  const fillDelta = primary.metrics.average_fill_price && baseline.metrics.average_fill_price
    ? Number(primary.metrics.average_fill_price) - Number(baseline.metrics.average_fill_price)
    : null;
  const completionDelta = primary.metrics.time_to_completion_ms !== null && baseline.metrics.time_to_completion_ms !== null
    ? primary.metrics.time_to_completion_ms - baseline.metrics.time_to_completion_ms
    : null;
  return <section className="replay-comparison"><div className="insight-heading"><span className="eyebrow">Parameter comparison</span><strong>{latency} ms vs 0 ms baseline</strong></div><div className="comparison-metrics"><Metric label="Fill price delta" value={fillDelta === null ? "—" : `${fillDelta >= 0 ? "+" : ""}$${fillDelta.toFixed(2)}`} /><Metric label="Completion delta" value={completionDelta === null ? "—" : `${completionDelta >= 0 ? "+" : ""}${completionDelta} ms`} /><Metric label="Baseline fill" value={formatPrice(baseline.metrics.average_fill_price)} /></div><p>Both runs use the same generated fixture and order parameters; only artificial latency changes.</p></section>;
}

function PipelineStatus({ health, connectionState }: { health: PipelineHealth | undefined; connectionState: string }) {
  return <section className="pipeline-status"><div className="insight-heading"><span className="eyebrow">Pipeline health</span><strong>{connectionState}</strong></div>{health ? <div className="health-metrics"><Metric label="Queue depth" value={String(health.queue_depth)} /><Metric label="Workers pending" value={String(health.engine_pending + health.persistence_pending)} /><Metric label="Throughput" value={`${health.throughput_events_per_second.toFixed(0)} events/s`} /><Metric label="Engine time" value={health.processing_latency_ms === null ? "—" : `${health.processing_latency_ms.toFixed(1)} ms`} /></div> : <p>{connectionState === "Replay complete" ? "Replay persisted successfully. Pipeline metrics are unavailable." : "Pipeline metrics unavailable; the replay connection is tracked above."}</p>}</section>;
}

function Metric({ label, value }: { label: string; value: string }) {
  return <div><span>{label}</span><strong>{value}</strong></div>;
}
