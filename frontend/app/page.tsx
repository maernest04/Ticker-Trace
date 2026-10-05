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
  symbol: string;
  final_state: string | null;
  remaining_quantity: number | null;
  metrics: ExecutionMetrics | null;
  fills: { fill_id: string; triggering_event_id: string; quantity: number; price: string; filled_at: string }[];
  transitions: { state: string; changed_at: string; triggering_event_id: string | null; reason?: string | null }[];
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
      setVisibleEventCount((current) => {
        if (current >= session.events.length) {
          clearInterval(timer);
          return current;
        }
        return current + 1;
      });
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

  if (mode === "private_live") {
    return <PrivateLiveTerminal />;
  }

  return (
    <main className="terminal">
      <header className="terminal-header">
        <div className="brand"><span className="brand-mark"><span>M</span></span><strong>Market Execution Lab</strong><span className="subtle">Execution terminal</span></div>
        <nav className="terminal-nav" aria-label="Terminal sections"><span className="active">Execution</span><span>Replay</span><span>Pipeline</span></nav>
        <div className="terminal-controls">
          <span className="connection"><i /> {connectionState}</span>
          <div className="playback-controls" aria-label="Trace playback speed"><span>Trace</span>{[1, 5, 20].map((speed) => <button className={playbackSpeed === speed ? "active" : ""} key={speed} onClick={() => setPlaybackSpeed(speed)} type="button">{speed}×</button>)}</div>
          <span className="mode">Public replay</span>
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
                const latestMarket = item.name === scenario.name && session?.market[item.symbol] ? session.market[item.symbol] : item.market;
                return <button className={`watchlist-row ${item.name === scenario.name ? "selected" : ""}`} key={item.name} title={scenarioTitle(item.name)} onClick={() => setScenarioName(item.name)} type="button"><strong>{item.symbol}<small className="dataset-name">{scenarioTitle(item.name)}</small></strong><span>{formatPrice(latestMarket.last_trade_price ?? latestMarket.ask_price ?? latestMarket.bid_price)}</span><Change value={item.replay_change_percent} /></button>;
              })}
            </div>
            <p className="panel-note">Replay Δ compares the first and final fixture event, not live day performance.</p>
            <div className="session-summary"><div><span>Mode</span><strong>Public replay</strong></div><div><span>Fixtures</span><strong>{scenarios.length} available</strong></div><div><span>Model</span><strong>Top of book</strong></div></div>
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
        <footer className="status-ticker"><span><i /> Generated replay fixtures</span><span>Datasets {scenarios.length}</span><span>Execution model: top of book</span><span>Redis Streams → workers → PostgreSQL</span><span>{connectionState}</span></footer>
      </> : null}
    </main>
  );
}

function Field({ children, label }: { children: ReactNode; label: string }) {
  return <label className="field"><span>{label}</span>{children}</label>;
}

type LiveStatus = {
  run_id: string;
  symbols: string[];
  status: string;
  fresh: boolean;
  market: Record<string, Scenario["market"] & { ingested_at?: string; quote_ingested_at?: string; quote_event_time?: string }>;
};

function PrivateLiveTerminal() {
  const [live, setLive] = useState<LiveStatus>();
  const [session, setSession] = useState<SessionSnapshot>();
  const [symbol, setSymbol] = useState("");
  const [symbols, setSymbols] = useState("");
  const [side, setSide] = useState<Side>("buy");
  const [orderType, setOrderType] = useState<OrderType>("market");
  const [quantity, setQuantity] = useState("50");
  const [limitPrice, setLimitPrice] = useState("");
  const [latency, setLatency] = useState("0");
  const [orderId, setOrderId] = useState("");
  const [fillEvents, setFillEvents] = useState<MarketEvent[]>([]);
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [connection, setConnection] = useState("Waiting for private ingestion");

  useEffect(() => {
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    const controller = new AbortController();
    const refresh = async () => {
      try {
        const response = await fetch("/backend/live/session", { signal: controller.signal });
        if (!response.ok) {
          throw new Error("Start the private ingestion and live workers to open a session.");
        }
        const status = await response.json() as LiveStatus;
        if (active) {
          setLive(status);
          setSymbol((current) => status.symbols.includes(current) ? current : status.symbols[0] ?? "");
        }
      } catch (reason) {
        if (active) {
          setLive(undefined);
          setError(reason instanceof Error ? reason.message : "Private feed unavailable");
        }
      } finally {
        if (active) {
          timer = setTimeout(() => void refresh(), 1_000);
        }
      }
    };
    void refresh();
    return () => { active = false; controller.abort(); clearTimeout(timer); };
  }, []);

  useEffect(() => {
    if (!live?.run_id) {
      return;
    }
    let active = true;
    let socket: WebSocket;
    let retry: ReturnType<typeof setTimeout>;
    let attempts = 0;
    setSession(undefined);
    setOrderId("");
    const connect = () => {
      const origin = process.env.NEXT_PUBLIC_API_ORIGIN ?? "http://localhost:8000";
      socket = new WebSocket(`${origin.replace(/^http/, "ws")}/ws/v1/sessions/${live.run_id}`);
      socket.onopen = () => { attempts = 0; setConnection("Private session connected"); };
      socket.onmessage = (message) => { if (active) { setSession(JSON.parse(message.data) as SessionSnapshot); } };
      socket.onerror = () => socket.close();
      socket.onclose = () => {
        if (active) { setConnection("Private session disconnected"); }
        if (active && attempts++ < 10) {
          retry = setTimeout(connect, 2_000);
        }
      };
    };
    connect();
    return () => { active = false; socket?.close(); clearTimeout(retry); };
  }, [live?.run_id]);

  const market = live?.market[symbol];
  const quoteTime = market?.quote_ingested_at || market?.ingested_at;
  const age = quoteTime ? Math.max(0, (Date.now() - Date.parse(quoteTime)) / 1000, (Date.now() - Date.parse(market?.quote_event_time ?? market?.event_time ?? quoteTime)) / 1000) : null;
  const fresh = Boolean(live?.fresh && market?.ask_price && age !== null && age <= 15);
  const events = session?.events.filter((event) => event.symbol === symbol) ?? [];
  const order = session?.orders.find((item) => item.order_id === orderId);

  useEffect(() => {
    const controller = new AbortController();
    setFillEvents([]);
    if (orderId && order?.fills.length) {
      fetch(`/backend/orders/${orderId}/events`, { signal: controller.signal })
        .then(async (response) => {
          if (!response.ok) {
            throw new Error("Fill quote history unavailable");
          }
          const events = await response.json() as MarketEvent[];
          if (!controller.signal.aborted) {
            setFillEvents(events);
          }
        })
        .catch(() => { if (!controller.signal.aborted) { setError("Fill quote history unavailable; durable event IDs remain shown."); } });
    }
    return () => controller.abort();
  }, [orderId, order?.fills.length]);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      const response = await fetch("/backend/orders", { method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ run_id: live?.run_id, symbol, side, order_type: orderType, quantity: Number(quantity),
          limit_price: orderType === "limit" ? Number(limitPrice) : null, latency_ms: Number(latency) }) });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.error?.message ?? "Live simulation rejected");
      }
      setOrderId(result.order_id);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Live simulation unavailable");
    } finally {
      setSubmitting(false);
    }
  }

  async function updateSymbols() {
    setError("");
    try {
      const response = await fetch("/backend/live/symbols", { method: "PUT", headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "Private live", symbols: symbols.split(",").map((value) => value.trim()).filter(Boolean) }) });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.error?.message ?? "Subscription update rejected");
      }
      setSymbols("");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Subscription update unavailable");
    }
  }

  return <main className="terminal">
    <header className="terminal-header"><div className="brand"><strong>Market Execution Lab</strong></div><a href="/recorded">Recorded comparisons</a><span className="mode">Private live · Alpaca IEX</span><span className="connection">{connection}</span></header>
    {error ? <p className="notice error" role="alert">{error}</p> : null}
    <section className="terminal-grid">
      <aside className="watchlist panel"><div className="panel-heading"><h2>Live watchlist</h2></div>
        {live?.symbols.map((item) => <button className={`watchlist-row ${item === symbol ? "selected" : ""}`} key={item} onClick={() => { setSymbol(item); setOrderId(""); }} type="button"><strong>{item}</strong><span>{formatPrice(live.market[item]?.ask_price ?? null)}</span></button>)}
        <div className="ticket-fields"><Field label="Symbols (up to 10)"><input placeholder="AAPL, MSFT, NVDA" value={symbols} onChange={(event) => setSymbols(event.target.value)} /></Field><button type="button" disabled={!live || !symbols.trim()} onClick={() => void updateSymbols()}>Update subscriptions</button></div>
        <p className="panel-note">Private operator session. Subscription changes need no environment edits. Continuous live operation uses resources while running.</p>
      </aside>
      <section className="market-workspace panel"><div className="market-heading"><h1>{symbol || "Waiting for feed"}</h1><span>{live?.status ?? "Unavailable"} · {age === null ? "No quote" : `${age.toFixed(1)}s old`} · {fresh ? "Fresh" : "Stale — orders disabled"}</span></div>
        <div className="quote-strip"><Quote label="Bid" price={market?.bid_price ?? null} size={market?.bid_size} tone="bid" /><Quote label="Ask" price={market?.ask_price ?? null} size={market?.ask_size} tone="ask" /><Quote label="Last trade" price={market?.last_trade_price ?? null} /></div>
        <div className="tape-panel"><div className="section-label">Real normalized market events</div>{events.length ? <PriceTrace events={events} /> : <p>Waiting for live persistence worker output…</p>}</div>
        <p className="model-note">Simulated orders only. Each order is an independent top-of-book experiment, without shared liquidity, queue position, hidden depth, market impact, fees, or real brokerage execution.</p>
      </section>
      <form className="order-ticket panel" onSubmit={submit}><div className="panel-heading"><h2>Live simulated order</h2></div><div className="ticket-fields">
        <Field label="Side"><select value={side} onChange={(event) => setSide(event.target.value as Side)}><option value="buy">Buy</option><option value="sell">Sell</option></select></Field>
        <Field label="Order type"><select value={orderType} onChange={(event) => setOrderType(event.target.value as OrderType)}><option value="market">Market</option><option value="limit">Limit</option></select></Field>
        <Field label="Quantity"><input required type="number" min="1" max="100000" value={quantity} onChange={(event) => setQuantity(event.target.value)} /></Field>
        {orderType === "limit" ? <Field label="Limit price"><input required type="number" min="0.01" step="0.01" value={limitPrice} onChange={(event) => setLimitPrice(event.target.value)} /></Field> : null}
        <Field label="Artificial latency (ms)"><input required type="number" min="0" max="60000" value={latency} onChange={(event) => setLatency(event.target.value)} /></Field>
      </div><button className="submit-order" disabled={submitting || !fresh} type="submit">{submitting ? "Submitting…" : "Submit private simulation"}</button><p className="ticket-note">Uses subsequent real quotes in this session, not generated scenarios. No zero-latency comparison is claimed for changing live conditions.</p></form>
    </section>
    <section className="execution-panel panel"><h2>Execution lifecycle</h2>{order ? <ExecutionDetails order={order} events={[...(session?.events ?? []), ...fillEvents]} /> : <p>{orderId ? "Waiting for engine and persistence…" : "Submit an independent simulated order."}</p>}</section>
  </main>;
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
  const firstTime = Date.parse(pricedEvents[0]?.event.event_time ?? "");
  const lastTime = Date.parse(pricedEvents.at(-1)?.event.event_time ?? "");
  const points = pricedEvents.map(({ event, price }) => {
    const x = lastTime === firstTime ? 50 : ((Date.parse(event.event_time) - firstTime) / (lastTime - firstTime)) * 100;
    const y = high === low ? 50 : 88 - ((Number(price) - low) / (high - low)) * 76;
    return `${x},${y}`;
  }).join(" ");

  return <div className="trace-content"><svg aria-label="Replay price trace" viewBox="0 0 100 100" preserveAspectRatio="none"><polyline fill="none" points={points} stroke="#00d971" strokeWidth="1.2" vectorEffect="non-scaling-stroke" />{pricedEvents.map(({ event }, index) => { const [x, y] = points.split(" ")[index].split(","); return <circle cx={x} cy={y} fill={event.event_type === "market.trade.v1" ? "#f5ca4a" : "#00d971"} key={event.event_id} r={pricedEvents.length > 100 ? 0.3 : 1.6} vectorEffect="non-scaling-stroke" />; })}</svg><div className="event-tape">{events.slice(-4).reverse().map((event) => <div key={event.event_id}><span>{event.event_type}</span><strong>{formatPrice(event.price ?? event.ask_price ?? event.bid_price)}</strong><small>{new Date(event.event_time).toLocaleTimeString()}</small></div>)}</div></div>;
}

function ExecutionDetails({ order, events }: { order: SessionOrder; events: MarketEvent[] }) {
  return <div className="execution-details">
    {order.metrics ? <div className="execution-metrics"><Metric label="Fill rate" value={`${(Number(order.metrics.fill_rate) * 100).toFixed(0)}%`} /><Metric label="Average fill" value={formatPrice(order.metrics.average_fill_price)} /><Metric label="Spread cost" value={formatPrice(order.metrics.spread_cost)} /><Metric label="Latency impact" value={formatPrice(order.metrics.latency_impact)} /><Metric label="Time to fill" value={order.metrics.time_to_completion_ms === null ? "—" : `${order.metrics.time_to_completion_ms} ms`} /></div> : null}
    <div className="transition-list">{order.transitions.map((transition) => { const event = events.find((item) => item.event_id === transition.triggering_event_id); return <div key={`${transition.state}-${transition.changed_at}`}><span className="transition-state">{transition.state}</span><span>{transition.reason ?? (event ? `${event.event_type} ${event.event_id}` : "order submitted")}</span><time>{new Date(transition.changed_at).toLocaleTimeString()}</time></div>; })}</div>
    <div className="fill-list">{order.fills.map((fill) => { const event = events.find((item) => item.event_id === fill.triggering_event_id); return <p className="fill-note" key={fill.fill_id}>Filled {fill.quantity} shares at {formatPrice(fill.price)} from event <code>{fill.triggering_event_id}</code>. {event ? `Quote: bid ${formatPrice(event.bid_price)} × ${event.bid_size}, ask ${formatPrice(event.ask_price)} × ${event.ask_size}.` : ""}</p>; })}</div>
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
