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

const scenarioTitle = (name: string) => name.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());

const formatPrice = (price: string | null) => price ? `$${Number(price).toFixed(2)}` : "—";

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

  const scenario = useMemo(() => scenarios.find((item) => item.name === scenarioName), [scenarioName, scenarios]);
  const spread = scenario?.market.bid_price && scenario.market.ask_price
    ? (Number(scenario.market.ask_price) - Number(scenario.market.bid_price)).toFixed(2)
    : null;

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
    setError("");
  }, [scenario]);

  async function submitOrder(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!scenario) {
      return;
    }
    setSubmitting(true);
    setError("");
    setQueued(undefined);
    try {
      const response = await fetch("/backend/orders", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          scenario_name: scenario.name,
          symbol: scenario.symbol,
          side,
          order_type: orderType,
          quantity: Number(quantity),
          limit_price: orderType === "limit" ? Number(limitPrice) : null,
          latency_ms: Number(latency),
        }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        throw new Error(payload?.error?.message ?? "The simulated order could not be queued.");
      }
      setQueued((await response.json()) as QueueResult);
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
          <span className="connection"><i /> Replay ready</span>
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
              {scenarios.map((item) => <button className={`watchlist-row ${item.name === scenario.name ? "selected" : ""}`} key={item.name} onClick={() => setScenarioName(item.name)} type="button"><strong>{item.symbol}</strong><span>{formatPrice(item.market.last_trade_price ?? item.market.ask_price ?? item.market.bid_price)}</span><Change value={item.replay_change_percent} /></button>)}
            </div>
            <p className="panel-note">Replay Δ compares the first and final fixture event, not live day performance.</p>
            <div className="session-summary"><div><span>Mode</span><strong>{mode === "private_live" ? "Private live" : "Public replay"}</strong></div><div><span>Fixtures</span><strong>{scenarios.length} available</strong></div><div><span>Model</span><strong>Top of book</strong></div></div>
          </aside>

          <section className="market-workspace panel">
            <div className="market-heading">
              <div><span className="eyebrow">Selected instrument</span><div className="symbol-title"><h1>{scenario.symbol}</h1><Change value={scenario.replay_change_percent} /></div></div>
              <div className="timestamp"><span>Replay timestamp</span><strong>{new Date(scenario.market.event_time).toLocaleTimeString()}</strong></div>
            </div>
            <div className="quote-strip"><Quote label="Bid" price={scenario.market.bid_price} size={scenario.market.bid_size} tone="bid" /><Quote label="Ask" price={scenario.market.ask_price} size={scenario.market.ask_size} tone="ask" /><Quote label="Spread" price={spread} /><Quote label="Last trade" price={scenario.market.last_trade_price} /></div>
            <div className="market-body">
              <div className="depth-panel"><div className="section-label"><span>Top of book</span><span>Visible liquidity</span></div><div className="book-row ask"><span>Ask</span><strong>{formatPrice(scenario.market.ask_price)}</strong><span>{scenario.market.ask_size ?? "—"} sh</span></div><div className="book-row mid"><span>Mid</span><strong>{scenario.market.bid_price && scenario.market.ask_price ? formatPrice(((Number(scenario.market.bid_price) + Number(scenario.market.ask_price)) / 2).toFixed(2)) : "—"}</strong><span>{spread ? `$${spread} spread` : "—"}</span></div><div className="book-row bid"><span>Bid</span><strong>{formatPrice(scenario.market.bid_price)}</strong><span>{scenario.market.bid_size ?? "—"} sh</span></div></div>
              <div className="tape-panel"><div className="section-label"><span>Replay price trace</span><span>4B event stream</span></div><div className="tape-empty"><strong>Replay state loaded</strong><p>Phase 4B adds the bounded event tape and price trace that explain each fill.</p></div><div className="trace-scale"><span>Bid / ask</span><span>Event time →</span></div></div>
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
          <div className="panel-heading"><div><span className="eyebrow">Execution lifecycle</span><h2>{queued ? "Simulation queued" : "No active simulation"}</h2></div><span className={queued ? "status queued-status" : "status"}>{queued ? "Queued" : "Standing by"}</span></div>
          {queued ? <div className="queue-details"><span>Run ID <code>{queued.run_id}</code></span><span>Order ID <code>{queued.order_id}</code></span><span>Partition {queued.partition}</span><span>Next: workers process the replay in Phase 4B.</span></div> : <div className="empty-lifecycle"><span>01</span><p>Configure a market or limit order in the ticket. Its queue, activation, fills, and explanation will appear here.</p></div>}
        </section>
        <footer className="status-ticker"><span><i /> {mode === "private_live" ? "Private live mode" : "Generated replay fixtures"}</span><span>Symbols {scenarios.length}</span><span>Execution model: top of book</span><span>Redis Streams → workers → PostgreSQL</span><span>Event trace arrives in 4B</span></footer>
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
