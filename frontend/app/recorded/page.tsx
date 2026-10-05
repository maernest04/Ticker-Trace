"use client";

import { type FormEvent, useEffect, useState } from "react";

type Recording = { recording_id: string; symbols: string[]; status: string; feed: string; started_at: string; ended_at: string | null; event_count: number; byte_count: number; checksum: string | null; reason: string | null; quality: string[]; model_version: string };
type EventRow = { index: number; cursor: string; event: { event_id: string; event_type: string; event_time: string; bid_price?: string; ask_price?: string; bid_size?: number; ask_size?: number; price?: string } };
type Decision = { state: string; eligible: boolean; filled_quantity: number; event_fill_quantity: number; event_fill_price: string | null };
type TraceRow = { index: number; event_id: string; decisions: Decision[] };
type Outcome = { state: string; remaining_quantity: number; metrics: Record<string, string | number | null>; fills: { triggering_event_id: string; quantity: number; price: string; filled_at: string }[] };
type Experiment = { experiment_id: string; status: string; error: string | null; checksum: string; model_version: string; request: { recording_id: string; symbol: string; entry_index: number }; runs: { run_id: string; order_id: string }[]; result?: { outcomes: Outcome[]; first_state_divergence: TraceRow | null; first_fill_divergence: TraceRow | null; outcome_differs: boolean; deltas: Record<string, string | null>; explanation: { cause: string; source_index: number; source_event: EventRow["event"]; rule: string; decisions: Decision[] } | null } };
type Parameter = "single" | "quantity" | "latency_ms" | "limit_price" | "order_type";

async function api(path: string, body?: unknown) {
  const response = await fetch(`/backend/${path}`, body === undefined ? { cache: "no-store" } : { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  const result = await response.json();
  if (!response.ok) throw new Error(result.error?.message ?? "Private recording service unavailable");
  return result;
}

function FillEvidence({ identity, outcome, inspect }: { identity: string; outcome: number; inspect: (index: number) => void }) {
  const [offset, setOffset] = useState(0);
  const [page, setPage] = useState<{ total: number; fills: { quantity: number; price: string; source: EventRow }[] }>();
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    void api(`experiments/${identity}/fills?outcome_index=${outcome}&offset=${offset}`).then((result) => { if (active) { setPage(result); setError(""); } }).catch((reason) => { if (active) setError(reason.message); });
    return () => { active = false; };
  }, [identity, outcome, offset]);
  return <section><h4>Quote-linked fills · {page?.total ?? "…"}</h4>{error ? <p role="alert">{error}</p> : null}{page?.fills.map((fill) => <p key={fill.source.index}><button type="button" onClick={() => inspect(fill.source.index)}>Inspect {fill.quantity} @ {fill.price}</button> · source {fill.source.index}</p>)}{page && page.total > 20 ? <><button type="button" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 20))}>Earlier fills</button><button type="button" disabled={offset + 20 >= page.total} onClick={() => setOffset(offset + 20)}>Later fills</button></> : null}</section>;
}

export default function RecordedLab() {
  const [privateMode, setPrivateMode] = useState<boolean>();
  const [recordings, setRecordings] = useState<Recording[]>([]);
  const [selected, setSelected] = useState("");
  const [symbol, setSymbol] = useState("");
  const [symbols, setSymbols] = useState("AAPL,MSFT");
  const [seconds, setSeconds] = useState("60");
  const [permission, setPermission] = useState(false);
  const [entry, setEntry] = useState("0");
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [events, setEvents] = useState<EventRow[]>([]);
  const [trace, setTrace] = useState<TraceRow[]>([]);
  const [side, setSide] = useState("buy");
  const [orderType, setOrderType] = useState("market");
  const [quantity, setQuantity] = useState("50");
  const [latency, setLatency] = useState("0");
  const [limitPrice, setLimitPrice] = useState("200");
  const [parameter, setParameter] = useState<Parameter>("latency_ms");
  const [variant, setVariant] = useState("100");
  const [speed, setSpeed] = useState("max");
  const [job, setJob] = useState<Experiment>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const recording = recordings.find((value) => value.recording_id === selected);
  const activeCapture = recordings.find((value) => ["capturing", "finalizing"].includes(value.status));
  const activeJob = job?.status === "queued" || job?.status === "running";

  function inspect(index: number) {
    if (!job) return;
    setSelected(job.request.recording_id); setSymbol(job.request.symbol);
    setOffset(Math.floor(index / 100) * 100);
  }

  useEffect(() => {
    let active = true;
    void api("configuration").then(async (config) => {
      if (!active) return;
      setPrivateMode(config.mode === "private_live");
      if (config.mode !== "private_live") return;
      const list: Recording[] = await api("recordings");
      if (!active) return;
      setRecordings(list);
      const ready = list.find((value) => value.status === "ready");
      if (ready) { setSelected(ready.recording_id); setSymbol(ready.symbols[0]); }
      const identity = localStorage.getItem("tickertrace-experiment");
      if (identity) {
        try { const restored: Experiment = await api(`experiments/${identity}`); if (active) setJob(restored); }
        catch { localStorage.removeItem("tickertrace-experiment"); }
      }
    }).catch((reason) => { if (active) setError(String(reason.message)); });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!activeCapture) return;
    let active = true;
    const timer = setTimeout(() => {
      void api("recordings").then((list) => { if (active) setRecordings(list); }).catch((reason) => { if (active) setError(reason.message); });
    }, 1000);
    return () => { active = false; clearTimeout(timer); };
  }, [activeCapture]);

  useEffect(() => {
    if (!activeJob || !job) return;
    let active = true;
    const timer = setTimeout(() => {
      void api(`experiments/${job.experiment_id}`).then((result) => { if (active) { setJob(result); setError(""); } }).catch((reason) => { if (active) setError(reason.message); });
    }, 1000);
    return () => { active = false; clearTimeout(timer); };
  }, [activeJob, job, error]);

  useEffect(() => {
    setEvents([]); setTrace([]); setTotal(0);
    if (!recording || recording.status !== "ready" || !symbol) return;
    let active = true;
    void api(`recordings/${selected}/events?symbol=${encodeURIComponent(symbol)}&offset=${offset}&limit=100`).then((page) => {
      if (active) { setEvents(page.events); setTotal(page.total); }
    }).catch((reason) => { if (active) setError(reason.message); });
    if (job?.status === "completed" && job.request.recording_id === selected && job.request.symbol === symbol) {
      void api(`experiments/${job.experiment_id}/trace?offset=${Math.max(0, offset - job.request.entry_index)}&limit=100`).then((page) => { if (active) setTrace(page.trace); }).catch((reason) => { if (active) setError(reason.message); });
    }
    return () => { active = false; };
  }, [selected, symbol, offset, recording, job]);

  async function capture(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError("");
    try {
      await api("recordings", { symbols: symbols.split(",").map((value) => value.trim().toUpperCase()).filter(Boolean), seconds: Number(seconds), permissions_confirmed: permission });
      setRecordings(await api("recordings"));
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Capture unavailable"); }
    finally { setBusy(false); }
  }

  async function stopCapture() {
    if (!activeCapture) return;
    try { await api(`recordings/${activeCapture.recording_id}/stop`, {}); setRecordings(await api("recordings")); }
    catch (reason) { setError(reason instanceof Error ? reason.message : "Stop unavailable"); }
  }

  async function run(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError("");
    const baseline = { order_type: orderType, quantity: Number(quantity), latency_ms: Number(latency), limit_price: orderType === "limit" ? limitPrice : null };
    let changed = null;
    if (parameter !== "single") {
      changed = { ...baseline };
      if (parameter === "quantity") changed.quantity = Number(variant);
      if (parameter === "latency_ms") changed.latency_ms = Number(variant);
      if (parameter === "limit_price") changed.limit_price = variant;
      if (parameter === "order_type") { changed.order_type = orderType === "market" ? "limit" : "market"; changed.limit_price = changed.order_type === "limit" ? variant : null; }
    }
    try {
      const result: Experiment = await api("experiments", { recording_id: selected, symbol, entry_index: Number(entry), side, baseline, variant: changed, publication_speed: speed });
      setJob(result); localStorage.setItem("tickertrace-experiment", result.experiment_id);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Replay unavailable"); }
    finally { setBusy(false); }
  }

  if (privateMode === false) return <main className="terminal"><h1>Private recorded laboratory</h1><p>Recordings are unavailable in public demo mode.</p><a href="/">Return to generated demo</a></main>;

  return <main className="terminal recorded-lab">
    <header className="terminal-header"><div className="brand"><strong>Ticker Trace</strong><span className="subtle">Execution comparisons</span></div><a href="/">Live dashboard</a><span className="mode">Recorded · private · never live</span></header>
    {error ? <p role="alert" className="notice error">{error}</p> : null}
    <div className="recorded-grid">
      <aside className="panel recorded-controls">
        <form onSubmit={capture}><h2>Capture an observed interval</h2><label>Already subscribed symbols<input value={symbols} onChange={(event) => setSymbols(event.target.value)} /></label><label>Duration (1–600 seconds)<input type="number" min="1" max="600" value={seconds} onChange={(event) => setSeconds(event.target.value)} /></label>
          <label className="permission"><input type="checkbox" checked={permission} onChange={(event) => setPermission(event.target.checked)} />I verified my account permits private recording/storage. This does not authorize public redistribution.</label>
          <button disabled={busy || !!activeCapture || activeJob || !permission || !privateMode} className="submit-order">Start private capture</button>
          {activeCapture ? <><p>{activeCapture.status} · {activeCapture.symbols.join(", ")}</p><button type="button" disabled={activeCapture.status !== "capturing"} onClick={() => void stopCapture()}>Stop and finalize</button></> : null}
        </form>
        <h2>Saved intervals</h2><select aria-label="Recording" value={selected} onChange={(event) => { const item = recordings.find((value) => value.recording_id === event.target.value); setSelected(event.target.value); setSymbol(item?.symbols[0] ?? ""); setOffset(0); setEntry("0"); }}><option value="">Select a recording</option>{recordings.map((value) => <option key={value.recording_id} value={value.recording_id}>{value.symbols.join("/")} · {value.status} · {value.started_at.slice(11, 19)}</option>)}</select>
        {recording ? <><p>{recording.feed.toUpperCase()} · {recording.status} · {recording.event_count.toLocaleString()} events · {(recording.byte_count / 1024).toFixed(1)} KiB</p><p>{recording.reason}</p><p className="recorded-identity">SHA-256 {recording.checksum ?? "Pending validation"}<br />{recording.model_version}</p>{recording.quality.map((value) => <p className="model-note" key={value}>{value}</p>)}</> : <p>Offline replay needs a saved, validated recording; it does not need Alpaca credentials.</p>}
      </aside>
      <section className="panel recorded-controls">
        <form onSubmit={run}><h2>One controlled change</h2><div className="recorded-fields">
          <label>Symbol<select value={symbol} onChange={(event) => { setSymbol(event.target.value); setOffset(0); setEntry("0"); }}>{recording?.symbols.map((value) => <option key={value}>{value}</option>)}</select></label>
          <label>Entry source index<input type="number" min="0" max={Math.max(0, total - 1)} value={entry} onChange={(event) => setEntry(event.target.value)} /></label>
          <label>Side<select value={side} onChange={(event) => setSide(event.target.value)}><option>buy</option><option>sell</option></select></label>
          <label>Baseline type<select value={orderType} onChange={(event) => { setOrderType(event.target.value); if (parameter === "limit_price") setParameter("latency_ms"); }}><option>market</option><option>limit</option></select></label>
          <label>Baseline shares<input type="number" min="1" max="10000" value={quantity} onChange={(event) => setQuantity(event.target.value)} /></label>
          <label>Baseline latency (ms)<input type="number" min="0" max="60000" value={latency} onChange={(event) => setLatency(event.target.value)} /></label>
          {orderType === "limit" ? <label>Baseline limit (USD)<input type="number" step="0.000001" min="0.000001" value={limitPrice} onChange={(event) => setLimitPrice(event.target.value)} /></label> : null}
          <label>Changed parameter<select value={parameter} onChange={(event) => { const value = event.target.value as Parameter; setParameter(value); setVariant(value === "quantity" ? "150" : value === "latency_ms" ? "100" : "200"); }}><option value="single">Standalone replay</option><option value="quantity">Quantity</option><option value="latency_ms">Latency</option>{orderType === "limit" ? <option value="limit_price">Limit price</option> : null}<option value="order_type">Market ↔ limit</option></select></label>
          {parameter !== "single" && !(parameter === "order_type" && orderType === "limit") ? <label>Variant {parameter === "latency_ms" ? "latency (ms)" : parameter === "quantity" ? "shares" : "limit (USD)"}<input type="number" min={parameter === "latency_ms" ? "0" : "0.000001"} step={parameter === "limit_price" || parameter === "order_type" ? "0.000001" : "1"} value={variant} onChange={(event) => setVariant(event.target.value)} /></label> : null}
          <label>Backend publication pacing<select value={speed} onChange={(event) => setSpeed(event.target.value)}><option value="max">Max</option><option value="1">1×</option><option value="5">5×</option><option value="20">20×</option></select></label>
        </div><button className="submit-order" disabled={busy || activeJob || !!activeCapture || recording?.status !== "ready" || !total}>{activeJob ? "Processing isolated runs…" : parameter === "single" ? "Run recorded simulation" : "Compare two experiments"}</button></form>
        <p className="model-note">Entry is before the selected source event. Earlier events only warm market state. Independent top-of-book simulations: no shared liquidity, queue position, depth, fees, market impact, or real broker guarantees. Pacing changes publication, not finite-worker processing.</p>
        {job ? <><p role="status">Experiment: {job.status} {job.error ?? ""}</p><p className="recorded-identity">{job.request.symbol} · entry {job.request.entry_index} · dataset {job.checksum} · {job.model_version}</p><div className="recorded-outcomes">{job.result?.outcomes.map((value, index) => <article key={`${job.experiment_id}:${index}`}><h3>{index === 0 ? "Baseline" : "Variant"}</h3><p>{value.state} · {value.remaining_quantity} shares remaining</p><dl>{[["Average fill (USD)", "average_fill_price"], ["Fill rate", "fill_rate"], ["Spread cost / share (USD)", "spread_cost"], ["Entry → first fill (ms)", "entry_to_first_fill_ms"], ["Activation → first fill (ms)", "time_to_first_fill_ms"], ["Entry → completion (ms)", "entry_to_completion_ms"]].map(([label, key]) => <div key={key}><dt>{label}</dt><dd>{value.metrics[key] ?? "Unavailable"}</dd></div>)}</dl><FillEvidence identity={job.experiment_id} outcome={index} inspect={inspect} /></article>)}</div>
          {job.result && job.result.outcomes.length === 2 ? <section className="recorded-explanation"><h3>{job.result.explanation ? `First difference: ${job.result.explanation.cause}` : "No source-event divergence"}</h3>{job.result.explanation ? <><p>Source index {job.result.explanation.source_index} · {job.result.explanation.source_event.event_id}</p><p>{job.result.explanation.rule}</p><p>Bid {job.result.explanation.source_event.bid_price ?? "—"} × {job.result.explanation.source_event.bid_size ?? "—"} · Ask {job.result.explanation.source_event.ask_price ?? "—"} × {job.result.explanation.source_event.ask_size ?? "—"}</p><button type="button" onClick={() => inspect(job.result!.explanation!.source_index)}>Inspect responsible event</button></> : null}<p>First state difference: {job.result.first_state_divergence?.index ?? "None"} · First fill difference: {job.result.first_fill_divergence?.index ?? "None"} · Final fills/state: {job.result.outcome_differs ? "Different" : "Same"}</p><p>Variant − baseline: average fill {job.result.deltas.average_fill_price ?? "Unavailable"} USD · fill rate {job.result.deltas.fill_rate ?? "Unavailable"} · entry → fill {job.result.deltas.entry_to_first_fill_ms ?? "Unavailable"} ms. Lower prices favor buys, higher prices favor sells; these are simulated outcomes, not recommendations.</p></section> : null}
        </> : null}
      </section>
    </div>
    <section className="panel recorded-timeline"><div className="panel-heading"><h2>Source-ordered timeline · {symbol || "Select a symbol"}</h2><span>{offset + 1}–{Math.min(offset + events.length, total)} / {total}</span><button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - 100))}>Previous</button><button disabled={offset + 100 >= total} onClick={() => setOffset(offset + 100)}>Next</button></div>
      {events.length > 1 ? <svg viewBox="0 0 1000 100" role="img" aria-label="Observed prices for the current source-event page"><polyline fill="none" stroke="#00da73" strokeWidth="2" points={(() => { const prices = events.map((row) => Number(row.event.ask_price ?? row.event.price)); const low = Math.min(...prices), span = Math.max(...prices) - low || 1; return prices.map((price, index) => `${index * 1000 / (prices.length - 1)},${95 - (price - low) * 90 / span}`).join(" "); })()} /></svg> : null}
      <p className="model-note">Chart shows this page only, in source order. The first-divergence result uses every eligible event, not chart sampling.</p>
      <div className="recorded-table"><table><thead><tr><th>Entry</th><th>Market time</th><th>Bid × shares</th><th>Ask × shares / trade</th><th>Baseline decision</th><th>Variant decision</th><th>Source</th></tr></thead><tbody>{events.map((row) => { const decision = trace.find((value) => value.index === row.index); const highlight = job?.result?.explanation?.source_index === row.index && job.request.recording_id === selected && job.request.symbol === symbol; return <tr className={highlight ? "divergence" : ""} key={row.index}><td><button onClick={() => setEntry(String(row.index))}>{row.index}</button></td><td>{row.event.event_time.slice(11, 23)}</td><td>{row.event.bid_price ?? "—"} × {row.event.bid_size ?? "—"}</td><td>{row.event.ask_price ?? row.event.price ?? "—"} × {row.event.ask_size ?? "—"}</td>{[0, 1].map((index) => <td key={index}>{decision?.decisions[index] ? `${decision.decisions[index].state} · +${decision.decisions[index].event_fill_quantity} @ ${decision.decisions[index].event_fill_price ?? "—"}` : "—"}</td>)}<td title={row.event.event_id}>{row.cursor}</td></tr>; })}</tbody></table></div>
    </section>
  </main>;
}
