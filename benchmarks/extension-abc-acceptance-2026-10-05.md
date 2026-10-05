# Extension A/B/C engineering evidence — October 5, 2026

Scope: local generated-input correctness, private recording/replay services, controlled comparison, and UI. This is not actual IEX recording acceptance, physical laptop recovery, multi-machine certification, or user-demand validation. The authoritative checklist is in `docs/IMPLEMENTATION_PLAN.md`.

## Automated verification

- Full isolated Python/integration suite: **168 passed in 146.19 seconds**, with the existing Starlette/httpx deprecation warning.
- After the final safe validation-message change: **43 extension tests passed in 10.77 seconds** using `tests/test_recorded_replay.py` and `tests/test_commit_boundaries.py`.
- Next.js 15.5.26 production build passed after the final standalone-result UI fix. `/recorded` is 5.23 kB with 108 kB first-load JavaScript.
- Private Compose configuration validated with `config --quiet`, without printing resolved environment values or launching the live stack.
- Recovery tests cover interruptions before/after source SQL commits, result SQL commits and engine result publication; stable partial-fill recovery, deliberate unfillable orders, and 205 pending source messages after commit-before-acknowledgement.
- Recording/replay tests cover immutable manifests, source identity/checksum/count validation, history loss, capture bounds, interrupted capture/replay, equal-time entry exclusion, stale/duplicate input, private/public REST/WebSocket separation, canonical outcomes, pacing and replacement.
- Comparison tests cover every supported varied parameter, earliest state versus fill divergence, identical controls, final-equivalent outcomes with intermediate differences, unavailable metrics, signed inversion and mismatched comparison identities.

## Lifecycle repetitions and preserved failures

Five final quiet repetitions passed: **45.71, 46.57, 44.16, 46.47, 47.60 seconds**. Final readiness waits for all expected engine/persistence ownership keys, a fresh specific successor-symbol quote, and a persisted simulated probe before suspension. Expected counts, PIDs, prior fills, retry budgets and recovery checks were retained.

The first readiness revision passed once then failed because ownership alone did not imply a fresh quote was available for the probe. The final revision includes quote readiness. The original October 5 17-versus-33 ownership failure is retained in `docs/PRE_PHASE_7.md`; its exact count was not independently reproduced again in this extension.

The initial contended attempt passed twice then lost persisted evidence while another regression test pruned closed sessions in the same PostgreSQL database. A separate Redis DB did not isolate SQL retention. This failed attempt is preserved in [initial contention artifact](extension-a-contention-2026-10-05.json); it is not counted as a successful gate.

After separating PostgreSQL databases as well as Redis namespaces, five contended repetitions passed: **46.34, 47.22, 47.59, 53.34, 51.53 seconds**. Simultaneous generated workload offered/persisted **90,000/90,000 events** over **300.15249 seconds**, approximately **299.980421 events/sec**, with zero final backlog and an empty error list. See [isolated contention artifact](extension-a-contention-isolated-2026-10-05.json). This is a scoped correctness contention gate, not a new production throughput claim.

The local Docker daemon stopped between attempts. Docker Desktop and only the two dedicated test dependencies were restarted before the isolated run; the reason for that stop is not established. Docker reported 29.8.2 after restart. Tests ran on the existing Apple M3/macOS host with Python 3.13 and local PostgreSQL/Redis, not Fly, Supabase or Upstash.

## Browser checks

The loopback-only production preview uses a **generated 300-event NVDA recording**, not vendor data. A baseline 0 ms and variant 100 ms comparison completed through separate existing engine/persistence subprocesses. The UI exposed original source-index quote-linked fills, first activation/fill divergence, signed price/time deltas, dataset checksum and model limitations. Source paging displayed entries 101–200 of 300; page reload restored the saved completed experiment. Invalid identical configurations were rejected. At a 390-pixel viewport, document width remained 390 pixels without horizontal overflow; the temporary viewport override was reset afterward.

Public `/recorded` access showed an unavailable state. Public generated AAPL replay completed with 50 shares filled at 200.00, queue depth zero and workers pending zero. The temporary public forever-workers were stopped afterward. Standalone recorded replay completed in the browser with 50 shares filled in four quote-linked fills. Its UI no longer displays comparison equality/deltas when there is only one outcome. Preview screenshots contain generated input only.

## Bounds and limitations

Capture is private, at most one operation, 1–600 seconds, one existing source run, up to two already-subscribed symbols, and ends at 80,000 events or 64 MiB. Aggregate private artifacts are capped at 256 MiB and ten saved experiments, with headroom admission; there is no automatic deletion. SQL history remains operator-managed. Recorded admission is scoped to 80,003 messages; the public 10,000 cap is unchanged. Publication finishes before finite processing, so pacing does not imply incremental engine execution.

Model: source-ordered, independent top-of-book simulations, not consolidated prices, exchange queue position, full depth, shared liquidity, fees, market impact or broker-exact execution. Interrupted experiment jobs are failed, not automatically resumed. Cooperative leases are not database fencing or consensus.

## Remaining acceptance

Actual account recording/storage permission is unconfirmed, so no actual vendor capture was started. A permission-confirmed saved IEX interval must be captured and replayed offline, with simultaneous live/recorded isolation checked. Physical laptop sleep/wake and network interruption require the user's participation and fresh quotes. A consenting participant must complete the comparison task before usefulness or repeat demand can be claimed. Operator-assisted startup is documented; unattended cold-boot recovery is not newly certified.

The user's ordinary local live stack was already stopped before this extension and was not started. Existing retained data was not deleted, Git commands were not run, real trades were not placed and hosted resources were not used. The generated preview and dedicated local test dependencies remain running for inspection.
