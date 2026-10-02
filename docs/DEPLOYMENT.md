# Deployment Plan

## Local Container Stack

Copy `infra/.env.production.example` to `infra/.env`, replace `POSTGRES_PASSWORD`, then run:

```bash
docker compose --env-file infra/.env -f infra/docker-compose.yml up --build --wait
```

The `migrate` service applies Alembic migrations before the API, engine, and persistence workers start. The local public replay stack exposes the frontend at `http://localhost:3000` and the API at `http://localhost:8000`.

The production Compose defaults only to `APP_MODE=public_replay`; it does not pass Alpaca credentials or start ingestion.

## Public Deployment: Vercel + Fly.io

The browser UI belongs on Vercel. Fly.io runs the API, engine, and persistence worker as three process groups from the same backend image. All public traffic is pinned to the API process; workers use private HTTP endpoints only. The portfolio deployment uses Supabase PostgreSQL and Upstash Redis over TLS.

This layout is intentionally replay-only. It does not deploy the Alpaca ingestion service or add any Alpaca credential to Fly or Vercel.

### One-Time Provider Setup

1. Install `flyctl`, authenticate with `fly auth login`, and replace the placeholder `app` name in [fly.toml](../fly.toml) with a globally unique Fly application name.
2. Set a Supabase PostgreSQL connection string as `DATABASE_URL`. The application accepts `postgresql://` and SQLAlchemy URLs. Use a connection endpoint reachable from Fly and require TLS.
3. Create an Upstash Redis database in the same region and set its TLS connection string as Fly's `REDIS_URL` secret.
4. Set Fly's public browser origin before deployment:

```bash
fly secrets set PUBLIC_ALLOWED_ORIGINS=https://your-vercel-project.vercel.app
```

5. Create an app-scoped Fly deploy token with a finite lifetime. Treat it as a server-side secret; never add it to Vercel or paste it into chat. Stage it before deploying so the new API can start:

```bash
fly tokens create deploy -a ticker-trace --expiry 2160h
fly secrets set --stage FLY_WORKER_TOKEN="<token-from-the-previous-command>" -a ticker-trace
```

The app-scoped deploy token can manage this application's resources, not just start workers. Rotate it before expiry; an expired token fails new submissions closed. `FLY_APP_NAME` is supplied by Fly at runtime. The new `FLY_WORKER_LIFECYCLE=on` setting in `fly.toml` enables demand dispatch; local Compose retains queue dispatch.

6. Deploy from the repository root without automatic spare machines, then keep exactly one existing machine per process group:

```bash
fly deploy -a ticker-trace --ha=false
fly scale count api=1 engine=1 persistence=1 -a ticker-trace
```

The deployment runs `alembic upgrade head` before replacing application machines. Workers exit cleanly after 60 idle seconds; the on-failure restart policy does not restart a successful idle exit. Fly Proxy stops the API when idle with `min_machines_running=0`. Do not scale worker counts to zero: that removes machines, while demand startup needs the existing stopped machines. Redeploy Vercel to apply completed-session WebSocket closure and bounded retries.

### Vercel Setup

Import `frontend` as the Vercel project root and set these production environment variables:

```text
API_URL=https://your-fly-app.fly.dev
NEXT_PUBLIC_API_ORIGIN=https://your-fly-app.fly.dev
```

Redeploy Vercel after setting them. `API_URL` powers server-side `/backend/*` rewrites; `NEXT_PUBLIC_API_ORIGIN` powers browser API and WebSocket connections. Add the final Vercel domain to Fly's `PUBLIC_ALLOWED_ORIGINS`; preview domains should be tested with the local or preview API rather than made a wildcard public origin.

### Production Verification

```bash
fly checks list
fly logs
curl -fsS https://your-fly-app.fly.dev/ready
curl -fsS https://your-fly-app.fly.dev/api/v1/configuration
```

The configuration endpoint must return `{"mode":"public_replay"}`. Submit one generated replay through the Vercel UI, confirm its WebSocket session completes, then verify the resulting order and events through the API. No public Fly or Vercel environment variable may contain `ALPACA_API_KEY` or `ALPACA_API_SECRET`.

## Free-Tier Lifecycle Plan

The target idle state is that Vercel serves only static frontend assets, Fly has no running API or worker machine, Redis receives no application commands, and Supabase receives no keepalive traffic. A user opening the dashboard is the event that wakes the public execution path.

The lifecycle implementation is locally tested. Production acceptance requires staging the token, deploying the updated backend and frontend, and observing actual provider counters. Fly autostop is traffic-based, not an immediate guarantee; external uptime monitors can keep the API awake. Do not schedule `/ready` or dashboard API polling.

### Idle State

- Vercel remains the static delivery layer; it does not connect directly to Redis or PostgreSQL.
- Fly API machines use scale-to-zero settings and a health endpoint that does not call Redis or PostgreSQL.
- One machine remains provisioned for each worker, but none is running after the idle grace period. Worker health and idle checks do not access Redis or PostgreSQL.
- Upstash receives no worker polling, rate-limit, cache, or stream commands while idle.
- Supabase receives no background query, WebSocket, or keepalive traffic. Its Free project may pause after a week of low activity and can be resumed from the dashboard.

### Wake and Sleep Flow

1. The frontend requests a replay session from the API.
2. Configuration/scenario reads need no Redis or database access and do not wake workers. A replay submission reserves a shared monthly allowance, starts the two existing workers, and checks readiness with bounded retries.
3. The API publishes an isolated replay stream only after both workers are ready, then calls engine `/jobs` followed by persistence `/jobs` over Fly's private network. There is no idle Redis job polling and no Redis lease heartbeat.
4. The submission returns after persistence finishes. The WebSocket sends the completed snapshot and closes; the UI animates the recorded events locally. This low-cost public mode does not stream intermediate worker progress. Local queue mode still supports asynchronous progress.
5. Each worker accepts at most four concurrent jobs, rejects duplicate active run IDs, and exits after 60 seconds with no active job. Redis temporary state expires; retention cleanup runs only on successful demand, not on a timer.
6. Fly Proxy subsequently stops the idle API. Browser reconnect attempts and incomplete server sessions are bounded. A disconnect does not cancel accepted worker work; dependency failures may leave an abandoned replay whose source expires after one hour.

### Free-Tier Guardrails

- Upstash command, bandwidth, and data-size usage is sampled before and after lifecycle tests.
- Redis streams, dead-letter streams, metrics hashes, market state, and order state receive explicit retention or trimming rules.
- Completed generated public replay history is retained for seven days and at most the newest 1,000 runs. Cleanup deletes at most 100 eligible runs per successful submission, including their associated events, orders, fills, transitions, and settings. Private/live runs, active runs, watchlists, and non-generated benchmark scenarios are excluded. Cleanup is irreversible and occurs on demand, so expired rows can remain while the app is idle.
- A shared Redis counter admits at most 1,000 weighted credits per UTC calendar month, including failed starts. Each attempt reserves `max(1, ceil(event_count / 25))` credits; a 300-event experiment costs 12 and its zero-latency comparison costs another 12. This is a conservative demo envelope, not a measured provider-quota guarantee. Redis outages reject submissions; quota exhaustion returns HTTP 429.
- Public replay creation and concurrent-session limits remain enforced server-side.
- No provider auto-upgrade or payment method is enabled solely to handle an unexpected quota spike.
- A lifecycle failure must fail closed by rejecting new replay work, not by creating an unbounded polling loop.

### Verification

- Record Upstash command/data/bandwidth counters, Supabase database-size/egress counters, and Fly machine states before testing. Do not automate polling against the application.
- Leave the dashboard unopened for 24 hours and confirm no application Redis commands, no new replay rows, and all three machines stopped. Redis TTL eviction is expected and may decrease data size without application commands.
- Open the dashboard and confirm the API wakes, workers start once, and one replay completes.
- Repeat five concurrent replay requests and confirm only the configured worker count starts.
- Leave the dashboard idle again and confirm workers stop after the grace period.
- Run the same test with Redis unavailable and confirm the API reports a bounded dependency error without retry storms.
- Record the same provider counters after one replay and five concurrent submissions. Use the measured command/egress delta and remaining monthly quota to lower the 1,000-attempt allowance if needed. Provider dashboards are the source of truth; no management credentials are required by the application.
- Check Fly billing separately. Stopped machines avoid running compute, but storage, network traffic, or other account resources can still cost money; this is not a guaranteed free deployment.

Fly lifecycle configuration follows the [Fly configuration reference](https://docs.fly.io/reference/configuration). Supabase pausing/resuming remains operator-managed; the application must not send keepalives to prevent a free project from pausing.

## Deployment Goals

- Provide a publicly accessible replay UI and API demonstration.
- Wake public replay workers on demand; keep any continuous live ingestion in a separate private deployment.
- Protect market-data credentials and other secrets.
- Keep private live data isolated from the public deployment.
- Support repeatable deployments and safe database migrations.
- Keep infrastructure cost appropriate for a portfolio project.

## Proposed Service Layout

| Service | Responsibility | Hosting Requirements |
| --- | --- | --- |
| Frontend | Web interface | Static or server-rendered web hosting |
| API | REST and WebSockets | Persistent process with WebSocket support |
| Ingestion | Private external feed connection | Separate private environment; not deployed to Fly public demo |
| Replay | Public fixture producer | Long-running or on-demand process |
| Workers | Stream processing | Independently scalable processes |
| Redis | Streams and cached state | Persistent managed service preferred |
| PostgreSQL | Durable and historical data | Managed database preferred |

## Vercel Boundary

Vercel hosts the Next.js frontend. The API, replay service, stream workers, Redis, database, and persistent WebSocket backend run on infrastructure designed for long-running processes and stateful dependencies.

Fly.io is the selected persistent-container provider. It runs the three backend process groups from one Docker image, while Vercel serves the Next.js frontend.

## Environments

- Local: complete development environment through containers
- Test: ephemeral dependencies for automated integration tests
- Preview: frontend and API validation using generated replay data only
- Public production demo: replay data only with durable dependencies
- Private live: operator-controlled environment with Alpaca credentials and no public redistribution

## Configuration and Secrets

- `APP_MODE`: `public_replay` or `private_live`
- `ALPACA_API_KEY` and `ALPACA_API_SECRET`: private-live environment only
- Database connection string
- Redis connection string
- `PUBLIC_ALLOWED_ORIGINS`: comma-separated browser origins allowed to call the public API
- Public API and WebSocket origins
- Enabled symbols and replay datasets
- Partition count, retention, retry, lag, and UI update thresholds
- Log and monitoring configuration

No secret values should be committed to the repository or exposed to the browser.

The application must refuse to start in `public_replay` mode when Alpaca credentials or live ingestion are enabled. The public frontend must receive mode from the server and cannot override it.

## Deployment Checklist

- [x] Build production frontend and backend images.
- [x] Run automated tests.
- [x] Apply database migrations through Fly's release command.
- [x] Validate public browser origins.
- [ ] Provision Fly Managed Postgres and Upstash Redis.
- [ ] Deploy Fly API, engine, and persistence process groups.
- [x] Deploy replay producer and approved generated public fixtures.
- [ ] Deploy frontend to Vercel.
- [ ] Verify deployed health endpoints.
- [ ] Run a production smoke test.
- [ ] Confirm Fly logs and metrics are available.
- [ ] Verify restart and rollback procedures.
- [x] Verify that public mode cannot start with live credentials.
- [ ] Inspect deployed browser traffic and bundles for credentials or restricted raw data.

## Operational Limits

- Private live mode subscribes to at most 10 configured symbols during the MVP.
- Public replay applies server-side limits to order submissions, replay creation, dataset size, and concurrent sessions.
- Raw public fixtures have documented generated or redistribution-safe provenance.
- Retention defaults are decided from measured storage growth before public launch.
- The deployment budget and provider limits are recorded during Phase 6.
- When a limit is reached, the API rejects new work explicitly instead of silently degrading correctness.

## Recovery and Rollback

### On-Demand Diagnostics

Run these only when investigating an issue; do not schedule them as uptime probes:

```bash
fly status -a ticker-trace
fly checks list -a ticker-trace
fly logs -a ticker-trace --no-tail
curl -fsS https://ticker-trace.fly.dev/health
curl -fsS https://ticker-trace.fly.dev/ready
```

`/health` checks process liveness without Redis or database traffic and is the Fly health-check target. `/ready` checks Redis connectivity only; it does not certify PostgreSQL, worker availability, or token validity. A generated replay is the end-to-end readiness check. HTTP requests can wake the API; `fly status` does not request the application. A stopped idle API is expected, not an uptime incident. Do not introduce an external dashboard/ready ping loop.

Structured worker events now include `worker_job_started`, `worker_job_completed`, `worker_job_failed`, and `worker_idle_shutdown`. The API logs `replay_submission_failed`, `dependency_unavailable`, and `public_replay_cleanup_failed`. Trace `request_id` across API dispatch and worker job logs, and use `run_id` to locate the affected replay. Expected dependency failures record the error class only, not exception text, tokens, DSNs, or SQL parameters. HTTP errors carry a correlation ID; worker dependency failures return 503; WebSocket dependency failures close with code 1013.

Read `/api/v1/replays/<run-id>/health?partition=<partition>` or `/metrics?run_id=<run-id>&partition=<partition>` only for an active investigation. Both use Redis; replay metrics expire after 15 minutes and are not durable monitoring history. Metrics include engine/persistence backlog, pending messages, processed-event throughput, and engine processing duration. Cross-machine timestamps use Unix time rather than process-relative monotonic clocks. Host clock skew can still affect short-run throughput; tiny fixture rates are diagnostic, not sustained-load resume evidence. Pre-6D timestamps are incompatible: run a fresh replay after deployment or let old metrics expire.

The frontend fetches final health once when a replay completes, then stops polling. If metrics cannot be retrieved, it reports that the replay persisted but metrics are unavailable instead of claiming persistence is still pending.

### Incident Recovery

| Incident | Expected behavior | Operator action |
| --- | --- | --- |
| Worker clean idle exit | Machine stops without restart after 60 idle seconds. | No action. The next replay starts the existing machine. |
| Worker process crash | Fly worker policy retries non-zero exits at most twice. | Inspect role/run logs. If crashes repeat, correct the dependency/configuration or roll back. Do not add spare machines or a polling loop. |
| Worker job dependency failure | Job returns 503, releases its concurrency slot, and leaves the process healthy. | Restore the dependency, then submit a new generated replay. A job error is not a process crash. |
| API crash/restart | Durable completed runs survive in PostgreSQL; in-flight HTTP requests may fail. | Confirm health/configuration, then run one smoke replay. Do not assume an interrupted HTTP job was cancelled. |
| Redis unavailable or exhausted | Readiness/submissions fail closed; liveness remains healthy. | Check Upstash status, TLS URL and quotas. Resume normal requests after recovery; never reset production Redis or increase quotas automatically. |
| Redis data loss/TTL expiry | Temporary streams/cache/metrics can be missing; PostgreSQL completion records remain. | Read durable order results or submit a fresh generated replay. Incomplete runs cannot resume without their source stream. |
| PostgreSQL unavailable/paused | Worker/API dependency requests return safe errors; no fake completion is reported. | Resume/check Supabase manually and verify the connection endpoint. Then retry a generated replay. |
| Fly worker token expired | Worker startup fails before replay publication. | Rotate the app-scoped token through Fly secrets, then verify one replay; never put it in Vercel or chat. |
| Monthly replay allowance reached | New replay attempts return 429. | Check actual provider usage. Wait for the next UTC month; do not delete the counter as an incident workaround. |

Public resubmission creates a new run ID; there is no automatic restart/resume loop for an abandoned public run. Source/result recovery within the TTL is tested using replacement consumers. An internal persistence retry for an already-completed run returns success without reprocessing. Acknowledged fills remain unique; do not delete durable data to recover a worker.

The local operational suite kills a real consumer process after it claims messages, replaces the engine, refuses the persistence database connection, retries after restoring the valid store, and recreates the API against the same database. It verifies one durable fill. Redis outage/readiness recovery, safe error payloads, request-ID propagation, and completed-job retry behavior are covered by fault-injected unit tests. These are not claims that production Supabase/Upstash were stopped and restarted. Live provider outage drills require a separate staging environment.

### Backend Rollback

Before deploying, record the current successful backend image, its matching Fly configuration, the database migration revision, and the production frontend deployment URL. Keep that release as the known-good recovery target:

```bash
fly releases -a ticker-trace --image
```

On a regression, first capture the failing request/run ID and logs. Confirm the previous image is compatible with the current database schema, API response contracts, and rotated secrets. Then deploy that exact image with its matching saved configuration:

```bash
fly deploy -a ticker-trace --image <known-good-image> --config <known-good-fly-config> --ha=false --skip-release-command
fly scale count api=1 engine=1 persistence=1 -a ticker-trace
```

Skip the release command only after checking schema compatibility; this avoids an old migration bundle trying to interpret a newer schema. If compatibility cannot be established, use a forward-fix rather than this rollback. Do not automatically run `alembic downgrade`, wipe Redis/PostgreSQL, restore exposed credentials, or destroy worker machines. No database migration is added by 6D. Rollback instructions are documented and CLI options checked, but no production rollback was executed.

Fly release commands stop a deployment when migration execution fails; process/restart configuration follows the [Fly configuration reference](https://docs.fly.io/reference/configuration).

### Frontend Rollback and Post-Recovery Check

In Vercel's deployment history, use Instant Rollback to restore the previous compatible production deployment. Existing build-time API origins are part of that build; do not assume changing current environment settings updates it. Follow [Vercel's Instant Rollback documentation](https://vercel.com/docs/instant-rollback) and explicitly restore normal production promotion after the fix is ready.

After either rollback or recovery:

1. Confirm `public_replay`, exact allowed browser origins, and no Alpaca credentials in public services.
2. Open a clean browser, submit one fixture, and verify final state, fill price, quantity, and final health metrics.
3. Confirm engine and persistence return to stopped state, close the test browser, then confirm API autostop through `fly status` without waking it.
4. Check provider counters manually and record the release/image, run ID, outcome, and any failure. The 24-hour idle-usage acceptance check remains separate.

## Separate Private Live Stack

Local execution is the current priority. Owner-only cloud authentication/deployment and hosted idle-usage acceptance are deferred. The application uses local Redis/PostgreSQL but still requires internet for Alpaca. Closing the dashboard does not stop ingestion, workers, or Docker. Existing Fly/Vercel resources are not stopped by local shutdown commands.

Do not change the public Fly app to private live. Use the isolated `infra/docker-compose.private.yml` stack with separate Redis/PostgreSQL and loopback-only ports. Copy `infra/.env.private.example` to ignored `infra/.env.private`, set operator Alpaca credentials and a local database password, then:

```bash
docker compose --env-file infra/.env.private -f infra/docker-compose.private.yml up --build
```

Private frontend: `http://localhost:3030`; API: `http://localhost:8030`. The frontend uses compile-time API origins. `localhost` is the supported private-browser origin; a browser resolving it outside this machine cannot connect. No authentication exists: never publish these ports or depend on CORS for security.

Ingestion initializes the session registry; live worker processes wait up to 30 seconds for it. The UI can change up to ten symbols without changing environment files. Source history is bounded at 100,000 messages per partition and orders at 256 per session. The MVP assumes 100 shares per quote round lot; verify stock units before use. Automatic coordinated session rollover and private retention are not implemented.

For the current implementation, starting a new session requires stopping ingestion and both workers, waiting for any old ingestion lease to expire (up to 30 seconds), starting ingestion, confirming a new session is registered, and only then starting engine/persistence. Do not restart the three services concurrently and assume workers attach to the new session. For the isolated env-file setup:

```bash
docker compose --env-file infra/.env.private -f infra/docker-compose.private.yml stop ingestion engine persistence
docker compose --env-file infra/.env.private -f infra/docker-compose.private.yml start ingestion
docker compose --env-file infra/.env.private -f infra/docker-compose.private.yml start engine persistence
```

Perform the wait and new-session checks between the commands above. If replacing only an execution or persistence worker within the same session, a surviving 30-second ownership lease can cause the first restart to exit. Wait for expiry and retry the worker without restarting ingestion or deleting another owner's lease. This workaround was required in the October 2 live test; automatic retry is planned in Local A.

Stop the private stack when finished; continuous ingestion intentionally consumes resources and does not share the public idle lifecycle:

```bash
docker compose --env-file infra/.env.private -f infra/docker-compose.private.yml stop
```

Private history remains in its database/Redis until the operator handles retention. The public prune policy excludes it. No automatic destructive private cleanup is introduced.

### Current Laptop Instance

The instance started October 1 uses Docker project `tickertrace-local`, the ignored repository-root `.env` for Alpaca credentials, and a local-only database password override. Its containers do not use the hosted database or Redis URLs from that file. For this existing instance, run from the repository root:

```bash
POSTGRES_PASSWORD=tickertrace-local-only docker compose --project-name tickertrace-local --env-file .env -f infra/docker-compose.private.yml ps
POSTGRES_PASSWORD=tickertrace-local-only docker compose --project-name tickertrace-local --env-file .env -f infra/docker-compose.private.yml stop
```

Use the same project name, env source, and local database password when starting this instance again. `stop` retains containers and database data. Do not use `down --volumes` to stop normal operation. After startup, verify registry/session IDs and all three live processes rather than assuming container start implies successful recovery. A fresh dedicated ignored env file remains the recommended reproducible setup for another laptop.

## Pre-Phase 7 External Acceptance Record

The real local feed smoke test is recorded below. Remaining cloud checks are deferred, not completed or local-first blockers; perform them only if hosting is resumed explicitly. Do not add scheduled application probes:

| Check | Evidence to record | Status |
| --- | --- | --- |
| Real IEX simulated fill | Active-window run/order/event IDs; provider quote time; persisted fill and UI explanation | Verified locally October 2; manual recovery only, see PRE_PHASE_7.md |
| Ten-minute rate certification | Nonzero measured peak; target = twice peak; offered rate/backlog/latency JSON with configuration | Pending Local C |
| New public browser workflow | Deployed backend/frontend versions; 300-event experiment, parameter comparison, final persisted result | Deferred cloud acceptance |
| Five concurrent submissions | Five outcomes; exact one machine per worker group; no extra/spare machines | Deferred cloud acceptance |
| Idle worker/API shutdown | Worker stop after grace period; API autostop after browser close | Deferred cloud acceptance |
| 24-hour provider idle usage | Before/after timestamps, Fly states, Upstash command/data/bandwidth, Supabase DB/egress deltas | Deferred cloud acceptance |

The 1,000-credit budget must be lowered if measured command/egress deltas suggest quota risk. Stopped compute is not a guarantee of zero billing. Local integration tests do not certify provider usage or an external market feed.

## Remaining Deferred Deployment Decisions

The following do not block implementation and are intentionally postponed until deployment measurements exist:

- Exact retention duration
- Public concurrency and rate-limit values
- Custom domain
