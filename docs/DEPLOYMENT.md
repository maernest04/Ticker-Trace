# Deployment Plan

## Local Container Stack

Copy `infra/.env.production.example` to `infra/.env`, replace `POSTGRES_PASSWORD`, then run:

```bash
docker compose --env-file infra/.env -f infra/docker-compose.yml up --build --wait
```

The `migrate` service applies Alembic migrations before the API, engine, and persistence workers start. The local public replay stack exposes the frontend at `http://localhost:3000` and the API at `http://localhost:8000`.

The production Compose defaults only to `APP_MODE=public_replay`; it does not pass Alpaca credentials or start ingestion.

## Public Deployment: Vercel + Fly.io

The browser UI belongs on Vercel. Fly.io runs the API, engine, and persistence worker as three process groups from the same backend image. All public traffic is pinned to the API process; workers have no public HTTP route. PostgreSQL is Fly Managed Postgres, and Redis is a managed Upstash Redis database connected over TLS.

This layout is intentionally replay-only. It does not deploy the Alpaca ingestion service or add any Alpaca credential to Fly or Vercel.

### One-Time Provider Setup

1. Install `flyctl`, authenticate with `fly auth login`, and replace the placeholder `app` name in [fly.toml](../fly.toml) with a globally unique Fly application name.
2. Create a Fly Managed Postgres cluster in `sjc`, then attach it to the app. Fly provides the connection string as `DATABASE_URL`; the application accepts both Fly's standard `postgresql://` URL and the local SQLAlchemy URL.
3. Create an Upstash Redis database in the same region and set its TLS connection string as Fly's `REDIS_URL` secret.
4. Set Fly's public browser origin before deployment:

```bash
fly secrets set PUBLIC_ALLOWED_ORIGINS=https://your-vercel-project.vercel.app
```

5. Deploy from the repository root, then create one machine for each worker process group:

```bash
fly deploy
fly scale count api=1 engine=1 persistence=1
```

The deployment runs `alembic upgrade head` as Fly's release command before replacing application machines. The API remains warm so a replay session WebSocket does not start from a cold machine.

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

## Deployment Goals

- Provide a publicly accessible replay UI and API demonstration.
- Keep long-running ingestion and worker services continuously available.
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

- Database migrations require a documented rollback or forward-fix plan.
- Workers must recover from acknowledged checkpoints.
- Cached state must be rebuildable.
- Deployment rollback must not require deleting durable data.
- Public replay remains available if the private live environment is offline.

## Deferred Deployment Decisions

The following do not block implementation and are intentionally postponed until deployment measurements exist:

- Exact retention duration
- Public concurrency and rate-limit values
- Custom domain
