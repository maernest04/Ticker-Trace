# Deployment Plan

## Local Container Stack

Copy `infra/.env.production.example` to `infra/.env`, replace `POSTGRES_PASSWORD`, then run:

```bash
docker compose --env-file infra/.env -f infra/docker-compose.yml up --build --wait
```

The `migrate` service applies Alembic migrations before the API, engine, and persistence workers start. The local public replay stack exposes the frontend at `http://localhost:3000` and the API at `http://localhost:8000`.

The production Compose defaults only to `APP_MODE=public_replay`; it does not pass Alpaca credentials or start ingestion.

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
| Ingestion | Private external feed connection | Long-running process; disabled publicly |
| Replay | Public fixture producer | Long-running or on-demand process |
| Workers | Stream processing | Independently scalable processes |
| Redis | Streams and cached state | Persistent managed service preferred |
| PostgreSQL | Durable and historical data | Managed database preferred |

## Vercel Boundary

Vercel hosts the Next.js frontend. The API, replay service, stream workers, Redis, database, and persistent WebSocket backend run on infrastructure designed for long-running processes and stateful dependencies.

The persistent-container provider may be selected during Phase 6 without changing application design. Selection is based on:

- WebSocket support
- Background-worker support
- Managed Redis and PostgreSQL availability
- Container deployment support
- Sleep and cold-start behavior
- Network transfer and connection limits
- Monthly cost

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
- Allowed origins
- Public API and WebSocket origins
- Enabled symbols and replay datasets
- Partition count, retention, retry, lag, and UI update thresholds
- Log and monitoring configuration

No secret values should be committed to the repository or exposed to the browser.

The application must refuse to start in `public_replay` mode when Alpaca credentials or live ingestion are enabled. The public frontend must receive mode from the server and cannot override it.

## Deployment Checklist

- [ ] Build production frontend and backend images.
- [ ] Run automated tests.
- [ ] Apply database migrations.
- [ ] Validate environment variables.
- [ ] Deploy stateful dependencies.
- [ ] Deploy API, ingestion, and workers.
- [ ] Deploy replay producer and approved public fixtures.
- [ ] Deploy frontend.
- [ ] Verify health endpoints.
- [ ] Run a production smoke test.
- [ ] Confirm logs and metrics are available.
- [ ] Verify restart and rollback procedures.
- [ ] Verify that the public environment cannot start the live adapter.
- [ ] Inspect browser traffic and bundles for credentials or restricted raw data.

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

- Persistent-container vendor
- Managed Redis and PostgreSQL vendors
- Exact retention duration
- Public concurrency and rate-limit values
- Custom domain
