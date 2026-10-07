# Generated-data Demo

This walkthrough uses `liquidity_replenishment_v1`: 300 generated NVDA observations, not an Alpaca capture. No account or market-hours connection is needed after dependencies are installed. Keep the preview on loopback; private-mode routes are not authenticated and must not be exposed publicly.

## Fresh local setup

Prerequisites: Python 3.13+, Node/npm, Docker Desktop. From the repository root, create the Python environment if absent and install existing project dependencies:

```bash
python3.13 -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

Create dedicated dependencies once. The password below is only for these loopback demo containers, not a provider credential. On later runs use `docker start tickertrace-demo-redis tickertrace-demo-postgres` instead of creating replacements. Wait for both checks to report healthy before migrating.

```bash
docker run -d --name tickertrace-demo-redis -p 127.0.0.1:56380:6379 --health-cmd 'redis-cli ping' --health-interval 2s redis:7-alpine
docker run -d --name tickertrace-demo-postgres -p 127.0.0.1:55433:5432 -e POSTGRES_DB=tickertrace -e POSTGRES_USER=tickertrace -e POSTGRES_PASSWORD=tickertrace-demo-local --health-cmd 'pg_isready -U tickertrace -d tickertrace' --health-interval 2s postgres:16-alpine
docker inspect --format '{{.Name}} {{.State.Health.Status}}' tickertrace-demo-redis tickertrace-demo-postgres
DATABASE_URL=postgresql+psycopg://tickertrace:tickertrace-demo-local@127.0.0.1:55433/tickertrace .venv/bin/alembic upgrade head
```

Prepare a new generated recording directory. Never point this helper at real recordings or a public deployment.

```bash
TASK_DEMO_DIR=$(mktemp -d /private/tmp/tickertrace-demo.XXXXXX)
.venv/bin/python tools/create_demo_recording.py "$TASK_DEMO_DIR"
APP_MODE=private_live RECORDING_DIRECTORY="$TASK_DEMO_DIR" PUBLIC_ALLOWED_ORIGINS=http://localhost:3031 .venv/bin/python -c 'import uvicorn; from market_execution_lab.api_service import create_app; uvicorn.run(create_app("postgresql+psycopg://tickertrace:tickertrace-demo-local@127.0.0.1:55433/tickertrace", "redis://127.0.0.1:56380/0"), host="127.0.0.1", port=8031)'
```

Leave that terminal running. In another terminal, from `frontend/`:

```bash
npm ci
API_URL=http://127.0.0.1:8031 NEXT_PUBLIC_API_ORIGIN=http://localhost:8031 npm run dev -- --hostname 127.0.0.1 --port 3031
```

Do not source a provider `.env` file for this preview. Live ingestion and capture are unnecessary. The UI's private-mode banner describes endpoint access, while the manifest explicitly labels the input GENERATED.

## Ninety-second walkthrough

1. Open `http://localhost:3031/recorded`. Confirm GENERATED, 300 events, and the model/checksum; do not enable capture.
2. Select NVDA, entry 0, buy, market, 50 shares, baseline latency 0 ms. Change only latency to 100 ms; keep backend publication pacing Max.
3. Click **Compare two experiments** and wait for **Experiment: completed**.
4. Compare average fill, entry-to-fill/completion times, and quote-linked fills. Click **Inspect responsible event** to inspect source 0.
5. Explain why the baseline fills partially at the first eligible quote while the delayed variant remains submitted. The variant's first fill is source 5.
6. Use **Next** to inspect source entries 101–200; reload to verify the latest completed job restores. These interactions do not place real orders.

## Verified October 7 result

| Metric | Baseline 0 ms | Variant 100 ms |
| --- | ---: | ---: |
| Filled shares | 50 | 50 |
| Average fill (USD) | 125.03252 | 125.08764 |
| Entry → first fill (ms) | 0 | 100 |
| Entry → completion (ms) | 75 | 150 |
| Quote-linked fills | 4 | 3 |

First divergence: source 0, activation delay. Variant minus baseline average fill: +0.05512 USD/share in this generated interval. This is a deterministic model example, not an expected market effect, trading advice, or performance benchmark.

![Generated comparison and first-divergence evidence](assets/generated-comparison.png)

![Generated fixture and controls](assets/generated-setup.png)

The October 7 walkthrough used existing dedicated local dependencies on ports 56379/55432, Redis DB 14, and a temporary generated-only directory; the fresh setup above uses separate ports to avoid those existing resources. No private vendor files or hosted resources were used. Owner usability is not accepted merely because the agent completed this walkthrough.

## Stop without deleting history

Ctrl+C in each preview terminal, then:

```bash
docker stop tickertrace-demo-redis tickertrace-demo-postgres
```

Retain the directory path to replay again. Do not remove containers/volumes or delete private recordings as part of normal shutdown. This preview does not replace the [actual private live runbook](DEPLOYMENT.md#separate-private-live-stack).
