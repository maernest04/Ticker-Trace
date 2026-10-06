# Actual private recording acceptance — October 6, 2026

Passed scoped local acceptance using actual IEX input. Permission is user-reported Alpaca approval for personal/local recording and offline replay, not independently reviewed written permission or public display/redistribution rights. No real broker trades, hosted resources, Git commands, raw-data publication, vendor screenshots, or application-code changes were used.

## Setup and capture

Docker Desktop was initially stopped; the installed Desktop CLI started it. All ordinary local containers were initially stopped. Existing Redis/PostgreSQL and additive migration containers resumed, preserving their storage. The already-built API/frontend images were installed locally without rebuilding or recreating dependency containers. Existing live ingestion/engine/persistence images resumed in dependency order; this is operator-assisted startup, not unattended cold-boot certification.

The private API reported fresh actual AAPL/MSFT provider quotes. One existing IEX connection captured a 30-second interval, finalized by its duration bound: **680 events**, **297,414 bytes**, comprising **341 AAPL** and **339 MSFT** observations. Loading the saved artifact independently validated its schema, checksum, source boundaries and contents. Actual files and detailed provenance remain in the ignored private `recordings/` directory, excluded from container build context. Files are owner-only; the recording directory is owner-only. No vendor quote payloads/prices are reproduced in this shareable summary.

## Live and recorded isolation

A five-share simulated live market order completed during capture. PostgreSQL independently verified exactly one unique five-share fill, its price matching the stored triggering ask, and a quote timestamp at or after submission. Its original fill ID/quantity remained unchanged after the recorded runs and API restart.

Two independent five-share AAPL market simulations used the saved interval and entry index zero, varying only artificial latency (0 versus 100 ms). The first comparison ran at maximum publication pace while ordinary live ingestion remained active. The current live run, AAPL/MSFT subscriptions and control cursor remained unchanged; actual live quotes continued updating. The comparison completed with separate source/run/order namespaces.

## Offline reproduction and durable checks

Ingestion and both ordinary live workers were stopped. The private session reported stopped/not fresh, and container status confirmed the provider service was not running. The replay API environment had no Alpaca credentials. The same saved input/order configurations then completed at 20x backend publication pacing through existing finite engine/persistence subprocesses.

Canonical outcomes, transitions, fill source identities, metrics, first state/fill divergence and rule explanation exactly matched the first comparison, excluding newly generated experiment run/order IDs in explanation configurations. Both configurations filled fully at the same simulated price, but against different source quotes; delayed activation caused an event-level/timing difference, not a price improvement or broker-execution claim.

For all four experiment runs, PostgreSQL contained **341 events and 341 distinct event IDs** per run. Each experiment order had one unique five-share fill, no remaining quantity, price equal to its persisted triggering ask and a quote time at or after submission plus configured latency. Fill-evidence endpoints independently returned original capture-run quotes/cursors at the correct source indices.

After an API restart, the completed offline job and canonical results were unchanged and original quote evidence remained retrievable. The live registry stayed unchanged apart from the expected stopped status; offline replay did not resume provider workers. This proves the scoped saved-input workflow, not universal exactly-once processing or recovery from destroyed history.

## Restore and remaining gates

All ordinary local services were stopped afterward, matching their initial state. Containers, recordings and volumes were not removed; normal application retention policies remained in effect. Docker Desktop remains open; no test containers were started. Private machine-readable evidence: `recordings/acceptance-2026-10-06.json`, alongside the immutable recording and two experiment/trace files.

Owner usability acceptance, fresh-live browser regression, physical laptop sleep/wake/network recovery and independent demand validation remain unverified. Previous generated browser checks and automated tests are separate evidence; they were not rerun or relabelled as actual-browser acceptance. No new throughput benchmark, multi-machine scaling, brokerage fidelity or public vendor rights are claimed.
