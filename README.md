# End-to-End Streaming Lakehouse — HR & Sport Activity Tracking

`Python` `Kafka/Redpanda` `Debezium` `Delta Lake` `Great Expectations` `Prometheus` `OSMNX`

---

## Overview

Production-grade event-driven pipeline simulating HR and sport activity tracking — from database state changes to business-ready lakehouse outputs.

The project covers the full data path: CDC capture, event streaming, business rule processing (including geospatial computation), lakehouse historization, data quality validation, and pipeline observability. Architecture decisions were driven by production constraints redefined from the original spec.

---

## Architecture

![Architecture diagram](YOUR_IMAGE_LINK_HERE)

---

## Architecture decisions

**CDC — adapted to each source table's nature.**  
The sport events table is a fact table: every INSERT is a business event by definition, CDC is the natural fit. The HR table is a state table, but only 2 columns are monitored alongside create/delete operations — with a small row count (161 employees at current scale), CDC remains well-suited and the captured change volume stays minimal and predictable.

**Poll interval at 100ms — latency over throughput.**  
The consumer polls Kafka every 100ms. This is not a throughput optimization — even at 10,000 employees, the expected volume is ~20,000 events/day (~0.2 events/second). In practice, most poll windows will return 0 or 1 message. The 100ms interval is a deliberate choice for **processing regularity and low latency**, not batch efficiency.

**SCD Type 2 scoped to auditability, not retroactive rule changes.**  
The Silver layer tracks the full history of each record. Once a row is closed (end date set), it becomes immutable — historical records reflect the state at the time they were active. This is intentional: the goal is lineage and auditability, not replaying updated business rules against past data.

**Quarantine logic for implausible records.**  
Records failing business plausibility checks are routed to a quarantine zone rather than rejected or silently dropped. This preserves the ability to investigate anomalies without polluting the Silver layer.

**PII excluded from the lakehouse.**  
Personally identifiable information is not persisted in Bronze, Silver, or Gold. Business rules requiring PII (e.g. address-based distance computation via OSMNX/BAN API) are applied in-stream — only the computed results are stored.

**Data quality gates at every layer transition.**  
Great Expectations validates data at Bronze → Silver and Silver → Gold. Catching issues at ingestion rather than at reporting time reduces the blast radius of upstream anomalies.

**At-least-once delivery with explicit crash handling.**  
Kafka delivers at-least-once by default. Exactly-once would require Kafka transactions and transactional writes to Delta Lake — complexity not justified at this scale. Instead, crash scenarios are handled explicitly in the consumer:
- Crash before any write → no offset commit → message re-delivered → processing is idempotent at this stage ✓
- Crash during Bronze/Silver write → failing batch routed to a **dead letter table** (Delta) for investigation and optional reinjection → offset committed ✓. If the dead letter write itself fails → error logged, offset committed anyway → loss is acceptable and diagnosable via logs ✓
- Crash after Silver, during Gold write → offset committed → Gold is recalculated from Silver on next run, idempotent by design ✓

---

## Initialization & bootstrap

On first launch, the `init-services` container:
1. Starts PostgreSQL and waits for readiness
2. Creates the second database and all tables
3. Registers Debezium connectors

The HR connector runs with `snapshot.mode = initial` — on registration, Debezium performs a full scan of the existing employees table. These records flow through the broker and are processed by the Python consumer, triggering home-to-work distance computation (OSMNX/BAN API) for all existing employees and populating Silver and Gold from the start.

No mock data is loaded at init. This is intentional: initialization only sets up infrastructure, consistent with how a production deployment would behave.

---

## Tech stack

| Layer | Tools |
|---|---|
| Source databases | PostgreSQL |
| CDC | Debezium |
| Event streaming | Redpanda (Kafka API) |
| Stream processing | Python (100ms poll interval) |
| Lakehouse storage | Delta Lake (Bronze / Silver / Gold) |
| Data quality | Great Expectations |
| Geospatial | OSMNX · BAN API |
| Observability | Prometheus |
| Notifications | Slack API |
| Infra | Docker · Docker Compose |

---

## Running the project

### Prerequisites

- Docker & Docker Compose
- Python ≥ 3.11 (event simulation only)
- Credentials via environment variables or `.env` (see `.env.example`)

```bash
cp .env.example .env
# fill in POSTGRES credentials + SLACK token if needed
docker compose up -d
```

The init sequence runs automatically on first launch. Silver and Gold are populated during init via the snapshot scan of the existing employees table.

### Simulate sport events

```bash
python ./py/create_sport_events.py -e 100  # adjust event count
```

Events flow through the full pipeline automatically from there.

---

## What's not in scope (and why)

**Exactly-once semantics** — handled via explicit crash recovery logic instead (see architecture decisions above). Kafka transactions would add disproportionate complexity for the delivery guarantees already achieved.

**Schema evolution handling** — Debezium supports schema change events, but handling them in the consumer adds non-trivial complexity. Flagged as a natural extension once the core pipeline is stable.

**Grafana dashboards** — Prometheus metrics are exposed and scrapable. Grafana is the logical next step but out of scope here; the focus was on instrumentation, not visualization.

---

## Possible extensions

- Schema evolution handling for CDC schema change events
- Grafana dashboards + alerting on pipeline metrics
- Unit and integration tests for the Python consumer
- Multi-region or cloud deployment
