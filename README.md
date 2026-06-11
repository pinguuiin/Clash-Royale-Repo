# Clash Royale Meta Pipeline

A game data pipeline that ingests real **Clash Royale** battle data at scale through the official API and processes it in a Databricks lakehouse to study the game's *meta* — which cards concentrate and win, and how fresh and complete the datasets are. The report layer currently focuses on Trophy Ladder mode, with room to extend to other modes.

Built on **Databricks Free Edition** with **PySpark** and **Delta Lake**, following a bronze → silver → gold medallion design, with data quality baked in as a complement of the silver layer rather than an afterthought.

---

## Status

| Stage | State | Notes |
|---|---|---|
| Ingestion (Python) | ✅ Done | API client, parsers, player discovery, battlelog + card pulls |
| Bronze (raw Delta) | ✅ Done | `bronze_battles`, `bronze_cards`, `bronze_players` |
| Silver (modelled) | ✅ Done | `dim_cards`, `silver_battles`, `silver_deck_cards` |
| Quality checks | ✅ Done | validation + quarantine + completeness reporting → `dq_results` |
| Gold (metrics) | ✅ Done | card win-rate + pick-rate (`gold_card_metrics`), KPI tiles (`gold_overview`) |
| Dashboard | ✅ Done | live data-quality tiles and card metrics |
| pytest suite | ✅ Done | unit tests for the pure parser functions |
| CI / scheduling | ✅ Done | GitHub Actions: pytest on push/PR + daily ingest cron → UC volume upload → Databricks job trigger |

**Current corpus:** 315,798 unique battles across 62 days, seeded from 9,964 top-ladder players, over 121 cards — ~5.2M card-appearances in the silver layer.

---

## The question

Competitive card games tend toward a **concentrated meta**: a handful of decks dominate the top of the ladder while the rest of the card pool sees little play. This pipeline is built to measure exactly how concentrated Clash Royale's high-ladder meta is, and which cards and decks drive it — win rate, pick rate, and how much of all play the top decks account for.

*The headline finding and chart will land here once the gold + dashboard layers are built.*

---

## Pipeline at a glance

```
Clash Royale API proxy
      │  Ingestion (Python)
      ▼  raw JSON
   Bronze ──────> Silver ──────> Quality ──────> Gold ──────> Dashboard
  (raw Delta)   (modelled)    (validate +      (metrics)    (finding +
                              quarantine)                    DQ tile)
```

- **Ingestion** is single-machine Python (it's I/O-bound on API rate limits — Spark would be overkill). Pure-function parsers make it unit-testable.
- **Bronze → Gold** is PySpark on Databricks, where distributed processing suits the best for millions of nested-JSON rows.

---

## Highlights

**Resilient ingestion**

- **Exponential backoff with full jitter** on `429` / `5xx` and network errors, honouring the server's `Retry-After` header when present.
- **Client-side throttle** that spaces requests to stay under the API's per-minute rate limit.
- **Checkpointed batches** — each player's battles are flushed to disk and the checkpoint updated via an atomic *write-then-rename*, so a crashed or interrupted run resumes from the last completed player without re-fetching or losing data.
- **Fault isolation** — a deleted player (`404`) or transient error is logged and skipped, never aborting the run; unfinished tags are simply retried next run.

**Idempotent & testable by design**

- **Pure-function parsers** (no I/O, clock, or global state) — verifiable with unit tests, no network required.
- **Deterministic `battle_id`** (SHA-1 of battle time + sorted player tags) makes the bronze `MERGE` idempotent and the uniqueness check meaningful.
- **Cross-batch dedup** — the same battle seen in two players' logs collapses to one row, handled once in bronze.

**Lakehouse engineering**

- **Schema enforced at the bronze boundary** — drift surfaces as a typed null in a known column, not a silently reshaped table.
- Silver demonstrates **`explode`** (decks → one row per card), a **broadcast join** (tiny card dim against millions of fact rows), and **date partitioning** for query pruning.
- **Order-independent deck hash** groups identical decks regardless of card order.

**Observability built in**

- Data-quality checks **record-and-continue** — failing rows are quarantined for inspection, never silently dropped.
- Every check writes a dashboard-ready numeric **`metric_value`** (failing-row count, missing-data %, or freshness hours), appended as run history.

---

## Data model

All tables live in Unity Catalog under `workspace.clash`.

| Layer | Table | Grain | Purpose |
|---|---|---|---|
| Bronze | `bronze_battles` | one row / battle | Raw parsed battles, schema-enforced, idempotent `MERGE` on `battle_id` |
| Bronze | `bronze_cards` | one row / card | Raw card dimension (overwrite each run) |
| Bronze | `bronze_players` | one row / player | Crawl seed, kept for lineage |
| Silver | `dim_cards` | one row / card | Modelled card lookup + `elixir_band` |
| Silver | `silver_battles` | one row / battle | Typed timestamp, `battle_date` partition, `crown_diff` |
| Silver | `silver_deck_cards` | one row / card played | Decks exploded (16/battle), broadcast-joined to `dim_cards` |
| Quality | `dq_results` | one row / check / run | Check outcomes + metrics (appended as history) |
| Quality | `quarantine_battles` | one row / flagged battle | Failing battles, retained for inspection |
| Gold | `gold_card_metrics` | one row / card | Win rate, pick rate, sample size + `elixir_band`, scoped to trophy ladder |
| Gold | `gold_overview` | one row | Dashboard KPIs — totals, distinct players, freshness, validity |
| Gold | `gold_mode_breakdown` | one row / game mode | Battle count + share per mode; shows the trophy-ladder slice of all play |

---

## Data quality

Two reliability layers, kept deliberately separate:

- **Code tests (pytest)** verify *the code is correct* — pure parser functions against fixtures (`tests/`, run with `pytest`).
- **In-pipeline checks** verify today's data is fit for use. They **record and continue** — bad rows are quarantined, never silently dropped — so the pipeline stays observable.


| Check | Action |
|---|---|
| `battle_id` unique | quarantine duplicates |
| `battle_time` parsed (not null) | quarantine |
| crowns within 0–3 | quarantine |
| starting trophies ≥ 0 | quarantine |
| every deck has 8 cards | quarantine |
| every `card_id` resolves in `dim_cards` | quarantine |
| per-field **missing-data %** (incl. player/opponent tags) | log only |
| freshness < 24h | log only |

Every outcome is written to `dq_results` with a numeric `metric_value` (failing-row count, missing-data %, or freshness hours) so the dashboard's quality tile can read it directly.

---

## Repository layout

```
clash-royale-meta/
├── README.md                       # Design roadmap + setup guide
├── pyproject.toml                  # package + entry points + pytest config
├── ingestion/                      # single-machine Python ETL
│   ├── client.py                   # proxied API client, backoff + throttle
│   ├── config.py                   # env-driven settings
│   ├── parsers.py                  # pure JSON → row functions (the unit under test)
│   ├── discover_players.py         # seed top-ladder player tags
│   ├── pull_battlelogs.py          # checkpointed per-player battlelog pull
│   └── pull_cards.py               # card dimension pull
├── orchestration/                  # glue: local raw JSON → Databricks
│   ├── upload_to_databricks.py     # mirror raw JSON into the UC volume (Databricks SDK)
│   └── trigger_databricks_job.py   # run the bronze→gold job + wait (Jobs API)
├── tests/                          # pytest — the code-reliability layer
│   ├── conftest.py                 # fixture loaders
│   ├── fixtures/                   # raw-API-shaped JSON (normal + edge cases)
│   ├── test_parsers.py             # battlelog parser tests
│   └── test_ingestion.py           # clan / member / card parser tests
├── notebooks/                      # PySpark on Databricks
│   ├── bronze_ingest.ipynb         # raw JSON → schema-enforced Delta
│   ├── silver_transform.ipynb      # type, model, explode, broadcast-join
│   ├── silver_quality_checks.ipynb # validate + quarantine + completeness
│   └── gold_metrics.ipynb          # card win/pick rate + overview KPIs
├── .github/workflows/              # GitHub Actions
│   ├── ci.yml                      # pytest on push/PR
│   └── daily.yml                   # daily cron: ingest → upload → trigger job
└── data/                           # local raw JSON (gitignored)
```

---

## Running it

### Ingestion (local — runs off Databricks to avoid burning compute on rate-limit waits):

1. Create a key on the official API to access Clash Royale data. The API token requires static IPs. For most dynamic home IPs, requests need to route through the community [RoyaleAPI proxy](https://docs.royaleapi.com/proxy.html)

2. Set up virtual environment and install dependencies

```bash
python3 -m venv .venv                  # create the python virtual environment
source .venv/bin/activate              # activate it
pip install -e ".[dev]"                # install requests + python-dotenv (+ pytest)
```

3. Copy `.env.example` to `.env` and add your `CR_API_TOKEN` to `.env`. **(Remember to add .env to .gitignore to protect you token info!)**

```bash
cp .env.example .env
```

4. Run the script `discover_players`/`pull_cards`/`pull_battlelogs` under ingestion/ to pull data from the server

Examples on flag usage:
```bash
python -m ingestion.discover_players --max-clans 200 --out data/raw/players.json          # pull player seed (e.g. all members from top 200 clans)
python -m ingestion.discover_players --location 57000000   # Europe only                  # can also add location id to limit the region

python -m ingestion.pull_cards --out data/raw/cards.json            # pull cards information

python -m ingestion.pull_battlelogs --players-file data/raw/players.json        # pull battlelog from all discovered players
python -m ingestion.pull_battlelogs --resume                                    # or --resume to continue the newest batch
python -m ingestion.pull_battlelogs --batch-id 20260530T0900                    # or specify a batch to continue 
```

### Transformation (Databricks):

5. Create a sub path `clash/raw/` under `workspace/` in your Databricks Unity Catalog volume. (one-time)

#### Option 1 (manually):

6. Copy the local data under `data/raw/` to `clash/raw/` in the UC volume.

7. Create another path in your Databricks workspace and copy your local notebooks under `notebooks/` there.

8. Run the notebooks in order — `bronze_ingest` → `silver_transform` → `silver_quality_checks` → `gold_metrics`.

#### Option 2 (orchestration):

6. Run the python scripts below to upload the local raw data to the Unity Catalog volume, then (optionally) trigger the notebook chain — both wrap the Databricks SDK and read `DATABRICKS_HOST` / `DATABRICKS_TOKEN` from the environment:

```bash
pip install -e ".[databricks]"                           # adds the Databricks SDK
python -m orchestration.upload_to_databricks             # mirror data/raw → /Volumes/workspace/clash/raw
python -m orchestration.trigger_databricks_job --job-id 123456789   # run bronze→gold + wait
```

### Scheduling (GitHub Actions):

Steps 4–7 run unattended every day via [`.github/workflows/daily.yml`](.github/workflows/daily.yml) (06:00 UTC, or trigger manually from the Actions tab): pull cards → discover the top 200 clans' members → pull battlelogs → upload raw JSON to the UC volume → trigger the Databricks job and wait.

Set these as **repository secrets** (Settings → Secrets and variables → Actions):

| Secret | Purpose |
|---|---|
| `CR_API_TOKEN` | Clash Royale token (created against the RoyaleAPI proxy IP) |
| `DATABRICKS_HOST` | Workspace URL, e.g. `https://xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | PAT scoped to Files + Jobs |
| `DATABRICKS_JOB_ID` | The Job id of the bronze→silver→gold notebook chain. Use 1049179186835703 |

Optionally set a repository **variable** `DATABRICKS_VOLUME` to override the default volume path (`/Volumes/workspace/clash/raw`).

---

## Tech stack

| Concern | Choice |
|---|---|
| Lakehouse | Databricks Free Edition, Delta Lake, Unity Catalog |
| Processing | PySpark (broadcast joins, window functions) |
| Ingestion | Python — `requests`, `python-dotenv` |
| Reliability | pytest + in-pipeline data-quality checks |
| Orchestration | GitHub Actions cron + Databricks SDK / Jobs API |


