# Maven Market — Synthetic Kafka Producers (Orders & Inventory)

## Why this exists

The Maven Market dataset has no real-time Orders or Inventory feed —
only static historical CSVs (Transactions, Returns, etc.). Your brief
requires a Kafka Structured Streaming ingestion path with a real POC
(SASL auth, `readStream`, schema enforcement, checkpoint recovery).
This toolkit is the documented stand-in data source: two producer
scripts that publish synthetic real-time events to Confluent Kafka so
the *consumer* side (built in Databricks, in `src/ingestion/`) has
something real to connect to and stream from.

**Scope note for your docs:** this is a data-generation tool that
simulates the external system (Confluent Kafka), not part of the
lakehouse codebase itself. It belongs in a `tools/` or
`synthetic-data/` folder in your repo, separate from `src/ingestion/`
(which is the actual Databricks-side `readStream` consumer code).

## What each script does

| Script | Source of truth | What it simulates |
|---|---|---|
| `orders_producer.py` | Real `store_id`/`product_id`/`customer_id` *universes* (from Stores/Products/Customers CSVs); the orders themselves are freshly generated, not replayed | A brand-new order happening right now — and occasionally, an order from a customer_id that doesn't exist in the customer master yet (a new walk-in customer) |
| `inventory_producer.py` | Real `store_id`/`product_id` values from `Stores.csv`/`Products.csv`, fabricated stock levels | Stock-on-hand ticking up (restock) and down (sale) per store/product over time |

**Why orders isn't a replay of `MavenMarket_Transactions_*.csv`:** those files are
already the authoritative historical record, loaded once via Auto Loader into
`bronze.transactions_raw`. Replaying the same rows into a "real-time" Kafka topic
would double-count the same sales facts through two different ingestion paths —
architecturally wrong, even though it's a convenient way to get *some* real-looking
data flowing for the auth/checkpoint POC. This producer instead generates genuinely
new events, built with respect to the real master-data ID universes (a real store, a
real product, usually a real customer) rather than replaying anything.

**Why ~5% of orders use an unrecognized `customer_id`:** this deliberately recreates
the early-arriving-fact scenario from your Silver-layer design — a transaction
arrives before the corresponding customer record has been pulled into Bronze via the
Mongo connector. Running this producer alongside your inferred-member Silver logic is
how you actually demonstrate that pattern working, rather than just describing it.
Tune `--new-customer-rate` up (e.g. `0.3`) if you want the scenario to trigger more
often during a demo.

**Schema parity with the historical data:** the order payload's `transaction_date`,
`stock_date`, `product_id`, `customer_id`, `store_id`, and `quantity` fields all match
the columns in `MavenMarket_Transactions_*.csv`, so Silver can conform both sources
into one `fact_sales` shape. `stock_date` is generated as `transaction_date` minus a
random 1-7 day lag — the same distribution observed in the real historical file — so
inventory-dwell-time metrics stay comparable between historical and streaming order
facts. `order_id` and `event_ts` are streaming-only additions with no equivalent in
the flat historical file (no natural order ID exists there, and `event_ts` captures
actual Kafka arrival time, separate from the business `transaction_date`). Dates are
emitted as ISO 8601 (`YYYY-MM-DD`), not the historical file's `M/D/YYYY` strings —
Silver already needs source-specific date parsing for the batch CSVs, so this isn't
new work, just a second format to handle.

Both authenticate to Confluent Cloud via **SASL_SSL / PLAIN**, with
credentials read from environment variables (`common.py` builds this
config and fails fast with a clear error if anything's missing).

## Setup (Windows)

These commands assume a VS Code integrated terminal. PowerShell is VS Code's
default on Windows, so that's shown first; cmd.exe is included as an alternative.

**PowerShell:**
```powershell
cd kafka_producers
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

Copy-Item .env.example .env
# now open .env in the editor and fill in your real Confluent Cloud values
```

**cmd.exe:**
```bat
cd kafka_producers
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r requirements.txt

copy .env.example .env
```

That's it for credentials -- `common.py` loads `.env` automatically via
`python-dotenv` as soon as any script imports it, on every OS. You do **not**
need to `set` or `$env:` anything yourself; just having a filled-in `.env` in
the folder you run the scripts from is enough.

> **If PowerShell blocks the activation script** with a message about
> execution policies, run this once in that terminal session, then retry
> `.venv\Scripts\Activate.ps1`:
> ```powershell
> Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
> ```

You'll need a Confluent Cloud cluster (the free tier is enough for
this) with two topics created: `orders` and `inventory` (or whatever
names you put in `.env`).

## Running

File paths below use forward slashes (`/`) -- these work fine as script
arguments on Windows too (Python handles both `/` and `\` the same way),
so you don't need to rewrite them for PowerShell or cmd.exe.

**Orders** — generate new synthetic orders at ~5/sec, with 5% of them using an unrecognized customer_id (simulating new walk-in customers):

```powershell
python orders_producer.py `
  --stores-csv "Data/SPTStore Data Files/MavenMarket_Stores.csv" `
  --products-csv "Data/SPTStore Data Files/MavenMarket_Products.csv" `
  --customers-csv "Data/SPTStore Data Files/MavenMarket_Customers.csv" `
  --topic orders `
  --rate 5 `
  --new-customer-rate 0.05
```

Quick test run, capped at 60 seconds, with new-customer events showing up more often for demo visibility:
```powershell
python orders_producer.py --stores-csv "Data/.../MavenMarket_Stores.csv" --products-csv "Data/.../MavenMarket_Products.csv" --customers-csv "Data/.../MavenMarket_Customers.csv" --duration 60 --new-customer-rate 0.3
```

**Inventory** — track 300 random (store, product) combos, ~3 ticks/sec, stop after 10 minutes:

```powershell
python inventory_producer.py `
  --stores-csv "Data/SPTStore Data Files/MavenMarket_Stores.csv" `
  --products-csv "Data/SPTStore Data Files/MavenMarket_Products.csv" `
  --topic inventory `
  --num-combos 300 `
  --rate 3 `
  --duration 600
```

> **Note on the line-continuation backtick (`` ` ``) above:** that's PowerShell's
> equivalent of bash's `\`. If you're in cmd.exe instead, just put the whole
> command on one line (cmd doesn't support multi-line continuation the same way).

Both scripts exit cleanly on `Ctrl+C` — `common.py`'s `graceful_shutdown`
context flushes any in-flight messages before the process ends, so a
demo interruption doesn't silently drop data. Neither script needs `--loop`
anymore — both generate events continuously by design; use `--duration` to
cap a run, or just Ctrl+C when you're done.

## Using this to actually prove checkpoint recovery (POC 4)

This is the test your brief specifically asks for. With one of these
producers running continuously:

1. Start your Databricks `readStream` consumer job pointed at the topic, with a checkpoint location set.
2. Let it process for a minute or two — confirm rows landing in `bronze.orders_stream` / `bronze.inventory_stream`.
3. **Kill the consumer** (cancel the job/cluster, or `Ctrl+C` if running interactively).
4. Note the last offset/row processed (query the Bronze table, check `_ingested_at`/`event_ts` of the latest row).
5. **Restart the consumer**, pointed at the *same* checkpoint location.
6. Confirm it resumes from where it left off — no gap, and no unbounded duplication of rows already ingested.

Record the before/after row counts and timestamps from this test —
that's your actual evidence for POC 4, not just a description of how
checkpointing is supposed to work.

## A note on realism vs. simplicity

- `inventory_producer.py` keeps all state in memory for the life of the
  process — if you restart the *producer* (not the consumer), stock
  levels reset. That's fine for this POC's purpose (proving the stream
  mechanics), but worth stating explicitly if asked: this is a data
  generator, not a system of record.
- Both scripts key messages by `store_id` (orders) or
  `"<store_id>-<product_id>"` (inventory) — useful if you want to
  demonstrate partition-aware consumption or per-store aggregation
  later, and worth mentioning in your architecture doc as a deliberate
  partitioning choice.