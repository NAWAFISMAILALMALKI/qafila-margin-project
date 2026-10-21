# Qafila gross margin

Submitted by: Nawaf Almalki — academy: @SDAIAAcademy

The task, rules and grading are in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py          # data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # contract self-check
```

## Design

`data/` files → **raw** (DuckDB, `ingest()` in Python) → **staging** (dbt views) → **silver / quality** (dbt tables) → **gold** (dbt table) → **Iceberg** (`publish()` with PyIceberg, SQLite catalog).

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `stg_supplier_prices`, `stg_products`, `stg_stores`, `stg_orders`, `stg_order_items` | staging | supplier file row / product / store / order / order line | view |
| `product_cost_history` | silver | SKU × cost version (SCD2) | table |
| `weekly_margin` | gold | ISO week × city × category with at least one counted line | table |
| `price_quarantine` | quality | rejected supplier row | table |

Staging only cleans and types: supplier costs stay as text in `raw_cost` and are parsed into `unit_cost_sar` (decimal comma → point, Arabic-Indic digits → ASCII, `try_cast`), SKUs are trimmed, and `order_date` is `created_at` (UTC) + 3 hours. Nothing is filtered before silver.

**How the SCD2 history is built:** only valid quotes are used (known SKU, positive cost, rounded to 2 decimals). If the same SKU and `valid_from` appear in several files, the file whose name sorts last wins (`row_number() ... order by source_file desc`), so `prices_2026-W39_fix.csv` replaces `prices_2026-W39.csv`. A week without a quote adds no row, so the previous version simply continues; a new version starts only when the cost differs from the previous quote (`lag`), and `valid_to` is the next version's `valid_from` (`lead`). Design choice: quarantine is applied before "last file wins", so a rejected row in a later file never hides a valid row in an earlier one.

**How each order line finds its cost:** counted lines (delivered, quantity > 0, order date ≥ 2026-08-31) are left-joined to the history on `sku` with `valid_from <= order_date < valid_to` (or `valid_to` empty). Versions of one SKU never overlap, so at most one matches; lines without a match are uncosted and count only in `lines`, `revenue_sar` and `uncosted_lines`.

`publish()` overwrites an Iceberg table only when its content changed, so a re-run on the same data adds no snapshot, while a correction adds one and leaves the older snapshots readable.

## Data problems I found

Counts are for the course dataset (`make_data.py`, seed 2026).

| Problem | Where | Rows affected | What I did |
|---|---|---|---|
| Decimal commas (`9,49`) | `prices_2026-W38.csv` | 126 | Replaced `,` with `.` before casting |
| Arabic-Indic digits (`٨٫٦٥`) | `prices_2026-W38.csv` | 34 | `translate()` to ASCII digits and `.` |
| Byte-order mark before the header | `prices_2026-W38.csv` | whole file | DuckDB's CSV reader strips it; checked `supplier_id` is clean |
| Column renamed `unit_cost` → `cost_sar`, new `currency` | `prices_2026-W39.csv` | 160 | `union_by_name` on read, `coalesce(cost_sar, unit_cost)` |
| Zero or negative cost | `prices_2026-W39.csv` | 4 | `price_quarantine`, reason `invalid_cost` |
| SKU not in `products` | `prices_2026-W39.csv` | 3 | `price_quarantine`, reason `unknown_sku` |
| SKUs with trailing spaces | `products` | 2 | `trim()` on both sides before joining |
| Non-positive quantity | `order_items` | 142 | Excluded from the margin (rule 7) |
| Order lines with no valid cost (product never quoted: 28 of 356; or ordered before its first quote) | `order_items` × history | 20,626 counted lines | Kept as uncosted lines (rule 9), visible in `uncosted_lines` |

## Tests

22 dbt tests, all passing (`dbt build` runs them on every `run.py`).

| Test | On | What it protects against |
|---|---|---|
| `unique` + `not_null` on keys | `stg_orders.order_id`, `stg_products.product_id`, `stg_stores.store_id`, `stg_order_items` (order_id, line_no) | Duplicated source rows multiplying revenue in joins |
| `unique` + `not_null` on `sku` | `stg_products` | Two products sharing a trimmed SKU, so a cost would apply twice |
| `unique` on (sku, valid_from) + `not_null` columns | `product_cost_history` | Two versions starting on the same day, which breaks the point-in-time join |
| `accepted_values` on `reason` | `price_quarantine` | A new rejection rule that the contract doesn't allow |
| `unique` on (iso_week, city, category) | `weekly_margin` | A grain bug that splits or duplicates weekly rows |
| `assert_margin_adds_up` (singular) | `weekly_margin` | margin ≠ costed revenue − cost, costed revenue > revenue, uncosted > lines |
| `not_null` on `order_date`, `source_file`, `lines`, `uncosted_lines` | staging / gold | A timestamp or file name that failed to parse silently |

`tools/check.py`: 22/22 on seed 2026 and on seed 11.

## Re-runs and the correction

Course dataset (seed 2026), one warehouse folder, three runs:

| Run | `gold.weekly_margin` | Total `margin_sar` |
|---|---|---|
| 1 · empty warehouse | snapshot `4522864601937005105` (append, 165 rows) | 801,016.88 |
| 2 · same data | all three tables `unchanged`, no new snapshot | 801,016.88 |
| 3 · after `make_data.py --add-correction` | snapshots `5220931318576486843` (delete) and `4518106165366200421` (append) | 803,515.80 |

After the correction, reading snapshot `4522864601937005105` still returns 801,016.88. One row, week 39 · Riyadh · Beverages: `cost_sar` 20,554.70 → 19,729.50 and `margin_sar` 7,730.40 → 8,555.60 (36 of 165 rows changed). `silver.product_cost_history` went from 633 to 637 versions; `quality.price_quarantine` did not change (7 rows), because the bad rows are still in `prices_2026-W39.csv`.

## What I'd do next

- **100× more data:** `publish()` reads each whole table into memory and compares it row by row, and every run rebuilds every model. The first thing to break is that full comparison and full rebuild. Next steps: incremental dbt models for order lines, partition the Iceberg tables (`weekly_margin` by `iso_week`), and write only the changed partitions instead of overwriting the table.
- **Daily supplier files:** the logic doesn't depend on weekly files, but "the file name that sorts last wins" relies on the naming convention. I would load files into an append-only raw table with a load timestamp and use it instead of the name, add a freshness check on the latest file, and add a `currency` check before any non-SAR file arrives.

## AI assistance

I used Claude (Anthropic's AI assistant) throughout this project. It wrote `ingest()` and `publish()`, the dbt models and tests, ran the pipeline in Google Colab against an independent Python re-implementation of the rules on two seeds, and drafted this README. I directed the work step by step.
