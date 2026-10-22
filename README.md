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

**Flow:** CSV + SQLite → `raw` (DuckDB, `ingest()`) → `staging` (dbt views) → `silver` + `quality` (dbt tables) → `gold` (dbt table) → Iceberg (`publish()`, PyIceberg).

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `stg_supplier_prices` | staging | row of a supplier file | view |
| `stg_products`, `stg_stores` | staging | product / store | view |
| `stg_orders`, `stg_order_items` | staging | order / order line | view |
| `product_cost_history` | silver | SKU × cost version (SCD2) | table |
| `weekly_margin` | gold | ISO week × city × category with at least one counted line | table |
| `price_quarantine` | quality | rejected supplier row | table |

- **raw** keeps sources exactly as they arrive. Supplier files are read by pattern (`prices_*.csv`), all as text, merged by column name, with the source file of each row. Every run reloads raw fully, so nothing duplicates.
- **staging** cleans and types, but filters nothing. `raw_cost` keeps the cost as written; `unit_cost_sar` is the parsed number. SKUs are trimmed. `order_date` = `created_at` (UTC) + 3 hours.

**How the SCD2 history is built:** Only valid quotes are used (known SKU, cost > 0), rounded to 2 decimals. When one SKU and `valid_from` appear in several files, the file name that sorts last wins, so `prices_2026-W39_fix.csv` replaces `prices_2026-W39.csv`. A week without a quote adds no row, so the previous version continues. A new version starts only when the cost differs from the previous quote (`lag`), and `valid_to` is the next version's `valid_from` (`lead`, exclusive). Quarantine is applied before "last file wins", so a rejected row in a later file never hides a valid row in an earlier one.

**How each order line finds its cost:** Counted lines (delivered, quantity > 0, order date ≥ 2026-08-31) are left-joined to the history on `sku` where `valid_from <= order_date < valid_to` (or `valid_to` is empty). Versions of one SKU never overlap, so at most one version matches. Lines with no match are uncosted: they count in `lines`, `revenue_sar` and `uncosted_lines`, not in the cost columns.

**Publishing:** `publish()` creates the namespaces and tables on the first run and never drops them. It overwrites a table only when its content changed, so a re-run on the same data adds no snapshot and a correction adds a new one.

## Data problems I found

Counts are for the course dataset (`make_data.py`, seed 2026).

| Problem | Where | Rows affected | What I did |
|---|---|---|---|
| Decimal commas (`9,49`) | `prices_2026-W38.csv` | 126 | `,` → `.` before casting |
| Arabic-Indic digits (`٨٫٦٥`) | `prices_2026-W38.csv` | 34 | `translate()` to ASCII digits and `.` |
| Byte-order mark before the header | `prices_2026-W38.csv` | whole file | Removed by DuckDB's CSV reader; checked that `supplier_id` is clean |
| `unit_cost` renamed to `cost_sar`, new `currency` column | `prices_2026-W39.csv` | 160 | Read with `union_by_name`, cost = `coalesce(cost_sar, unit_cost)` |
| Zero or negative cost | `prices_2026-W39.csv` | 4 | Quarantined as `invalid_cost` |
| SKU not in `products` | `prices_2026-W39.csv` | 3 | Quarantined as `unknown_sku` |
| SKUs with trailing spaces | `products` | 2 | `trim()` on both sides before joining |
| Quantity ≤ 0 | `order_items` | 142 | Not counted (rule 7) |
| Order line with no valid cost: product never quoted (28 of 356 products) or ordered before its first quote | counted order lines | 20,626 | Kept as uncosted (rule 9), shown in `uncosted_lines` |

## Tests

22 dbt tests, all passing; `run.py` runs them with `dbt build` on every run. `tools/check.py` passes 22/22 on seed 2026 and on seed 11.

| Test | On | What it protects against |
|---|---|---|
| `unique` + `not_null` on keys | `stg_orders.order_id`, `stg_products.product_id`, `stg_stores.store_id`, `stg_order_items` (order_id, line_no) | Duplicated source rows multiplying revenue in joins |
| `unique` + `not_null` | `stg_products.sku` | Two products with the same trimmed SKU, so one cost would apply twice |
| `unique` on (sku, valid_from), `not_null` on every column | `product_cost_history` | Two versions starting on the same day, which breaks the point-in-time join |
| `accepted_values` on `reason` | `price_quarantine` | A rejection reason the contract doesn't allow |
| `unique` on (iso_week, city, category) | `weekly_margin` | A grain bug that splits or duplicates weekly rows |
| `assert_margin_adds_up` (singular test) | `weekly_margin` | margin ≠ costed revenue − cost, costed revenue > revenue, or uncosted lines > lines |
| `not_null` | `stg_orders.order_date`, `stg_supplier_prices.source_file`, `weekly_margin.lines`, `weekly_margin.uncosted_lines` | A date or file name that silently failed to parse |

## Re-runs and the correction

Course dataset (seed 2026), one warehouse folder, three runs:

| Run | `gold.weekly_margin` snapshots | Total `margin_sar` |
|---|---|---|
| 1 · empty warehouse | `4522864601937005105` (append, 165 rows) | 801,016.88 |
| 2 · same data | none: all three tables `unchanged` | 801,016.88 |
| 3 · after `make_data.py --add-correction` | `5220931318576486843` (delete) + `4518106165366200421` (append) | 803,515.80 |

- After the correction, scanning snapshot `4522864601937005105` still returns **801,016.88**.
- One row, week 39 · Riyadh · Beverages: `cost_sar` 20,554.70 → 19,729.50 and `margin_sar` 7,730.40 → 8,555.60. In total, 36 of 165 rows changed.
- `silver.product_cost_history` went from 633 to 637 versions. `quality.price_quarantine` stayed at 7 rows, because the bad rows are still in `prices_2026-W39.csv`.

## What I'd do next

- **100× more data:** every run rebuilds every model, and `publish()` reads each whole table into memory to compare it before writing. These full rebuilds and comparisons break first. I would make the order-line models incremental, partition `weekly_margin` by `iso_week` in Iceberg, and rewrite only the partitions that changed.
- **Daily supplier files:** the SCD2 logic doesn't assume weekly files, but "the file name that sorts last wins" depends on the naming convention. I would land each file in an append-only raw table with a load timestamp and use that instead of the name, add a freshness check on the latest file, and reject or convert non-SAR rows using `currency`.

## AI assistance

I used Claude (Anthropic's AI assistant) throughout this project. It wrote `ingest()` and `publish()`, the dbt models (staging, silver, quality, gold) and the dbt tests, tested the pipeline in Google Colab against an independent Python re-implementation of the rules on two seeds, and drafted this README. I directed the work step by step. I reviewed each step and can explain the SCD2 history, the point-in-time join and the publish logic. I ran the pipeline and `tools/check.py` on my own machine.
