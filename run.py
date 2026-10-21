"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Every data_dir/suppliers/prices_*.csv, keeping the file name of each row.

    Every table is fully reloaded (CREATE OR REPLACE), so running this twice never duplicates anything.
    """
    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")

        # App database: the four tables we need, copied as they are.
        sqlite_path = (data_dir / "qafila_oltp.sqlite").as_posix()
        con.sql(f"ATTACH '{sqlite_path}' AS app (TYPE sqlite, READ_ONLY)")
        try:
            for table in ["orders", "order_items", "products", "stores"]:
                con.sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM app.{table}")
        finally:
            con.sql("DETACH app")

        # Supplier files: found by pattern, never by name (weekly files and the late correction alike).
        # - all_varchar: keep costs exactly as written ("9,49", Arabic-Indic digits) for staging and quarantine
        # - union_by_name: W39 renames unit_cost -> cost_sar and adds currency
        # - filename: each row keeps the file it came from
        pattern = (data_dir / "suppliers" / "prices_*.csv").as_posix()
        con.sql(f"""
            CREATE OR REPLACE TABLE raw.supplier_prices AS
            SELECT * FROM read_csv('{pattern}', header = true, all_varchar = true,
                                   union_by_name = true, filename = true)
        """)


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


# The three published tables: the contract's columns in a fixed order, and the key that identifies a row.
PUBLISHED = {
    "silver.product_cost_history": (
        "SELECT sku, unit_cost_sar, valid_from, valid_to, is_current FROM silver.product_cost_history",
        ["sku", "valid_from"],
    ),
    "gold.weekly_margin": (
        """SELECT CAST(iso_week AS INTEGER) AS iso_week, city, category, CAST(lines AS INTEGER) AS lines,
                  revenue_sar, costed_revenue_sar, cost_sar, margin_sar,
                  CAST(uncosted_lines AS INTEGER) AS uncosted_lines
           FROM gold.weekly_margin""",
        ["iso_week", "city", "category"],
    ),
    "quality.price_quarantine": (
        "SELECT source_file, sku, valid_from, raw_cost, reason FROM quality.price_quarantine",
        ["source_file", "sku", "valid_from", "raw_cost"],
    ),
}


def _rows(data, keys):
    """Rows as Python tuples in key order, to compare a table's content regardless of file layout."""
    return data.sort_by([(k, "ascending") for k in keys]).to_pylist()


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg (see the brief for the exact columns).

        silver.product_cost_history   gold.weekly_margin   quality.price_quarantine

    Update existing tables in place, never drop and recreate them: the table's snapshots are
    how we see what a number was before the supplier's correction arrived.

    A table is overwritten (one new snapshot) only when its content changed, so a re-run on the
    same data leaves every table, and its snapshot history, exactly as it was.
    """
    catalog = get_catalog(warehouse)
    with duckdb.connect(str(db), read_only=True) as con:
        new = {name: con.sql(sql).fetch_arrow_table() for name, (sql, _) in PUBLISHED.items()}

    for name, data in new.items():
        keys = PUBLISHED[name][1]
        catalog.create_namespace_if_not_exists(name.split(".")[0])
        if not catalog.table_exists(name):
            catalog.create_table(name, schema=data.schema)
        table = catalog.load_table(name)

        if table.current_snapshot() is not None and _rows(table.scan().to_arrow(), keys) == _rows(data, keys):
            print(f"{name}: unchanged ({data.num_rows} rows), no new snapshot")
            continue
        with table.update_schema() as update:  # new columns are added, never dropped
            update.union_by_name(data.schema)
        table.overwrite(data.sort_by([(k, "ascending") for k in keys]))
        print(f"{name}: {data.num_rows} rows, snapshot {table.current_snapshot().snapshot_id}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
