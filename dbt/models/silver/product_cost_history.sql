-- Grain: one row per SKU per cost version (SCD2).
-- valid_from is inclusive, valid_to is exclusive and empty for the current version.
with valid_quotes as (
    -- Rule 3: quarantined rows are ignored everywhere else. Valid = a known SKU and a positive cost,
    -- the exact complement of price_quarantine.
    select
        p.sku,
        p.valid_from,
        round(p.unit_cost_sar, 2) as unit_cost_sar,
        p.source_file
    from {{ ref('stg_supplier_prices') }} p
    join {{ ref('stg_products') }} pr on pr.sku = p.sku
    where p.unit_cost_sar > 0
),

latest_file_wins as (
    -- Rule 5: same SKU and valid_from in several files -> the file name that sorts last wins
    -- (prices_2026-W39_fix.csv replaces prices_2026-W39.csv).
    select *
    from valid_quotes
    qualify row_number() over (partition by sku, valid_from order by source_file desc) = 1
),

changes_only as (
    -- Rules 4 and 6: a missing week simply continues the previous version;
    -- a new version starts only when the cost differs from the previous quote.
    select *
    from latest_file_wins
    qualify unit_cost_sar is distinct from lag(unit_cost_sar) over (partition by sku order by valid_from)
)

select
    sku,
    unit_cost_sar,
    valid_from,
    lead(valid_from) over (partition by sku order by valid_from) as valid_to,
    lead(valid_from) over (partition by sku order by valid_from) is null as is_current
from changes_only
