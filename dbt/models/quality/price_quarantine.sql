-- Grain: one row per rejected supplier row (every file, including any late correction).
-- Rule 3: unknown SKU first; otherwise a cost that is not a positive number.
with prices as (
    select * from {{ ref('stg_supplier_prices') }}
),

products as (
    select sku from {{ ref('stg_products') }}
),

checked as (
    select
        p.source_file,
        p.sku,
        p.valid_from,
        p.raw_cost,
        case
            when pr.sku is null                                       then 'unknown_sku'
            when p.unit_cost_sar is null or p.unit_cost_sar <= 0      then 'invalid_cost'
        end as reason
    from prices p
    left join products pr on pr.sku = p.sku
)

select source_file, sku, valid_from, raw_cost, reason
from checked
where reason is not null
