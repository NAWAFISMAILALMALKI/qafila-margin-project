-- One row per row of every supplier price file, typed but not yet filtered.
-- Rule 1: the cost is in unit_cost or, from week 39 on, cost_sar; "9,49" and "٩٫٤٩" both mean 9.49.
-- Rule 2: SKUs are compared after trimming spaces.
with source as (
    select * from {{ source('raw', 'supplier_prices') }}
),

renamed as (
    select
        regexp_extract(filename, '[^/\\]+$')        as source_file,
        supplier_id,
        sku                                          as raw_sku,
        trim(sku)                                    as sku,
        coalesce(cost_sar, unit_cost)                as raw_cost,
        valid_from                                   as raw_valid_from,
        currency
    from source
)

select
    source_file,
    supplier_id,
    raw_sku,
    sku,
    raw_cost,
    -- Arabic-Indic digits and separator -> ASCII, decimal comma -> point, then a safe cast (NULL if not a number)
    try_cast(
        replace(translate(trim(raw_cost), '٠١٢٣٤٥٦٧٨٩٫', '0123456789.'), ',', '.')
        as double
    )                                                as unit_cost_sar,
    try_cast(translate(trim(raw_valid_from), '٠١٢٣٤٥٦٧٨٩', '0123456789') as date) as valid_from,
    currency
from renamed
