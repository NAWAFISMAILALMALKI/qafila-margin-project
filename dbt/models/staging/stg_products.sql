-- One row per product. Two SKUs arrive with trailing spaces: trim them so they match the supplier files.
select
    product_id,
    trim(sku)    as sku,
    name_en      as product_name,
    category
from {{ source('raw', 'products') }}
