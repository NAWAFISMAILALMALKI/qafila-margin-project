-- One row per order line (order_id, line_no). Rule 8: line revenue = round(quantity * unit_price - discount, 2).
select
    order_id,
    line_no,
    product_id,
    quantity,
    unit_price,
    discount,
    round(quantity * unit_price - discount, 2)  as line_revenue_sar
from {{ source('raw', 'order_items') }}
