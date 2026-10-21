-- One row per order. created_at is UTC text; Qafila's business day is Riyadh time (UTC+3, no daylight saving).
select
    order_id,
    store_id,
    status,
    cast(created_at as timestamp)                              as created_at_utc,
    cast(cast(created_at as timestamp) + interval 3 hour as date) as order_date
from {{ source('raw', 'orders') }}
