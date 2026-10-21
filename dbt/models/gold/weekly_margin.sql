-- Grain: one row per iso_week x city x category that has at least one counted order line.
-- Rule 7: a line counts when the order is delivered, quantity > 0 and the Riyadh order date is on or
--         after 2026-08-31 (the first supplier week).
-- Rule 9: each line is costed at the version valid on its order date (valid_from <= order_date < valid_to);
--         a line with no valid version is uncosted: it counts in lines and revenue, not in the cost columns.
{% set first_supplier_week = '2026-08-31' %}

with counted_lines as (
    select
        o.order_date,
        s.city,
        p.category,
        p.sku,
        i.quantity,
        i.line_revenue_sar
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }}   o on o.order_id   = i.order_id
    join {{ ref('stg_stores') }}   s on s.store_id   = o.store_id
    join {{ ref('stg_products') }} p on p.product_id = i.product_id
    where o.status = 'delivered'
      and i.quantity > 0
      and o.order_date >= date '{{ first_supplier_week }}'
),

costed_lines as (
    -- Point-in-time join: at most one version matches, because versions of a SKU never overlap.
    select
        l.*,
        l.quantity * h.unit_cost_sar as line_cost_sar
    from counted_lines l
    left join {{ ref('product_cost_history') }} h
           on h.sku = l.sku
          and h.valid_from <= l.order_date
          and (h.valid_to is null or l.order_date < h.valid_to)
),

weekly as (
    select
        cast(weekofyear(order_date) as integer)                                              as iso_week,
        city,
        category,
        count(*)                                                                             as lines,
        round(sum(line_revenue_sar), 2)                                                      as revenue_sar,
        round(coalesce(sum(line_revenue_sar) filter (where line_cost_sar is not null), 0), 2) as costed_revenue_sar,
        round(coalesce(sum(line_cost_sar), 0), 2)                                            as cost_sar,
        count(*) filter (where line_cost_sar is null)                                        as uncosted_lines
    from costed_lines
    group by all
)

select
    iso_week,
    city,
    category,
    lines,
    revenue_sar,
    costed_revenue_sar,
    cost_sar,
    round(costed_revenue_sar - cost_sar, 2) as margin_sar,
    uncosted_lines
from weekly
