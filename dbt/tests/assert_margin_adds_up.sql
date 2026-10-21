-- Fails if any weekly_margin row is internally inconsistent: margin must equal costed revenue minus cost,
-- costed revenue cannot exceed revenue, and uncosted lines cannot exceed lines.
select *
from {{ ref('weekly_margin') }}
where abs(margin_sar - (costed_revenue_sar - cost_sar)) > 0.011
   or costed_revenue_sar > revenue_sar + 0.01
   or uncosted_lines > lines
