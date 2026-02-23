-- FAIL any fact row that does NOT find exactly one SCD2 policy version on its snapshot_date
with f as (
  select claim_id, policy_id, snapshot_date
  from {{ ref('fct_claim_reserve_snapshots') }}
),
p as (
  select policy_id, effective_from, coalesce(effective_to, '2999-12-31') as effective_to
  from {{ ref('dim_policy') }}
),
matched as (
  select
    f.claim_id,
    f.policy_id,
    f.snapshot_date,
    count(*) as match_cnt
  from f
  join p
    on f.policy_id = p.policy_id
   and f.snapshot_date >= p.effective_from
   and f.snapshot_date <  p.effective_to
  group by 1,2,3
)
select *
from matched
where match_cnt != 1
