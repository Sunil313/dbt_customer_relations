{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}

with reserve_snapshots as (

    select
        claim_id,
        cast(reserve_snapshot_date as date) as snapshot_date,
        reserve_type,
        reserve_amount
    from {{ ref('stg_claim_reserves_snapshots') }}

    {% if is_incremental() %}
        where reserve_snapshot_date > (
            select coalesce(max(snapshot_date), '1900-01-01') from {{ this }}
        )
    {% endif %}

),

current_claims as (

    select
        claim_id,
        policy_id
    from {{ ref('fct_claims') }}

),

current_policy as (

    select
        policy_id,
        product_code,
        customer_id
    from {{ ref('dim_policy') }}
    where is_current_record = true

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'r.claim_id',
            'r.snapshot_date',
            'r.reserve_type'
        ]) }} as claim_reserve_sk,

        r.claim_id,
        c.policy_id,
        p.product_code,
        p.customer_id,

        r.snapshot_date,
        r.reserve_type,
        r.reserve_amount,

        current_timestamp as load_timestamp

    from reserve_snapshots r
    join current_claims c
        on r.claim_id = c.claim_id
    join current_policy p
        on c.policy_id = p.policy_id

)

select * from final
