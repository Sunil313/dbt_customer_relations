{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['entity_id', 'entity_type', 'score_date']
) }}

with source_scores as (
    select
        entity_id,
        entity_type,
        risk_score,
        upper(risk_category) as risk_category,
        cast(score_date as date) as score_date
    from {{ ref('stg_risk_scores') }}

    {% if is_incremental() %}
        where score_date > (
            select coalesce(max(score_date), '1900-01-01') from {{ this }}
        )
    {% endif %}
),

entity_details as (
    select
        entity_id,
        entity_type,
        case
            when entity_type = 'CUSTOMER' then c.risk_rating
            when entity_type = 'ACCOUNT' then a.account_risk_rating
            when entity_type = 'POLICY' then p.policy_risk_rating
            else null
        end as historic_risk_rating
    from source_scores s
    left join {{ ref('dim_customer_360') }} c
        on s.entity_type = 'CUSTOMER' and s.entity_id = c.customer_id
    left join {{ ref('dim_account_lifecycle') }} a
        on s.entity_type = 'ACCOUNT' and s.entity_id = a.account_id and a.is_current_status = true
    left join {{ ref('dim_policy') }} p
        on s.entity_type = 'POLICY' and s.entity_id = p.policy_id and p.is_current_record = true
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'entity_id',
            'entity_type',
            'score_date'
        ]) }} as risk_score_sk,

        s.entity_id,
        s.entity_type,
        s.risk_score,
        s.risk_category,
        e.historic_risk_rating,
        s.score_date,

        current_timestamp as load_timestamp

    from source_scores s
    left join entity_details e
        on s.entity_id = e.entity_id
       and s.entity_type = e.entity_type
)

select * from final
