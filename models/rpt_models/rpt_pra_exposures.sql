{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "reporting_period",
        "data_type": "date"
    },
    unique_key=['exposure_id', 'reporting_period']
) }}

with exposures_raw as (
    select
        exposure_id,
        account_id,
        counterparty_id,
        exposure_amount,
        exposure_currency,
        risk_type,
        asset_class,
        exposure_date
    from {{ ref('fct_credit_exposure') }}

    {% if is_incremental() %}
      where exposure_date > (
        select coalesce(max(reporting_period), '1900-01-01') from {{ this }}
      )
    {% endif %}
),

account_info as (
    select
        account_id,
        product_code,
        customer_id
    from {{ ref('dim_account_lifecycle') }}
    where is_current_status = true
),

reporting_periods as (
    select
        date_trunc('month', exposure_date) as reporting_period,
        exposure_id,
        account_id,
        counterparty_id,
        exposure_amount,
        exposure_currency,
        risk_type,
        asset_class
    from exposures_raw
),

aggregated_exposures as (
    select
        reporting_period,
        risk_type,
        asset_class,
        sum(exposure_amount) as total_exposure
    from reporting_periods rp
    group by 1, 2, 3
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'reporting_period',
            'risk_type',
            'asset_class'
        ]) }} as pra_exposure_sk,

        reporting_period,
        risk_type,
        asset_class,
        total_exposure,

        current_timestamp as load_timestamp
    from aggregated_exposures
)

select * from final
