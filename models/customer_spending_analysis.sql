{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'  -- use 'insert_overwrite' if your warehouse requires
) }}

with source_data as (

    select
        transaction_id,               -- Unique transaction ID
        customer_id,
        transaction_date,
        store_area,                  -- Geographic area / store location
        payment_bank,                -- Bank of card used
        payment_card_type,           -- Debit / Credit / Prepaid etc.
        transaction_amount,
        transaction_timestamp,
        loyalty_member_flag,
        loyalty_points_earned,
        last_updated_at              -- timestamp of last update for CDC (change detection)
    from {{ source('retail', 'daily_transactions') }}

    {% if is_incremental() %}
    where last_updated_at > (
        select coalesce(max(last_updated_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

),

aggregated_data as (

    select
        customer_id,
        transaction_date,
        store_area,
        payment_bank,
        payment_card_type,

        count(distinct transaction_id) as transactions_count,
        sum(transaction_amount) as total_spent,
        avg(transaction_amount) as avg_transaction_value,
        sum(loyalty_points_earned) as total_loyalty_points,

        max(transaction_timestamp) as last_transaction_time

    from source_data
    group by
        customer_id,
        transaction_date,
        store_area,
        payment_bank,
        payment_card_type

),

final as (

    select
        customer_id,
        transaction_date,
        store_area,
        payment_bank,
        payment_card_type,
        transactions_count,
        total_spent,
        avg_transaction_value,
        total_loyalty_points,
        last_transaction_time,

        -- Metadata columns
        current_date as business_date,
        current_timestamp as ingestion_timestamp,
        '{{ invocation_id }}' as dbt_invocation_id

    from aggregated_data

)

select * from final
