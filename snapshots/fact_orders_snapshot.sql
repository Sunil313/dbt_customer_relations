{% snapshot fact_orders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=true
    )
}}

select
    order_id,
    customer_id,
    order_date,
    order_status,
    payment_status,
    order_amount,
    currency,
    updated_at
from {{ ref('fact_orders') }}

{% endsnapshot %}
