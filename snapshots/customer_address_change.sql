{% snapshot customer_address_snapshot %}

    {{
      config(
        target_schema='snapshots',
        target_database='your_database',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='last_updated_at'
      )
    }}

    select
        customer_id,
        address_line1,
        address_line2,
        city,
        state,
        postal_code,
        country,
        last_updated_at
    from {{ source('crm', 'customers') }}

{% endsnapshot %}
