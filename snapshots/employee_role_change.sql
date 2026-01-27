{% snapshot employee_role_snapshot %}

    {{
      config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='role_last_updated'
      )
    }}

    select
        employee_id,
        role,
        department,
        role_last_updated
    from {{ source('hr', 'employee_roles') }}

{% endsnapshot %}
