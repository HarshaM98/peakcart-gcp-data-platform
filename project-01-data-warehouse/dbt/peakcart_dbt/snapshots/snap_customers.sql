{% snapshot snap_customers %}

    {{
        config(
            target_schema='peakcart_snapshots',
            unique_key='customer_id',
            strategy='check',
            check_cols=['segment', 'city', 'state'],
            invalidate_hard_deletes=True
        )
    }}

    select * from {{ ref('stg_customers') }}

{% endsnapshot %}
