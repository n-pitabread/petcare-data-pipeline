{% snapshot snap_owners %}

{{
    config(
      target_schema='snapshots',
      unique_key='owner_id',
      strategy='check',
      check_cols=['owner_name', 'email', 'country'],
      updated_at='dbt_updated_at',
    )
}}

SELECT * FROM {{ ref('stg_owners') }}

{% endsnapshot %}

