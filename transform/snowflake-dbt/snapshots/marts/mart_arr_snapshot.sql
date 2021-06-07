{% snapshot mart_arr_snapshot %}
    -- Using dbt updated at field as we want a new set of data everyday.
    {{
        config(
          unique_key='primary_key',
          strategy='timestamp',
          updated_at='dbt_created_at',
          invalidate_hard_deletes=True
         )
    }}
    WITH base AS (

        SELECT *
        FROM {{ ref('mart_arr') }}
    )

    SELECT * FROM base

{% endsnapshot %}
