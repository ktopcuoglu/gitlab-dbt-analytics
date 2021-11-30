{% snapshot mart_waterfall_snapshot %}
    -- Using dbt updated at field as we want a new set of data everyday.
    {{
        config(
          unique_key='primary_key',
          strategy='timestamp',
          updated_at='dbt_created_at',
          invalidate_hard_deletes=True

         )
    }}

    SELECT
    {{
          dbt_utils.star(
            from=ref('mart_waterfall'),
            except=['DBT_UPDATED_AT']
            )
      }}
    FROM {{ ref('mart_waterfall') }}

{% endsnapshot %}