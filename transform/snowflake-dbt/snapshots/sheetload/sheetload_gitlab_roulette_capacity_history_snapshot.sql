{% snapshot sheetload_gitlab_roulette_capacity_history_snapshot %}

{{
    config(
      unique_key='unique_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

SELECT
  {{ dbt_utils.surrogate_key(['project','role','timestamp']) }} AS unique_id,
  *,
  _UPDATED_AT::NUMBER::TIMESTAMP as updated_at
FROM {{ source('sheetload','gitlab_roulette_capacity_history') }}

{% endsnapshot %}
