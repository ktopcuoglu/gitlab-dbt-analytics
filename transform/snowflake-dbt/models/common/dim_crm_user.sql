WITH final AS (

    SELECT 
      {{ dbt_utils.star(
           from=ref('prep_crm_user'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
    FROM {{ ref('prep_crm_user') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2020-11-20",
    updated_date="2021-07-28"
) }}
