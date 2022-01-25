WITH final AS (

    SELECT 
      {{ dbt_utils.star(
           from=ref('prep_crm_account'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
    FROM {{ ref('prep_crm_account') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@michellecooper",
    created_date="2020-06-01",
    updated_date="2022-01-25"
) }}
