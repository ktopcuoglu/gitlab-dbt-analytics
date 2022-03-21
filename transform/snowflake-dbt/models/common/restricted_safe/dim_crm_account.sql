{{ config({
    "alias": "dim_crm_account",
    "post-hook": '{{ apply_dynamic_data_masking(columns = [{"account_owner":"string"},{"carr_this_account":"float"},{"created_by":"string"},{"crm_account_name":"string"},{"crm_account_owner":"string"},{"crm_account_owner_team":"string"},{"crm_account_zoom_info_dozisf_zi_id":"string"},{"crm_account_zoom_info_ultimate_parent_company_name":"string"},{"crm_account_zoom_info_ultimate_parent_company_zi_id":"string"},{"crm_account_zoom_info_website":"string"},{"dim_crm_account_id":"string"},{"federal_account":"boolean"},{"merged_to_account_id":"string"},{"parent_crm_account_demographics_max_family_employee":"float"},{"parent_crm_account_demographics_upa_postal_code":"string"},{"parent_crm_account_demographics_upa_street":"string"},{"parent_crm_account_lam":"float"},{"parent_crm_account_name":"string"},{"parent_crm_account_owner_team":"string"},{"parent_crm_account_zoom_info_parent_company_name":"string"},{"parent_crm_account_zoom_info_parent_company_zi_id":"string"},{"crm_account_demographics_employee_count":"float"},{"tsp_account_employees":"float"},{"tsp_max_family_employees":"float"},{"crm_account_tsp_account_employees":"float"},{"crm_account_zoom_info_company_other_domains":"string"},{"parent_crm_account_zoom_info_website":"string"},{"technical_account_manager":"string"},{"record_type_id":"string"},{"parent_crm_account_tsp_account_employees":"float"},{"parent_crm_account_tsp_max_family_employees":"float"},{"crm_account_tsp_max_family_employees":"float"},{"parent_crm_account_zoom_info_company_other_domains":"string"},{"parent_crm_account_lam_dev_count":"float"}]) }}'
}) }}

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
