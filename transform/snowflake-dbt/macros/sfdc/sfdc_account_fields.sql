{%- macro sfdc_account_fields() -%}

    CASE
      WHEN LOWER(sfdc_account.gtm_strategy) IN ('account centric', 'account based - net new', 'account based - expand') THEN 'Focus Account'
      ELSE 'Non - Focus Account'
    END                                                                                                                                                     AS crm_account_focus_account,
    IFF(sfdc_record_type.record_type_label = 'Partner'
        AND sfdc_account.partner_type IN ('Alliance', 'Channel')
        AND sfdc_account.partner_status = 'Authorized',
        TRUE, FALSE)                                        AS is_reseller,
    CASE
      WHEN LOWER(ultimate_parent_account.gtm_strategy) IN ('account centric', 'account based - net new', 'account based - expand') THEN 'Focus Account'
      ELSE 'Non - Focus Account'
    END                                                                                                                                                    AS parent_crm_account_focus_account,
    CASE
       WHEN ultimate_parent_account.tsp_max_family_employees > 2000 THEN 'Employees > 2K'
       WHEN ultimate_parent_account.tsp_max_family_employees <= 2000 AND ultimate_parent_account.tsp_max_family_employees > 1500 THEN 'Employees > 1.5K'
       WHEN ultimate_parent_account.tsp_max_family_employees <= 1500 AND ultimate_parent_account.tsp_max_family_employees > 1000  THEN 'Employees > 1K'
       ELSE 'Employees < 1K'
    END                                                                                                                                                    AS parent_crm_account_employee_count_band,
    CASE
       WHEN sfdc_account.tsp_max_family_employees > 2000 THEN 'Employees > 2K'
       WHEN sfdc_account.tsp_max_family_employees <= 2000 AND sfdc_account.tsp_max_family_employees > 1500 THEN 'Employees > 1.5K'
       WHEN sfdc_account.tsp_max_family_employees <= 1500 AND sfdc_account.tsp_max_family_employees > 1000  THEN 'Employees > 1K'
       ELSE 'Employees < 1K'
    END                                                 AS crm_account_employee_count_band

{%- endmacro -%}