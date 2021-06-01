{{ simple_cte([('prep_host', 'prep_host'),
('prep_usage_ping', 'prep_usage_ping')])}}

, joined AS (

    SELECT 
    {{ dbt_utils.surrogate_key(['prep_host.dim_host_id', 'dim_instance_id'])}} AS dim_installation_id,
    dim_instance_id,
    prep_host.dim_host_id,
    prep_host.host_name
    FROM prep_usage_ping
    INNER JOIN prep_host ON prep_usage_ping.dim_host_id = prep_host.dim_host_id
    {{ dbt_utils.group_by(n=4) }}
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-05-20",
    updated_date="2021-05-20"
) }}


