
{{ simple_cte([
    ('saas_usage_ping_namespace', 'saas_usage_ping_namespace'),
    ('dim_date', 'dim_date')
]) }}

, transformed AS (

    SELECT 
      ping_name,
      ping_date,
      ping_level,  
      IFF(error = 'Success', TRUE, FALSE)          AS is_success,
      COUNT(DISTINCT namespace_ultimate_parent_id) AS namespace_with_value
    FROM saas_usage_ping_namespace
    GROUP BY 1,2,3,4

), joined AS (

    SELECT *
    FROM transformed
    INNER JOIN dim_date ON ping_date = date_day

)

SELECT *
FROM joined
