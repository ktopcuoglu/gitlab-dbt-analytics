----------------------------------------------------------------------
-- This is one time script and will be deleted after reviewing
----------------------------------------------------------------------


----------------------------------------------------------------------
-- Check newly created tables
----------------------------------------------------------------------
SELECT *
  FROM "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW".saas_usage_ping.instance_sql_metrics
 LIMIT 10;

 SELECT *
  FROM "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW".saas_usage_ping.instance_sql_errors
 LIMIT 10;


----------------------------------------------------------------------
-- 1/3 Insert script for data into new table
----------------------------------------------------------------------
INSERT INTO "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW".saas_usage_ping.instance_sql_metrics(query_map,
                                                                                                    run_results,
                                                                                                    ping_date,
                                                                                                    _uploaded_at)
SELECT query_map,
       run_results,
       ping_date,
       _uploaded_at
  FROM "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW"."SAAS_USAGE_PING"."GITLAB_DOTCOM" -- rename schema_name for PROD
 ORDER BY ping_date DESC;


----------------------------------------------------------------------
-- 2/3 Delete script for the old table.
----------------------------------------------------------------------

DROP TABLE "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW"."SAAS_USAGE_PING"."GITLAB_DOTCOM"."SAAS_USAGE_PING"."GITLAB_DOTCOM"; -- rename schema_name for PROD


----------------------------------------------------------------------
-- 3/3 UPDATE script for RUN_ID column.
----------------------------------------------------------------------
UPDATE "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW".saas_usage_ping.instance_sql_metrics -- rename schema_name for PROD
   SET run_id = md5_hex(ping_date)
 WHERE run_id IS NULL;


COMMIT;

----------------------------------------------------------------------
-- Test cases
----------------------------------------------------------------------

-- 1/2 Test RAW
SELECT COUNT(1) as CNT -- expect 0, OK
  FROM "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW".saas_usage_ping.instance_sql_metrics -- rename schema_name for PROD
 WHERE run_id IS NULL;


SELECT COUNT(1) as CNT -- expect 31, OK
  FROM "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW".saas_usage_ping.instance_sql_metrics; -- rename schema_name for PROD


SELECT COUNT(1) -- expect 1, OK
  FROM "8994-CHANGE-IN-SAAS_USAGE_PING-GITLAB_DOTCOM_RAW".saas_usage_ping.instance_sql_errors;  -- rename schema_name for PROD

-- 2/2 Test PREP - after DBT job is run:
-- # dbt run --models saas_usage_ping_instance
 SELECT COUNT(1) as CNT -- expect 31, OK
   FROM "RBACOVIC_PREP"."SAAS_USAGE_PING"."SAAS_USAGE_PING_INSTANCE";  -- rename schema_name for PROD