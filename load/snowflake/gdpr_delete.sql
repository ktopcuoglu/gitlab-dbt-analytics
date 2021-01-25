-- Run locally 'echo -n emailtosha  | shasum -a 256'
-- ====================
-- set email_sha = 'SHA';
-- ====================

SELECT 
  LOWER(table_catalog) || '.' || LOWER(table_schema) || '.' || LOWER(table_name)        AS fqd_name,
  'delete from ' || fqd_name || ' where SHA2(trim(' || lower(column_name) || ')) = ' || '\'' || $email_sha || '\'' as command
FROM "RAW"."INFORMATION_SCHEMA"."COLUMNS"
WHERE LOWER(column_name) LIKE '%email%' 
  AND table_schema NOT IN ('SNAPSHOTS') -- Get columns that aren't in the snaphsots table
  AND data_type NOT IN ('BOOLEAN','TIMESTAMP_TZ','NUMBER');

-- =========
-- Snapshots
-- =========

-- Update non-email columns
WITH email_columns AS (
    SELECT table_catalog,
           table_schema,
           data_type,
           lower(table_catalog)||'.'||lower(table_schema)||'.'||lower(table_name) AS fqd_name,
           column_name
    FROM "RAW"."INFORMATION_SCHEMA"."COLUMNS"
    WHERE lower(column_name) LIKE '%email%'
      AND table_schema IN ('SNAPSHOTS')
      AND data_type NOT IN ('BOOLEAN', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ', 'FLOAT', 'DATE', 'NUMBER')
      AND lower(column_name) NOT IN
          ('dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to', '_task_instance', '_uploaded_at', '_sdc_batched_at', '_sdc_extracted_at',
           '_sdc_received_at', '_sdc_sequence', '_sdc_table_version')
),

non_email_columns AS (
    SELECT table_catalog,
        table_schema,
        data_type,
        lower(table_catalog)||'.'||lower(table_schema)||'.'||lower(table_name) AS fqd_name,
        column_name
    FROM "RAW"."INFORMATION_SCHEMA"."COLUMNS" AS a
    WHERE lower(column_name) NOT LIKE '%email%'
        AND table_schema IN ('SNAPSHOTS')
        AND data_type NOT IN ('BOOLEAN', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ', 'FLOAT', 'DATE', 'NUMBER')
        AND lower(column_name) NOT IN
            ('dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to', '_task_instance', '_uploaded_at', '_sdc_batched_at', '_sdc_extracted_at',
            '_sdc_received_at', '_sdc_sequence', '_sdc_table_version')
        AND lower(column_name) NOT LIKE '%id%'
)

SELECT 'udpate '||non_email_columns.fqd_name||' set '||lower(non_email_columns.column_name)||' = '||'\'GDPR Redacted\''||' where SHA2(trim('||
       lower(email_columns.column_name)||')) = '||'\''||$email_sha||'\'' AS command
FROM non_email_columns
     LEFT JOIN email_columns ON non_email_columns.fqd_name=email_columns.fqd_name;



-- Update email columns
WITH email_columns AS (
    SELECT table_catalog,
           table_schema,
           data_type,
           lower(table_catalog)||'.'||lower(table_schema)||'.'||lower(table_name) AS fqd_name,
           column_name
    FROM "RAW"."INFORMATION_SCHEMA"."COLUMNS"
    WHERE lower(column_name) LIKE '%email%'
      AND table_schema IN ('SNAPSHOTS')
      AND data_type NOT IN ('BOOLEAN', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ', 'FLOAT', 'DATE', 'NUMBER')
      AND lower(column_name) NOT IN
          ('dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to', '_task_instance', '_uploaded_at', '_sdc_batched_at', '_sdc_extracted_at',
           '_sdc_received_at', '_sdc_sequence', '_sdc_table_version')
)

SELECT 'udpate '||fqd_name||' set '||lower(column_name)||' = '||'\'GDPR Redacted\''||' where SHA2(trim('||lower(column_name)||')) = '||'\''||$email_sha||
       '\'' AS command
FROM email_columns
WHERE lower(column_name) LIKE '%email%'
  AND table_schema IN ('SNAPSHOTS')
  AND data_type NOT IN ('BOOLEAN', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ', 'FLOAT', 'DATE', 'NUMBER')
  AND lower(column_name) NOT IN
      ('dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to', '_task_instance', '_uploaded_at', '_sdc_batched_at', '_sdc_extracted_at',
       '_sdc_received_at', '_sdc_sequence', '_sdc_table_version');
