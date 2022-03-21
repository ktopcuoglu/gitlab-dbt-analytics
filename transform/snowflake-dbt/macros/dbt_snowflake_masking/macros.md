{% docs create_masking_policy_hide_string_column_values %}
This macro is designed to apply Snowflake Dynamic Masking to PII `string` type columns.
Currently we are masking these columns for the DATA OBSERVABILITY role only, but these macros can be extended acordingly if we need to add more roles in the future. 

```
{% raw %}

CREATE MASKING POLICY IF NOT EXISTS {{database}}.{{schema}}.hide_string_column_values AS (val string) 
  RETURNS string ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN '**********'
      ELSE val
      END

{% endraw %}
```

It takes 2 parameters:
* `database`: which is the name of the database we want to create this policy in
* `schema`: which is the schema name where we want to create the policy


Output:

* If the logged in user has the role Data Observability assigned to it, then it will only see `'******'` instead of values, for the PII columns
* Otherwise, the actual values will be visible

{% enddocs %}

{% docs create_masking_policy_hide_array_column_values %}
This macro is designed to apply Snowflake Dynamic Masking to PII `array` type columns.
Currently we are masking these columns for the DATA OBSERVABILITY role only, but these macros can be extended acordingly if we need to add more roles in the future. 

```
{% raw %}

CREATE MASKING POLICY IF NOT EXISTS {{database}}.{{schema}}.hide_array_column_values AS (val array) 
  RETURNS array ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN NULL
      ELSE val
      END

{% endraw %}
```

It takes 2 parameters:
* `database`: which is the name of the database we want to create this policy in
* `schema`: which is the schema name where we want to create the policy


Output:

* If the logged in user has the role Data Observability assigned to it, then it will only see `NULL` instead of values, for the PII columns
* Otherwise, the actual values will be visible

{% enddocs %}

{% docs create_masking_policy_hide_boolean_column_values %}
This macro is designed to apply Snowflake Dynamic Masking to PII `boolean` type columns.
Currently we are masking these columns for the DATA OBSERVABILITY role only, but these macros can be extended acordingly if we need to add more roles in the future. 

```
{% raw %}

CREATE MASKING POLICY IF NOT EXISTS {{database}}.{{schema}}.hide_boolean_column_values AS (val boolean) 
  RETURNS boolean ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN NULL
      ELSE val
      END

{% endraw %}
```

It takes 2 parameters:
* `database`: which is the name of the database we want to create this policy in
* `schema`: which is the schema name where we want to create the policy


Output:

* If the logged in user has the role Data Observability assigned to it, then it will only see `NULL` instead of values, for the PII columns
* Otherwise, the actual values will be visible

{% enddocs %}

{% docs create_masking_policy_hide_float_column_values %}
This macro is designed to apply Snowflake Dynamic Masking to PII `float` type columns.
Currently we are masking these columns for the DATA OBSERVABILITY role only, but these macros can be extended acordingly if we need to add more roles in the future. 

```
{% raw %}

CREATE MASKING POLICY IF NOT EXISTS {{database}}.{{schema}}.hide_float_column_values AS (val float) 
  RETURNS float ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN 0
      ELSE val
      END

{% endraw %}
```

It takes 2 parameters:
* `database`: which is the name of the database we want to create this policy in
* `schema`: which is the schema name where we want to create the policy


Output:

* If the logged in user has the role Data Observability assigned to it, then it will only see `0` instead of values, for the PII columns
* Otherwise, the actual values will be visible

{% enddocs %}

{% docs create_masking_policy_hide_number_column_values %}
This macro is designed to apply Snowflake Dynamic Masking to PII `number(38,0)` type columns.
Currently we are masking these columns for the DATA OBSERVABILITY role only, but these macros can be extended acordingly if we need to add more roles in the future. 

```
{% raw %}

CREATE MASKING POLICY IF NOT EXISTS {{database}}.{{schema}}.hide_number_column_values AS (val number(38,0)) 
  RETURNS number(38,0) ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN 0
      ELSE val
      END

{% endraw %}
```

It takes 2 parameters:
* `database`: which is the name of the database we want to create this policy in
* `schema`: which is the schema name where we want to create the policy


Output:

* If the logged in user has the role Data Observability assigned to it, then it will only see `0` instead of values, for the PII columns
* Otherwise, the actual values will be visible

{% enddocs %}


{% docs create_masking_policy_hide_variant_column_values %}
This macro is designed to apply Snowflake Dynamic Masking to PII `variant` type columns.
Currently we are masking these columns for the DATA OBSERVABILITY role only, but these macros can be extended acordingly if we need to add more roles in the future. 

```
{% raw %}

CREATE MASKING POLICY IF NOT EXISTS {{database}}.{{schema}}.hide_variant_column_values AS (val variant) 
  RETURNS variant ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN NULL
      ELSE val
      END

{% endraw %}
```

It takes 2 parameters:
* `database`: which is the name of the database we want to create this policy in
* `schema`: which is the schema name where we want to create the policy


Output:

* If the logged in user has the role Data Observability assigned to it, then it will only see `NULL` instead of values, for the PII columns
* Otherwise, the actual values will be visible

{% enddocs %}

{% docs create_masking_policy_hide_date_column_values %}
This macro is designed to apply Snowflake Dynamic Masking to PII `date` type columns.
Currently we are masking these columns for the DATA OBSERVABILITY role only, but these macros can be extended acordingly if we need to add more roles in the future. 

```
{% raw %}

CREATE MASKING POLICY IF NOT EXISTS {{database}}.{{schema}}.hide_date_column_values AS (val string) 
  RETURNS string ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN '**********'
      ELSE val
      END

{% endraw %}
```

It takes 2 parameters:
* `database`: which is the name of the database we want to create this policy in
* `schema`: which is the schema name where we want to create the policy


Output:

* If the logged in user has the role Data Observability assigned to it, then it will only see `NULL` instead of values, for the PII columns
* Otherwise, the actual values will be visible

{% enddocs %}