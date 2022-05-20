## Transform

Creating dbt's `schema.yml` files is a requirement of the development process, but can be a bit tedious. 
This script creates one massive `schema.yml` file that creates a template for all of your dbt models, assuming they're in your `PROD` schema. You can just select the relevant lines for the models you're working on and paste them in your `schema.yml` file. 

It expects the following environment variables:
* `SNOWFLAKE_ACCOUNT`
* `SNOWFLAKE_TRANSFORM_USER`
* `SNOWFLAKE_PASSWORD`
* `SNOWFLAKE_PROD_DATABASE`
* `SNOWFLAKE_SCHEMA`
* `SNOWFLAKE_TRANSFORM_WAREHOUSE`
* `SNOWFLAKE_TRANSFORM_ROLE`
* `SNOWFLAKE_TIMEZONE`

Example:
```snowflake
SNOWFLAKE_ACCOUNT=gitlab
SNOWFLAKE_TRANSFORM_USER={user}
SNOWFLAKE_PASSWORD={password}
SNOWFLAKE_PROD_DATABASE=PROD
SNOWFLAKE_SCHEMA=INFORMATION_SCHEMA 
SNOWFLAKE_TRANSFORM_WAREHOUSE=TRANSFORMING
SNOWFLAKE_TRANSFORM_ROLE={role}
SNOWFLAKE_TIMEZONE=America/Los_Angeles
```

