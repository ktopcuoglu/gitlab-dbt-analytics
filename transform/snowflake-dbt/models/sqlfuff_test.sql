SELECT
    table_id,
    {{ sqlfluff_test_macro('other_id') }} AS other_id,
    {{ dbt_utils.surrogate_key(['other_id']) }} AS an_other_id
FROM my_table
