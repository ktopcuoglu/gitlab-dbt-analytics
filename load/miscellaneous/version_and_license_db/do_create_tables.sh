#!/bin/bash
# for version_db
# declare TABLES_TO_EXPORT=("conversational_development_indices" "fortune_companies" "hosts" "raw_usage_data" "usage_data" "users" "version_checks" "versions");

# for license_db
# declare TABLES_TO_EXPORT=("add_ons" "granted_add_ons" "licenses" "users"); 
for table in "${TABLES_TO_EXPORT[@]}"
do
	.  $PATH_TO_MANIFESTS/$table.sql.sh
	export table_name=$table
	python3 ./parse_bash_sql.py
done
