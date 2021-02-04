from typing import Dict, Any

class PostgresPipelineTable:

    def __init__(
        self,
        table_config: Dict[str, str]
    ):
        self.query                    = table_config["import_query"]
        self.import_db                = table_config["import_db"]
        self.export_schema            = table_config["export_schema"]
        self.export_table             = table_config["export_table"]
        self.export_table_primary_key = table_config["export_table_primary_key"]
        if "additional_filtering" in table_config:
            self.additional_filtering = table_config["additional_filtering"]
        self.advanced_metadata = ("advanced_metadata" in table_config) and table_config["advanced_metadata"] == "true"

    def is_scd(self) -> bool:
        return not self.is_incremental()

    def do_scd(self) -> bool:
        if not self.is_scd():
            return True

    def is_incremental(self) -> bool:
        return "{EXECUTION_DATE}" in self.query

    def do_incremental(self) -> bool:
        if not self.needs_incremental_backfill():
            return True

    def needs_incremental_backfill(self) -> bool:
        if not self.is_incremental():
            return False

    def do_incremental_backfill(self) -> bool:
        if not self.needs_incremental_backfill():
            return True

