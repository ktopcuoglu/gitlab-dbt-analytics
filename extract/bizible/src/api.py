import logging
import os

from gitlabdata.orchestration_utils import (
    query_dataframe,
    snowflake_stage_load_copy_remove,
    snowflake_engine_factory,
    bizible_snowflake_engine_factory,
)
from typing import Dict

from dateutil import rrule
from datetime import datetime, timedelta


class BizibleSnowFlakeExtractor:
    def __init__(self, config_dict: Dict):
        """

        :param config_dict: To be passed from the execute.py, should be ENV.
        :type config_dict:
        """
        self.bizible_engine = bizible_snowflake_engine_factory(
            config_dict, "BIZIBLE_USER"
        )
        self.snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    def get_bizible_query(self, full_table_name: str, date_column: str) -> Dict:
        """

        :param full_table_name: Table to retrieve
        :type full_table_name: str
        :param date_column: Date column to use for incrementing
        :type date_column: str
        :return: Dict containing the table name and last modified date (if it exists).
        :rtype: Dict
        """
        table_name = full_table_name.split(".")[-1]
        if len(date_column) > 0:
            snowflake_query_max_date = f"""
                            SELECT 
                                max({date_column}) as last_modified_date
                            FROM "BIZIBLE".{table_name} 
                        """
            df = query_dataframe(self.snowflake_engine, snowflake_query_max_date)

            last_modified_date_list = df["last_modified_date"].to_list()

            snowflake_last_modified_date = None

            if len(last_modified_date_list) > 0 and last_modified_date_list[0]:
                snowflake_last_modified_date = last_modified_date_list[0]

            if snowflake_last_modified_date:
                return {
                    table_name: {"last_modified_date": snowflake_last_modified_date}
                }

        return {table_name: {}}

    def upload_query(self, table_name: str, file_name: str, query: str) -> None:
        """

        :param file_name:
        :type file_name:
        :param table_name:
        :type table_name:
        :param query:
        :type query:
        """
        logging.info(f"Running {query}")
        df = query_dataframe(self.bizible_engine, query)

        logging.info(f"Creating {file_name}")
        df.to_csv(file_name, index=False, sep="|")

        logging.info(f"Processing {file_name} to {table_name}")
        snowflake_stage_load_copy_remove(
            file_name,
            f"BIZIBLE.BIZIBLE_LOAD",
            f"BIZIBLE.{table_name.lower()}",
            self.snowflake_engine,
            "csv",
            file_format_options="trim_space=true field_optionally_enclosed_by = '0x22' SKIP_HEADER = 1 field_delimiter = '|' ESCAPE_UNENCLOSED_FIELD = None",
        )
        logging.info(f"Processed {file_name}")

        logging.info(f"To delete {file_name}")
        os.remove(file_name)

    def upload_partitioned_files(
        self, table_name: str, last_modified_date: datetime, date_column: str
    ) -> None:
        """
        Created due to memory limitations, increments over the data set in hourly batches, primarily to ensure
        the BIZ.FACTS data size doesn't exceed what is available in K8

        :param table_name:
        :type table_name:
        :param last_modified_date:
        :type last_modified_date:
        :param date_column:
        :type date_column:
        """
        end_date = datetime.now()
        for dt in rrule.rrule(
            rrule.HOURLY, dtstart=last_modified_date, until=end_date, interval=2
        ):
            query_start_date = dt
            query_end_date = dt + timedelta(hours=2)

            query = f"""
            SELECT *, SYSDATE() as uploaded_at FROM BIZIBLE_ROI_V3.GITLAB.{table_name}
            WHERE {date_column} >= '{query_start_date}' 
            AND {date_column} < '{query_end_date}'
            """

            file_name = f"{table_name}_{str(dt.year)}-{str(dt.month)}-{str(dt.day)}-{str(dt.hour)}.csv"

            self.upload_query(table_name, file_name, query)

    def upload_complete_file(
        self,
        table_name: str,
    ) -> None:
        """

        :param table_name:
        :type table_name:
        """
        query = f"""
        SELECT *, SYSDATE() as uploaded_at FROM BIZIBLE_ROI_V3.GITLAB.{table_name}
        """

        file_name = f"{table_name}.csv"
        self.upload_query(table_name, file_name, query)

    def check_records_updated(
        self, table_name: str, last_modified_date: datetime, date_column: str
    ) -> int:
        """
        Small process written to check if there are records available for a given table before loading it.
        Solves a problem which causes the process to run for ages if the table hasn't been updated in a while.
        :param table_name:
        :type table_name:
        :param last_modified_date:
        :type last_modified_date:
        :param date_column:
        :type date_column:
        """
        query = f"""
        SELECT COUNT(*) as record_count FROM BIZIBLE_ROI_V3.GITLAB.{table_name}
        WHERE {date_column} >= '{last_modified_date}' 
        """

        record_count = query_dataframe(self.bizible_engine, query)[
            "record_count"
        ].to_list()[0]

        return record_count

    def upload_batch(
        self, table_name: str, last_modified_date: datetime, date_column: str
    ) -> None:
        """
        Written to upload smaller batches as one file.
        :param table_name:
        :type table_name:
        :param last_modified_date:
        :type last_modified_date:
        :param date_column:
        :type date_column:
        """
        query = f"""
        SELECT *, SYSDATE() as uploaded_at FROM BIZIBLE_ROI_V3.GITLAB.{table_name}
        WHERE {date_column} >= '{last_modified_date}' 
        """

        file_name = f"{table_name}.csv"
        self.upload_query(table_name, file_name, query)

    def process_bizible_query(self, query_details: Dict, date_column: str) -> None:
        """

        :param query_details:
        :type query_details:
        :param date_column:
        :type date_column:
        """
        for table_name in query_details.keys():
            logging.info(f"Running {table_name} query")
            last_modified_date = query_details[table_name].get("last_modified_date")
            if last_modified_date:
                record_count = self.check_records_updated(
                    table_name, last_modified_date, date_column
                )
                if record_count >= 10000:
                    self.upload_partitioned_files(
                        table_name,
                        last_modified_date,
                        date_column,
                    )
                elif record_count < 10000:
                    self.upload_batch(
                        table_name,
                        last_modified_date,
                        date_column,
                    )
                else:
                    logging.info(f"No records available for {table_name}")
            else:
                self.upload_complete_file(table_name)

    def extract_latest_bizible_file(
        self, table_name: str, date_column: str = ""
    ) -> None:
        """

        :param date_column:
        :type date_column:
        :param table_name:
        :type table_name:
        """

        query = self.get_bizible_query(
            full_table_name=table_name, date_column=date_column
        )
        self.process_bizible_query(query_details=query, date_column=date_column)
