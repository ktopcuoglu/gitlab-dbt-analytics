import logging
import subprocess
import sys
import requests
import math
import json
import time
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from os import environ as env


class ZuoraRevProAPI:
    def __init__(self, config_dict: Dict[str, str]):
        self.headers = config_dict["headers"]
        self.authenticate_url_zuora_revpro = config_dict[
            "authenticate_url_zuora_revpro"
        ]
        self.zuora_fetch_data_url = config_dict["zuora_fetch_data_url"]
        self.bucket_name = config_dict["bucket_name"]
        self.clientId = config_dict["clientId"]
        logging.basicConfig(
            filename="zuora_extract.log",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level=20,
        )
        self.logger = logging.getLogger(__name__)

    def get_start_date(self, table_name: str):
        """
        This function is responsible to pull the last extract date value present in the file named start_date_<table_name>.csv
        in GCS for the given table name and pass it back to the calling function. For each of the table this file
        start_date_<table_name>.csv is generated as part of the extraction process.
        The function check that the table name mentioned inside the file under the table_name column is the same as what is being
        passed as table_name. If the file is not having any rows only a header it will return null.
        """
        bucket_name = self.bucket_name
        cmd_to_download_start_date = f"gsutil cp gs://{bucket_name}/RAW_DB/staging/{table_name}/start_date_{table_name}.csv ."

        # Download the start_dare_<table_name>.csv fle from bucket. If the file is not present exit the process with the exception message.

        try:
            self.logger.info(cmd_to_download_start_date)
            subprocess.run(cmd_to_download_start_date, shell=True, check=True)
        except Exception:
            self.logger.error(
                "Error while downloading the startdate file from GCS", exec_info=True
            )
            sys.exit(1)

        try:
            # read the csv file and match the table_name. If there is value present for the Load_date i.e. then return that else return Null.
            table_name_date = pd.read_csv(f"start_date_{table_name}.csv")
            if table_name_date.iloc[0]["table_name"] == table_name:
                start_date = table_name_date.loc[
                    table_name_date["table_name"] == table_name, "load_date"
                ]
                return (start_date.where(pd.notnull(start_date), None)).to_list()[0]
            else:
                self.logger.info(
                    "Treating this as first run and will start download from day 0"
                )
                return None
        except Exception:
            self.logger.error(
                "Error while reading csv file to fetch start date", exec_info=True
            )
            sys.exit(1)

    def set_load_date(self, table_name: str, load_date: str) -> None:
        """
        This function take input table_name and to_date value as load_date. It is responsible to update the load_date value in start_date_{table_name}.csv file
        and upload it to GCS for next extraction process.
        """
        bucket_name = self.bucket_name
        load_date_file_name = f"start_date_{table_name}.csv"
        cmd_to_upload_file = f"gsutil cp start_date_{table_name}.csv gs://{bucket_name}/RAW_DB/staging/{table_name}/"
        try:
            self.logger.info(cmd_to_upload_file)
            table_name_date = pd.read_csv(load_date_file_name)
            table_name_date.loc[
                table_name_date["table_name"] == table_name, "load_date"
            ] = load_date
            table_name_date.to_csv(load_date_file_name, index=False)
            subprocess.run(cmd_to_upload_file, shell=True, check=True)
            self.logger.info(
                f"The start date for next run for table {table_name} is set to {load_date}"
            )
        except Exception:
            self.logger.error(
                "Error while uploading updated file to GCS", exec_info=True
            )
            sys.exit(1)

    def date_range(self, start_date: str = None) -> tuple:
        """
        This function will return start_date and end_date for  querying the zuora revenue BI view or table_name.
        If start_date is not passed then it sets itself to zuora default start_date.
        end_date is always set to now.
        """
        if not start_date:
            start_date = "2016-07-26T00:00:00"

        now = datetime.now()
        end_date = now.strftime("%Y-%m-%dT%H:%M:%S")
        return start_date, end_date

    def get_auth_token(self) -> str:
        """
        This function receives url and header information and generate authentication token in response.
        If we don't receive the response status code  200 then exit the program.
        """
        response = requests.post(
            self.authenticate_url_zuora_revpro, headers=self.headers
        )
        if response.status_code == 200:
            self.logger.info("Authentication token generated successfully")
            authToken = response.headers["Revpro-Token"]
            return authToken
        else:
            self.logger.error(
                f"HTTP {response.status_code} - {response.reason}, Message {response.text}"
            )
            sys.exit(1)

    def get_number_of_page(self, url_para_dict: Dict[str, Any]) -> int:
        """
        This function get the total number of records and then divide it with 10000 rows to  determine how many pages
        this BI views has for given date range. Rounded the value to nearest whole number.
        """

        url = f"{self.zuora_fetch_data_url}count/{url_para_dict['table_name']}?fromDate={url_para_dict['fromDate']}&toDate={url_para_dict['toDate']}&clientId={self.clientId}"
        count_response = requests.get(url, headers=url_para_dict["header_auth_token"])

        if count_response.status_code == 200:
            res = json.loads(count_response.text)
        else:
            self.logger.error(
                f"HTTP {count_response.status_code} - {count_response.reason}, Message {count_response.text}"
            )
            sys.exit(1)
        number_of_records = res.get("count")
        self.logger.info(
            f"Number of Records for table {url_para_dict['table_name']}:- {number_of_records}"
        )
        number_of_page = math.ceil(res.get("count") / 10000)
        self.logger.info(
            f"Number of page to extract for table {url_para_dict['table_name']}:- {number_of_page}"
        )

        return number_of_page

    def check_response_204(self, response_output, table_name: str):
        """
        Function checks the response from the request and check if the status code
        is 204 then it is success and no need to proceed further with extraction.
        """
        if (
            "The number of total pages and the number of total rows are returned"
            in response_output.text
            or response_output.status_code == 204
        ):
            self.logger.info(
                f"Reached end of table {table_name} extraction.Stopping the loop and move to next.\
                         Or no records in the table_name {response_output.text}"
            )
            return True

    def check_response_200_500(
        self, response_output, page_number: int, table_name: str
    ):
        """
        Takes input as result of rest call and validate if the status code is 200. If not then check for 500 or page still not cached error.
        If status is 200 then write the output to the .csv file and then request to upload and delete.
        """

        if response_output.status_code != 200:
            self.logger.info(
                f"Made an unsuccessful rest call for page number {page_number} for table_name {table_name}"
            )
            self.logger.info(response_output.text)
            if (
                "Page still not yet cached" in response_output.text
                or response_output.status_code == 500
            ):
                self.logger.info(
                    f"Made a Un successful rest call for page number {page_number} for table_name  {table_name}, because of page not being cached {response_output.text}. \
                     Going for retrial."
                )
                return False

        elif response_output.status_code == 200:
            self.logger.info(
                f"Made successfull attempt for page  {page_number } for table_name  {table_name}."
            )
            self.write_to_file_upload_to_gcs(
                response_output.text, table_name, page_number
            )
            return True
        else:
            self.logger.info(
                f"A different error which is not caught in program for {table_name}. The error message is {response_output.text}"
            )
            sys.exit(1)

    def write_to_file_upload_to_gcs(
        self, response_output, table_name: str, pagenum: int = 1
    ):
        """
        Taken input as respounce_output and then write it to file , upload the file to GCS and then delete local file.
        """
        bucket_name = self.bucket_name
        try:
            f = open(f"{table_name}_{pagenum}.csv", "w+")
            f.write(response_output)
            f.close()
            self.logger.info("File generated successfully")
        except Exception:
            self.logger.error("Error while writing to file", exc_info=True)
            sys.exit(1)
        command_upload_to_gcs = f"gsutil cp {table_name}_{pagenum}.csv gs://{bucket_name}/RAW_DB/staging/{table_name}/"
        command_delete_file = f"rm {table_name}_{pagenum}.csv"
        try:
            self.logger.info(command_upload_to_gcs)
            subprocess.run(command_upload_to_gcs, shell=True, check=True)
        except Exception:
            self.logger.error("Error while uploading the file to GCS", exec_info=True)
            sys.exit(1)
        try:
            self.logger.info(command_delete_file)
            subprocess.run(command_delete_file, shell=True, check=True)
        except Exception:
            self.logger.error(
                "Error while deleting the file post upload to GCS", exec_info=True
            )
            sys.exit(1)

    def retry_download(
        self, url: str, header_auth_token: dict, page_number: int, table_name: str
    ):
        for attempt in range(1, 50):
            time.sleep(60 + attempt)
            self.logger.info(
                f"Making attempt number {attempt} for page  {page_number } for table_name  {table_name}."
            )
            response_output = requests.get(url, headers=header_auth_token)
            attempt_status = self.check_response_200_500(
                response_output, page_number, table_name
            )
            if attempt_status == False:
                continue
            elif attempt_status == True:
                self.logger.info(
                    f"Made successfull attempt for page  {page_number } for table_name  {table_name}.\
                            The attempt number is {attempt}"
                )
                return response_output
                break

    def pull_zuora_table_data(
        self,
        table_name: str,
        fromDate: str,
        toDate: str,
    ) -> None:
        """
        Function pull the Zuora Revenue BI view or table and download it to file and upload it to the GCS bucket.
        """
        self.logger.info("Generate the authentication token")
        header_auth_token = {"Token": self.get_auth_token()}
        # Combine the variables in to one dict for better parameter management
        url_para_dict = {
            "table_name": table_name,
            "fromDate": fromDate,
            "toDate": toDate,
            "header_auth_token": header_auth_token,
        }

        # Fetch number of page this table will generate for given date range.
        number_of_page = self.get_number_of_page(url_para_dict)
        self.logger.info(
            f"{table_name} file extraction started at :: {datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}"
        )

        if number_of_page >= 1:
            page_number = 1
            url = f"{self.zuora_fetch_data_url}{table_name}?fromDate={fromDate}&toDate={toDate}&pagenum={page_number}&pageSize=10000&clientId={self.clientId}&outputType=csv"
            # Fetch first page for the table name
            response_output = requests.get(url, headers=header_auth_token)
            self.logger.info(response_output.status_code)
            # Check for the response from the above call. If the response is success and status code is 200 then the request was made successfully.
            if (
                self.check_response_200_500(response_output, page_number, table_name)
                == False
            ):
                self.logger.info("Going for retrials for 1st page")
                retrial_response = self.retry_download(
                    url, header_auth_token, page_number, table_name
                )
                continuation_token = retrial_response.headers.get("Continuation-Token")
            else:
                self.logger.info("File uploaded to GCS")
                continuation_token = response_output.headers.get("Continuation-Token")
            header_auth_token["Continuation-Token"] = continuation_token

            page_number = 2
            while number_of_page >= page_number:
                url = f"{self.zuora_fetch_data_url}{table_name}?fromDate={fromDate}&toDate={toDate}&pagenum={page_number}&pageSize=10000&clientId={self.clientId}&outputType=csv"
                # Fetch first page for the table name
                response_output = requests.get(url, headers=header_auth_token)
                # Check for the response from the above call. If the response is success and status code is 200 then the request was made successfully.
                if self.check_response_204(response_output, table_name) is True:
                    break

                if response_output.status_code == 401:
                    header_auth_token = {"Token": self.get_auth_token()}
                    header_auth_token["Continuation-Token"] = continuation_token
                    response_output = requests.get(url, headers=header_auth_token)

                if (
                    self.check_response_200_500(
                        response_output, page_number, table_name
                    )
                    == False
                ):
                    self.logger.info("Going for retrials for 1st page")
                    retrial_response = self.retry_download(
                        url, header_auth_token, page_number, table_name
                    )
                else:
                    self.logger.info("File uploaded to GCS")

                page_number = page_number + 1
