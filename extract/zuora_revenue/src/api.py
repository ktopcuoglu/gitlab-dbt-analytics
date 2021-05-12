import logging
import subprocess
import sys
import requests
import json
import time

from os import environ as env


class ZuoraRevProAPI:
    logging.basicConfig(level=20, filename="logging_file.log")
    """
    def __init__(self, zuora_header: dict):
        self.headers = zuora_header
    """

    def get_auth_token(self, authenticate_url_zuora_revpro: str, headers: dict) -> str:
        response = requests.post(authenticate_url_zuora_revpro, headers=headers)
        if response.status_code == 200:
            logging.info("Authenticate token generated successfully")
            authToken = response.headers["Revpro-Token"]
            logging.info(f"Auth token:= {authToken}")
            return authToken
        else:
            logging.ERROR(
                "HTTP {status_code} - {reason}, Message {text}".format(
                    response.status_code, response.reason, response.text
                )
            )
            sys.exit(1)

    def get_table_record_count(self, url_para_dict: dict) -> int:
        url = "{zuora_fetch_data_url}count/{tablename}?fromDate={fromDate}&toDate={toDate}&clientId={clientId}".format(
            zuora_fetch_data_url=url_para_dict["zuora_fetch_data_url"],
            tablename=url_para_dict["tablename"],
            fromDate=url_para_dict["fromDate"],
            toDate=url_para_dict["toDate"],
            clientId=url_para_dict["clientId"],
        )
        count_response = requests.get(url, headers=url_para_dict["header_auth_token"])

        if count_response.status_code == 200:
            res = json.loads(count_response.text)
        else:
            logging.error(count_response.text)
            sys.exit(1)
        number_of_records = res.get("count")
        logging.info(f"Number of Records :- {number_of_records}")
        number_of_page = round(res.get("count") / 10000) + 1
        return number_of_page

    def write_to_file_upload_to_gcs(
        self, response_output, bucket_name: str, tablename: str, pagenum: int = 1
    ):
        f = open(f"{tablename}_{pagenum}.csv", "w+")
        f.write(response_output)
        f.close()
        command = f"gsutil cp {tablename}_{pagenum}.csv gs://{bucket_name}/RAW_DB/{tablename}/"
        logging.info(command)
        p = subprocess.run(command, shell=True)

    def pull_zuora_table_data(
        self,
        zuora_fetch_data_url,
        tablename: str,
        fromDate,
        toDate,
        clientId,
        headers,
        authenticate_url_zuora_revpro,
    ):
        header_auth_token = {
            "Token": self.get_auth_token(authenticate_url_zuora_revpro, headers)
        }
        # Combine the variables in to one dict for better parameter management
        url_para_dict = {
            "zuora_fetch_data_url": zuora_fetch_data_url,
            "tablename": tablename,
            "fromDate": fromDate,
            "toDate": toDate,
            "clientId": clientId,
            "header_auth_token": header_auth_token,
        }

        # Fetch number of page this table will generate for given date range.
        number_of_page = self.get_table_record_count(url_para_dict)
        logging.info(f"{tablename} has these many pages {number_of_page}")
        logging.info(f"{tablename} file extraction started at :: {time.time()}")
        page_number = 1
        url = f"{zuora_fetch_data_url}{tablename}?fromDate={fromDate}&toDate={toDate}&pagenum={page_number}&pageSize=10000&clientId={clientId}&outputType=csv"
        # Fetch first page for the table name

        response_output = requests.get(url, headers=header_auth_token)
        logging.info(response_output.status_code)
        # Check for the response from the above call. If the response is success and status code is 2    00 then the request was made successfully.
        if response_output.status_code != 200:
            logging.info(
                f"Made a unsuccessful rest call for page number {page_number} for tablename {tablename}"
            )
            logging.info(response_output.text)
            # If the page is still not cached then we need to perform a retry to fetch the page.
            if (
                "Page still not yet cached" in response_output.text
                or response_output.status_code == 500
            ):
                logging.info(
                    f"Made a Un successful rest call for page number {page_number } for tablename  {tablename}, because of page not being cached {response_output.text}. Going for retrial."
                )
                for attempt in range(1, 10):
                    time.sleep(60 + attempt)
                    logging.info(
                        f"Making attempt for page  {page_number } for tablename  {tablename}.The attempt number is {attempt}"
                    )
                    response_output = requests.get(url, headers=header_auth_token)
                    if response_output.status_code == 200:
                        logging.info(response_output.status_code)
                        logging.info(
                            f"Made successfull attempt for page  {page_number } for tablename  {tablename}.The attempt number is {attempt}"
                        )
                        self.write_to_file_upload_to_gcs(
                            response_output.text, "zuora_revpro_gitlab", tablename
                        )
                        break

        elif response_output.status_code == 200:
            logging.info(
                f"Made successfull attempt for page  {page_number } for tablename  {tablename}."
            )
            self.write_to_file_upload_to_gcs(
                response_output.text, "zuora_revpro_gitlab", tablename
            )

        if number_of_page > 1:
            continuation_token = response_output.headers.get("Continuation-Token")
            header_auth_token["Continuation-Token"] = continuation_token

            while page_number <= number_of_page:
                url = f"{zuora_fetch_data_url}{tablename}?fromDate={fromDate}&toDate={toDate}&pagenum={page_number}&pageSize=10000&clientId={clientId}&outputType=csv"
                # Fetch first page for the table name
                response_output = requests.get(url, headers=header_auth_token)
                # Check for the response from the above call. If the response is success and status code is 200 then the request was made successfully.
                if (
                    "The number of total pages and the number of total rows are returned"
                    in response_output.text
                    or response_output.status_code == 204
                ):
                    logging.info(
                        f"Reached end of table {tablename} extraction.Stopping the loop and move to next. Or no records in the tablename {response_output.text}"
                    )
                    break
                if response_output.status_code == 401:
                    header_auth_token = {
                        "Token": self.get_auth_token(
                            authenticate_url_zuora_revpro, headers
                        )
                    }
                    header_auth_token["Continuation-Token"] = continuation_token
                    response_output = requests.get(url, headers=header_auth_token)

                if response_output.status_code != 200:
                    logging.info(
                        f"Made a unsuccessful rest call for page number {page_number} for tablename /tablename {tablename}"
                    )
                    # If the page is still not cached then we need to perform a retry to fetch the page.
                    if (
                        "Page still not yet cached" in response_output.text
                        or response_output.status_code == 500
                    ):
                        logging.info(
                            f"Made a Un successful rest call for page number {page_number } for tablename  {tablename}, because of page not being cached {response_output.text}. Going for retrial."
                        )
                        for attempt in range(1, 10):
                            time.sleep(60 + attempt)
                            logging.info(
                                f"Making attempt for page  {page_number } for tablename  {tablename}.The attempt number is {attempt}"
                            )
                            response_output = requests.get(
                                url, headers=header_auth_token
                            )
                            if response_output.status_code == 200:
                                logging.info(
                                    f"Made successfull attempt for page  {page_number } for tablename  {tablename}.The attempt number is {attempt}"
                                )
                                self.write_to_file_upload_to_gcs(
                                    response_output.text,
                                    "zuora_revpro_gitlab",
                                    tablename,
                                    page_number,
                                )
                                break
                elif response_output.status_code == 200:
                    self.write_to_file_upload_to_gcs(
                        response_output.text,
                        "zuora_revpro_gitlab",
                        tablename,
                        page_number,
                    )

                page_number = page_number + 1
