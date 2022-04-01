import logging
import time
import json
from io import StringIO

import requests
import pandas as pd

import yaml
from logging import info, error, warning
from os import environ as env


class ZuoraQueriesAPI:
    def __init__(self):
        zuora_api_client_id = env["ZUORA_API_CLIENT_ID"]
        zuora_api_client_secret = env["ZUORA_API_CLIENT_SECRET"]
        self.base_url = "https://rest.zuora.com"

        zuora_token = self.authenticate_zuora(
            zuora_api_client_id, zuora_api_client_secret
        )

        self.request_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {zuora_token}",
        }

    def authenticate_zuora(self, zuora_api_client_id: str, zuora_api_client_secret: str) -> str:
        """
        Written to encapsulate Zuora's authentication functionality
        :param zuora_api_client_id:
        :type zuora_api_client_id:
        :param zuora_api_client_secret:
        :type zuora_api_client_secret:
        :return:
        :rtype:
        """
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data_auth = {
            "client_id": zuora_api_client_id,
            "client_secret": zuora_api_client_secret,
            "grant_type": "client_credentials",
        }
        auth_url = f"{self.base_url}/oauth/token"
        response = requests.post(auth_url, headers=headers, data=data_auth)
        if response.ok:
            info("Successful auth")
            return response.json()["access_token"]
        else:
            error("COULD NOT AUTHENTICATE")
            exit(1)

    def request_data_query_data(self, query_string: str) -> str:
        """

        :param query_string: Written in ZQL (check Docs to make changes),
        :param query_type:
        :return:
        """
        api_url = f"{self.base_url}/query/jobs"

        payload = dict(compression="NONE", output=dict(target="S3"), outputFormat="CSV", query=query_string)

        response = requests.post(
            api_url,
            headers=self.request_headers,
            data=json.dumps(payload))

        info(response.status_code)

        if response.status_code == 200:
            return response.json().get('data').get('id')
        else:
            logging.error(response.json)

    def wait_for_zuora_to_complete(self, job_id, wait_time=30):
        """ """
        api_url = f"{self.base_url}/v1/batch-query/jobs/{job_id}"

        response = requests.get(url=api_url, headers=self.request_headers).json()
        file_id = response["batches"][0].get("fileId")
        status = response["batches"][0].get("status")
        file_type = response["name"]

        while status == "pending":
            time.sleep(wait_time)

            response = requests.get(url=api_url, headers=self.request_headers).json()

            file_id = response["batches"][0].get("fileId")
            status = response["batches"][0].get("status")

            info("Waiting for report to complete")
        return file_id, file_type

    def get_file_from_zuora(self, job_id):
        """
        :param job_id:
        :return:
        """
        info("Retrieving file")
        file_id, file_type = self.wait_for_zuora_to_complete(job_id)
        info("File complete")
        file_url = f"{self.base_url}/v1/file/{file_id}/"

        response = requests.get(url=file_url, headers=self.request_headers)
        if response.ok:
            df = pd.read_csv(StringIO(response.text))
            return df
        else:
            return False

    def process_scd(self, scd_file: str = "./zuora_data_query/src/scd_queries.yml"):

        with open(scd_file) as file:
            query_specs = yaml.load(file, Loader=yaml.FullLoader)

        tables = query_specs.get("tables")
        for table_spec in tables:
            job_id = self.request_data_query_data(
                query_string=tables.get(table_spec).get("query")
            )
            df = self.get_file_from_zuora(job_id)

    def main(self) -> None:
        """
        Read data from a postgres DB and upload it directly to Snowflake.
        """
        self.process_scd()


if __name__ == "__main__":
    zq = ZuoraQueriesAPI()
    zq.main()
