import time
import datetime
import argparse
import json
from io import StringIO

import requests
import pandas as pd

from logging import info, error, warning
from os import environ as env


class ZuoraQueriesAPI:
    def __init__(self):
        user = env["ZUORA_API_USER"]
        password = env["ZUORA_PASSWORD"]
        self.base_url = "https://rest.zuora.com"

        zuora_token = self.authenticate_zuora(user, password)

        self.request_headers = {
            "Content-Type" : "application/json",
            "Authorization": f"Bearer {zuora_token}"
        }

    def authenticate_zuora(self, user, password):
        """
            Written to encapsulate Zuora's authentication functionality
            :return:
        """
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data_auth = {
            "client_id"    : user,
            "client_secret": password,
            "grant_type"   : "client_credentials"
        }
        auth_url = f"{self.base_url}/oauth/token"
        response = requests.post(auth_url, headers=headers, data=data_auth)
        if response.ok:
            info("Successful auth")
            return response.json()["access_token"]
        else:
            error("COULD NOT AUTHENTICATE")
            exit(1)

    def request_query_data(self, query_string, query_type):
        """

        :param query_string: Written in ZQL (check Docs to make changes),
        :param query_type:
        :return:
        """
        api_url = f"{self.base_url}/v1/batch-query/"

        data_query_request = {
            "format"   : "csv",
            "version"  : "1.1",
            "name"     : query_type,
            "encrypted": "none",
            "partner"  : "",
            "project"  : "",
            "notifyUrl": " ",
            "queries"  : [{
                "name" : query_type,
                "query": query_string,
                "type" : "zoqlexport"
            }]
        }
        info(f"{data_query_request}")
        response = requests.post(api_url,
                                 headers=self.request_headers,
                                 data=json.dumps(data_query_request),
                                 verify=False).json()

        if response.get("id") is None:
            warning(response)
            raise Exception("Request unsuccessful")
        else:
            return response["id"]

    def wait_for_zuora_to_complete(self, job_id, wait_time=30):
        """

        """
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

    def process_scd(self, scd_file: str = './scd_queries.yml'):

        query_specifications = []

        for file_spec in self.query_specifications():
            info(f"Working on: {file_spec['query_type']}")
            job_id = self.request_query_data(query_string=file_spec["query_string"],
                                             query_type=file_spec["query_type"])
            df = self.get_file_from_zuora(job_id)


    def main(self, file_path: str, load_type: str, load_only_table: str = None) -> None:
        """
        Read data from a postgres DB and upload it directly to Snowflake.
        """
        self.process_scd()

