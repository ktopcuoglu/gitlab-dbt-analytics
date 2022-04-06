import logging
import time
import json
from io import StringIO

import requests
import pandas as pd

import yaml
from logging import info, error, warning
from os import environ as env

from gitlabdata.orchestration_utils import (dataframe_uploader, snowflake_engine_factory)

class ZuoraQueriesAPI:
    def __init__(self, config_dict):
        zuora_api_client_id = env["ZUORA_API_CLIENT_ID"]
        zuora_api_client_secret = env["ZUORA_API_CLIENT_SECRET"]
        self.base_url = "https://rest.zuora.com"

        self.snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")


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
    
    def get_job_data(self, job_id: str) -> str:
        """

        :param job_id:
        :type job_id:
        :return:
        :rtype:
        """
        api_url = f"{self.base_url}/query/jobs"
        response = requests.get(
            api_url,
            headers=self.request_headers,
        )
        data = response.json()
        job = [j for j in data.get('data') if j.get('id') == job_id]
        if len(job) > 0:
            return job
        else:
            logging.error('Didnt get job id, error ')
            raise Exception
        
    def get_data_query_file(self, job_id: str, wait_time: int = 30) -> pd.DataFrame:
        """

        :param job_id:
        :type job_id:
        :param wait_time:
        :type wait_time:
        :return:
        :rtype:
        """
        job = self.get_job_data(job_id)
        
        job_status = job[0].get('queryStatus')

        if job_status in ["failed", "cancelled"]:
            logging.error("Job failed or cancelled")
            raise Exception

        while job_status in ["accepted", "in_progress"]:
            time.sleep(wait_time)
            
            job = self.get_job_data(job_id)
                                              
            job_status = job[0].get('queryStatus')
            info("Waiting for report to complete")
           
        if job_status == "completed":
            print("Will download file")
            file_url = job[0].get('dataFile')
            response = requests.get(url=file_url)

            df = pd.read_csv(StringIO(response.text))
            return df 

    def process_scd(self, scd_file: str = "./zuora_query_api/src/scd_queries.yml"):

        with open(scd_file) as file:
            query_specs = yaml.load(file, Loader=yaml.FullLoader)

        tables = query_specs.get("tables")
        for table_spec in tables:
            info(f"Processing {table_spec}")
            job_id = self.request_data_query_data(
                query_string=tables.get(table_spec).get("query")
            )
            df = self.get_data_query_file(job_id)
            dataframe_uploader(df, self.snowflake_engine, table_spec, schema="ZUORA_QUERY_API", if_exists='replace')
            info(f"Processed {table_spec}")

    def main(self) -> None:
        """
        Read data from a postgres DB and upload it directly to Snowflake.
        """
        info("Procesing Zuora queries")
        self.process_scd()
        info("Zuora queries processed")


if __name__ == "__main__":
    config_dict = env.copy()
    zq = ZuoraQueriesAPI(env)
    zq.main()
