import logging
import subprocess
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


if __name__ == "__main__":

    handbook_dict = dict(
        categories="categories",
        stages="stages",
        team="team",
    )

    pi_file_dict = dict(
        chief_of_staff_team_pi="chief_of_staff_team",
        corporate_finance_pi="corporate_finance",
        customer_support_pi="customer_support_department",
        dev_section_pi="dev_section",
        development_department_pi="development_department",
        enablement_section_pi="enablement_section",
        engineering_function_pi="engineering_function",
        finance_team_pi="finance_team",
        infrastructure_department_pi="infrastructure_department",
        marketing_pi="marketing",
        ops_section_pi="ops_section",
        people_success_pi="people_success",
        product_pi="product",
        quality_department_pi="quality_department",
        recruiting_pi="recruiting",
        sales_pi="sales",
        secure_and_protect_section_pi="secure_and_protect_section",
        security_department_pi="security_department",
        ux_department_pi="ux_department",
    )

    comp_calc_dict = dict(
        location_factors="location_factors",
        roles="job_families",
        geo_zones="geo_zones",
    )

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    handbook_url = "https://gitlab.com/gitlab-com/www-gitlab-com/raw/master/data/"
    pi_url = f"{handbook_url}performance_indicators/"

    comp_calc_url = f"https://gitlab.com/api/v4/projects/21924975/repository/files/data%2F"

    job_failed = False

    def curl_and_upload(table_name, file_name, base_url, private_token=None):
        
        logging.info(f"Downloading {file_name}.yml to {file_name}.json file.")

        if private_token is not None:
            header = f'--header "PRIVATE-TOKEN: {private_token}"'
            try:
                command = f"curl {header} '{base_url}{file_name}%2Eyml/raw?ref=master' | yaml2json -o {file_name}.json"
                p = subprocess.run(command, shell=True)
                p.check_returncode()
            except:
                job_failed = True
        else:
            try:
                command = f"curl {header} {base_url}{file_name}.yml | yaml2json -o {file_name}.json"
                p = subprocess.run(command, shell=True)
                p.check_returncode()
            except:
                job_failed = True

        logging.info(f"Uploading to {file_name}.json to Snowflake stage.")

        snowflake_stage_load_copy_remove(
            f"{file_name}.json",
            "raw.gitlab_data_yaml.gitlab_data_yaml_load",
            f"raw.gitlab_data_yaml.{table_name}",
            snowflake_engine,
        )

    # for key, value in handbook_dict.items():
    #     curl_and_upload(key, value, handbook_url)

    # for key, value in pi_file_dict.items():
    #     curl_and_upload(key, value, pi_url)

    for key, value in comp_calc_dict.items():
        curl_and_upload(key, value, comp_calc_url, config_dict['GITLAB_ANALYTICS_PRIVATE_TOKEN'])

    if job_failed:
        sys.exit(1)
