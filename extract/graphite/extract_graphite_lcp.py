import json
import logging
import requests
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


def make_api_call(
    target, from_time, until_time, username, password, host, port: int = 8080
):
    param_dict = {
        "target": target,
        "from": from_time,
        "until": until_time,
        "format": "json",
    }

    url = "http://" + host + ":" + str(port) + "/render"
    response = requests.get(url, params=param_dict, auth=(username, password))
    return response.json()


def get_targets():
    return [
        "sitespeed_io.desktop.gitlab.pageSummary.gitlab_com.GitLab_Project_Home.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.gitlab_com.GitLab_Issue_list.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.gitlab_com.GitLab_Issue_Detail.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.gitlab_com.GitLab_Merge_List.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.gitlab_com.GitLab_Merge_Detail.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.github_com.GitHub_Project_Home.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.github_com.GitHub_Issue_list.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.github_com.GitHub_Issue_Detail.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.github_com.GitHub_Merge_List.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
        "sitespeed_io.desktop.gitlab.pageSummary.github_com.GitHub_Merge_Detail.chrome.cable.browsertime.statistics.timings.largestContentfulPaint.renderTime.*",
    ]


if __name__ == "__main__":

    config_dict = env.copy()

    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    for target in get_targets():
        lcp_data = make_api_call(
            target,
            "-30d",
            config_dict["START_DATE"],
            config_dict["GRAPHITE_USERNAME"],
            config_dict["GRAPHITE_PASSWORD"],
            config_dict["GRAPHITE_HOST"],
        )

        with open("lcp.json", "w") as out_file:
            json.dump(lcp_data, out_file)

        snowflake_stage_load_copy_remove(
            "lcp.json",
            "engineering_extracts.lcp_load",
            "engineering_extracts.lcp",
            snowflake_engine,
        )
