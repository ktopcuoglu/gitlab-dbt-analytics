import json
import logging
import re
import sys
import yaml
import os
from os.path import join, getsize, dirname
from os import environ as env
from subprocess import run

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    command = (
        "cd /usr/local/ && "
        "mkdir -p gitlab && "
        "cd gitlab && "
        "git init && "
        "git remote add origin https://gitlab.com/gitlab-org/gitlab.git && "
        "git checkout -b master && "
        "git config core.sparsecheckout true && "
        "echo config/feature_flags/ >> .git/info/sparse-checkout && "
        "echo ee/config/feature_flags/ >> .git/info/sparse-checkout && "
        "git pull origin master"
    )

    logging.info("Cloning GitLab...")

    p = run(command, shell=True)
    p.check_returncode()

    logging.info("Parsing all YAML into JSON...")

    all_ffs = []

    for root, dirs, files in os.walk("/usr/local/gitlab/"):
        for yaml_file in files:
            full_dir = os.path.join(root, yaml_file)
            if re.search("yaml|yml", full_dir):
                with open(full_dir) as file:
                    ff = yaml.load(file, Loader=yaml.SafeLoader)
                    if "name" in ff and "type" in ff:
                        all_ffs.append(ff)

    logging.info("Writing JSON...")

    with open("all_feature_flags.json", "w") as outfile:
        json.dump(all_ffs, outfile)

    snowflake_stage_load_copy_remove(
        "all_feature_flags.json",
        "raw.gitlab_data_yaml.gitlab_data_yaml_load",
        "raw.gitlab_data_yaml.feature_flags",
        snowflake_engine,
    )
