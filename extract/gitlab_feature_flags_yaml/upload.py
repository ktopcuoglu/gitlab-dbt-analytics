import json
import logging
import glob
import sys
import yaml
from subprocess import run
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    command = (
        "rm -rf gitlab && "
        "mkdir -p gitlab && "
        "cd gitlab && "
        "git sparse-checkout init --cone && "
        "git sparse-checkout set config/feature_flags ee/config/feature_flags && "
        "git remote add origin https://gitlab.com/gitlab-org/gitlab.git && "
        "git fetch origin --depth=1 --shallow && "
        "git checkout master"
    )

    logging.info("Cloning GitLab...")

    p = run(command, shell=True)
    p.check_returncode()

    logging.info("Parsing all YAML into JSON...")

    all_ffs = []

    for flag_path in glob.glob("gitlab/*/config/feature_flags/**/*.yaml", recursive=True):
        with open(flag_path) as file:
            ff = yaml.load(file, Loader=yaml.SafeLoader)
            if "name" in ff and "type" in ff:
                all_ffs.append(ff)

    logging.info("Writing JSON...")

    with open('all_feature_flags.json', 'w') as outfile:
        json.dump(all_ffs, outfile)

    snowflake_stage_load_copy_remove(
        "all_feature_flags.json",
        "raw.gitlab_data_yaml.gitlab_data_yaml_load",
        "raw.gitlab_data_yaml.feature_flags",
        snowflake_engine,
    )
