import json
import logging
import pandas as pd
from pandas.io.json import json_normalize


def check_safe_models(file):
    # select data
    df = json_normalize(pd.Series(open(file).readlines()).apply(json.loads))
    df = df[["name", "tags", "config.schema"]]
    # filter data
    is_restricted = df["config.schema"].str.contains("restricted_safe")
    ## results
    results = df[~is_restricted]
    results_count = results.count()["name"]
    error_message = "⚠️ The following models are not SAFE ⚠️:\r\n" + results.to_csv(
        index=False
    )
    if results_count == 0:
        logging.info("All models are safe 🥂")
    else:
        raise ValueError(error_message)


if __name__ == "__main__":
    logging.basicConfig(level=20)
    logging.info("Stating safe check for dbt models... ")

    file = "safe_models.json"
    logging.info("File found... ")

    check_safe_models(file)
