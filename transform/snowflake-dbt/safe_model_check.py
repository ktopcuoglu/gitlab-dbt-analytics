import json
import logging
import pandas as pd
from pandas.io.json import json_normalize


def check_safe_models(file):

    with open(file) as json_file:
        first_char = json_file.read(1)
        if not first_char:
            logging.info("All models are safe 🥂")
        else:
            df = json_normalize(pd.Series(open(file).readlines()).apply(json.loads))
            df = df[["name", "tags", "config.schema"]]
            error_message = "⚠️ The following models are not SAFE ⚠️:\r\n" + df.to_csv(
                index=False
            )
            raise ValueError(error_message)


if __name__ == "__main__":
    logging.basicConfig(level=20)
    logging.info("Stating safe check for dbt models... ")

    file = "safe_models.json"
    logging.info("File found... ")

    check_safe_models(file)
