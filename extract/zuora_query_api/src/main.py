from os import environ as env
from api import ZuoraQueriesAPI


if __name__ == "__main__":
    config_dict = env.copy()
    zq = ZuoraQueriesAPI(env)
    zq.process_queries()
