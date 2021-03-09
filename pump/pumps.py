from yaml import load, safe_load, YAMLError

with open("pump/pumps.yml", "r") as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)

    # there has to be a better way to do this
    pumps = [(pump) for pump in stream["pumps"]]

# placeholders for DAG
inc_start = "2021-02-26"
inc_end = "2021-02-27"
timename = "20210207_080000"

# command generation (will need to remove owner and schedule arguments)
def get_copy_command(
    model, sensitive, timestamp, schedule, owner, inc_start, inc_end, timename
):

    from_statement = "FROM PROD.{schema}.{model}".format(
        model=model, schema="pumps" if sensitive == False else "pumps_sensitive"
    )

    where_statement = "WHERE {timestamp} between {inc_start} and {inc_end}".format(
        timestamp=timestamp,
        inc_start=inc_start,
        inc_end=inc_end,
    )

    if timestamp == None:
        query = "SELECT * " + from_statement
    else:
        query = "SELECT * " + from_statement + where_statement

    copy_command_tmp = """
      COPY INTO @RAW.PUBLIC.S3_DATA_PUMP/{model}/{timename}
      FROM ({query})
      FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY = '"', COMPRESSION=NONE)
      HEADER = TRUE
      INCLUDE_QUERY_ID = TRUE;
    """
    copy_command = copy_command_tmp.format(
        model=model,
        timename=timename,
        query=query,
    )

    return copy_command


# output for testing
for pump_models in pumps:

    print(pump_models)  # for audit

    out = get_copy_command(
        **pump_models,
        inc_start=inc_start,
        inc_end=inc_end,
        timename=timename,
    )

    print(out)
