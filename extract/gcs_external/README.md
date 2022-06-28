## GCS External

This pipeline extract data from GCS which where we _are_ doing some transformation on the data before load. The current use case with Container Registry Log data is to reduce the size of the data before loading into Snowflake.

A future iteration may replace this pipeline with using External Tables in Snowflake, but this will require more intelligent partitioning upstream, which we may not always have control over.

If we do need to extend this pipeline to include other data sources in GCS which need transformation upstream of snowflake then a manifest should be introduced to parameterize the sql.