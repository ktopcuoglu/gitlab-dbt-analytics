## Graphite Extractor -- Displaying data like Grafana

The Engineering department is using Grafana and Graphite to track and visualize a number of important metrics.  In GitLab's particular setup, Grafana is being used as a quick and simple visualization tool on top of Graphite.  Graphite is essentially a time-series database that is being used to track the performance of GitLab.com itself.  Since all of the data is stored within Graphite, to extract the desired data into the data warehouse, the Graphite API is queried directly.

### Knowing What Query to Run

The Graphite API has [extensive documentation](https://graphite-api.readthedocs.io/en/latest/) to help guide the API calls to make in order to retrieve the data of interest. In general, retrieving metrics intended for ingestion into Snowflake should be made using the [render API](https://graphite-api.readthedocs.io/en/latest/api.html#the-render-api-render), and [setting the format](https://graphite-api.readthedocs.io/en/latest/api.html#format) to `json` for easier manipulation inside a Snowflake VARIANT column.

### Replicating Grafana Queries
If there is already a Grafana graph with data that needs to be replicated into Snowflake, it is even easier to understand what API call to make as Grafana is actually using the render API of Graphite.  To see the API call that Grafana is making, go into the "edit" view of the graph.  Then from there, click "Query Inspector".  Then, click the "Refresh" button.  Inside the displayed `request` object, it should show that it is using the `render` url.  The `data` attribute of the request object then is the query parameters of the Graphite `render` request.

### Currently pulled metrics

There are currently different metrics being pulled from Graphite, all having to do with GitLab web page load times.

* [LCP](https://gitlab.com/gitlab-data/analytics/-/issues/6283) or Largest Contentful Paint, which is the first metric to have been extracted from Graphite
* [CLS](https://gitlab.com/gitlab-data/analytics/-/issues/8464) or Cumulative Layout Shift
* [TBT](https://gitlab.com/gitlab-data/analytics/-/issues/8464) or Total Blocking Time

Every DAG run, the metric values are pulled for a number of different web pages.  

There are different statistics available from Graphite for each webpage.  For example, the mean AND median LCP for the GitLab Home Page is available from Graphite.  For maximum data availability, this extract is setup to pull all possible aggregations/statistics by using the `.*` at the end of the requested metric name.