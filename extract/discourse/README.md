### Discourse Extract

Utility to extract analytics data from [Community Forum](https://forum.gitlab.com/) to Snowflake RAW.DISCOURSE schema.
Current implementation is extracting predefined reports in form of JSON by adding .json on to the end of the report URL with admin auth details.

Reports to be extracted are defined in [reports.yml](reports.yml) file. 

#### Future improvements:
 - Using Data Explorer queries to extract data without the need of parsing JSON file
 

[Link to issue](https://gitlab.com/gitlab-data/analytics/-/issues/6007)

> **Note:** For more details on our Python coding standards, please refer to [GitLab Python Guide](https://about.gitlab.com/handbook/business-technology/data-team/platform/python-guide/).