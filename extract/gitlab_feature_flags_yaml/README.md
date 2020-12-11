de### YML Extractor

This job pulls an YAML files into tables in Snowflake within the raw.gitlab_data_yaml.feature_flags table. It uses [PyYAML](https://pypi.org/project/pyyaml/) to serialize YAML to JSON.

Current files are:

* [FOSS Feature Flags](https://gitlab.com/gitlab-org/gitlab/-/tree/master/config/feature_flags/)
* [EE Feature Flags](https://gitlab.com/gitlab-org/gitlab/-/tree/master/ee/config/feature_flags/)
