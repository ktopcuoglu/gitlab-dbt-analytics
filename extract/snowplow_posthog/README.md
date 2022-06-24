# Snowpolow - Posthog back-fill

## Resources 

* Issue: [Build PH: Snowplow Loader (Data team) - data backfill implementation](https://gitlab.com/gitlab-data/analytics/-/issues/13055)
* [Slack #gitlab-posthog-data](https://gitlab.slack.com/archives/C02QQGGG6FJ/p1654690509663749?thread_ts=1654635836.118379&cid=C02QQGGG6FJ)
* [GitLab->PostHog Project Plan](https://docs.google.com/spreadsheets/d/1zsm-vGz1cuwO0x-5HH0grpP6Oj3c_Y6trED6A11ipDY/edit#gid=0)
* [GitLab / PostHog Regular Check-in](https://docs.google.com/document/d/1mSzO7bdJwGP2OHz7Xig1u7ZwdkqiyjhQO3v632md5nE/edit#)
* [Existing Snowplow load process in the handbook](https://about.gitlab.com/handbook/business-technology/data-team/platform/snowplow/)
* [GitLab Snowplow infrastrucure](https://docs.gitlab.com/ee/development/snowplow/infrastructure.html)

## Motivation 

Create a new PH Snowplow Loader program to feed the Snowplow JSON files in S3 to the PH Snowplow Adapter. Our goal is to have new Snowplow files fed into PostHog as often as possible (min hourly). You can read about the [existing Snowplow load process in the handbook](https://about.gitlab.com/handbook/business-technology/data-team/platform/snowplow/) - perhaps this provides some ideas to leverage when building the new PH Snowplow Loader.

## Installation

* Libraries
    * `posthog`
    * `python-decouple`
    ```bash
    pip install posthog
    pip install python-decouple
    ```

* Secrets:
    * `aws_access_key_id`
    * `aws_secret_access_key`
    * `aws_s3_snowplow_bucket`
    * `posthog_project_api_key`
    * `posthog_personal_api_key `
    * `posthog_ host`