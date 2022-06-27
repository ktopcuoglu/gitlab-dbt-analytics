# Snowpolow - Posthog back-fill

## Resources 

* Issue: [Build PH: Snowplow Loader (Data team) - data backfill implementation](https://gitlab.com/gitlab-data/analytics/-/issues/13055)
* [Slack #gitlab-posthog-data](https://gitlab.slack.com/archives/C02QQGGG6FJ/p1654690509663749?thread_ts=1654635836.118379&cid=C02QQGGG6FJ)
* [GitLab->PostHog Project Plan](https://docs.google.com/spreadsheets/d/1zsm-vGz1cuwO0x-5HH0grpP6Oj3c_Y6trED6A11ipDY/edit#gid=0)
* [GitLab / PostHog Regular Check-in](https://docs.google.com/document/d/1mSzO7bdJwGP2OHz7Xig1u7ZwdkqiyjhQO3v632md5nE/edit#)
* [Existing Snowplow load process in the handbook](https://about.gitlab.com/handbook/business-technology/data-team/platform/snowplow/)
* [GitLab Snowplow infrastrucure](https://docs.gitlab.com/ee/development/snowplow/infrastructure.html)

## Motivation 

Create a new PH Snowplow Loader program to feed the Snowplow JSON files in S3 to the PH Snowplow Adapter.
Our goal is to have new Snowplow files fed into PostHog as often as possible (min hourly). 
You can read about the [Existing Snowplow load process in the handbook](https://about.gitlab.com/handbook/business-technology/data-team/platform/snowplow/) - perhaps this provides some ideas to leverage when building the new PH Snowplow Loader.

## Installation

* Libraries
    * [posthog](https://posthog.com/docs/integrate/server/python)
  
    ```bash
    pip install posthog
    ```

* Secrets:
    * `aws_access_key_id`
    * `aws_secret_access_key`
    * `aws_s3_snowplow_bucket`
    * `posthog_project_api_key`
    * `posthog_personal_api_key `
    * `posthog_host`
* `Airflow` variables
```bash
POSTHOG_BACKFILL_START_DATE=YYYY-MM-DD
POSTHOG_BACKFILL_END_DATE=YYYY-MM-DD
```
## Testing PostHog instance


Install needed library for testing (as this is optional, but handy):
* [python-decouple](https://pypi.org/project/python-decouple/)

```python
pip install python-decouple
```

You need to have `.env` file in order to save secrets. 
```dotenv
posthog_project_api_key=**********
posthog_personal_api_key=**********
posthog_host=https://***********
```

This also can be done as a part of environment variables.

```python
"""
Test mode to check PostHog backfilling with one record

Resources: https://posthog.com/docs/integrate/server/python
"""

from datetime import datetime
import posthog
from decouple import config

from dateutil.tz import tzutc

# get config properties - in this case, picked up from .env file
posthog.project_api_key = config("posthog_project_api_key")
posthog.personal_api_key = config("posthog_personal_api_key")
posthog.host = config("posthog_host")

# Optional
# posthog.debug = True


posthog.capture(
    "gitlab_dotcom",
    event="test_table_backfill",
    properties={"id": "123", "category": "gitlab_events"},
    timestamp=datetime.utcnow().replace(tzinfo=tzutc()),
)

```