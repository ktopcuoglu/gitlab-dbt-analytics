import requests


def get_stats():
    return requests.get(
        f"{base_url}/stats/total",
        auth=("api", api_key),
        params={"event": ["accepted", "delivered", "failed"],"duration": "1m"})


def get_logs(api_key, domain, event, formatted_date):

    return requests.get(
        f"https://api.mailgun.net/v3/{domain}/events",
        auth=("api", api_key),
        params={"begin": formatted_date,
                "ascending": "yes",
                "event": event},
      )