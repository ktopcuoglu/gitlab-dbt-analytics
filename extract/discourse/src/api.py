import requests


class DiscourseClient():

    def __init__(self, api_token, base_url = 'https://forum.gitlab.com'):
        self.api_token = api_token
        self.base_url = base_url


    def get_json(self, endpoint):

        get_url = f"{self.base_url}/{endpoint}.json"

        headers = {
            'Api-Username' : 'system',
            'Api-Key' : self.api_token
        }

        req = requests.get(get_url, headers=headers)
        if req.status_code == 200:
            json_data = req.content

            return json_data
