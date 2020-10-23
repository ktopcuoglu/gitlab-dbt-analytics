import os

from api import DiscourseClient

DISCOURSE_API_TOKEN = os.environ.get('DISCOURSE_API_TOKEN')

if __name__ == "__main__":
    reports = { 'consolidated_page_views' : 'admin/reports/consolidated_page_views',
               'accepted_solutions' : 'admin/reports/accepted_solutions',
               'posts' : '/admin/reports/posts',
               'dau_by_mau' : 'admin/reports/dau_by_mau',

    }

    for report, endpoint in reports.items():
        print(report)
        api_client = DiscourseClient(api_token = DISCOURSE_API_TOKEN)
        data = api_client.get_json(endpoint)
        print(data)