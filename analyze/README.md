## Generating Periscope Embed Links
# TODO: rbacovic
Edit the `chart_slide_deck.csv` file to include the chart name, the dashboard ID, and the widget ID.
These all come from the chart's URL.
Follow the example in the csv here.
Set the Periscope API key in your env vars. (You will need to ping a Periscope Admin to get this.)
Run `generate_periscope_chart_embed_code.py`.
The embed URL will be output in the `chart_links.csv` file.
**You should not commit changes to either of these two csv files.**


> **Note:** For more details on how to make standardization in Python coding, refer to [GitLab Python Guide](https://about.gitlab.com/handbook/business-technology/data-team/platform/python-guide/).