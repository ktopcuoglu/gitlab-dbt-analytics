## Generating Sisense Embed Links

Guideline on how to generate `Sisense` Embed links
* Edit the `chart_slide_deck.csv` file to include the chart name, the dashboard ID, and the widget ID.
* These all come from the chart's URL.
* Follow the example in the csv here.
* Set the `Sisense` API key in your env vars. _(You will need to ping a `Sisense` Admin to get this)_.
* Run file `generate_periscope_chart_embed_code.py`.
* The embed URL will be output in the `chart_links.csv` file.
* **You should not commit changes to either of these two csv files.**

