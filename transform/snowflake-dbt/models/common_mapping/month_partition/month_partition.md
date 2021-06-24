{% docs prep_dotcom_usage_events %}

prep table to build the fct_dotcom_usage_events table. The grain of the table is one row per event.

The primary key of this table is the `event_id`.

This table is actually a long event list table with the following foreign keys:

- `dim_project_id`: in which project a specific event has been generated. For some events, there is no such data like boards which are at the group-level
- `ultimate_parent_namespace_id`: the top-level namespace in which the event has been generated. This is ALWAYS filled
- `dim_user_id`: the user who generated the event (for example the user who triggered a CI Pipeline)
- `dim_plan_id`
- `dim_date_id`

Then other metadata available in the model are :


- user_created_at
- namespace_created_at
- 

- Project specific metadata:
  - is_learn_gitlab
  - is_imported_project
A specific handbook page has been created for this table. This has more information on how to add events and some analysis that can be run with this model.

{% enddocs %}
