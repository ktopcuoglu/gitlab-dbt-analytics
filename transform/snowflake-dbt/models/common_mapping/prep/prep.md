{% docs prep_sfdc_account %}

SFDC Account Prep table, used to clean and dedupe fields from a common source for use in further downstream dimensions.
Cleaning operations vary across columns, depending on the nature of the source data. See discussion in [MR](https://gitlab.com/gitlab-data/analytics/-/merge_requests/3782) for further details

{% enddocs %}

{% docs prep_crm_person%}

This table creates a singular source for crm_person logic to be used in dim_crm_person and fct_crm_person. It combines unconverted leads and contacts, and maps both to a bizible_person_id.

{% enddocs %}
