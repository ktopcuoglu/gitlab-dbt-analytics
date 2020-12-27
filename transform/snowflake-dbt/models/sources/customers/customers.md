
{% docs customers_db_customers_source %}
This model is the data from tap-postgres for the customers table from customers.gitlab.com. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).
{% enddocs %}

{% docs customers_db_eula_requests_source %}
This model contains records of EULA requests sent to customers.
{% enddocs %}

{% docs customers_db_eulas_source %}
This model contains details for each available EULA.
{% enddocs %}

{% docs customers_db_license_seat_links_source %}
Self-managed EE instances will send seat link information to the customers portal on a daily basis. This information includes a count of active users and a maximum count of users historically in order to assist the true up process. Additional detail can be found in [this doc](https://gitlab.com/gitlab-org/customers-gitlab-com/-/blob/staging/doc/reconciliations.md).
{% enddocs %}

{% docs customers_db_orders_source %}
This model is the data from tap-postgres for the orders table from customers.gitlab.com. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).
{% enddocs %}

{% docs customers_db_trial_histories_source %}
This model is the data from tap-postgres for the trial_histories table from customers.gitlab.com. The schema of the database is defined in [this ruby code](https://gitlab.com/gitlab-org/customers-gitlab-com/blob/master/db/schema.rb).
{% enddocs %}