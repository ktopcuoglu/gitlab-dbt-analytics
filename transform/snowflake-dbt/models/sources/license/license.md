{% docs license_db_add_ons_source %}
This source model is the data from a bi-weekly export of the license-db-restore CloudSQL instance for the add_ons table from license.gitlab.com. This data represents license add-ons such as support packages.

See the [LicenseApp domain model](https://gitlab.com/gitlab-org/license-gitlab-com/-/blob/master/doc/db_erd.pdf) for details on how the license models relate to each other.
{% enddocs %}

{% docs license_db_granted_add_ons_source %}
This source model is the data from a bi-weekly export of the license-db-restore CloudSQL instance for the granted_add_ons table from license.gitlab.com. This data represents which add_ons are valid to which licenses.

See the [LicenseApp domain model](https://gitlab.com/gitlab-org/license-gitlab-com/-/blob/master/doc/db_erd.pdf) for details on how the license models relate to each other.
{% enddocs %}

{% docs license_db_licenses_source %}
This source model is the data from from a bi-weekly export of the license-db-restore CloudSQL instance for the licenses table from license.gitlab.com. This data represents licenses and associated metadata such as expiration date and zuora subscription id.

See the [LicenseApp domain model](https://gitlab.com/gitlab-org/license-gitlab-com/-/blob/master/doc/db_erd.pdf) for details on how the license models relate to each other.
{% enddocs %}