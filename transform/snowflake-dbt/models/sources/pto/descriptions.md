## Source

{% docs gitlab_pto %}
Data from the PTO by Roots system
{% enddocs %}

## Tables
{% docs gitlab_pto_source %}
PTO information from PTO by Roots on the grain of a single missed day
source documentation https://www.tryroots.io/api-docs/pto#tag/OOO-Event-Model
{% enddocs %}

## Columns

### end_date
{% docs gitlab_pto_end_date %}
The end date (timezone naive, ISO-formatted) of the event
{% enddocs %}
### start_date
{% docs gitlab_pto_start_date %}
The start date (timezone naive, ISO-formatted) of the event
{% enddocs %}
### pto_status
{% docs gitlab_pto_pto_status %}
Enum: "AP" "RQ" "DN" "CN"
2-character enumeration denoting the status of the OOO Event.

"AP" = Approved

"RQ" = Requested

"DN" = Denied

"CN" = Cancelled

{% enddocs %}
### employee_day_length
{% docs gitlab_pto_employee_day_length %}
How long this User's "day" is in hours
{% enddocs %}
### employee_department
{% docs gitlab_pto_employee_department %}
The department of the team member as recorded in BambooHR
{% enddocs %}
### employee_division
{% docs gitlab_pto_employee_division %}
The division of the team member as recorded in BambooHR
{% enddocs %}
### hr_employee_id
{% docs gitlab_pto_hr_employee_id %}
If using an external HRIS, this is the User's ID in that HRIS
{% enddocs %}
### employee_uuid
{% docs gitlab_pto_employee_uuid %}
Unique identifier for the employee
{% enddocs %}
### pto_uuid
{% docs gitlab_pto_pto_uuid %}
Unique identifier for this pto event
{% enddocs %}
### pto_date
{% docs gitlab_pto_pto_date %}
The date (timezone naive, ISO-formatted) of this OOO Day
{% enddocs %}
### pto_ends_at
{% docs gitlab_pto_pto_ends_at %}
If this user has designated a start time, meaning they aren't taking the whole OOO day off, this is the timezone-aware ISO-formatted datetime when the OOO ends on this day.
{% enddocs %}
### is_holiday
{% docs gitlab_pto_is_holiday %}
If this OOO Day overlaps a holiday.
{% enddocs %}
### recorded_hours
{% docs gitlab_pto_recorded_hours %}
How many hours were recorded as OOO for this event.
{% enddocs %}
### pto_starts_at
{% docs gitlab_pto_pto_starts_at %}
If this user has designated a start time, meaning they aren't taking the whole OOO day off, this is the timezone-aware ISO-formatted datetime when the OOO starts on this day.
{% enddocs %}
### total_hours
{% docs gitlab_pto_total_hours %}
How long the user was OOO for. This is different from `recorded_hours` in that it is unaware of holidays and weekends.
{% enddocs %}