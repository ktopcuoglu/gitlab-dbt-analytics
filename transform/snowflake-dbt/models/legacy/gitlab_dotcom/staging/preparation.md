{% docs gitlab_dotcom_memberships_prep %}

Key logical differences between `gitlab_dotcom_memberships_prep` and `gitlab_dotcom_memberships`:
 - `_prep` considers all users, as opposed to filtering out blocked users.
 - `_prep` filters out _all_ bots (`user_type IS NULL`) from `is_active` and `is_billable`, as opposed to just project bots (`user_type != 6`).
 - `_prep` includes flags beyond those included in `_memberships` for downstream summarization and reporting purposes.
 - In `_prep` all users with `access_level <= 10 OR group_access <= 10` are considered guests as opposed to either access code being _equal to_ 10. 

This model unions together all of the other models that represent a user having (full or partial) access to a namespace, AKA "membership". 

There are 5 general ways that a user can have access to a group G:
* Be a **group member** of group G.
* Be a **group member** of G2, where G2 is a descendant (subgroup) of group G.
* Be a **project member** of P, where P is owned by G or one of G's descendants.
* Be a group member of X or a parent group of X, where X is invited to a project underneath G via [project group links](https://docs.gitlab.com/ee/user/group/#sharing-a-project-with-a-group).
* Be a group member of Y or a parent group of Y, where Y is invited to G or one of G's descendants via [group group links](https://docs.gitlab.com/ee/user/group/#sharing-a-group-with-another-group).

An example of these relationships is shown in this diagram:

<div style="width: 720px; height: 480px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:720px; height:480px" src="https://app.lucidchart.com/documents/embeddedchart/9f529269-3e32-4343-9713-8eb311df7258" id="WRFbB73aKeB3"></iframe></div>

Additionally, this model calculates the field `is_billable` - i.e. if a member should be counted toward the seat count for a subscription (note: this also applies to namespaces without a subscription for the convenience of determining seats in use). To determine the number of seats in use for a given namespace, a simple query such as the following will suffice: 

```
SELECT COUNT(DISTINCT user_id)
FROM legacy.gitlab_dotcom_memberships
WHERE is_billable = TRUE
  AND ultimate_parent_id = 123456
```  

{% enddocs %}
