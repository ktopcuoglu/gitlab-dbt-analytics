## Issue

Closes # <!--- Link the Issue this MR closes --->

## Background

- [ ] Target Application: <!-- Salesforce? Marketo? -->
- [ ] [Tech stack](https://gitlab.com/gitlab-com/www-gitlab-com/-/blob/master/data/tech_stack.yml) technical owner of target application: <!-- tag them here -->
- [ ] Application has an available [workato integration](https://www.workato.com/integrations)
- [ ] Business DRI for integration: <!-- tag them here -->
- [ ] MR for the model in `PROD.PUMPS` or `PROD.PUMPS_SENSITIVE`: <!-- link here -->

## Compliance

- [ ] If this pump contains [RED or ORANGE Data](https://about.gitlab.com/handbook/engineering/security/data-classification-standard.html#data-classification-levels) I have correctly use the pumps_sensitive directory and schema and indicated this in `pumps.yml`
  * [ ] This pump does not contain any Red or ORANGE Data

## Next Steps

- [ ] Link to created integration issue: [`use this template to create it`](https://gitlab.com/gitlab-com/business-technology/enterprise-apps/integrations/integrations-work/-/issues/new?issuable_template=New%20Data%20Pump)

----
/label ~Infrastructure ~"Data Team - Engineering" ~"EntApp Ecosystem" ~"Data Pump"
/assign @jjstark @gitlab-com/business-technology/enterprise-apps/integrations
