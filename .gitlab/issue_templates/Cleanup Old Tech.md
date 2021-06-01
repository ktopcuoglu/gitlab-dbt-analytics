<!-- This issue template is to be used for any clean up activities in the GitLab Data environment -->

# Deletion scope
See the handbook page for information about [removal and deletion process](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/#removal-and-deletion-process). 
<!-- Describe here what's going to be deleted in full detail -->

# Risk Score
See the actual risk score for likelihood and impact on the [handbook](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/#calculate-a-risk-score) page. 

## Probability that it will break something 

Questions to be asked here:
- How difficult is the code change?
- How certain is it that the component/code is not used anymore?

The likelihood that it will break something is: `1/2/3`
<!-- Provide the rational behind your score -->

## Impact if the deletion is executed by mistake or is execute wrongly

Questions to be asked here:
- What will break?
- Is it easy to revert the change?
- How many users will be impacted?

The impact if it will break something: `1/2/3`
<!-- Provide the rational behind your score -->

## Outcome

Fill in your score here:<br>
`Probability` * `Impact` = `Risk Score`

## Next steps
<!-- Provide how to proceed, according to handbook -->

/label ~"Team::Data Platform" ~"Priority::3-Other" ~"workflow::1 - triage"
