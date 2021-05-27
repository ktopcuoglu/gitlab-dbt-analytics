<!-- This issue template is to be used for any clean up activities in the GitLab Data environment -->

# Deletion scope

<!-- Describe here what's going to be deleted in full detail -->

# Risk Score

## Probability that it will break something 

Questions to be asked here:
- How difficult is the code change?
- How certain is it that the component/code is not used anymore?

|  Probability | Score |
| ------- | ----- |
| Low     |   1   |
| Medium  |   2   |
| High    |   3   |

The likelihood that it will break something is: `1/2/3`
<!-- Provide the rational behind your score -->

## Impact if the deletion is executed by mistake or is execute wrongly

Questions to be asked here:
- What will break?
- Is it easy to revert the change?
- How many users will be impacted?

|  Impact | Score |
| ------- | ----- |
| Negligible | 1  |
| Lenient |   2   |
| Severe  |   3   |

The impact if it will break something: `1/2/3`
<!-- Provide the rational behind your score -->

## Outcome

Fill in your score here:<br>
`Probability` * `Impact` = `Risk Score`


| Risk Score | Outcome |
| ---------- | ------- |
| 1 - 2      | Create a MR and have it reviewed by 2 code owners |
| 3 - 4      | Create a MR, tag `@gitlab-data/engineers` with a deadline to object and have it reviewed by 2 code owners |
| 6 - 9      | Create a MR, to be discussed in the DE-Team meeting and have it reviewed by 2 code owners |

/label ~"Team::Data Platform" ~"Priority::3-Other" ~"workflow::1 - triage"
