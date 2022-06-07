{% docs pte_scores_source %}
Propensity to expand scores created by ML algorithms. 
{% enddocs %}

{% docs ptc_scores_source %}
Propensity to contract and churn scores created by ML algorithms. 
{% enddocs %}

{% docs ptpt_scores_source %}
Propensity to purchase from trial scores created by ML algorithms. 
{% enddocs %}

{% docs pte_scores %}
Propensity to expand by at least 10% in the next 3 months. All Salesforce accounts are scored and updated monthly (excludes Russian and Belarusian accounts). 
{% enddocs %}

{% docs ptc_scores %}
Propensity to churn or contract by at least 10% in the next 6 months. All Salesforce accounts > 100 USD ARR are scored and updated monthly (excludes Russian and Belarusian accounts).  
{% enddocs %}

{% docs ptpt_scores %}
Propensity to purchase within 40 days of trial expiration. All trial namespaces with an active trial in the last 40 days are scored. 
{% enddocs %}
