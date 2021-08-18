## Overview

Goal: To help bring you, our new data team member, up to speed in the GitLab Data Team's analytics stack as efficiently as possible, without sacrificing quality for speed. There is a lot of information in the on-boarding issue, so please bookmark handbook pages, documentation pages, and log-ins for future reference. The goal is for you to complete and close the Data Team on-boarding issue within 1 week after you have completed the GitLab company on-boarding issue. These resources will be super helpful and serve as great reference material as you get up to speed and learn to work through issues and merge requests [over your first 90 days](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/source/job-families/finance/data-analyst/index.html.md#how-youll-ramp).


## Access Requests

- [ ] Join the `new slack channel?` slack channel
- [ ] Manager: Add to Stitch
- [ ] Manager: Add to Fivetran.(For enabling Fivetran in Okta [use google groups](https://about.gitlab.com/handbook/business-ops/okta/#managing-okta-access-using-google-groups)) 
- [ ] Manager: Add to Airflow as Admin? - Is there an other suitable access level? 


### Getting your computer set up locally

* Make sure that you have [created your SSH keys](https://docs.gitlab.com/ee/gitlab-basics/create-your-ssh-keys.html) prior to running this. You can check this by typing `ssh -T git@gitlab.com` into your terminal which should return "Welcome to GitLab, " + your_username. Make the SSH key with no password.

*THE NEXT STEPS SHOULD ONLY BE RUN ON YOUR GITLAB-ISSUED LAPTOP. If you run this on your personal computer, we take no responsibility for the side effects.*

* [ ] Open your computer's built-in terminal app. Run the following:
<!---
Would like to avoid installing Anaconda, might have a new feeder script for each role to fine tune the installs
--->
```
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/onboarding_script.zsh > ~/onboarding_script.zsh
zsh ~/onboarding_script.zsh
rm ~/onboarding_script.zsh
```
   
   * This may take a while, and it might ask you for your password (multiple times) before it's done. Here's what this does:
      * Installs iTerm, a mac-OS terminal replacement
      * Installs VSCode, an open source text editor. VSCode is recommended for multiple reasons including community support, the [GitLab workflow](https://marketplace.visualstudio.com/items?itemName=fatihacet.gitlab-workflow) extension, and the LiveShare features.
      * Installs oh-my-zsh for easy terminal theming, git autocomplete, and a few other plugins. If you are curious or would like to change the look and feel of your shell please [go here](https://github.com/ohmyzsh/ohmyzsh).
      * Installing dbt, the open source tool we use for data transformations. <!--  Why are we installing dbt when we use a virtual environment?-->
      * Installing jump, an easy way to move through the file system. [Please find here more details on how to use jump](https://github.com/ohmyzsh/ohmyzsh/tree/master/plugins/jump)
      * Installing anaconda, how we recommend folks get their python distribution.
      * Installs all-the-things needed to contribute to [the handbook](about.gitlab.com/handbook) locally and build it locally. <!-- Do we need to build it locally?-->
   * You will be able to `jump analytics` from anywhere to go to the analytics repo locally (you will have to open a new terminal window for `jump` to start working.) If it doesn't work, try running `cd ~/repos/analytics`. Once in "analytics" folder run command `mark analytics` then quit + reopen your terminal before trying again. Now path ~/repos/analytics has been named "analytics" and you can enter to it by using command `mark analytics`.
   * You will be able to `gl_open` from anywhere within the analytics project to open the repo in the UI. If doesn't work, visually inspect your `~/.bashrc` file to make sure it has [this line](https://gitlab.com/gitlab-data/analytics/blob/master/admin/make_life_easier.sh#L14).
   * Your default python version should now be python 3. Typing `which python` into a new terminal window should now return `/usr/local/anaconda3/bin/python` <!-- Is there a good reason to switch the default version of python?  I have ahd problems with this messing up some of the OS stuff in the past. -->
   * dbt will be installed at its latest version. Typing `dbt --version` will output the current version.
   * To get to the handbook project, you'll be able to use `jump handbook`, and to build the handbook locally, you'll be able to use the alias `build_hb!`.

* [ ] Install [Python 3.8.6](https://www.python.org/downloads/release/python-386/) is a good resource

* [ ] Install Data Grip (from JetBrains) for interfacing with databases
    * [ ] Follow [this process](https://about.gitlab.com/handbook/tools-and-tips/#jetbrains) for requesting a license for Data Grip.  Until you have a license, you can easily use Data Grip on a trial basis for 30 days
    - Change your formatting preferences in Data Grip by going to Preferences > Editor > Code Style > HTML. You should have:
        * Use tab character: unchecked
        * Tab size: 4
        * Indent: 4
        * Continuation indent: 8
        * Keep indents on empty lines: unchecked
    - You can use `Command + Option + L` to format your file.

#### Tips and Recommendations

* [ ] We strongly recommend configuring VSCode (via the VSCode UI) with the [VSCode setup](https://discourse.getdbt.com/t/how-we-set-up-our-computers-for-working-on-dbt-projects/243?) section of Claire's post and [adding the tip](https://discourse.getdbt.com/t/how-we-set-up-our-computers-for-working-on-dbt-projects/243/10?u=tmurphy) from tmurphy later in the thread. It will make your life much easier.
* [ ] Your editor should be configured so that all tabs are converted to 4 spaces. This will minimize messy looking diffs and provide consistency across the team.
    * VSCode
        * `Editor: Detect Indentation` is deselected
        * `Editor: Insert Spaces` is selected
        * `Editor: Tab Size` is set to 4 spaces per tab
* [ ] Consider following [these instructions](https://stackoverflow.com/a/23963086) so you can have option + arrow keys to move around the terminal easier

* [ ] If you get a weird semaphore issue error when running dbt try [this script](https://gist.github.com/llbbl/c54f44d028d014514d5d837f64e60bac) which is sourced from this [Apple forum thread](https://forums.developer.apple.com/thread/119429)
* [ ] (Optional) - Better terminal theming - In the onboarding script the terminal has been configured to use the [bira OhMyZsh theme](https://github.com/ohmyzsh/ohmyzsh/wiki/Themes#bira). However if you would like an improved and configurable theme install [PowerLevel10K](https://github.com/romkatv/powerlevel10k) by running the below command from your terminal: 
    ``` 
    git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k
    ```
    * Then reopen your terminal and you will be asked to configure this theme. If you would like to reconfigure the theme run `p10k configure`

## Data stack

Review our [Data Stack](https://about.gitlab.com/handbook/business-technology/data-team/platform/) for a general overview of the system.
On [the Data team handbook page](https://about.gitlab.com/handbook/business-ops/data-team/platform/#extract-and-load), we explain the variety of methods used to extract data from its raw sources (`pipelines`) to load into our Snowflake data warehouse. We use open source dbt (more on this in a moment) as our transformation tool. The bulk of your projects and tasks will be in dbt , so we will spend a lot of time familiarizing yourself with those tools and then dig into specific data sources.
 - [ ] Our current data infrastructure is represented in this [system diagram](https://about.gitlab.com/handbook/business-ops/data-team/platform/infrastructure/#system-diagram)

### The Data Warehouse - Connecting to Snowflake

- [ ] Login with the credentials that your manager created following the instructions at https://about.gitlab.com/handbook/business-ops/data-team/platform/#warehouse-access. Please note that currently Snowflake is accessed through Okta, however you still need to raise access request to get credentials, as you will need to restart your password and update dbt profile with Snowflake credentials. Access request should be raised the same way as it was for Google Cloud Platform credentials.
- [ ] Snowflake has a Web UI for querying the data warehouse that can be found under [Worksheets](https://gitlab.snowflakecomputing.com/console#/internal/worksheet). Familiarize yourself with it. Change your password and update your role, warehouse, and database to the same info you're instructed to put in your dbt profile (Ask your manager if this is confusing or check out [roles.yml](https://gitlab.com/gitlab-data/analytics/blob/master/load/snowflake/roles.yml) to see which roles, warehouses, and databases you've been assigned). The schema does not matter because your query will reference the schema.
- [ ] Run `alter user "your_user" set default_role = "your_role";` to set the UI default Role to your appropriate role instead of `PUBLIC`. (E.g. `alter user "KDIETZ" set default_role = "KDIETZ";`)
- [ ] You can test your Snowflake connection in the UI by first running selecting which warehouse to use (e.g. `use warehouse ANALYST_XS;`), clicking the "play" button, and then querying a database you have access to (e.g. `select * from "PROD"."COMMON"."DIM_CRM_PERSON" limit 10;`) 
- [ ] We STRONGLY recommend using the UI, but if you must download a SQL development tool, you will need one that is compatible with Snowflake, such as [SQLWorkbench/J](http://sql-workbench.net) or [DataGrip](https://www.jetbrains.com/datagrip/). If you're interested in DataGrip, follow the [instructions to get a JetBrains license in the handbook](https://about.gitlab.com/handbook/tools-and-tips/#jetbrains). If using DataGrip, you may need to download the [Driver](https://docs.snowflake.net/manuals/user-guide/jdbc-download.html#downloading-the-driver). This template may be useful as you're configuring the DataGrip connection to Snowflake `jdbc:snowflake://{account:param}.snowflakecomputing.com/?{password}[&db={Database:param}][&warehouse={Warehouse:param}][&role={Role:param}]` We recommend not setting your schema so you can select from the many options. If you do use Data Grip, please set up the following configuration:

### Snowflake SQL

Snowflake SQL is probably not that different from the dialects of SQL you're already familiar with, but here are a couple of resources to point you in the right direction:
- [ ] [Differences we found while transition from Postgres to Snowflake](https://gitlab.com/gitlab-data/analytics/issues/645)
- [ ] [How Compatible are Redshift and Snowflake’s SQL Syntaxes?](https://medium.com/@jthandy/how-compatible-are-redshift-and-snowflakes-sql-syntaxes-c2103a43ae84)
- [ ] [Snowflake Functions](https://docs.snowflake.net/manuals/sql-reference/functions-all.html)

### dbt - Data Build Tool

#### What is dbt?

DBT is our data transformation engine that we use to build our dimensional model tables and related tables.

- [ ] Familiarize yourself with [dbt](https://www.getdbt.com/) and how we use it by reading our [dbt Guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/dbt-guide/).


<img src = "https://d33wubrfki0l68.cloudfront.net/18774f02c29380c2ca7ed0a6fe06e55f275bf745/a5007/ui/img/svg/product.svg">

- [ ] Refer to http://jinja.pocoo.org/docs/2.10/templates/ as a resource for understanding Jinja which is used extensively in dbt.
- [ ] [This podcast](https://www.dataengineeringpodcast.com/dbt-data-analytics-episode-81/) is a general walkthrough of dbt/interview with its creator, Drew Banin.
- [ ] Read our [SQL Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/sql-style-guide/).
- [ ] Watch [video](https://www.youtube.com/watch?v=P_NQ9qHnsyQ&feature=youtu.be) of Thomas and Israel discussing getting started with dbt locally.
- [ ] Peruse the [Official Docs](https://docs.getdbt.com).
- [ ] In addition to using dbt to manage our transformations, we use dbt to maintain [our own internal documentation](https://dbt.gitlabdata.com) on those data transformations. This is a public link. We suggest bookmarking it.
- [ ] Read about and and watch [Drew demo dbt docs to Emilie & Taylor](https://blog.fishtownanalytics.com/using-dbt-docs-fae6137da3c3). Read about [Scaling Knowledge](https://blog.fishtownanalytics.com/scaling-knowledge-160f9f5a9b6c) and the problem we're trying to solve with our documentation.
- [ ] Consider joining [dbt slack](https://slack.getdbt.com) (Not required, but strongly recommended; if you join use your personal email).
- [ ] Information and troubleshooting on dbt is sparse on Google & Stack Overflow, we recommend the following sources of help when you need it:
   * Your teammates! We are all here to help!
   * dbt slack has a #beginners channel and they are very helpful.
   * [Fishtown Analytics Blog](https://blog.fishtownanalytics.com)
   * [dbt Discourse](http://discourse.getdbt.com)


### Getting Set up with dbt locally

_Ensure you've set up your SSH configuration in the previous step as this is required to connect to one our dbt packages_

- [ ] Follow the [instructions](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#running-dbt) found in the handbook for running and configuring dbt.
- [ ] Run the `run-dbt` command from the analytics repository.  This will load the dbt dependencies and open a shell to virtual environment where dbt is installed allowing you run dbt commands
- [ ] Run `dbt seed` to import the CSV's from the analytics/data into your schema. For dbt to compile this needs to be completed as some of the models have dependencies on the tables which are created by the CSV's.
- [ ] Run `dbt run --models +staging.sfdc` from within the container to know that your connection has been successful, you are in the correct location, and everything will run smoothly.  For more details on the syntax for how to select and run the models, please refer to this [page](https://docs.getdbt.com/reference/node-selection/syntax#examples).  Afterwards, you can also try running `dbt compile` to ensure that the entire project will compile correctly.
- [ ] Test the command `make help` and use it to understand how to use various `make *` commands available to you.

- Note: When launching dbt you may see `WARNING: The GOOGLE_APPLICATION_CREDENTIALS variable is not set. Defaulting to a blank string.` Unless you are developing on Airflow this is ok and expected. If you require GOOGLE_APPLICATION_CREDENTIALS please follow the steps outlined below in the DataLab section.

### Google Cloud

Data team uses GCP (Google Cloud Platform) as our cloud provider. GCP credentials are needed if you plan on connecting on your local machine to airflow or any CGP service (storage buckets, etc.) . Follow below steps to get running instance for yourself.

- [ ] Raise Access Request (AR) for Google Cloud Credentials. To do that please follow instructions here or create separate issue and copy contents from [here](https://about.gitlab.com/handbook/business-technology/team-member-enablement/onboarding-access-requests/access-requests/) or create separate issue and copy contents from [here](https://gitlab.com/gitlab-com/team-member-epics/access-requests/-/issues/10306#note_622125437). Ensure you update your name and other personal details and project name is ``gitlab-analysis.`` Assign it to your manager.
- [ ] Please follow next step after running onboarding template, once you added GOOGLE_APPLICATION_CREDENTIALS path to your .zshrc file which can be accessed by `vi ~/.zshrc``. One of the project owners should send you configuration json file, which is important to add in your google credentials. Follow below steps:
- [ ] Download the json file provided and move to your home directory (e.g. `/Users/yourusername`)
- [ ] Open terminal and run the following command, replacing `yourusername` with your actual user name on your computer (type `pwd` into the terminal if you don’t know it — the path should contain your user name) and `filename.json` with you name of the file.
    - echo export  GOOGLE_APPLICATION_CREDENTIALS=/Users/yourusername/filename.json >> ./.zshrc
    - If you already have the variable  GOOGLE_APPLICATION_CREDENTIALS  modify its value to the file path and file name instead of adding a new one. 
- [ ] Refresh this file by sourcing it back, by running command in terminal: ``source ~/.zshrc``.


## Data Operations

### Triage

Data triagers are the first responders to requests and problems for the Data team.
- [ ] Read about the [Data Triage Process](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/triage/)
- [ ] Checkout the Triage [template](https://gitlab.com/gitlab-data/analytics/-/blob/master/.gitlab/issue_templates/Data%20Triage.md)

## How GitLab the Product Works

- [ ] Familiarize yourself with GitLab CI https://docs.gitlab.com/ee/ci/quick_start/ and our running pipelines.
- [ ] Become familiar with the [API docs](https://gitlab.com/gitlab-org/gitlab/tree/master/doc/api)
- [ ] This is the [schema for the database](https://gitlab.com/gitlab-org/gitlab/-/blob/master/db/structure.sql)
- [ ] If you ever want to know what queries are going on in the background while you're using GitLab.com, enable the [Performance Bar](https://docs.gitlab.com/ee/administration/monitoring/performance/performance_bar.html) and click on the numbers to the left of `pg`. This is useful for learning how the gitlab.com schema works. The performance bar can be enable by pressing `p + b` ([Shortcut Docs](https://docs.gitlab.com/ee/user/shortcuts.html)).

## Important Data Sets

### Usage/Version Ping (Product)

Service Data aka Usage Ping is generated from individual installations of GitLab hosted by our customers. These are also called Self-Managed instaces, as opposed to our GitLab.com SaaS instance.

- [ ] Read about the [usage ping](https://docs.gitlab.com/ee/user/admin_area/settings/usage_statistics.html).
- [ ] To understand how this is implemented at GitLab read [Feature Implementation](https://about.gitlab.com/handbook/product/feature-instrumentation/#instrumentation-for-gitlabcom).
- [ ] Read the product vision for [telemetry](https://about.gitlab.com/direction/telemetry/).
- [ ] There is not great documentation on the usage ping, but you can get a sense from looking at the `usage.rb` file for [GitLab CE](https://gitlab.com/gitlab-org/gitlab/blob/master/lib/gitlab/usage_data.rb).
- [ ] It might be helpful to look at issues related to telemetry [here](https://gitlab.com/gitlab-org/telemetry/issues) and [here](https://gitlab.com/groups/gitlab-org/-/issues?scope=all&utf8=✓&state=all&search=~telemetry).
- [ ] Watch the [pings brain dump session](https://drive.google.com/file/d/1S8lNyMdC3oXfCdWhY69Lx-tUVdL9SPFe/view).  This video is outdated.  The tables that are related to the usage ping now reside in the [version model](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.version_usage_data).

### Salesforce (Sales, Marketing, Finance)

Also referred as SFDC, Salesforce.com (Sales Force Dot Com).
- [ ] Become familiar with Salesforce using [Trailhead](https://trailhead.salesforce.com/).
- [ ] If you are new to Salesforce or CRMs in general, start with [Intro to CRM Basics](https://trailhead.salesforce.com/trails/getting_started_crm_basics).
- [ ] If you have not used Salesforce before, take this [intro to the platform](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/starting_force_com).
- [ ] To familiarize yourself with the Salesforce data model, take [Data Modeling](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/data_modeling).
- [ ] You can review the general data model in [this reference](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/data_model.htm). Pay particular attention to the [Sales Objects](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_erd_majors.htm).
- [ ] To familiarize yourself with the Salesforce APIs, take [Intro to SFDC APIs](https://trailhead.salesforce.com/trails/force_com_dev_intermediate/modules/api_basics).
- [ ] For access to SFDC, your manager will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request). Please confirm with your manager that this has been done.
- [ ] Watch the [SalesForce brain dump session](https://youtu.be/KwG3ylzWWWo).

### Zuora (Finance, Billing SSOT)

- [ ] Become familiar with Zuora.
- [ ] Watch Brian explain Zuora to Taylor [GDrive Link](https://drive.google.com/file/d/1fCr48jZbPiW0ViGr-6rZxVVdBpKIoopg/view).
- [ ] [Zuora documentation](https://knowledgecenter.zuora.com/).
- [ ] [Data Model from Zuora for Salesforce](https://knowledgecenter.zuora.com/CA_Commerce/A_Zuora_CPQ/A2_Zuora4Salesforce_Object_Model).
- [ ] [Data Model inside Zuora](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Zuora_Business_Objects_Relationship).
- [ ] [Definitions of Objects](https://knowledgecenter.zuora.com/CD_Reporting/D_Data_Sources_and_Exports/AB_Data_Source_Availability).
- [ ] [Zuora Subscription Data Management](https://about.gitlab.com/handbook/finance/accounting/#zuora-subscription-data-management).
- [ ] For access to Zuora, your manager will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request). Please confirm with your manager that this has been done.

### Snowplow (Product)

[Snowplow](https://snowplowanalytics.com) is an open source web analytics collector and is what we use to instrument our Front-End SaaS product and Back-End SaaS product.

- [ ] To understand how this is implemented at GitLab read [Feature Implementation](https://about.gitlab.com/handbook/product/feature-instrumentation/#instrumentation-for-gitlabcom).
- [ ] Also read how we pull data from [S3 into Snowflake](https://about.gitlab.com/handbook/business-ops/data-team/platform/#snowplow-infrastructure)
- [ ] Familiarize yourself with the [Snowplow Open Source documentation](https://github.com/snowplow/snowplow).
- [ ] We use the [Snowplow dbt package](https://hub.getdbt.com/fishtown-analytics/snowplow/latest/) on our models. Their documentation does show up in our dbt docs.

### Marketo (email Campaign Management)

- [ ] For access to Marketo, your manager will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request). Please confirm with your manager that this has been done.

### Netsuite (Accounting)

- [ ] Netsuite dbt models 101: Familiarize yourself with the Netsuite models by watching this [Data Netsuite dbt models](https://www.youtube.com/watch?v=u2329sQrWDY&feature=youtu.be). You will need to be logged into [GitLab Unfiltered](https://www.youtube.com/channel/UCMtZ0sc1HHNtGGWZFDRTh5A/).
- [ ] For access to Netsuite, your manager will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request). Please confirm with your manager that this has been done.

### Zendesk (Support)

- [ ] For access to Zendesk, please follow the instructions in the [handbook](https://about.gitlab.com/handbook/support/internal-support/#light-agent-zendesk-accounts-available-for-all-gitlab-staff)

## Metrics and Methods

- [ ] Read through [SaaS Metrics 2.0](http://www.forentrepreneurs.com/saas-metrics-2/) to get a good understanding of general SaaS metrics.
- [ ] Check out [10 Reads for Data Scientists Getting Started with Business Models](https://www.conordewey.com/blog/10-reads-for-data-scientists-getting-started-with-business-models/) and read through the collection of articles to deepen your understanding of SaaS metrics.
- [ ] Familiarize yourself with the GitLab Metrics Sheet (search in Google Drive, it should come up) which contains most of the key metrics we use at GitLab and the [definitions of these metrics](https://about.gitlab.com/handbook/business-ops/data-team/kpi-index/).
- [ ] Optional, for more information on Finance KPIs, you can watch this working session between the Manager, Financial Planning and Analysis and Data Analyst, Finance: [Finance KPIs](https://www.youtube.com/watch?v=dmdilBQb9PY&feature=youtu.be)

## Good First Issues:

- [ ] [Replace]
- [ ] [Replace]

## Resources to help you get started with your first issue

- [ ] Pairing session between a new Data Analyst and a Staff Data Engineer working on the new analyst's first issue: [Pair on Lost MRR Dashboard Creation](https://www.youtube.com/watch?v=WuIcnpuS2Mg)
- [ ] 2nd part of pairing session between a new Data Analyst and a Staff Data Engineer working on the new analyst's first issue: [Pair on Lost MRR Dashboard Creation Part 2](https://www.youtube.com/watch?v=HIlDH5gaL3M)
- [ ] Setting up visual studio and git terminals to use for testing locally. (https://youtu.be/t5eoNLUl3x0)

 (Not required, but recommended).
- [ ] [Company Call Agenda](https://docs.google.com/document/d/1JiLWsTOm0yprPVIW9W-hM4iUsRxkBt_1bpm3VXV4Muc/edit)
- [ ] [DataOps Meeting Agenda](https://docs.google.com/document/d/1qCfpRRKQfSU3VplI45huE266CT0nB82levb3lF9xeUs/edit)
- [ ] Optional, for more information on Finance KPIs, you can watch this working session between the Manager, Financial Planning and Analysis and Data Analyst, Finance: [Finance KPIs](https://www.youtube.com/watch?v=dmdilBQb9PY&feature=youtu.be)

## Other Stuff

- [ ] Familiarize yourself with the [Stitch](http://stitchdata.com) UI, as this is mostly the source of truth for what data we are loading. An email will have been sent with info on how to get logged in.
- [ ] Consider joining [Locally Optimistic slack](https://www.locallyoptimistic.com/community/)
 (Not required, but recommended).
- [ ] Consider subscribing to the [Data Science Roundup](http://roundup.fishtownanalytics.com) (Not required, but recommended).
- [ ] There are many Slack channels organized around interests, such as `#fitlab`, `#bookclub`, and `#woodworking`. There are also many organized by location (these all start with `#loc_`). This is a great way to connect to GitLab team members outside of the Data-team. Join some that are relevant to your interests, if you'd like.
- [ ] Familiarize yourself with [SheetLoad](https://about.gitlab.com/handbook/business-ops/data-team/platform/#using-sheetload).
- [ ] Really really useful resources in [this Drive folder](https://drive.google.com/drive/folders/1wrI_7v0HwCwd-o1ryTv5dlh6GW_JyrSQ?usp=sharing) (GitLab Internal); Read the `a_README` file first.
- [ ] Save the [Data Kitchen Data Ops Cookbook](https://drive.google.com/file/d/14KyYdFB-DOeD0y2rNyb2SqjXKygo10lg/view?usp=sharing) as a reference.
- [ ] Save the [Data Engineering Cookbook](https://drive.google.com/file/d/1Tm3GiV3P6c5S3mhfF9bm7VaKCtio-9hm/view?usp=sharing) as a reference.

## Suggested Bookmarks None of these are required, but bookmarking these links will make life at GitLab much easier. Some of these are not hyperlinked for security concerns.

- [ ] 1:1 with Manager Agenda
- [ ] [Create new issue in Analytics Project](https://gitlab.com/gitlab-data/analytics/issues/new?issue%5Bassignee_id%5D=&issue%5Bmilestone_id%5D=)
- [ ] [Data team page of Handbook](https://about.gitlab.com/handbook/business-ops/data-team/)
- [ ] [dbt Docs](https://docs.getdbt.com)
- [ ] [dbt Discourse](http://discourse.getdbt.com)
- [ ] [GitLab's dbt Documentation](https://dbt.gitlabdata.com)
- [ ] [Data Team GitLab Activity](https://gitlab.com/groups/gitlab-data/-/activity)
        


