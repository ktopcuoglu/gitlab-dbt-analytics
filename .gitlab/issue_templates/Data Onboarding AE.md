
# Welcome to the GitLab Data Program

Welcome to the GitLab Data Program -- we're excited to have you! The goal of this issue is to help bring you, our new data team member, up to speed in the GitLab Data Team's analytics stack as efficiently as possible, without sacrificing quality for speed. You should complete and close the Data Team on-boarding issue within 1 week after you have completed the GitLab company on-boarding issue. 

## Table of Contents
* [Getting Started](#getting-started)
* [Access Requests](#access-requests)
* [Slack Channels](#slack-channels)
* [Team Introductions](#team-introductions)
* [Computer Set Up](#computer-set-up)
* [Data Stack](#data-stack)
* [Data Operations](#data-operations)
* [Important Data Sets](#important-data-sets)
* [Supporting Information](#supporting-information)

## Getting Started

- [ ] Read (skim) through this full issue, just so you have a sense of what's coming.
- [ ] Create a new issue in the Analytics project (this project). As you proceed and things are unclear, document it in the issue. Don't worry about organizing it; just brain dump it into the issue! This will help us iterate on the onboarding process.
- [ ] Read the following pages of the handbook in their entirety. Bookmark them as you should soon be making MR's to improve our documentation!
   - [ ] [Data Team](https://about.gitlab.com/handbook/business-ops/data-team/)
   - [ ] [Data Direction page](https://about.gitlab.com/handbook/business-technology/data-team/direction/) to get a sense of what our short and longer-term roadmap.
   - [ ] [Data Catalog](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/)
- [ ] Watch @tlapiana's [talk at DataEngConf](https://www.youtube.com/watch?v=eu623QBwakc) that gives a phenomenal overview of how the team works.
- [ ] Watch [this great talk](https://www.youtube.com/watch?v=prcz0ubTAAg) on what Analytics is
- [ ] Watch [Overview of Growth Data at GitLab](https://www.youtube.com/watch?v=eNLkj3Ho2bk&feature=youtu.be) from Eli at the Growth Fastboot. (You'll need to be logged into GitLab Unfiltered.)
- [ ] Browse a few of the [Data Team videos in GitLab Unfiltered](https://www.youtube.com/playlist?list=PL05JrBw4t0KrRVTZY33WEHv8SjlA_-keI)

### Take a Deep Breath

There is a lot of information being thrown at you over the last couple of days.
It can all feel a bit overwhelming.
The way we work at GitLab is unique and can be the hardest part of coming on board.
It is really important to internalize that we work **handbook-first** and that **everything is always a work in progress**.
Please watch one minute of [this clip](https://www.youtube.com/watch?v=LqzDY76Q8Eo&feature=youtu.be&t=7511) (you will need to be logged into GitLab unfiltered) where Sid gives a great example of why its important that we work this way.
*This is the most important thing to learn during all of onboarding.*

## Access Requests

- [ ] Manager: Submit an Access Request to provision the new member with Data Tools
    - [ ] Manager: Add to Lucidchart (via Okta)
    - [ ] Manager: Add to Sisense as editor (via Okta) [example AR](https://gitlab.com/gitlab-com/team-member-epics/access-requests/-/issues/10858)
    - [ ] Manager: Add to Snowflake [process](https://about.gitlab.com/handbook/business-ops/data-team/platform/#warehouse-access) and [example AR](https://gitlab.com/gitlab-com/team-member-epics/access-requests/-/issues/10857)
     <!-- Analytics Engineer -->
    - [ ] Manager: Add to Stitch
    - [ ] Manager: Add to Fivetran.(For enabling Fivetran in Okta [use google groups](https://about.gitlab.com/handbook/business-ops/okta/#managing-okta-access-using-google-groups)) 
    - [ ] Manager: Add to Airflow as Admin
- [ ] Manager: Request addition to `@datateam` alias on Slack in PeopleOps Onboarding issue
- [ ] Manager: Request addition to `@data-analysts` alias on Slack in PeopleOps Onboarding issue
- [ ] Manager: Request addition to `Data Team` 1password vault in PeopleOps Onboarding issue
- [ ] Manager: Add to the `gitlab-data` namespace as a Developer.
- [ ] Manager: Update codeowners file in the handbook to include the new team member
- [ ] Manager: Add to daily Geekbot standup (send `dashboard` to Geekbot on slack, click into a particular standup in the web UI, add via Manage button)
- [ ] Manager: Invite to SheetLoad & Boneyard folders in Google Drive
- [ ] Manager: Add to data team calendar as a calendar admin
- [ ] Manager: Add to Data Team calendar
- [ ] Manager: Add to the `GitLab Data Team` project as a Developer.
- [ ] Manager: Add to [data triage](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/duties/#triager) in third week at GitLab (Week 1 = Company onboarding; Week 2 = Data team onboarding)
- [ ] Manager: Add to Lucidchart `Data Team` team folder


<!-- These permissions are optional and only thous needed should be let in the issue -->
- [ ] Manager: Add to SFDC. [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request)
- [ ] Manager: Add to Zuora. [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request)
- [ ] Manager: Marketo [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request)
- [ ] Manager: Netsuite [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request)
- [ ] Manager: Zendesk [Instructions](https://about.gitlab.com/handbook/support/internal-support/#regarding-licensing-and-subscriptions)

## Team Introductions

Getting to know the team

- [ ] Review the [org chart](https://about.gitlab.com/company/team/org-chart/) to find your immediate team and the greater Data Team. Using your browser search to find your self on the page can be a quick way to find your team.
- [ ] Schedule a Quarterly 1:1 meeting with the Sr. Director of Data and Analytics
- [ ] Schedule coffee chats with members of the Data Team starting with thous in your immediate team. These should be in addition to the ones you do with other GitLab team members. Consider making these recurring meetings for every 3-4 weeks with everyone you will work closely with. In addition, you should also consider scheduling chats with Business Technology (IT, Enterprise Apps, Procurement) people as well.


## Slack Channels

Join these Slack channels:

- [ ] `data`
- [ ] `data-lounge`
- [ ] `data-onboarding`
- [ ] `data-daily`
- [ ] `data-triage`
- [ ] `data-engineering`
- [ ] `business-technology`
- [ ] `bt-team-lounge`
- [ ] `dbt-runs`


## Computer Set Up

* [ ] Check that you have create your SSH keys by typing `ssh -T git@gitlab.com` into your terminal which should return "Welcome to GitLab, " + your_username.  :red_circle: This set up is required for subsequent steps
  * [ ] If your SSH keys have not been created follow [these steps](https://docs.gitlab.com/ee/gitlab-basics/create-your-ssh-keys.html).  Make the SSH key with no password.

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
      * Installs oh-my-zsh for easy terminal theming, git autocomplete, and a few other plugins. If you are curious or would like to change the look and feel of your shell please [go here](https://github.com/ohmyzsh/ohmyzsh).<!-- Is this requeued or recommended? -->
      * Installing dbt, the open source tool we use for data transformations. <!--  Why are we installing dbt when we use a virtual environment?-->
      * Installing jump, an easy way to move through the file system. [Please find here more details on how to use jump](https://github.com/ohmyzsh/ohmyzsh/tree/master/plugins/jump)
      * Installing anaconda, how we recommend folks get their python distribution. <!-- Is this required? I don't thing that an Analytics Engineer would use most of these tools and a clean install of python 3.8 has less integration problems -->
      * Installs all-the-things needed to contribute to [the handbook](about.gitlab.com/handbook) locally and build it locally. <!-- Do we need to build it locally? It seem that the MR pipeline replaces this for general use-->
      * Adds alias and environment variables needed for running dbt and other helper commands

   
   * Your default python version should now be python 3. Typing `which python` into a new terminal window should now return `/usr/local/anaconda3/bin/python` <!-- Is there a good reason to switch the default version of python?  I have ahd problems with this messing up some of the OS stuff in the past. -->
   * dbt will be installed at its latest version. Typing `dbt --version` will output the current version.

* [ ] Open a new terminal and test the following commands
    * [ ] `jump analytics` this should change the directory to `~/repos/analytics`
    * [ ] `jump handbook` this should change the directory to  `~/repos/www-gitlab-com`
    * [ ] `gl_open` if you are in a repo directory this command will open that repo on GitLab
    
    **Note:** If the `jump` commands do not work they can be set manually by navigating to the desired repo with the terminal and using the `mark` command and the appropriate label.

    **Note:** If the `gl_open` command does not work inspect your `~/.zshrc` file to make sure it has the command `source make_life_easier.zsh`

* [ ] Install [Python 3.8.6](https://www.python.org/downloads/release/python-386/)

* [ ] Configure VSCode (via the VSCode UI) with the [VSCode setup](https://discourse.getdbt.com/t/how-we-set-up-our-computers-for-working-on-dbt-projects/243?) section of Claire's post and [adding the tip](https://discourse.getdbt.com/t/how-we-set-up-our-computers-for-working-on-dbt-projects/243/10?u=tmurphy) from tmurphy later in the thread. It will add improved syntax highlighting and searching capabilities.
* [ ] Your editor should be configured so that all tabs are converted to 4 spaces. This will minimize messy looking diffs and provide consistency across the team.
    * VSCode
        * `Editor: Detect Indentation` is deselected
        * `Editor: Insert Spaces` is selected
        * `Editor: Tab Size` is set to 4 spaces per tab

### Optional Steps
* Set up environment to build the handbook locally. [Instructions](https://about.gitlab.com/handbook/git-page-update/) 
* Install Data Grip (from JetBrains) for interfacing with databases
    * Follow [this process](https://about.gitlab.com/handbook/tools-and-tips/#jetbrains) for requesting a license for Data Grip.  Until you have a license, you can easily use Data Grip on a trial basis for 30 days
    - Change your formatting preferences in Data Grip by going to Preferences > Editor > Code Style > HTML. You should have:
        * Use tab character: unchecked
        * Tab size: 4
        * Indent: 4
        * Continuation indent: 8
        * Keep indents on empty lines: unchecked
    - You can use `Command + Option + L` to format your file.

#### Terminal Improvements 
[Improved terminal navigation](https://stackoverflow.com/a/23963086) with arrow keys.

Terminal theming - In the onboarding script the terminal has been configured to use the [bira OhMyZsh theme](https://github.com/ohmyzsh/ohmyzsh/wiki/Themes#bira). However if you would like an improved and configurable theme install [PowerLevel10K](https://github.com/romkatv/powerlevel10k) by running the below command from your terminal: 
    ``` 
    git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k
    ```
    Then reopen your terminal and you will be asked to configure this theme. If you would like to reconfigure the theme run `p10k configure`

## Data Stack

Review our [Data Stack](https://about.gitlab.com/handbook/business-technology/data-team/platform/) for a general overview of the system.
On [the Data team handbook page](https://about.gitlab.com/handbook/business-ops/data-team/platform/#extract-and-load), we explain the variety of methods used to extract data from its raw sources (`pipelines`) to load into our Snowflake data warehouse. We use open source dbt (more on this in a moment) as our transformation tool. The bulk of your projects and tasks will be in dbt , so we will spend a lot of time familiarizing yourself with those tools and then dig into specific data sources.

 - [ ] Review our current data infrastructure is represented in this [system diagram](https://about.gitlab.com/handbook/business-ops/data-team/platform/infrastructure/#system-diagram)

### The Data Warehouse - Connecting to Snowflake

- [ ] Login to Snowflake using [Okta](https://gitlab.okta.com/app/UserHome) 
- [ ] Familiarize yourself with the [Snowflake Web UI](https://docs.snowflake.com/en/user-guide/snowflake-manager.html#worksheet-page)for querying the data warehouse. 
- [ ] Update your role, warehouse, and database to the same info you're instructed to put in your dbt profile (Ask your manager if this is confusing or check out [roles.yml](https://gitlab.com/gitlab-data/analytics/blob/master/load/snowflake/roles.yml) to see which roles, warehouses, and databases you've been assigned). The schema does not matter because your query will reference the schema.
  - [ ] Run `alter user "your_user" set default_role = "your_role";` to set the UI default Role to your appropriate role instead of `PUBLIC`. (E.g. `alter user "KDIETZ" set default_role = "KDIETZ";`)
- [ ] Test your Snowflake connection in the UI by first running selecting which warehouse to use (e.g. `use warehouse ANALYST_XS;`), clicking the "play" button, and then querying a database you have access to (e.g. `select * from "PROD"."COMMON"."DIM_CRM_PERSON" limit 10;`) 

We STRONGLY recommend using the UI, but if you must download a SQL development tool, you will need one that is compatible with Snowflake, such as [SQLWorkbench/J](http://sql-workbench.net) or [DataGrip](https://www.jetbrains.com/datagrip/). If you're interested in DataGrip, follow the [instructions to get a JetBrains license in the handbook](https://about.gitlab.com/handbook/tools-and-tips/#jetbrains). If using DataGrip, you may need to download the [Driver](https://docs.snowflake.net/manuals/user-guide/jdbc-download.html#downloading-the-driver). This template may be useful as you're configuring the DataGrip connection to Snowflake `jdbc:snowflake://{account:param}.snowflakecomputing.com/?{password}[&db={Database:param}][&warehouse={Warehouse:param}][&role={Role:param}]` We recommend not setting your schema so you can select from the many options. 
### Snowflake SQL

Snowflake SQL is probably not that different from the dialects of SQL you're already familiar with, but here are a couple of resources to point you in the right direction:
- [ ] [Differences we found while transition from Postgres to Snowflake](https://gitlab.com/gitlab-data/analytics/issues/645)
- [ ] [How Compatible are Redshift and Snowflake’s SQL Syntaxes?](https://medium.com/@jthandy/how-compatible-are-redshift-and-snowflakes-sql-syntaxes-c2103a43ae84)
- [ ] [Snowflake Functions](https://docs.snowflake.net/manuals/sql-reference/functions-all.html)

### dbt - Data Build Tool

DBT is our data transformation engine that we use to build our dimensional model tables and related tables.

<img src = "https://d33wubrfki0l68.cloudfront.net/18774f02c29380c2ca7ed0a6fe06e55f275bf745/a5007/ui/img/svg/product.svg">

#### **Learning** 

- [ ] Familiarize yourself with [dbt](https://www.getdbt.com/) and how we use it by reading our [dbt Guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/dbt-guide/).
- [ ] Watch [This podcast](https://www.dataengineeringpodcast.com/dbt-data-analytics-episode-81/). It is a general walkthrough of dbt/interview with its creator, Drew Banin.
- [ ] Read about and and watch [Drew demo dbt docs to Emilie & Taylor](https://blog.fishtownanalytics.com/using-dbt-docs-fae6137da3c3). 
- [ ] Read about [Scaling Knowledge](https://blog.fishtownanalytics.com/scaling-knowledge-160f9f5a9b6c) and the problem we're trying to solve with our documentation.

#### **References** 
- [ ] Peruse the [Official Docs](https://docs.getdbt.com).
- [ ] Read our [SQL Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/sql-style-guide/).
- [ ] Refer to http://jinja.pocoo.org/docs/2.10/templates/ as a resource for understanding Jinja which is used extensively in dbt.
- [ ] Bookmark [our own internal documentation](https://dbt.gitlabdata.com). In addition to using dbt to manage our transformations, we use dbt to maintain documentations on those data transformations.

#### **Getting Help**
- Consider joining [dbt slack](https://slack.getdbt.com) (Not required, but strongly recommended; if you join use your personal email).
- Information and troubleshooting on dbt is sparse on Google & Stack Overflow, we recommend the following sources of help when you need it:
   * Your teammates! We are all here to help!
   * dbt slack has a #beginners channel and they are very helpful.
   * [Fishtown Analytics Blog](https://blog.fishtownanalytics.com)
   * [dbt Discourse](http://discourse.getdbt.com)


#### **Configuring**

_Ensure you've set up your SSH configuration in the previous step as this is required to connect to one our dbt packages_

- [ ] Follow the [instructions](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#running-dbt) found in the handbook for running and configuring dbt.
- [ ] Run the `run-dbt` command from the analytics repository.  This will load the dbt dependencies and open a shell to virtual environment where dbt is installed allowing you run dbt commands
- [ ] Run `dbt seed` to import the CSV's from the analytics/data into your schema. For dbt to compile this needs to be completed as some of the models have dependencies on the tables which are created by the CSV's.
- [ ] Run `dbt run --models +staging.sfdc` from within the container to know that your connection has been successful, you are in the correct location, and everything will run smoothly.  For more details on the syntax for how to select and run the models, please refer to this [page](https://docs.getdbt.com/reference/node-selection/syntax#examples).  Afterwards, you can also try running `dbt compile` to ensure that the entire project will compile correctly.
- [ ] Test the command `make help` and use it to understand how to use various `make *` commands available to you.

**Note:** When launching dbt you may see `WARNING: The GOOGLE_APPLICATION_CREDENTIALS variable is not set. Defaulting to a blank string.` Unless you are developing on Airflow this is ok and expected. If you require GOOGLE_APPLICATION_CREDENTIALS please follow the steps outlined below in the DataLab section.

**Note:** If you get a weird semaphore issue error when running dbt try [this script](https://gist.github.com/llbbl/c54f44d028d014514d5d837f64e60bac) which is sourced from this [Apple forum thread](https://forums.developer.apple.com/thread/119429)

### Sisense 

Sisense is our enterprise standard data visualization application and is the only application approved for connecting to our Enterprise Data Warehouse.

- [ ] Review the following training materials from the [Data Team Sisense](https://about.gitlab.com/handbook/business-technology/data-team/platform/periscope/#training-resources) page.
  - [ ] Watch Accessing Sisense
  - [ ] Review the Getting Started article
  - [ ] Watch the Gitlab's Sisence Editor Training
- [ ] [Sisense Admin Training Part 1](https://www.youtube.com/watch?v=YspSfOuEQV4&list=PL05JrBw4t0KrRVTZY33WEHv8SjlA_-keI&index=16)
- [ ] [Sisense Admin Training Part 2](https://www.youtube.com/watch?v=LQT9fXw1EaE&list=PL05JrBw4t0KrRVTZY33WEHv8SjlA_-keI&index=14)

## Data Operations

## How GitLab the Product Works

- [ ] Familiarize yourself with GitLab CI https://docs.gitlab.com/ee/ci/quick_start/ and our running pipelines.
- [ ] Become familiar with the [API docs](https://gitlab.com/gitlab-org/gitlab/tree/master/doc/api)
- [ ] This is the [schema for the database](https://gitlab.com/gitlab-org/gitlab/-/blob/master/db/structure.sql)
- [ ] If you ever want to know what queries are going on in the background while you're using GitLab.com, enable the [Performance Bar](https://docs.gitlab.com/ee/administration/monitoring/performance/performance_bar.html) and click on the numbers to the left of `pg`. This is useful for learning how the gitlab.com schema works. The performance bar can be enable by pressing `p + b` ([Shortcut Docs](https://docs.gitlab.com/ee/user/shortcuts.html)).

### Metrics and Methods

- [ ] Read through [SaaS Metrics 2.0](http://www.forentrepreneurs.com/saas-metrics-2/) to get a good understanding of general SaaS metrics.
- [ ] Check out [10 Reads for Data Scientists Getting Started with Business Models](https://www.conordewey.com/blog/10-reads-for-data-scientists-getting-started-with-business-models/) and read through the collection of articles to deepen your understanding of SaaS metrics.
- [ ] Familiarize yourself with the GitLab Metrics Sheet (search in Google Drive, it should come up) which contains most of the key metrics we use at GitLab and the [definitions of these metrics](https://about.gitlab.com/handbook/business-ops/data-team/kpi-index/).
- [ ] Optional, for more information on Finance KPIs, you can watch this working session between the Manager, Financial Planning and Analysis and Data Analyst, Finance: [Finance KPIs](https://www.youtube.com/watch?v=dmdilBQb9PY&feature=youtu.be)


### Triage

Data triagers are the first responders to requests and problems for the Data team.
- [ ] Read about the [Data Triage Process](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/triage/)
- [ ] Checkout the Triage [template](https://gitlab.com/gitlab-data/analytics/-/blob/master/.gitlab/issue_templates/Data%20Triage.md)

## Important Data Sets

There are many data sets brought into the Enterprise Data Warehouse, the following sections highlight some of them.  You should review the sections that are relevant for your area of focus, each data set is tagged with areas they support.  The other data sets can be skimmed for your general knowledge and information. 

### GitLab.com (Product)
This data comes from our GitLab.com SaaS product.

<details>

<summary>Review Steps</summary>

- [ ] Become familiar with the [API docs](https://gitlab.com/gitlab-org/gitlab/tree/master/doc/api)
- [ ] This is the [schema for the database](https://gitlab.com/gitlab-org/gitlab/-/blob/master/db/structure.sql)
- [ ] If you ever want to know what queries are going on in the background while you're using GitLab.com, enable the [Performance Bar](https://docs.gitlab.com/ee/administration/monitoring/performance/performance_bar.html) and click on the numbers to the left of `pg`. This is useful for learning how the gitlab.com schema works. The performance bar can be enable by pressing `p + b` ([Shortcut Docs](https://docs.gitlab.com/ee/user/shortcuts.html)).

</details>

---

### Usage/Version Ping (Product)

Service Data aka Usage Ping is generated from individual installations of GitLab hosted by our customers. These are also called Self-Managed instances, as opposed to our GitLab.com SaaS instance.

<details>

<summary>Review Steps</summary>

- [ ] Read about the [usage ping](https://docs.gitlab.com/ee/user/admin_area/settings/usage_statistics.html).
- [ ] Read [Feature Implementation](https://about.gitlab.com/handbook/product/feature-instrumentation/#instrumentation-for-gitlabcom) to understand how this is implemented at GitLab read.
- [ ] Read the product vision for [telemetry](https://about.gitlab.com/direction/telemetry/).
- [ ] Look at the `usage.rb` file for [GitLab CE](https://gitlab.com/gitlab-org/gitlab/blob/master/lib/gitlab/usage_data.rb) to get a sense on using ping.
- [ ] Look at issues related to telemetry [here](https://gitlab.com/gitlab-org/telemetry/issues) and [here](https://gitlab.com/groups/gitlab-org/-/issues?scope=all&utf8=✓&state=all&search=~telemetry).
- [ ] Watch the [pings brain dump session](https://drive.google.com/file/d/1S8lNyMdC3oXfCdWhY69Lx-tUVdL9SPFe/view).  Note that this video is outdated and the tables that are related to the usage ping now reside in the [version model](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.version_usage_data).

</details>

---

### Salesforce (Sales, Marketing, Finance)

Also referred as SFDC, Salesforce.com (Sales Force Dot Com).

<details>

<summary>Review Steps</summary>

- [ ] Familiarize yourself with Salesforce using [Trailhead](https://trailhead.salesforce.com/).
  - [ ] [Intro to CRM Basics](https://trailhead.salesforce.com/trails/getting_started_crm_basics).
  - [ ] [Intro to the Salesforce Platform](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/starting_force_com).
  - [ ] [Data Modeling](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/data_modeling).
  - [ ] [Intro to SFDC APIs](https://trailhead.salesforce.com/trails/force_com_dev_intermediate/modules/api_basics).
- [ ] Review the [general](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/data_model.htm) data model. 
- [ ] Review the [Sales Objects](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_erd_majors.htm) data model.
- [ ] Watch the [SalesForce brain dump session](https://youtu.be/KwG3ylzWWWo).

</details>

---


### Zuora (Finance, Billing SSOT)

[Zuora](https://www.zuora.com/) is used for managing customer subscriptions to GitLab

<details>

<summary>Review Steps</summary>

- [ ] Become familiar with Zuora.
    - [ ] Watch Brian explain Zuora to Taylor [GDrive Link](https://drive.google.com/file/d/1fCr48jZbPiW0ViGr-6rZxVVdBpKIoopg/view).
    - [ ] [Zuora Subscription Data Management](https://about.gitlab.com/handbook/finance/accounting/#zuora-subscription-data-management).
    - [ ] Review the [Zuora documentation](https://knowledgecenter.zuora.com/).
    - [ ] Review the [Data Model from Zuora for Salesforce](https://knowledgecenter.zuora.com/CA_Commerce/A_Zuora_CPQ/A2_Zuora4Salesforce_Object_Model).
    - [ ] Review the [Data Model inside Zuora](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Zuora_Business_Objects_Relationship).
    - [ ] Review the [Definitions of Objects](https://knowledgecenter.zuora.com/CD_Reporting/D_Data_Sources_and_Exports/AB_Data_Source_Availability).
    

</details>

---


### Snowplow (Product)

[Snowplow](https://snowplowanalytics.com) is an open source web analytics collector and is what we use to instrument our Front-End SaaS product and Back-End SaaS product.

<details>

<summary>Review Steps</summary>

- [ ] To understand how this is implemented at GitLab read [Feature Implementation](https://about.gitlab.com/handbook/product/feature-instrumentation/#instrumentation-for-gitlabcom).
- [ ] Also read how we pull data from [S3 into Snowflake](https://about.gitlab.com/handbook/business-ops/data-team/platform/#snowplow-infrastructure)
- [ ] Familiarize yourself with the [Snowplow Open Source documentation](https://github.com/snowplow/snowplow).
- [ ] We use the [Snowplow dbt package](https://hub.getdbt.com/fishtown-analytics/snowplow/latest/) on our models. Their documentation does show up in our dbt docs.
</details>

---


### Marketo (email Campaign Management)

Marketo is used for ...

<details>

<summary>Review Steps</summary>

`Under Construction`

</details>

---


### Netsuite (Accounting)

Netsuite is used for ...

<details>

<summary>Review Steps</summary>

- [ ] Netsuite dbt models 101: Familiarize yourself with the Netsuite models by watching this [Data Netsuite dbt models](https://www.youtube.com/watch?v=u2329sQrWDY&feature=youtu.be). You will need to be logged into [GitLab Unfiltered](https://www.youtube.com/channel/UCMtZ0sc1HHNtGGWZFDRTh5A/).

</details>

---


### Zendesk (Support)

Zendesk is used for ...

<details>

<summary>Review Steps</summary>

`Under Construction`

</details>

---

### Sheetload (Various)

Sheetloads are ...

<details>

<summary>Review Steps</summary>

- [ ] Familiarize yourself with [SheetLoad](https://about.gitlab.com/handbook/business-ops/data-team/platform/#using-sheetload).

</details>

---

## Supporting Information 

- Consider joining [Locally Optimistic slack](https://www.locallyoptimistic.com/community/)
- Consider subscribing to the [Data Science Roundup](http://roundup.fishtownanalytics.com) (Not required, but recommended).
- There are many Slack channels organized around interests, such as `#fitlab`, `#bookclub`, and `#woodworking`. There are also many organized by location (these all start with `#loc_`). This is a great way to connect to GitLab team members outside of the Data-team. Join some that are relevant to your interests, if you'd like.
- Really really useful resources in [this Drive folder](https://drive.google.com/drive/folders/1wrI_7v0HwCwd-o1ryTv5dlh6GW_JyrSQ?usp=sharing) (GitLab Internal); Read the `a_README` file first.

### Resources to help you get started with your first issue

- Pairing session between a new Data Analyst and a Staff Data Engineer working on the new analyst's first issue: [Pair on Lost MRR Dashboard Creation](https://www.youtube.com/watch?v=WuIcnpuS2Mg)
- nd part of pairing session between a new Data Analyst and a Staff Data Engineer working on the new analyst's first issue: [Pair on Lost MRR Dashboard Creation Part 2](https://www.youtube.com/watch?v=HIlDH5gaL3M)
- Setting up visual studio and git terminals to use for testing locally. [Visual Studio Setup - Data Team](https://youtu.be/t5eoNLUl3x0)


### References 

Suggested bookmarks: none of these are required, but bookmarking these links will make life at GitLab much easier. Some of these are not hyperlinked for security concerns.

-  1:1 with Manager Agenda
- [Create new issue in Analytics Project](https://gitlab.com/gitlab-data/analytics/issues/new?issue%5Bassignee_id%5D=&issue%5Bmilestone_id%5D=)
- [Data team page of Handbook](https://about.gitlab.com/handbook/business-ops/data-team/)
- [dbt Docs](https://docs.getdbt.com)
- [dbt Discourse](http://discourse.getdbt.com)
- [GitLab's dbt Documentation](https://dbt.gitlabdata.com)
- [Data Team GitLab Activity](https://gitlab.com/groups/gitlab-data/-/activity)
- [Data Kitchen Data Ops Cookbook](https://drive.google.com/file/d/14KyYdFB-DOeD0y2rNyb2SqjXKygo10lg/view?usp=sharing) 
- [Data Engineering Cookbook](https://drive.google.com/file/d/1Tm3GiV3P6c5S3mhfF9bm7VaKCtio-9hm/view?usp=sharing) 
        


