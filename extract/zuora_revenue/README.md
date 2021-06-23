### Zuora Revenue Extract 
Below is the information around the extraction of Zuora Revenue extraction pipeline. 

### Setup the environment in Compute engine 
Do SSH to the zuora compute engine using your service account.
Below is the server details in GCP 
https://console.cloud.google.com/compute/instancesDetail/zones/us-west1-a/instances/zuora-revenue-extract-server?project=gitlab-analysis&rif_reserved

 ssh -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no -i $HOME/.ssh/google_compute_engine -A -p 22  <username>@<external_ip>


From separate terminal go to ` ~/repos/analytics/extract/zuora_revenue/src` directory and run below command to upload whole directory to compute engine. This directory contains the code for the extraction process.

`gcloud compute scp --recurse src --zone "us-west1-a" zuora-revenue-extract-server:/home/vedprakash/zuora_revenue`

This will upload the src folder from your branch your local to the compute engine branch. 

### Post connection and upload of file. 
`Step 1 `: Create a virtual environment inside the compute engine named --- `zuora-revenue-extract-venv` 

Keep the same name to keep the changes minimal. 

`python3 -m venv zuora-revenue-extract-venv`

`Step 2`: Activate the venv

`source /home/vedprakash/zuora-revenue-extract-venv/bin/activate` 

`Step 3`: Post that upgrade the pip

`pip install --upgrade pip`

`Step 4`: Go to src folder and install all the required package. 

`pip install -r requirements.txt`

`Notes:`  Step 1 to Step 4 is required only when the environment is crashed and we have got to build it from start not required for general operations. 

### Below Steps is required if we have accidentally deleted the GCS bucket folder, then we need to do below steps for each table. Also if there is requirement to add new table into the system then also we below steps can be used.


`Step 5`: Create the start_date_<table_name>.csv file which holds   table_name,load_date information.
For example for table BI3_MJE the file name will be 
 `start_date_BI3_MJE.csv` and file content will be below.

table_name,load_date   
BI3_ACCT_TYPE,                   
load_date for new table should be left blank because it will start to download the file from start. For other we can pick up the last load date from airflow log. 
For the current table below is the list of command to create the file, this can be done from local or from compute engine as well. 

```
echo "table_name,load_date
BI3_ACCT_TYPE," > start_date_BI3_ACCT_TYPE.csv
echo "table_name,load_date
BI3_APPR_DTL," > start_date_BI3_APPR_DTL.csv
echo "table_name,load_date
BI3_CALENDAR," > start_date_BI3_CALENDAR.csv
echo "table_name,load_date
BI3_MJE," > start_date_BI3_MJE.csv
echo "table_name,load_date
BI3_RC_BILL," > start_date_BI3_RC_BILL.csv
echo "table_name,load_date
BI3_RC_HEAD," > start_date_BI3_RC_HEAD.csv
echo "table_name,load_date
BI3_RC_HOLD," > start_date_BI3_RC_HOLD.csv
echo "table_name,load_date
BI3_RC_LNS," > start_date_BI3_RC_LNS.csv
echo "table_name,load_date
BI3_RC_POB," > start_date_BI3_RC_POB.csv
echo "table_name,load_date
BI3_RC_SCHD," > start_date_BI3_RC_SCHD.csv
echo "table_name,load_date
BI3_RC_SCHD_DEL," > start_date_BI3_RC_SCHD_DEL.csv
echo "table_name,load_date
BI3_RI_ACCT_SUMM," > start_date_BI3_RI_ACCT_SUMM.csv
```

This command create the file for each table and then putting required column name and value. 
The load_date is set to null because it will be treated as first run.   
`Note:` If we know the load date then place in `2016-07-26T00:00:00`  format `%Y-%m-%dT%H:%M:%S` for the particular table. 

`Step6`: Now we need to upload the file in staging area. Below is the set of command for upload each file to respective table in staging area. 

```
gsutil cp start_date_BI3_MJE.csv             gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_MJE/
gsutil cp start_date_BI3_ACCT_TYPE.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_ACCT_TYPE/
gsutil cp start_date_BI3_APPR_DTL.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_APPR_DTL/
gsutil cp start_date_BI3_CALENDAR.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_CALENDAR/
gsutil cp start_date_BI3_RC_BILL.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_RC_BILL/
gsutil cp start_date_BI3_RC_HEAD.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_RC_HEAD/
gsutil cp start_date_BI3_RC_HOLD.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_RC_HOLD/
gsutil cp start_date_BI3_RC_LNS.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_RC_LNS/
gsutil cp start_date_BI3_RC_POB.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_RC_POB/
gsutil cp start_date_BI3_RC_SCHD.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_RC_SCHD/
gsutil cp start_date_BI3_RC_SCHD_DEL.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_RC_SCHD_DEL/
gsutil cp start_date_BI3_RI_ACCT_SUMM.csv  gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_RI_ACCT_SUMM/
```

`Step 7`: To run the extract below variable needs to be declared in the .bash_profile file of the server.   
```     
    export zuora_bucket=""
    export zuora_dns=""
    export authorization_code=""
    export python_venv="source /home/vedprakash/zuora-revenue-extract-venv/bin/activate" #From step 2
    export zuora_extract_log="/home/vedprakash/zuora_revenue/src/logs/"
    export zuora_src="/home/vedprakash/zuora_revenue/src" #The path of source code
```
`Note:` The credentials is present in 1 password under `zuora_revenue_prod`.

`Step 8`: The last step is to do the schedule.Add below command to crontab. 
Once edited and ready add the required command to crontab of that machine. 
The current schedule is set to run at 02:00 AM UTC every day. 
```
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_ACCT_TYPE     -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_APPR_DTL      -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_CALENDAR      -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_MJE           -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_RC_BILL       -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_RC_HEAD       -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_RC_HOLD       -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_RC_LNS        -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_RC_POB        -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_RC_SCHD       -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_RC_SCHD_DEL   -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
00 02 * * * . $HOME/.bash_profile;$python_venv && cd $zuora_src && python3 extract_zuora_revenue.py -table_name BI3_RI_ACCT_SUMM  -bucket_name $zuora_bucket -api_dns_name $zuora_dns -api_auth_code "$authorization_code" &>/tmp/mycommand.log
```

At the end of the process below will be output. 
1) A success log file is present named `<table_name>_DD-MM-YYYY.log`  upload to path zuora_revpro_gitlab/RAW_DB/staging/<table_name>/<table_name>_DD-MM-YYYY.log.log. For example for table BI3_MJE the log file for the day will be named `BI3_MJE_21-06-2021.log` and it will be uploaded to the path `gs://zuora_revpro_gitlab/RAW_DB/staging/BI3_MJE/BI3_MJE_21-06-2021.log`
2) Any file for the date range wil be present in the GCS bucket. 
 

### For Derived table 
Zuora have provided view definition for the derived view.As extracting data from the derived view is not feasible in production. Hence for table BI3_WF_SUMM we prepare the data in the DBT model in PREP layer with the DDL provided from Zuora. 

```
Prompt View RPRO_BI3_WF_SUMM_V;
CREATE OR REPLACE VIEW RPRO_BI3_WF_SUMM_V
(AS_OF_PRD_ID, SCHD_ID, LINE_ID, ROOT_LINE_ID, PRD_ID, 
 POST_PRD_ID, SEC_ATR_VAL, BOOK_ID, CLIENT_ID, ACCTG_SEG, 
 ACCTG_TYPE_ID, NETTING_ENTRY_FLAG, SCHD_TYPE_FLAG, T_AT, F_AT, 
 R_AT, CRTD_PRD_ID, CRTD_DT, CRTD_BY, UPDT_DT, 
 UPDT_BY, INCR_UPDT_DT)
AS 
SELECT
         /***************************************************************************************************
         * Name        : RPRO_BI3_WF_SUMM_V                                                               *
         * Author      : LEEYO                                                                             *
         * Date        : 16-JUN-2016                                                                       *
         * Description :                                                                                   *
         * #Version: 1
         * #Tag: 3210 
         * #Last Modified by:
         * #RnStg      : vw+10000 
         * $Header: /BI Views/RP3/rpro_bi3_wf_summ_v.sql 1 2016-06-24 21:45:40 yik.sim $                                                                                      *
         * Modifications History:                                                                          *
         *                                                                                                 *
         * Modified By     Date         Version  Description                                               *
         * --------------  -----------  -------  ----------------------------------------------------------*
         * LEEYO           16-JUN-2016     1     Initial Version                                           *
         ***************************************************************************************************/
        /***************************************************************************************************
         * Copyright - 2009 Leeyo Software. All rights reserved                                            *
         *                                                                                                 *
         * Leeyo REVPRO product is an application modified and extended for clients, based on the REVPRO   *
         * owned by Leeyo Software.                                                                        *
         * All related documentation, code, methods and other materials concerning REVPRO are the property *
         * of Leeyo Software and may not be reused, copied, or transmitted in material or electronic form  *
         * without the express written consent of Leeyo Software.                                          *
        ***************************************************************************************************/
       C.ID AS AS_OF_PRD_ID,
       S.SCHD_ID as SCHD_ID,
       S.LINE_ID,
       S.ROOT_LINE_ID,
       S.PRD_ID,
       S.POST_PRD_ID,
       S.SEC_ATR_VAL,
       S.BOOK_ID,
       S.CLIENT_ID,
       S.ACCTG_SEG,
       S.ACCTG_TYPE as ACCTG_TYPE_ID,
       S.NETTING_ENTRY_FLAG,
       S.SCHD_TYPE_FLAG,
       S.AMOUNT AS T_AT,
       S.AMOUNT * S.F_EX_RATE AS F_AT,
       (S.AMOUNT * S.F_EX_RATE) * S.G_EX_RATE AS R_AT,
       S.CRTD_PRD_ID,
       S.CRTD_DT,
       S.CRTD_BY,
       S.UPDT_DT,
       S.UPDT_BY,
       S.UPDT_DT as INCR_UPDT_DT
  FROM RPRO_BI3_RC_SCHD_V S,
       RPRO_BI3_CALENDAR_V C,
       RPRO_BI3_ACCT_TYPE_V A
 WHERE CRTD_PRD_ID <= C.ID AND PRD_ID >= C.ID
   AND S.ACCTG_TYPE = A.ID
   AND A.WATERFALL_FLAG = 'Y';
```