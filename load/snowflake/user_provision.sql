-- RUN THE FOLLOWING BEFORE THE SCRIPT (with correct values) using the SECURITYADMIN role
-- ====================
-- set email = 'email@gitlab.com';
-- set firstname  = 'Sasha';
-- set lastname = 'Ulyanov';
-- ====================
use role securityadmin;

use warehouse ADMIN;

set username = (select upper(LEFT($email, CHARINDEX('@', $email) - 1)));

set loginname = (select lower($email));

CREATE USER identifier($username) 
LOGIN_NAME = $loginname
DISPLAY_NAME = $username 
FIRST_NAME = $firstname
LAST_NAME = $lastname 
EMAIL = $email;

CREATE ROLE identifier($username) ;
GRANT ROLE identifier($username) TO ROLE "SYSADMIN";

GRANT ROLE identifier($username) to user identifier($username);

-- IF PASSWORD NEEDED (PROBABLY NOT NEEDED)
-- RUN THE FOLLOWING TO SET PASSWORD AND FORCE RESET (with randomly generated values) https://passwordsgenerator.net
-- ====================
-- ALTER USER identifier($username) SET PASSWORD ='randomGeneratedPassword' MUST_CHANGE_PASSWORD = TRUE;

-- IF GOING TO BE A DBT USER, run this to create the development databases

set prod_db = (select $username || '_PROD');
set prep_db = (select $username || '_PREP');
set analytics_db = (select $username || '_ANALYTICS');

user role sysadmin;

CREATE DATABASE identifier($prod_db);
GRANT OWNERSHIP ON DATABASE identifier($prod_db) to role identifier($username);
GRANT ALL PRIVILEGES ON DATABASE identifier($prod_db) to role identifier($username);

CREATE DATABASE identifier($prep_db);
GRANT OWNERSHIP ON DATABASE identifier($prep_db) to role identifier($username);
GRANT ALL PRIVILEGES ON DATABASE identifier($prep_db) to role identifier($username);

CREATE DATABASE identifier($analytics_db);
GRANT OWNERSHIP ON DATABASE identifier($analytics_db) to role identifier($username);
GRANT ALL PRIVILEGES ON DATABASE identifier($analytics_db) to role identifier($username);
