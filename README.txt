When a new set of code is received from ISG, ensure the appropriate scripts are placed into the correct directories.

Under each Module, create a new directory with todays date ie 20150806 and copy in the previous set of scripts for that module

For example cp -p 20150801/* 20150806

Once the new directory is a replica of the old directory, add in the new scripts received.

Compare the scripts received with what is in our original directory to ensure we are receiving a later version of the code.
The only reason we should be receiving new code is because of a new release or a fix to the previous release from the UK.

Once we are happy with the new set of scripts, amend install_dload.sh with the new directory created.

Then run the install.

Lock all users except NPS_SUPPORT
start /app/first/dataload/scripts/upd_NPS_users.sql

****************************************
*DO NOT run installer against HOUSETUP.*
****************************************

cd /app/first/dataload

sh install_dload.sh hou/hou install_output.txt

Ensure all is recompiled and nothing is invalid. As the HOU user

start /app/first/dataload/scripts/recompile_all.sql

Before running the migration, ensure the database is not in archive log mode.

sqlplus sys as sysdba
archive log list;

If this returns archive log mode
Database log mode              Archive Mode

The need to shut down database and disable archive mode

shutdown immediate
startup mount
alter database noarchivelog;
alter database open;

stop and start the GPI as the Oracle user

dbsctl stop HOUMIG1
dbsctl start HOUMIG1
ps -ef | grep -i gpi | grep -i HOUMIG1
sqlplus fsc/fsc
set pages 200 lines 200
select username,module
from v$session
where module like '%MSG%'
order by 2;

dbsctl start HOUMIG1
 
ps -ef | grep -i gpi | grep -i HOUMIG1
 
should be 1 DBS per instance and 1 BSS if not, try stopping and restarting
 
IF NOT started
   dbsctl stop HOUTEST1
   dbsctl start HOUTEST1
END IF
 
sqlplus fsc
 
should be 2 x 4 as below if not shutdown and restart listeners
 
USERNAME                       MODULE
------------------------------ ----------------------------------------------------------------
FSC                            MSG Listen Process (dbms_job)
FSC                            MSG Listen Process (dbms_job)
FSC                            MSGPLSQL Listen Process (dbms_job)
FSC                            MSGPLSQL Listen Process (dbms_job)
FSC                            MSGPLSQL Listen Process (dbms_job)
FSC                            MSGPLSQL Listen Process (dbms_job)
 
6 rows selected.
 
IF not 6
   exec shutdown_q_listeners
 
   exec startup_q_listeners
   check to make sure correct number started
END IF
 
check the sizing is correct

SQL> show sga

Total System Global Area 6415597568 bytes
Fixed Size                  6787200 bytes
Variable Size            1308626816 bytes
Database Buffers         5033164800 bytes
Redo Buffers               67018752 bytes

PGA should be 6Gb

SQL> show parameter pga_aggregate_target
 
NAME                                 TYPE        VALUE
------------------------------------ ----------- ------------------------------
pga_aggregate_target                 big integer  6G
 
This should be 6Gb for a normal environment.

GATHER STATS

As the SYS user:

sqlplus sys as sysdba

start /app/first/dataload/scripts/gather_stats_dbms.sql

exit

This will install statspack as well and setup the necessary dbms jobs to run statspack and gather stats every 15 minutes.

check they have been installed as the HOU user

start /app/first/dataload/scripts/jobs.sql

column GRANTEE format a20
column OWNER format a20
column TABLE_NAME format a20
column GRANTOR format a20
column PRIVILEGE format a20
SELECT GRANTEE,OWNER,TABLE_NAME,GRANTOR,PRIVILEGE FROM dba_tab_privs WHERE table_name = 'USERS'
ORDER BY 1,2
/

/* Following not needed now as been applied to HOUSETUP
AS the FSC user remove access to the USERS table.
The reason for this is to prevent anyone logging onto the database viewing the users table.
The problem is, v5 login needs to read from the users table. The choice we have is to leave it and not
revoke all as below or revoke all but then grant to only those users we want to login to v5.
REVOKE ALL ON "USERS" FROM FSC_FULL;
REVOKE ALL ON "USERS" FROM FRB;
REVOKE ALL ON "USERS" FROM HOU;
#GRANT ALL ON "USERS" TO FSC_FULL;
#GRANT ALL ON "USERS" TO KATRINA_SMART;
#GRANT ALL ON "USERS" TO NATALIE_TRACEY;
*/

Reset any user preferences as the APEX_040200 user
start /app/first/dataload/scripts/reset_user_pref.sql

exit

Create new pel index for load and OCR and PSI batch programs as the HOU user
start /app/first/dataload/scripts/aue_pel_indexes.sql

Set the address usage type for PAR to be Free format as the HOU user
start /app/first/dataload/scripts/aut_par_ffa.sql

Set the date format to be as expected by fdl100.sh as the HOU user
start /app/first/dataload/scripts/pre_mig_dateformat.sql

Create dataload performance indexes as the HOU user and INDEXES when prompted
start /app/first/dataload/scripts/cr_indexes.sql

Install the migration teams views as the HOU user
start /export/home/first/dbessell/ro_users/Reside_DL_Views.sql

Install the bespoke users view as the HOU user
start /export/home/first/dbessell/ro_users/cr_users_view.sql

Create the migration job role as the HOU user
start /export/home/first/dbessell/ro_users/ro_reside_mig.sql

As the HOUDBA user:
Create the migration user 
start /export/home/first/dbessell/ro_users/ro_reside_mig_user.sql

-- If environment is HOUMIG2
Create the reside read only job role same as the reporting environment
As the houdba user:
start /export/home/first/dbessell/ro_users/ro_reside.sql

Create the reside read only user
start /export/home/first/dbessell/ro_users/ro_reside_user.sql

exit

cd /app/first/dataload/scripts

Disable triggers pel and aue triggers for performance as the HOU user
start /app/first/dataload/scripts/pel_triggers.sql
start /app/first/dataload/scripts/aue_triggers.sql
set feedback on termout on
start /app/first/dataload/scripts/disable_pel_triggers.sql
start /app/first/dataload/scripts/disable_aue_triggers.sql

Install INF408 -- check in install_all.sh that int2311 latest is the same for generic, int2311 and frv
cd /export/home/first/install
sh install_inf408.sh hou/password HOUMIG1_inf408.txt

Update the index page with the date of the clone for the mig environment

vi /app/first/iworld/html/index.html
vi /app/first/iworld/html/qld.html

Copy in Reside images following guide as below for the migration environment
vi /export/home/first/dbessell/theme/install_azure.txt -- 6.14
vi /export/home/first/dbessell/theme/install_wideblue.txt -- 6.15
vi /export/home/first/dbessell/theme/install_wideblue_new_logo.txt -- 6.15

Unlock the necessary users as the HOU user

start /app/first/dataload/scripts/upd_MIG_users.sql

MIGRATION FINISHED AND CLONED INTO HOUTEST1

Lock all users except NPS_SUPPORT
start /app/first/dataload/scripts/lock_all_users.sql

stop oeg if HOUTEST1, HOUINT
As the oracle user
cd /opt/bin
stop_oeg.sh HOUTEST1

As the SYS user:

sqlplus sys as sysdba

start /app/first/dataload/scripts/drop_gather_stats.sql

exit

Drop Dataload objects as the HOU user
start /app/first/dataload/scripts/drop_dl.sql

Reset any user preferences as the APEX_040200 user
start /app/first/dataload/scripts/reset_user_pref.sql

commit;

exit

Set the address usage type for PAR to be Structured as the HOU user
start /app/first/dataload/scripts/aut_par_sia.sql

Set the date format to be as required by the business as the HOU user
start /app/first/dataload/scripts/post_mig_dateformat.sql

Enable the  triggers pel and aue triggers
start /app/first/dataload/scripts/enable_pel_triggers.sql
start /app/first/dataload/scripts/enable_aue_triggers.sql

Drop dataload performance indexes as the HOU user
start /app/first/dataload/scripts/drop_indexes.sql

Reset any sequences as the HOU user
start /app/first/dataload/scripts/advance_hou_sequences.sql

Enable ban triggers disabled by the migration as the HOU user
start /app/first/dataload/scripts/enable_ban_triggers.sql

Drop the migration user and objects on any cloned environment as the HOU user
start /export/home/first/dbessell/ro_users/drop_mig_objects.sql

Reset any sequences as the FSC user
start /app/first/dataload/scripts/advance_fsc_sequences.sql

Create the reside read only job role on the Reporting environment HOUREPRT (temp HOUTEST1)
As the houdba user:

sqlplus houdba@(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = 10.65.5.2)(PORT = 1521))) (CONNECT_DATA = (SERVICE_NAME = HOUREPRT)))
start /export/home/first/dbessell/ro_users/ro_reside.sql

Create the reside read only user on the Reporting environment HOUREPRT
start /export/home/first/dbessell/ro_users/ro_reside_user.sql

exit

-- Install enterprise items. For example HOUTEST1
-- First stop the sftp process
-- Logon as the Oracle user
crontab -e
Comment out the running of the Interface file transfers
:x to save the file
cd /export/home/oracle/logs
rm * -- may need to mess around if the number of files to be deleted is too large.
For example rm *ToR* and then rm *

Move back to the first UNIX user session
install enterprise items see ~first/install/install_all.sh. Take a copy and amend variables for HOUTEST1. DBNAME, connect_to_port, soa_machine
sh install_all_HOUTEST1_clonenn.sh hou/password HOUTEST1_20161025.txt

-- this will take about an hour for int2302, int2310, int2312 to complete.
--
-- At this point you can continue with the rest of the instructions and allow Carrie to make a 
-- start. However when int2302, int2310, int2312 has completed, come back to this point.
--
-- Check in the GPI that the job completed successfully.
-- Once completed go to $PROD_REPORTS /spp/spool/HOUTEST1/hou_output 
--
-- int2302
ls -lrt int2302*SAPRE.txt
rm int2302*SAPRE.txt
-- int2310
ls -lrt *Assetcreate.csv *Assetretire.csv *Assetchange.csv int2310*CREATE.csv int2310*RETIRED.csv int2310*CHANGED.csv
rm *Assetcreate.csv *Assetretire.csv *Assetchange.csv int2310*CREATE.csv int2310*RETIRED.csv int2310*CHANGED.csv
-- int2312
ls -lrt 2303Reside_ION_*.csv int2312*INTCLSD.csv int2312*INTORD.csv
rm 2303Reside_ION_*.csv int2312*INTCLSD.csv int2312*INTORD.csv

Gather stats as the HOU user

execute dbms_stats.gather_schema_stats(OWNNAME=>'HOU',OPTIONS=>'GATHER STALE',CASCADE=>TRUE,NO_INVALIDATE=>FALSE);

exit

Gather stats as the FSC user
execute dbms_stats.gather_schema_stats(OWNNAME=>'FSC',OPTIONS=>'GATHER STALE',CASCADE=>TRUE,NO_INVALIDATE=>FALSE);

exit

-- Logon as the Oracle user
crontab -e
Un comment out the running of the Interface file transfers for database being made ready
:x to save the file

-- Whilst int2302 is running carry on from this point.

Run Buisness Objects admin unit table create 

sh $PROD_HOME/bin/fqvpgr.sh hou/xxxx aa

Ensure all is recompiled and nothing is invalid. As the HOU user

start /app/first/dataload/scripts/recompile_all.sql

-- If environment is HOUTEST1
Ask Sydney DBA to start DataGuard

Install Data Warehouse Views as the HOU user.
start /app/first/dataload/scripts/DatawarehouseViews.sql

-- Install Social Media as HOU user -- 6.15 functionality.

start /app/first/dataload/scripts/ins_social_media.sql

Copy in Reside images following guide as below for the migration environment
vi /export/home/first/dbessell/theme/install_azure.txt -- 6.14
vi /export/home/first/dbessell/theme/install_wideblue.txt -- 6.15
vi /export/home/first/dbessell/theme/install_wideblue_new_logo.txt -- 6.15

Update the index page with the date of the clone

vi /app/first/iworld/html/index.html
vi /app/first/iworld/html/qld.html

Unlock the necessary users as the HOU user
start /app/first/dataload/scripts/upd_NPS_users.sql

-- End of setup for HOUTEST1, pass to Carrie for producing report.

-- After Carrie has done her report.

Reset any user preferences as the APEX_040200 user
start /app/first/dataload/scripts/reset_user_pref.sql

exit

-- Suggest updating all user accounts n HOUTEST1 to be non current to prevent unwanted access to environments until released
Lock all users except NPS_SUPPORT
start /app/first/dataload/scripts/lock_all_users.sql

-- Ask Sydney DBA to clone HOUTEST1 to other environments.
--
-- Once done

-- Install enterprise items if HOUINT
install enterprise items see ~first/install/install_all.sh. Take a copy and amend variables for HOUINT.
sh install_all_HOUINT_clonenn.sh hou/password HOUINT_yyyymmdd.txt

start oeg if HOUTEST1, HOUINT
As the oracle user
cd /opt/bin
start_oeg.sh HOUTEST1

-- HOUINT and HOUCONF2 NPS only initially
Unlock the necessary users as the HOU user for the environment being handed over
start /app/first/dataload/scripts/upd_{DBNAME}_clone{nn}_users.sql
or NPS
start /app/first/dataload/scripts/upd_NPS_users.sql

--IF ENV CLONED INTO IS NOT HOUTEST1

--Drop the reside user and objects on any cloned environment.
As the houdba user:

start /export/home/first/dbessell/ro_users/drop_reside_users.sql

exit

Copy in Reside images following guide as below for the migration environment
vi /export/home/first/dbessell/theme/install_azure.txt -- 6.14
vi /export/home/first/dbessell/theme/install_wideblue.txt -- 6.15
vi /export/home/first/dbessell/theme/install_wideblue_new_logo.txt -- 6.15

Update the index page with the date of the clone

vi /app/first/iworld/html/index.html
vi /app/first/iworld/html/qld.html

--For cutover to Production, remove the licence for Create User OBT.
--/export/home/first/install/release/lic/20161025/del_lic_config.sql
