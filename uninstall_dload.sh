#!/bin/sh
##############################################################################
# Script to execute for data-loader uninstall#                               #
#                                                                            #
##############################################################################
# Parameters :-                                                              #
#      $1 - Message Output File Name eg. log                                 #
#      $2 - HOU password                                                     #
#      $3 - FSC Password                                                     #
#      $4 - SYS Password                                                     #
# Run from the command line with:                                            #
# sh uninstall_dload.sh name_of_log.txt hou_password fscpw syspw             #
##############################################################################
#The following subroutine are used in this script                            #
##############################################################################
# get_date_time ()      -   Get current date and time                        #
# write_sum_file ()     -   Write text to the DLOAD_FILE                     #
# write_sum_file_all () -   Write text to the DLOAD_FILE_ALL                 #
# get_db ()             -   Write database name to the header                #
# write_rep_hddr ()     -   Write text to the Header                         #
# write_rep_trail ()    -   Write text to the Trailer                        #
# check_for_errors ()   -   Checks script run for errors                     #
# run_script ()         -   Runs the install script                          #
# copy_script ()        -   Copies the executable script                     #
# copy_all_script ()    -   Copies all the executable scripts                #
# create_dload_dir ()   -   Create dload directory                           #
# empty_dload_dir ()    -   Removes contents from dload                      #
# grant_dba ()          -   Grants dba role to HOU and FSC for installs      #
# revoke_dba ()         -   Revokes dba role to HOU and FSC for installs     #
# try_logon ()          -   Connects to database as user test password       #
##############################################################################
##############################################################################
# Check user parameters
# Error if less than four parameters
if [ $# -lt 4 ]
then
  echo "Error: ($0)"
  echo "Incorrect number of Parameters."
  echo "Usage: "`basename $0` "logfile houpw fscpw syspw"
  echo " "
  exit 1
fi

##############################################################################
#Assign Internal Variables                                                   #
##############################################################################
log="$1"
HOU_PW=HOU/"$2"
FSC_PW="FSC"/"$3"
SYS_PW="SYS"/"$4"
rcode=0
LOGFILE_HOME=$PWD
INSTALL_HOME=${LOGFILE_HOME}/DataLoadAreas
DLOAD_FILE=${LOGFILE_HOME}/$log
DLOAD_FILE_ALL=${LOGFILE_HOME}/all_${log}
##############################################################################
#OK so now setup the subroutines                                             #
##############################################################################
#
#get the current Date and Time
#
get_date_time ()
{
DTE=`date +%d-%b-%Y`
TME=`date +%H:%M:%S`
}
#
#Write text into the DLOAD_FILE
#
write_sum_file ()
{
echo $*    >> ${DLOAD_FILE}
echo ""    >> ${DLOAD_FILE}
}
#
#Write text into the DLOAD_FILE_ALL
#
write_sum_file_all ()
{
echo ""    >> ${DLOAD_FILE_ALL}
echo $*    >> ${DLOAD_FILE_ALL}
}
#
#Get Database name
#
get_db ()
{
DB_NAME=`sqlplus -s $HOU_PW << SQLEND1
set feedback off pages 0 echo off pause off term off
SELECT ora_database_name
FROM DUAL;
SQLEND1`
}
#
#
#Write out header information to Summary file
#
write_rep_hddr ()
{
echo ""    > ${DLOAD_FILE}
echo ""    > ${DLOAD_FILE_ALL}
get_date_time
get_db
write_sum_file "Install - Started at "$TME" on "$DTE" on "$DB_NAME
}
#
#Write out trailer information to Summary file
#
write_rep_trail ()
{
get_date_time
if [ $rcode -eq 0 ]
then
   write_sum_file "SUCCESS - Install - Ended at "$TME" on "$DTE
   echo "SUCCESS - Install - Ended at "$TME" on "$DTE
else
   write_sum_file "FAILED - Install - Ended at "$TME" on "$DTE
   echo "FAILED - Install - Ended at "$TME" on "$DTE
fi
}
#
#Check script for errors
#
check_for_errors ()
{
if [ $rcode -eq 0 ]
then
   get_date_time
   write_sum_file "Checking log "$RUN_SCRIPT" at "$TME" on "$DTE
#   grep ORA- $RUN_LOG|grep -v 00942|grep -v 01418|grep -v 02429|grep -v 2289|grep -v 12003|grep -v 12002|grep -v 25176|grep -v 14451|grep -v 25191|grep -v 01408|grep -v 01432|grep -v 04080
   grep ORA- $RUN_LOG|grep -v 00955|grep -v 01432|grep -v 01418|grep -v 01430|grep -v 00942|grep -v 00001
# David Bessell add in the following rerunning install |grep -v 01727
#   grep ORA- $RUN_LOG|grep -v 00955|grep -v 01432|grep -v 01418|grep -v 01430|grep -v 00942|grep -v 00001|grep -v 01727

   if [ "$?" = "0" ]
   then
      SUBJECT=${RUN_SCRIPT}" FAILED Oracle Errors"
      rcode=1
   else
      grep SP2- $RUN_LOG
      if [ "$?" = "0" ]
      then
         SUBJECT=${RUN_SCRIPT}" FAILED System Errors"
         rcode=1
      else
#      uuencode $DLOAD_FILE $DLOAD_FILE | mailx -s "${SUBJECT}" David.Bessell@Northgate-is.com
         SUBJECT=${RUN_SCRIPT}" SUCCESS"
      fi
   fi
   write_sum_file "Log "${SUBJECT}" at "$TME" on "$DTE
#   uuencode $RUN_LOG $RUN_LOG | mailx -s "${SUBJECT}" David.Bessell@Northgate-is.com
fi
}
#
#Run specified script
#
run_script ()
{
if [ $rcode -eq 0 ]
then
   RUN_DIR=${1}
   if [ ! -d ${x} ]
   then
      write_sum_file ${RUN_DIR}" does not exist cannot continue "$PROD_HOME
      rcode=1
   else
      cd ${RUN_DIR}
   fi
   RUN_SCRIPT=$2
   RUN_LOG=${INSTALL_HOME}/${RUN_SCRIPT}.txt
   get_date_time
   write_sum_file "Running script "$RUN_SCRIPT" at "$TME" on "$DTE
   if [ ! -f ${RUN_DIR}/${RUN_SCRIPT} ]
   then
      write_sum_file "Cannot find script "${RUN_DIR}/${RUN_SCRIPT}
      rcode=1
   else 
`sqlplus -s $HOU_PW >${RUN_LOG} << SQLEND1
   DEFINE table_tablespace='TABLES'
   DEFINE table_space='TABLES'
   DEFINE index_tablespace='INDEXES'
   start ${RUN_DIR}/${RUN_SCRIPT}
SQLEND1`
      check_for_errors
      write_sum_file_all "$RUN_SCRIPT"
      cat $RUN_LOG >> $DLOAD_FILE_ALL
      if [ "$RUN_SCRIPT" = "sel_invalid.sql" ]
      then
         cat $RUN_LOG
      fi
      rm -f $RUN_LOG
      LST=`echo ${RUN_SCRIPT} | cut -f1 -d'.'`
      if [ -f ${LST}.lst ]
      then
         rm -f ${LST}.lst
      fi
   fi
fi
}

#
#Run specified script (as sysdba)
#
run_script_dba ()
{
if [ $rcode -eq 0 ]
then
   RUN_DIR=${1}
   RUN_SCRIPT=$2
   MODULE=$3
   RUN_LOG=${INSTALL_HOME}/${RUN_SCRIPT}.txt
   
   get_date_time
   write_sum_file "Running script "$RUN_SCRIPT" at "$TME" on "$DTE
   if [ ! -f ${RUN_DIR}/${RUN_SCRIPT} ]
   then
      write_sum_file "Cannot find script "${RUN_DIR}/${RUN_SCRIPT}
      rcode=1
   else
`sqlplus -s $HOU_PW >${RUN_LOG} << SQLEND1
   conn $SYS_PW as sysdba
   DEFINE Module='$MODULE'
   DEFINE indextablespace='INDEXES'
   DEFINE data_tablespace='TABLES'
   DEFINE index_tablespace='INDEXES'
   DEFINE DBNAME='$DB_NAME'
--PROMPT Typical value for machine DEV=172.28.2.82,TST=172.28.2.82,PRD=172.28.2.82
--Axway is single instance serving all configured environments.
   DEFINE connect_to_machine=${DB_NAME} == 'HOUINT' ? '172.28.2.82' : (${DB_NAME} == 'HOUTEST1' ? '172.28.2.82' : '172.28.2.82')
--PROMPT Typical value for port DEV=http:23100 https:23101, TST=http:23150 https:23151, PRD=http:23250 https:23251
   DEFINE connect_to_port=${DB_NAME} == 'HOUINT' ? '23101' : (${DB_NAME} == 'HOUTEST' ? '23151' : '23251')
   start ${RUN_DIR}/${RUN_SCRIPT}
SQLEND1`
      check_for_errors
      write_sum_file_all "$RUN_SCRIPT"
      cat $RUN_LOG >> $DLOAD_FILE_ALL
      rm -f $RUN_LOG
      LST=`echo ${RUN_SCRIPT} | cut -f1 -d'.'`
      if [ -f ${LST}.lst ]
      then
         rm -f ${LST}.lst
      fi
   fi
fi
}

#
#Run specified fsc script
#
run_fsc_script ()
{
if [ $rcode -eq 0 ]
then
   RUN_DIR=${1}
   RUN_SCRIPT=$2
   RUN_LOG=${INSTALL_HOME}/${RUN_SCRIPT}.txt
   get_date_time
   write_sum_file "Running script "$RUN_SCRIPT" at "$TME" on "$DTE
   if [ ! -f ${RUN_DIR}/${RUN_SCRIPT} ]
   then
      write_sum_file "Cannot find script "${RUN_DIR}/${RUN_SCRIPT}
      rcode=1
   else 
`sqlplus -s $FSC_PW >${RUN_LOG} << SQLEND1
   start ${RUN_DIR}/${RUN_SCRIPT}
SQLEND1`
      check_for_errors
      write_sum_file_all "$RUN_SCRIPT"
      cat $RUN_LOG >> $DLOAD_FILE_ALL
      rm -f $RUN_LOG
      LST=`echo ${RUN_SCRIPT} | cut -f1 -d'.'`
      if [ -f ${LST}.lst ]
      then
         rm -f ${LST}.lst
      fi
   fi
fi
}

#
#Grant dba to hou and fsc
#
grant_dba ()
{
if [ $rcode -eq 0 ]
then
   RUN_SCRIPT="grant_dba"
   RUN_LOG=${INSTALL_HOME}/${RUN_SCRIPT}.txt
   get_date_time
   write_sum_file "Granting DBA to HOU and FSC at "$TME" on "$DTE
`sqlplus -s $HOU_PW >${RUN_LOG} << SQLEND1
   conn $SYS_PW as sysdba
   grant dba to hou with admin option;
   grant dba to fsc with admin option;
SQLEND1`
   check_for_errors
   cat $RUN_LOG >> $DLOAD_FILE_ALL
   rm -f $RUN_LOG
fi
}
#
#Create dload directory specified script
#
create_dload_dir ()
{
if [ $rcode -eq 0 ]
then
   if [ ! -d $PROD_HOME ]
   then
      write_sum_file "$PROD_HOME does not exist cannot continue "$PROD_HOME
      rcode=1
   else
      if [ ! -d $PROD_HOME/dload ]
      then
         write_sum_file "Creating directory "$PROD_HOME/dload
         cd $PROD_HOME
         mkdir dload
      fi
   fi
fi
}
#
#Empty dload directory
#
empty_dload_dir ()
{
if [ $rcode -eq 0 ]
then
   if [ ! -d $PROD_HOME/dload ]
   then
      write_sum_file "$PROD_HOME/dload does not exist cannot continue "
   else
      rm -f $PROD_HOME/dload/*
   fi
fi
}

#
#Revoke dba to hou and fsc
#
revoke_dba ()
{
if [ $rcode -eq 0 ]
then
   RUN_SCRIPT="revoke_dba"
   RUN_LOG=${INSTALL_HOME}/${RUN_SCRIPT}.txt
   get_date_time
   write_sum_file "Revoking DBA from HOU and FSC at "$TME" on "$DTE
`sqlplus -s $HOU_PW >${RUN_LOG} << SQLEND1
   conn $SYS_PW as sysdba
   revoke dba from hou;
   revoke dba from fsc;
SQLEND1`
   check_for_errors
   cat $RUN_LOG >> $DLOAD_FILE_ALL
   rm -f $RUN_LOG
fi
}
#
#try logon
#
try_logon ()
{
TEST_USER=$1
DB_STATUS=`sqlplus -s /nolog <<!
set sqlprompt ""
set heading off pagesize 0 feedback off verify off
conn $TEST_USER
SELECT 'ACTIVE' FROM DUAL;
!`

if [ `echo $DB_STATUS |grep -ci "ORA-01017: invalid username/password"` != 0 ]; then
   echo ""
   echo "   Invalid "$TEST_USER" Password."
   echo ""
   INVALID_PWD="Y"
   return
fi
}

case ${ORACLE_SID} in
 HOUSETUP) echo DO NOT RUN AGAINST HOUSETUP
 exit 1
esac
#
INVALID_PWD="N"
try_logon $HOU_PW
try_logon $FSC_PW
try_logon $SYS_PW
if [ ${INVALID_PWD} = "Y" ]
then
   exit
fi
#
write_rep_hddr; #Write out the report header
create_dload_dir; #Creates $PROD_HOME/dload directory
empty_dload_dir; #Removes all from $PROD_HOME/dload directory
# Grant DBA
grant_dba;
run_script     ${LOGFILE_HOME}/scripts   drop_dl.sql
run_script     ${LOGFILE_HOME}/scripts   enable_pel_triggers.sql
run_script     ${LOGFILE_HOME}/scripts   enable_aue_triggers.sql
run_script     ${LOGFILE_HOME}/scripts   advance_hou_sequences.sql
run_fsc_script ${LOGFILE_HOME}/scripts   advance_fsc_sequences.sql
run_script     ${LOGFILE_HOME}/scripts   enable_ban_triggers.sql
run_script     ${LOGFILE_HOME}/scripts   enable_triggers.sql
run_script_dba ${LOGFILE_HOME}/scripts   drop_gather_stats.sql
run_script     ${LOGFILE_HOME}/scripts   ins_bru.sql
