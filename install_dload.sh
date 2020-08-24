#!/bin/sh
##############################################################################
# Script to install all dataload code                                        #
# This shell script will replicate the script gen_all_dl.sql which is        #
# supplied from the ISG team. The data load areas have been split up into    #
# separate folders to maintain greater visibility of what actual gets run    #
#                                                                            #
##############################################################################
# Parameters :-                                                              #
#      $1 - Message Output File Name eg. log                                 #
#      $2 - HOU password                                                     #                                                #
# Run from the command line with:                                            #
# sh install_dload.sh name_of_log.txt hou_password fscpw syspw               #
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
if [ $# -lt 2 ]
then
  echo "Error: ($0)"
  echo "Incorrect number of Parameters."
  echo "Usage: "`basename $0` "logfile houpw"
  echo " "
  exit 1
fi
##############################################################################
#Assign Internal Variables                                                   #
##############################################################################
log="$1"
HOU_PW=HOU/"$2"
#FSC_PW="FSC"/"$3"
#SYS_PW="SYS"/"$4"
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
#Copy specified script
#
copy_script ()
{
if [ $rcode -eq 0 ]
then
   COPY_DIR=${INSTALL_HOME}/${1}
   COPY_SCRIPT=$2
   COPY_DEST=$3
   write_sum_file "Copying script "$COPY_SCRIPT" to "$COPY_DEST
   if [ ! -f ${COPY_DIR}/${COPY_SCRIPT} ]
   then
      write_sum_file "Cannot find script "${COPY_DIR}/${COPY_SCRIPT}
      rcode=1
   else 
      cp -p ${COPY_DIR}/${COPY_SCRIPT} ${COPY_DEST}
      chmod 755 ${COPY_DEST}/${COPY_SCRIPT}
      dos2unix -q -437 ${COPY_DEST}/${COPY_SCRIPT} ${COPY_DEST}/${COPY_SCRIPT}
      NOEND="`tail -1c ${COPY_DEST}/${COPY_SCRIPT} | wc -l`"
      if [ ${NOEND} = "0" ]
      then
         write_sum_file "Added end line to "$COPY_SCRIPT
         echo "" >> ${COPY_DEST}/${COPY_SCRIPT}
      fi
   fi
fi
}
#
#Copy all specified script
#
copy_all_script ()
{
if [ $rcode -eq 0 ]
then
   COPY_DIR=${INSTALL_HOME}/${1}
   COPY_SCRIPT=$2
   COPY_DEST=$3
   set +f
   file_list=${COPY_DIR}/${COPY_SCRIPT}
   file_list=`eval echo $file_list`

   for FILE in $file_list
   do
      FILEP="`basename ${FILE}`"
      DIRPA="`dirname ${FILE}`"

      if [ ! -f ${FILE} ]
      then
         write_sum_file "Cannot find script "${FILE}
         rcode=1
      else
         write_sum_file "Copying script "${FILE}" to "$COPY_DEST
         cp -p ${FILE} ${COPY_DEST}
         chmod 755 ${COPY_DEST}/${FILEP}
         dos2unix -q -437 ${COPY_DEST}/${FILEP} ${COPY_DEST}/${FILEP}
         NOEND="`tail -1c ${COPY_DEST}/${FILEP} | wc -l`"
         if [ ${NOEND} = "0" ]
         then
            write_sum_file "Added end line to "$FILEP
            echo "" >> ${COPY_DEST}/${FILEP}
         fi
      fi
   done
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
#
##############################################################################
#End of subroutine section-Start of Main routine
##############################################################################
#
# Ensure this is not HOUSETUP
case ${ORACLE_SID} in
 HOUSETUP) echo DO NOT RUN AGAINST HOUSETUP
 exit 1
esac
#
INVALID_PWD="N"
try_logon $HOU_PW
#try_logon $FSC_PW
#try_logon $SYS_PW
if [ ${INVALID_PWD} = "Y" ]
then
   exit
fi
#
write_rep_hddr; #Write out the report header
# create_dload_dir; #Creates $PROD_HOME/dload directory
# empty_dload_dir; #Removes all from $PROD_HOME/dload directory
# Grant DBA
# grant_dba;
#
# Estates HEM
copy_all_script HEM       "*"               $PROD_HOME/dload
copy_all_script HEM       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   gen_hem_dl.sql
# Person Attributes
run_script      $PROD_HOME/dload   dl_hem_person_attributes_tab.sql
run_script      $PROD_HOME/dload   s_dl_hem_person_peo_attributes.pks
run_script      $PROD_HOME/dload   s_dl_hem_person_peo_attributes.pkb
run_script      $PROD_HOME/dload   s_dl_hem_person_peo_att_hists.pks
run_script      $PROD_HOME/dload   s_dl_hem_person_peo_att_hists.pkb
run_script      $PROD_HOME/dload   hem_person_attributes_dlas_in.sql
# Voids
run_script      $PROD_HOME/dload   gen_hem_voids_dl.sql
# Income
run_script      $PROD_HOME/dload   income_dl_install.sql
# Organisations
run_script      $PROD_HOME/dload   gen_hem_org_dl.sql
# Object Level Security
run_script      $PROD_HOME/dload   dl_hem_oau_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hem_object_admin_units.pks
run_script      $PROD_HOME/dload   s_dl_hem_object_admin_units.pkb
run_script      $PROD_HOME/dload   s_dl_hem_user_obj_admin_units.pks
run_script      $PROD_HOME/dload   s_dl_hem_user_obj_admin_units.pkb
run_script      $PROD_HOME/dload   hdl_hem_oau_dlas_in.sql
# Person Name History
run_script      $PROD_HOME/dload   dl_hem_person_name_history_tab.sql
run_script      $PROD_HOME/dload   s_dl_hem_person_name_history.pks
run_script      $PROD_HOME/dload   s_dl_hem_person_name_history.pkb
run_script      $PROD_HOME/dload   hem_person_name_history_dlas.sql
# Property Landlord Bank Details
run_script      $PROD_HOME/dload   dl_hem_plb_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hem_prop_landlord_banks.pks
run_script      $PROD_HOME/dload   s_dl_hem_prop_landlord_banks.pkb
run_script      $PROD_HOME/dload   dl_hem_plb_dlas_in.sql
# ICS
run_script      $PROD_HOME/dload   dl_hem_ics_income_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hem_consents.pks
run_script      $PROD_HOME/dload   s_dl_hem_consents.pkb
run_script      $PROD_HOME/dload   s_dl_hem_income_detail_reqs.pks
run_script      $PROD_HOME/dload   s_dl_hem_income_detail_reqs.pkb
run_script      $PROD_HOME/dload   s_dl_hem_ics_request_statuses.pks
run_script      $PROD_HOME/dload   s_dl_hem_ics_request_statuses.pkb
run_script      $PROD_HOME/dload   s_dl_hem_ics_incomes.pks
run_script      $PROD_HOME/dload   s_dl_hem_ics_incomes.pkb
run_script      $PROD_HOME/dload   s_dl_hem_ics_benefit_payments.pks
run_script      $PROD_HOME/dload   s_dl_hem_ics_benefit_payments.pkb
run_script      $PROD_HOME/dload   s_dl_hem_ics_payment_cmpts.pks
run_script      $PROD_HOME/dload   s_dl_hem_ics_payment_cmpts.pkb
run_script      $PROD_HOME/dload   s_dl_hem_ics_deductions.pks
run_script      $PROD_HOME/dload   s_dl_hem_ics_deductions.pkb
run_script      $PROD_HOME/dload   hem_ics_income_dlas.sql

run_script      $PROD_HOME/dload   gen_hem_person_also_known_as.sql

# Allocations HAT
copy_all_script HAT       "*"               $PROD_HOME/dload
copy_all_script HAT       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   gen_hat_dl.sql
run_script      $PROD_HOME/dload   dlas_in_hat.sql
run_script      $PROD_HOME/dload   dl_hat_tab_new.sql
# Allocations Offers
run_script      $PROD_HOME/dload   gen_hat_org_offers.sql
#
# Allocations Config ## Not installed but in /app/first/dataload/DataLoadAreas/NOTINSTALLED
#DL_HAT_ATTRIBUTES.ctl, DL_HAT_LETTINGS_AREAS.ctl, DL_HAT_ALLOC_PROP_TYPES.ctl
#DL_HAT_ALLOC_PROP_ATTRS.ctl, DL_HAT_ELEMENTS.ctl, DL_HAT_ELIG_CRITERIAS.ctl
#
# Rents HRA
copy_all_script HRA       "*"               $PROD_HOME/dload
copy_all_script HRA       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   gen_hra_dl.sql
# Subsidy
run_script      $PROD_HOME/dload   dl_hra_subsidy_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_applications.pks
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_applications.pkb
run_script      $PROD_HOME/dload   s_dl_hra_group_subsidy_reviews.pks
run_script      $PROD_HOME/dload   s_dl_hra_group_subsidy_reviews.pkb
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_debt_assmnts.pks
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_debt_assmnts.pkb
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_reviews.pks
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_reviews.pkb
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_income_items.pks
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_income_items.pkb
run_script      $PROD_HOME/dload   s_dl_hra_account_rent_limits.pks
run_script      $PROD_HOME/dload   s_dl_hra_account_rent_limits.pkb
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_grace_periods.pks
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_grace_periods.pkb
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_letters.pks
run_script      $PROD_HOME/dload   s_dl_hra_subsidy_letters.pkb
run_script      $PROD_HOME/dload   hra_sub_dlas.sql
# RDS
run_script      $PROD_HOME/dload   dl_hra_rds_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hra_rds_authorities.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_authorities.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_auth_deductions.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_auth_deductions.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_acc_deductions.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_acc_deductions.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_instructions.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_instructions.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_allocations.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_allocations.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_account_allocs.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_account_allocs.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_trans_files.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_trans_files.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_errors.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_errors.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi100.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi100.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi110.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi110.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi500.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi500.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi510.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi510.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi512.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi512.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi513.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi513.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi520.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi520.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi530.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi530.pkb
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi540.pks
run_script      $PROD_HOME/dload   s_dl_hra_rds_pyi540.pkb
run_script      $PROD_HOME/dload   hra_rds_dlas.sql
# Payment Arrangements
run_script      $PROD_HOME/dload   dl_hra_pay_arrange_tab.sql
run_script      $PROD_HOME/dload   s_dl_hra_expected_payments.pks
run_script      $PROD_HOME/dload   s_dl_hra_expected_payments.pkb
run_script      $PROD_HOME/dload   s_dl_hra_payment_expectations.pks
run_script      $PROD_HOME/dload   s_dl_hra_payment_expectations.pkb
run_script      $PROD_HOME/dload   dl_hra_pay_arrange_dlas.sql
# Arrears Arrangements
run_script      $PROD_HOME/dload   dl_hra_arrs_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hra_arrears_arrangements.pks
run_script      $PROD_HOME/dload   s_dl_hra_arrears_arrangements.pkb
run_script      $PROD_HOME/dload   s_dl_hra_arrears_instalments.pks
run_script      $PROD_HOME/dload   s_dl_hra_arrears_instalments.pkb
run_script      $PROD_HOME/dload   dl_hra_arrs_dlas_in.sql
# Bank Details
run_script      $PROD_HOME/dload   dl_hra_bde_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hra_bank_details.pks
run_script      $PROD_HOME/dload   s_dl_hra_bank_details.pkb
run_script      $PROD_HOME/dload   hdl_dlas_bde_in.sql
# Tenant Allowances
run_script      $PROD_HOME/dload   dl_hra_tenant_allowances_tabs.sql
run_script      $PROD_HOME/dload   s_dl_hra_tenant_allowances.pks
run_script      $PROD_HOME/dload   s_dl_hra_tenant_allowances.pkb
run_script      $PROD_HOME/dload   dl_hra_tenant_allowances_dlas.sql

#
# Repairs HRM
copy_all_script HRM       "*"               $PROD_HOME/dload
copy_all_script HRM       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   gen_install_budgets.sql
run_script      $PROD_HOME/dload   gen_hrm_dl.sql
run_script      $PROD_HOME/dload   gen_hrm_cont_dl.sql
#
# Private Leasing HPL
copy_all_script HPL       "*"               $PROD_HOME/dload
copy_all_script HPL       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   gen_hpl_dl.sql
run_script      $PROD_HOME/dload   dl_hpl_psl_schemes_tab_new.sql
run_script      $PROD_HOME/dload   psl_hpl_schemes_dlas.sql
run_script      $PROD_HOME/dload   s_dl_hpl_psl_schemes.pks
run_script      $PROD_HOME/dload   s_dl_hpl_psl_schemes.pkb
#
# Property Purchase HPP ## Not used at QLD
#copy_all_script HPP/20160404       "*"               $PROD_HOME/dload
#copy_all_script HPP/20160404       "*.ctl"           $PROD_HOME/bin
#run_script      $PROD_HOME/dload   gen_hpp_dl.sql
#
# Planned Maintenance HPM
copy_all_script HPM       "*"               $PROD_HOME/dload
copy_all_script HPM       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   gen_hpm_dl.sql
run_script      $PROD_HOME/dload   dl_hpm_mab_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hpm_man_area_budgets.pks
run_script      $PROD_HOME/dload   s_dl_hpm_man_area_budgets.pkb
run_script      $PROD_HOME/dload   hdl_hpm_mab_dlas_in.sql
run_script      $PROD_HOME/dload   gen_hpm_dl_Qaus_HPM_bespoke.sql
run_script      $PROD_HOME/dload   dlas_in_Qaus_HPM_bespoke.sql
#
# Service Charges HSC ## Not used at QLD
#copy_all_script HSC/20151007       "*"               $PROD_HOME/dload
#copy_all_script HSC/20151007       "*.ctl"           $PROD_HOME/bin
#run_script      $PROD_HOME/dload   gen_hsc_dl.sql
#
# Contractors HCO ## Not used at QLD
#copy_all_script HCO/20151007       "*"               $PROD_HOME/dload
#copy_all_script HCO/20151007       "*.ctl"           $PROD_HOME/bin
#run_script      $PROD_HOME/dload   gen_hco_dl.sql
#
# Customer Services HCS
copy_all_script HCS       "*"               $PROD_HOME/dload
copy_all_script HCS       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   dl_hcs_customer_services_tab_new.sql
run_script      $PROD_HOME/dload   dl_hcs_customer_services_dlas.sql
run_script      $PROD_HOME/dload   dl_hcs_people_group_tabs.sql
run_script      $PROD_HOME/dload   dl_hcs_people_group_dlas.sql
run_script      $PROD_HOME/dload   dl_hcs_business_act_parties_tabs.sql
run_script      $PROD_HOME/dload   dl_hcs_business_act_parties_dlas.sql
run_script      $PROD_HOME/dload   s_dl_hcs_business_actions.pks
run_script      $PROD_HOME/dload   s_dl_hcs_business_actions.pkb
run_script      $PROD_HOME/dload   s_dl_hcs_business_act_events.pks
run_script      $PROD_HOME/dload   s_dl_hcs_business_act_events.pkb
run_script      $PROD_HOME/dload   s_dl_hcs_people_group_members.pks
run_script      $PROD_HOME/dload   s_dl_hcs_people_group_members.pkb
run_script      $PROD_HOME/dload   s_dl_hcs_people_group_roles.pks
run_script      $PROD_HOME/dload   s_dl_hcs_people_group_roles.pkb
run_script      $PROD_HOME/dload   s_dl_hcs_people_groups.pks
run_script      $PROD_HOME/dload   s_dl_hcs_people_groups.pkb
run_script      $PROD_HOME/dload   s_dl_hcs_business_act_parties.pks
run_script      $PROD_HOME/dload   s_dl_hcs_business_act_parties.pkb

# Contacts
run_script      $PROD_HOME/dload   dl_hcs_contacts_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hcs_util.pks
run_script      $PROD_HOME/dload   s_dl_hcs_util.pkb
run_script      $PROD_HOME/dload   dl_hcs_contacts_dlas.sql
run_script      $PROD_HOME/dload   dl_hcs_people_group_dlas.sql
run_script      $PROD_HOME/dload   s_dl_hcs_contacts.pks
run_script      $PROD_HOME/dload   s_dl_hcs_contacts.pkb
#
# Property LifeCycle PLC
copy_all_script PLC       "*"               $PROD_HOME/dload
copy_all_script PLC       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   dl_hem_plc_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_hem_plc_prop_requests.pks
run_script      $PROD_HOME/dload   s_dl_hem_plc_prop_requests.pkb
run_script      $PROD_HOME/dload   s_dl_hem_plc_request_props.pks
run_script      $PROD_HOME/dload   s_dl_hem_plc_request_props.pkb
run_script      $PROD_HOME/dload   s_dl_hem_plc_req_data_items.pks
run_script      $PROD_HOME/dload   s_dl_hem_plc_req_data_items.pkb
run_script      $PROD_HOME/dload   s_dl_hem_plc_req_act_hist.pks
run_script      $PROD_HOME/dload   s_dl_hem_plc_req_act_hist.pkb
run_script      $PROD_HOME/dload   s_dl_hem_plc_req_prop_links.pks
run_script      $PROD_HOME/dload   s_dl_hem_plc_req_prop_links.pkb
run_script      $PROD_HOME/dload   s_dl_hem_plc_prop_act_hist.pks
run_script      $PROD_HOME/dload   s_dl_hem_plc_prop_act_hist.pkb
run_script      $PROD_HOME/dload   dl_hem_plc_dlas.sql
run_script      $PROD_HOME/dload   dl_hem_plc_errs_in.sql
#
# Land Titles LTL
copy_all_script LTL       "*"               $PROD_HOME/dload
copy_all_script LTL       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   dl_hem_ltl_tab_new.sql
run_script      $PROD_HOME/dload   hem_ltl_dlas.sql
run_script      $PROD_HOME/dload   s_dl_ltl_land_titles.pks
run_script      $PROD_HOME/dload   s_dl_ltl_land_titles.pkb
run_script      $PROD_HOME/dload   s_dl_ltl_land_title_assign.pks
run_script      $PROD_HOME/dload   s_dl_ltl_land_title_assign.pkb
run_script      $PROD_HOME/dload   s_dl_ltl_land_title_releases.pks
run_script      $PROD_HOME/dload   s_dl_ltl_land_title_releases.pkb
run_script      $PROD_HOME/dload   hem_ltl_cachesize_inc.sql
run_script      $PROD_HOME/dload   hem_ltl_grants.sql
run_script      $PROD_HOME/dload   hem_ltl_synonyms.sql
run_script      $PROD_HOME/dload   hem_ltl_indexes.sql
#
# Multi Area Dataloads MAD
copy_all_script MAD       "*"               $PROD_HOME/dload
copy_all_script MAD       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   gen_mad_dl.sql
#
# Housing Advice HAD
copy_all_script HAD       "*"               $PROD_HOME/dload
copy_all_script HAD       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   gen_had_dl.sql
run_script      $PROD_HOME/dload   had_dlas.sql
# Advice Case Associations
run_script      $PROD_HOME/dload   dl_had_acan_tab_new.sql
run_script      $PROD_HOME/dload   s_dl_had_adv_case_associations.pks
run_script      $PROD_HOME/dload   s_dl_had_adv_case_associations.pkb
run_script      $PROD_HOME/dload   dl_had_acan_dlas_in.sql
#
# Support Services Dataload HSS
copy_all_script HSS       "*"               $PROD_HOME/dload
copy_all_script HSS       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   dl_hss_tab_new.sql
run_script      $PROD_HOME/dload   hss_dlas_in.sql
run_script      $PROD_HOME/dload   s_dl_hss_referrals.pks
run_script      $PROD_HOME/dload   s_dl_hss_referrals.pkb
#
# Generic code
 copy_all_script generic   "*"               $PROD_HOME/dload
# run_script      $PROD_HOME/dload   s_dl_utils.pks
# run_script      $PROD_HOME/dload   s_dl_utils.pkb
# run_script      $PROD_HOME/dload   hdl_indexes.sql
run_script      $PROD_HOME/dload   hdl_grants.sql
run_script      $PROD_HOME/dload   hdl_synonyms.sql
run_script      $PROD_HOME/dload   hdl_errs_in.sql
run_script      $PROD_HOME/dload   hd1_errs_in.sql
run_script      $PROD_HOME/dload   hd2_errs_in.sql
run_script      $PROD_HOME/dload   hd3_errs_in.sql
run_script      $PROD_HOME/dload   cdl_errs_in.sql
run_script      $PROD_HOME/dload   dlo_errs_in.sql
run_script      $PROD_HOME/dload   hdl_dlas_in.sql
run_script      $PROD_HOME/dload   dl_indexes.sql
run_script      $PROD_HOME/dload   dl_alt_indexes.sql
run_script      $PROD_HOME/dload   dl_legacy_indexes.sql
run_script      $PROD_HOME/dload   hdl_invalid.sql
#
# LWR Dataloader
copy_all_script LWR       "*"               $PROD_HOME/dload
copy_all_script LWR       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   DL_HRA_LWR_APPORTND_ASS_DETS.sql
run_script      $PROD_HOME/dload   DL_HRA_LWR_APPORTND_ASSESS.sql
run_script      $PROD_HOME/dload   DL_HRA_LWR_ASSESS_VAL_ERRORS.sql
run_script      $PROD_HOME/dload   DL_HRA_LWR_ASSESSMENTS.sql
run_script      $PROD_HOME/dload   DL_HRA_LWR_BATCHES.sql
run_script      $PROD_HOME/dload   DL_HRA_LWR_RATE_ASSESS_DETS.sql
run_script      $PROD_HOME/dload   DL_HRA_LWR_WATER_METER_DETS.sql
run_script      $PROD_HOME/dload   DL_HRA_LWR_WATER_USAGE_DETS.sql
run_script      $PROD_HOME/dload   DL_HRA_WAT_CHRG_CALC_AUDITS.sql
run_script      $PROD_HOME/dload   S_DL_HRA_LWR_APPORTND_ASS_DETS.sql
run_script      $PROD_HOME/dload   S_DL_HRA_LWR_APPORTND_ASSESS.sql
run_script      $PROD_HOME/dload   S_DL_HRA_LWR_ASSESS_VAL_ERRORS.sql
run_script      $PROD_HOME/dload   S_DL_HRA_LWR_ASSESSMENTS.sql
run_script      $PROD_HOME/dload   S_DL_HRA_LWR_BATCHES.sql
run_script      $PROD_HOME/dload   S_DL_HRA_LWR_RATE_ASSESS_DETS.sql
run_script      $PROD_HOME/dload   S_DL_HRA_LWR_WATER_METER_DETS.sql
run_script      $PROD_HOME/dload   S_DL_HRA_LWR_WATER_USAGE_DETS.sql
run_script      $PROD_HOME/dload   S_DL_HRA_WAT_CHRG_CALC_AUDITS.sql
run_script      $PROD_HOME/dload   lwr_errs_in.sql
run_script      $PROD_HOME/dload   dl_hra_lwr_dataload.sql
run_script      $PROD_HOME/dload   dl_hra_lwr_batches_grants.sql
#
# Bespoke Dataload CUS
copy_all_script CUS       "*"               $PROD_HOME/dload
copy_all_script CUS       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   dl_cus_dataload_batches_tab.sql
run_script      $PROD_HOME/dload   cus_errs_in.sql
run_script      $PROD_HOME/dload   dl_cus_dataload_batches.sql
run_script      $PROD_HOME/dload   S_DL_CUS_DATALOAD_BATCHES.pks
run_script      $PROD_HOME/dload   S_DL_CUS_DATALOAD_BATCHES.pkb
run_script      $PROD_HOME/dload   dl_cus_dataload_batches_grants.sql
run_script      $PROD_HOME/dload   trigger_dps_after_u.sql
#
# Revoke DBA
# revoke_dba
#
run_script      $LOGFILE_HOME/scripts sel_invalid.sql
write_rep_trail; #Write out the report trailer
#
##############################################################################
#End of install_dload.sh
##############################################################################

