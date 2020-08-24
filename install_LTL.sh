#!/bin/sh
##############################################################################
# Script to install all dataload code                                        #
# This shell script will replicate the script gen_all_dl.sql which is        #
# supplied from the ISG team. The data load areas have been split up into    #
# separate folders to maintain greater visibility of what actual gets run    #
#                                                                            #
# Run from the command line with:                                            #
# sh install_dload.sh hou/password name_of_log.txt                           #
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
##############################################################################
##############################################################################
#Assign Internal Variables                                                   #
##############################################################################
uname=$1
log=$2
rcode=0
LOGFILE_HOME=/app/first/dataload
INSTALL_HOME=${LOGFILE_HOME}/DataLoadAreas
DLOAD_FILE=${LOGFILE_HOME}/$2
DLOAD_FILE_ALL=${LOGFILE_HOME}/all_${2}
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
DB_NAME=`sqlplus -s $uname << SQLEND1
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
   if [ ! -d ${RUN_DIR} ]
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
`sqlplus -s $uname >${RUN_LOG} << SQLEND1
   DEFINE table_tablespace='TABLES'
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
##############################################################################
#End of subroutine section-Start of Main routine
##############################################################################
#
# Ensure this is not HOUSETUP
case ${ORACLE_SID} in
 HOUSETUP) echo DO NOT RUN AGAINST HOUSETUP
 exit 1
esac

write_rep_hddr; #Write out the report header
create_dload_dir; #Creates $PROD_HOME/dload directory
empty_dload_dir; #Removes all from $PROD_HOME/dload directory
#
# Land Titles LTL
copy_all_script LTL/20151022       "*"               $PROD_HOME/dload
copy_all_script LTL/20151022       "*.ctl"           $PROD_HOME/bin
run_script      $PROD_HOME/dload   dl_hem_ltl_tab_new.sql
run_script      $PROD_HOME/dload   hd1_errs_in.sql
run_script      $PROD_HOME/dload   hem_ltl_dlas.sql
run_script      $PROD_HOME/dload   s_dl_ltl_land_titles.pks
run_script      $PROD_HOME/dload   s_dl_ltl_land_titles.pkb
run_script      $PROD_HOME/dload   s_dl_ltl_land_title_assign.pks
run_script      $PROD_HOME/dload   s_dl_ltl_land_title_assign.pkb
run_script      $PROD_HOME/dload   s_dl_ltl_land_title_releases.pks
run_script      $PROD_HOME/dload   s_dl_ltl_land_title_releases.pkb
run_script      $PROD_HOME/dload   hem_ltl_grants.sql
#
run_script      /app/first/dataload/scripts sel_invalid.sql
write_rep_trail; #Write out the report trailer
#
##############################################################################
#End of install_dload.sh
##############################################################################

