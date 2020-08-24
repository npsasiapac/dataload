# Shell Script to do SQL loader functions                                  #
#                                                                          #
############################################################################
# History                                                                  #
# Ver  Date        By       Description                                    #
# 1.0 15/07/2001                                                           #
# 1.1 11/09/2001            Modified copy sent by Pete Davies              #
# 1.2 12/09/2001            Changes after review by pete davies            #
# 1.3 07/12/2001            Script working on site from paul hearty        #
# 1.4 10/01/2002            Changed success call to                        #
#                           s_dl_process_summary.update_summary, made      #
#                           last param = 'N' (was 'Y').                    #       
# 1.5  12/04/2002  PJD      Now populates dps_total figures etc.           #
# 1.6  23/05/2002  PJD      Inc the batch ref in the CTLFILE variable      #
# 1.7  17/09/2002  PJD      Change initial insert into dl_process_summary  # 
# 1.8  28/11/2002  PJD      Remove duplicated update of dps_status         #                                                  
# 1.9  06/03/2003  SB       Added end to first statement                   #
# 1.10 09/06/2004  PJD      Added nvl clauses in update statement          #
############################################################################
#                                                                          #
# Daemon Parameters                                                        # 
#     $1    - oracle userid/password                                       #
#     $2    - output file i.e.$PROD_REPORTS/FDL100_<GPI_RUN_ID>.LIS        #
# Module Parameters                                                        #
#     $3    - fdl product area i.e. HEM                                    #
#     $4    - fdl dataload area i.e. PROPERTIES                            #	
#     $5    - fdl batch reference                                          # 
#     $6    - fdl process datetime                                         #
#                                                                          #
#                                                                          #
############################################################################
#
# Update the front end to show the process as running
############################################################################

sqlplus -s $1 <<!SQLENDA >>$2
REM whenever sqlerror exit sql.sqlcode rollback
set echo off
set feed off
set verify off
--
--
declare
l_date date;
l_batch VARCHAR2(30);
--
begin
l_date:= to_date('$6','DD-MON-YYYY HH24:MI:SS');
l_batch:= rtrim('$5');
--
insert into dl_process_summary
(DPS_DLB_BATCH_ID      
,DPS_PROCESS           
,DPS_DATE              
,DPS_STATUS            
,DPS_FAILURES_IND      
,DPS_TOTAL_RECORDS     
,DPS_FAILED_RECORDS    
,DPS_PROCESSED_RECORDS 
)
select
l_batch,'LOAD',l_date,'QUEUED','N',0,0,0
from dual
where not exists (select null from dl_process_summary d2
                   where d2.dps_dlb_batch_id = l_batch
                     and d2.dps_process      = 'LOAD' 
                     and d2.dps_date         = l_date);
--
update dl_process_summary
set dps_status = 'RUNNING'
where dps_dlb_batch_id = l_batch
and   dps_process = 'LOAD'
and   dps_date    = l_date;
--
commit;
--
end;
/
--
!SQLENDA
############################################################################

# Set -f stops unix expanding * in error messages into list of files.
set -f

#generic process LIS file
OUTFILE=$2

#cut extension from outfile
#LOAD=${PROD_DATALOAD}/`basename $2|cut -f 1 -d "."`
REPLOAD=${PROD_REPORTS}/`basename $2|cut -f 1 -d "."`
echo repload
echo $REPLOAD
#cut gpi batch id from outfile
GPI_BATCH_REFERENCE=`basename $2|cut -f 2 -d "_" | cut -f 1 -d "."`

#dataload data .DAT file
LOADFILE=${PROD_DATALOAD}/$5

#dataload control .CTL file
FDL_PRODUCT_AREA=$3
FDL_DATALOAD_AREA=$4
CTLFILE=${PROD_DATALOAD}/${3}_${4}_${5}

#dataload process parameters to return status
FDL_BATCH_REFERENCE=$5
FDL_PROCESS_DATETIME=$6

#return code
rcode=0

#
echo "DLLOAD: #################################################" >>$OUTFILE
echo "DLLOAD: Load file utility" >>$OUTFILE

#
##################################################################
# Section  Load file  
#################################################################@

# check datafile exits
if [ ! -f ${LOADFILE}.dat ]
then
   echo "DLLOAD: No datafile found for loading" >>$OUTFILE
   rcode=1
fi

# check file permissions
if [ $rcode -eq 0 ]
then
   if [ -f $REPLOAD.bad ]
   then
      if [ ! -w $REPLOAD.bad ]
      then
         uid=`who am i | cut -c1,2,3,4,5,6,7,8,9,10,11`
         echo "DLLOAD: Sqlloader file "$REPLOAD.bad >>$OUTFILE
         echo "DLLOAD: already exists and "$uid "does not" >>$OUTFILE
         echo "DLLOAD: have write permission to this file." >>$OUTFILE
         echo "DLLOAD: Please remove or change its permissions." >>$OUTFILE
         rcode=1
      fi
   fi

   if [ -f $REPLOAD.dsc ]
   then
      if [ ! -w $REPLOAD.dsc ]
      then
         uid=`who am i | cut -c1,2,3,4,5,6,7,8,9,10,11`
         echo "DLLOAD: Sqlloader file "$REPLOAD.dsc >>$OUTFILE
         echo "DLLOAD: already exists and "$uid "does not" >>$OUTFILE
         echo "DLLOAD: have write permission to this file." >>$OUTFILE
         echo "DLLOAD: Please remove or change its permissions." >>$OUTFILE
         rcode=1
      fi
   fi

   if [ -f $REPLOAD.log ]
   then
      if [ ! -w $REPLOAD.log ]
      then
         uid=`who am i | cut -c1,2,3,4,5,6,7,8,9,10,11`
         echo "DLLOAD: Sqlloader file "$REPLOAD.log >>$OUTFILE
         echo "DLLOAD: already exists and "$uid "does not" >>$OUTFILE
         echo "DLLOAD: have write permission to this file." >>$OUTFILE
         echo "DLLOAD: Please remove or change its permissions." >>$OUTFILE
         rcode=1
      fi
   fi
fi

# purge files 
if [ $rcode -eq 0 ]
then
   if [ -s $REPLOAD.bad ]
   then rm $REPLOAD.bad
   fi
   if [ -s $REPLOAD.dsc ]
   then rm $REPLOAD.dsc
   fi
   if [ -s $REPLOAD.log ]
   then rm $REPLOAD.log
   fi
fi


if [ $rcode -eq 0 ]
then

   # edit copy of the template control file - 
   # create temp copy of default control file without the DL prefix into the dataload folder
   # replace $batch_no with a unique rund id and remove the redundant infile datafile line
#
   sed -e s/'$batch_no'/$FDL_BATCH_REFERENCE/g -e s/'$BATCH_NO'/$FDL_BATCH_REFERENCE/g  -e 's/infile .*$/ /' $PROD_HOME/bin/DL_${FDL_PRODUCT_AREA}_${FDL_DATALOAD_AREA}.ctl >${CTLFILE}.ctl

   # load datafile
   sqlldr userid=$1 control=${CTLFILE}.ctl data=${LOADFILE}.dat log=${REPLOAD}.log bad=${REPLOAD}.bad discard=${REPLOAD}.dis rows=64 errors=99999  > ${REPLOAD}.out

   rm ${CTLFILE}.ctl
   #
   if [ "$?" != "0" ]
   then
      echo "DLLOAD: Sqlload failed." >>$OUTFILE
      rcode=1
   fi
   #
   egrep 'ORA-|ERROR' ${REPLOAD}.out>$null_device
   if [ "$?" = "0" ]
   then
      echo "DLLOAD: Sql loader failed." >>$OUTFILE
      #cat ${REPLOAD}.out
      rcode=1
   fi
   #
#   egrep 'ORA-' ${REPLOAD}.log > $null_device
#   if [ "$?" = "0" ]
#   then
#      echo "DLLOAD: An oracle error has occured during load." >>$OUTFILE
#      echo "DLLOAD: Please check the sqlldr log file that follows " >>$OUTFILE
#      echo "DLLOAD: Correct the problem and re submit" >>$OUTFILE
#      echo "DLLOAD: --- No Data has been processed. --" >>$OUTFILE
#      echo "DLLOAD: " >>$OUTFILE
#      #cat ${REPLOAD}.log
#      rcode=1
#   fi
   #
   if [ $rcode -eq 0 ]
   then
      echo "DLLOAD: ###############################" >>$OUTFILE
      echo "DLLOAD: Sql loader Load Summary" >>$OUTFILE
      echo "DLLOAD: "`grep '^Total.*read' ${REPLOAD}.log` >>$OUTFILE
      echo "DLLOAD: "`grep '^Total.*reject' ${REPLOAD}.log` >>$OUTFILE
      echo "DLLOAD: "`grep '^Total.*disca' ${REPLOAD}.log` >>$OUTFILE
      echo "DLLOAD: ###############################" >>$OUTFILE
      #
      # Set some variables for reporting purposes
      #
      LTOTAL=`grep '^Total.*read' ${REPLOAD}.log`
      LREJECT=`grep '^Total.*reject' ${REPLOAD}.log` 
      LDISCARD=`grep '^Total.*disca' ${REPLOAD}.log` 
      #
      if [ -s ${REPLOAD}.bad ]
      then
         echo "DLLOAD: Data has been rejected please check" >>$OUTFILE
         echo "DLLOAD: the sqlldr log file " >>$OUTFILE
         echo "DLLOAD: " >>$OUTFILE
         #cat ${REPLOAD}.log
         rcode=1
      fi
      if [ $rcode -eq 0 ]
      then
         if [ -s ${REPLOAD}.dsc ]
         then
            echo "DLLOAD: Data has been discarded please check" >>$OUTFILE
            echo "DLLOAD: The sqlldr log file " >>$OUTFILE
            echo "DLLOAD: " >>$OUTFILE
            #cat ${REPLOAD}.log
            rcode=1
         fi
      fi
   fi
fi
#
##################################################################
#: ---- END OF SECTION ----                                      #
##################################################################
if [ $rcode -eq 0 ]
then
  echo "DLLOAD: Interface load utility terminated successfully." >>$OUTFILE
  echo "DLLOAD: #################################################" >>$OUTFILE
  echo "DLLOAD: " >>$OUTFILE
#
sqlplus -s $1 <<!SQL1 >>$2
  REM whenever sqlerror exit sql.sqlcode rollback
  set echo off
  set feed off
  set verify off
  set serverout on size 1000000
declare
l_date date;
l_trimmed VARCHAR2(1) := 'N';
l_count   INTEGER := 0;
--
l_tot_str varchar2(100);
l_rej_str varchar2(100);
l_dis_str varchar2(100);
--
l_tot_num integer;
l_rej_num integer;
l_dis_num integer;
--
begin
l_tot_str := '$LTOTAL';
l_rej_str := '$LREJECT';
l_dis_str := '$LDISCARD';
--
WHILE l_trimmed = 'N' LOOP
l_tot_str := SUBSTR(l_tot_str,2,100);
IF SUBSTR(l_tot_str,1,1) IN ('1','2','3','4','5','6','7','8','9','0')
THEN l_trimmed := 'Y';
     l_tot_str := ltrim(rtrim(l_tot_str));
END IF;
l_count := l_count +1;
IF l_count > 50 THEN l_trimmed := 'Y'; END IF;
END LOOP;
l_trimmed := 'N';
l_count := 0;
--
WHILE l_trimmed = 'N' LOOP
l_rej_str := SUBSTR(l_rej_str,2,100);
IF SUBSTR(l_rej_str,1,1) IN ('1','2','3','4','5','6','7','8','9','0')
THEN l_trimmed := 'Y';
     l_rej_str := ltrim(rtrim(l_rej_str));
END IF;
l_count := l_count +1;
IF l_count > 50 THEN l_trimmed := 'Y'; END IF;
END LOOP;
--
l_trimmed := 'N';
l_count := 0;
--
WHILE l_trimmed = 'N' LOOP
l_dis_str := SUBSTR(l_dis_str,2,100);
IF SUBSTR(l_dis_str,1,1) IN ('1','2','3','4','5','6','7','8','9','0')
THEN l_trimmed := 'Y';
     l_dis_str := ltrim(rtrim(l_dis_str));
END IF;
l_count := l_count +1;
IF l_count > 50 THEN l_trimmed := 'Y'; END IF;
END LOOP;
--
-- dbms_output.put_line('l_tot_str '||l_tot_str);
-- dbms_output.put_line('l_rej_str '||l_rej_str);
-- dbms_output.put_line('l_dis_str '||l_dis_str);
--
BEGIN
l_tot_num := to_number(l_tot_str);
l_rej_num := to_number(l_rej_str);
l_dis_num := to_number(l_dis_str);
--
EXCEPTION
WHEN OTHERS THEN 
l_tot_num := 0;
l_rej_num := 0; 
l_dis_num := 0;
--
END;
--
-- dbms_output.put_line('l_tot_num '||l_tot_num);
-- dbms_output.put_line('l_rej_num '||l_rej_num);
-- dbms_output.put_line('l_dis_num '||l_dis_num);
--
l_date:= to_date('$FDL_PROCESS_DATETIME','DD_MON_YYYY HH24:MI:SS');
--
UPDATE dl_process_summary
SET dps_status            = 'COMPLETED'           
,   dps_failures_ind      = 'N'     
,   dps_total_records     = nvl(l_tot_num,0)
,   dps_failed_records    = nvl(l_rej_num,0)+nvl(l_dis_num,0)
,   dps_processed_records = nvl(l_tot_num,0)
                            -(nvl(l_rej_num,0)+nvl(l_dis_num,0))
WHERE dps_dlb_batch_id = '$FDL_BATCH_REFERENCE'
AND   dps_process      = 'LOAD'
AND   dps_date         = l_date;
--
end;
/
commit;
!SQL1
#
else
  echo "DLLOAD: Interface load utility reported errors " >>$OUTFILE
  echo "DLLOAD: please investigate and re run if necessary." >>$OUTFILE
  echo "DLLOAD: " >>$OUTFILE
  echo "DLLOAD: Shell script ends   " `date` >>$OUTFILE
  echo "DLLOAD: #################################################" >>$OUTFILE
#      LTOTAL=`grep '^Total.*read' ${REPLOAD}.log`
#      LREJECT=`grep '^Total.*reject' ${REPLOAD}.log` 
#      LDISCARD=`grep '^Total.*disca' ${REPLOAD}.log` 
#
# echo "FAILED"
#
sqlplus -s $1 <<!SQL2 >>$2
  REM whenever sqlerror exit sql.sqlcode rollback
  set echo off
  set feed off
  set verify off
  set serverout on size 1000000
declare
l_date date;
l_trimmed VARCHAR2(1) := 'N';
l_count   INTEGER := 0;
--
l_tot_str varchar2(100);
l_rej_str varchar2(100);
l_dis_str varchar2(100);
--
l_tot_num integer;
l_rej_num integer;
l_dis_num integer;
--
begin
l_tot_str := '$LTOTAL';
l_rej_str := '$LREJECT';
l_dis_str := '$LDISCARD';
--
WHILE l_trimmed = 'N' LOOP
l_tot_str := SUBSTR(l_tot_str,2,100);
IF SUBSTR(l_tot_str,1,1) IN ('1','2','3','4','5','6','7','8','9','0')
THEN l_trimmed := 'Y';
     l_tot_str := ltrim(rtrim(l_tot_str));
END IF;
l_count := l_count +1;
IF l_count > 50 THEN l_trimmed := 'Y'; END IF;
END LOOP;
l_trimmed := 'N';
l_count := 0;
--
WHILE l_trimmed = 'N' LOOP
l_rej_str := SUBSTR(l_rej_str,2,100);
IF SUBSTR(l_rej_str,1,1) IN ('1','2','3','4','5','6','7','8','9','0')
THEN l_trimmed := 'Y';
     l_rej_str := ltrim(rtrim(l_rej_str));
END IF;
l_count := l_count +1;
IF l_count > 50 THEN l_trimmed := 'Y'; END IF;
END LOOP;
--
l_trimmed := 'N';
l_count := 0;
--
WHILE l_trimmed = 'N' LOOP
l_dis_str := SUBSTR(l_dis_str,2,100);
IF SUBSTR(l_dis_str,1,1) IN ('1','2','3','4','5','6','7','8','9','0')
THEN l_trimmed := 'Y';
     l_dis_str := ltrim(rtrim(l_dis_str));
END IF;
l_count := l_count +1;
IF l_count > 50 THEN l_trimmed := 'Y'; END IF;
END LOOP;
--
-- dbms_output.put_line('l_tot_str '||l_tot_str);
-- dbms_output.put_line('l_rej_str '||l_rej_str);
-- dbms_output.put_line('l_dis_str '||l_dis_str);
--
BEGIN
l_tot_num := to_number(l_tot_str);
l_rej_num := to_number(l_rej_str);
l_dis_num := to_number(l_dis_str);
--
EXCEPTION
WHEN OTHERS THEN 
l_tot_num := 0;
l_rej_num := 0; 
l_dis_num := 0;
--
END;
--
-- dbms_output.put_line('l_tot_num '||l_tot_num);
-- dbms_output.put_line('l_rej_num '||l_rej_num);
-- dbms_output.put_line('l_dis_num '||l_dis_num);
--
l_date:= to_date('$FDL_PROCESS_DATETIME','DD_MON_YYYY HH24:MI:SS');
--
UPDATE dl_process_summary
SET dps_status            = 'COMPLETED'           
,   dps_failures_ind      = 'Y'     
,   dps_total_records     = nvl(l_tot_num,0)
,   dps_failed_records    = nvl(l_rej_num,0)+nvl(l_dis_num,0)
,   dps_processed_records = nvl(l_tot_num,0)-
                           (nvl(l_rej_num,0)+nvl(l_dis_num,0))
WHERE dps_dlb_batch_id = '$FDL_BATCH_REFERENCE'
AND   dps_process      = 'LOAD'
AND   dps_date         = l_date;
--
end;
/
commit;
!SQL2
fi
#
exit $rcode
##################################################################
#                ---- END OF DLLOAD  ----                        #
##################################################################



