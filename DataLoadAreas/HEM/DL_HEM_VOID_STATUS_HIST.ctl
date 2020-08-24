-- *********************************************************************
-- CHANGE CONTROL
-- Version   Rel     Who  Date         Why
-- 1.0       6.14    AJ   06-FEB-2017  Initial creation of data loader for
--                                     Queensland CR461
-- 1.1       6.14/5  AJ   12-FEB-2017  Minor changes during testing
--
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HEM_VOID_STATUS_HIST
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LVSH_DLB_BATCH_ID         CONSTANT "$batch_no"
,LVSH_DL_SEQNO            RECNUM
,LVSH_DL_LOAD_STATUS      CONSTANT "L"
,LVSH_PRO_PROPREF         CHAR "RTRIM(:LVSH_PRO_PROPREF)"
,LVSH_VIN_HPS_START_DATE  DATE "DD-MON-YYYY" NULLIF LVSH_VIN_HPS_START_DATE=BLANKS
,LVSH_VIN_HPS_END_DATE    DATE "DD-MON-YYYY" NULLIF LVSH_VIN_HPS_START_DATE=BLANKS
,LVSH_VIN_HPS_HPC_CODE    CHAR "RTRIM(:LVSH_VIN_HPS_HPC_CODE)"
,LVSH_VIN_VST_CODE        CHAR "RTRIM(:LVSH_VIN_VST_CODE)"
,LVSH_VIN_STATUS_STARTED  DATE "DD-MON-YYYY" NULLIF LVSH_VIN_STATUS_STARTED=BLANKS
,LVSH_MODIFIED_DATE       DATE "DD-MON-YYYY" NULLIF LVSH_MODIFIED_DATE=BLANKS
,LVSH_USERNAME            CHAR "RTRIM(:LVSH_USERNAME)"
)

