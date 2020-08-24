-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.0          MJK         12-Nov-15  Initial Creation
-- 1.1          AJ          21-Mar-15  Change Control added as Time stamp
--                                     added to event,target and sys date
--                                     for Queensland 
--
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HEM_VOID_EVENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LVEV_DLB_BATCH_ID        CONSTANT "$batch_no"
,LVEV_DL_SEQNO            RECNUM
,LVEV_DL_LOAD_STATUS      CONSTANT "L"
,LVEV_PRO_PROPREF         CHAR "rtrim(:LVEV_PRO_PROPREF)"
,LVEV_VIN_HPS_START_DATE  DATE "DD-MON-YYYY" NULLIF LVEV_VIN_HPS_START_DATE=BLANKS
,LVEV_VIN_HPS_END_DATE    DATE "DD-MON-YYYY" NULLIF LVEV_VIN_HPS_END_DATE=BLANKS
,LVEV_VIN_HPS_HPC_CODE    CHAR "rtrim(:LVEV_VIN_HPS_HPC_CODE)"
,LVEV_INT_SEQNO           CHAR "rtrim(:LVEV_INT_SEQNO)"
,LVEV_ORDER_SEQNO         CHAR "rtrim(:LVEV_ORDER_SEQNO)"
,LVEV_EVT_CODE            CHAR "rtrim(:LVEV_EVT_CODE)"
,LVEV_EVENT_DATE          DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LVEV_EVENT_DATE=BLANKS
,LVEV_TARGET_DATE         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LVEV_TARGET_DATE=BLANKS
,LVEV_CALC_EXT            CHAR "rtrim(:LVEV_CALC_EXT)"
,LVEV_DEV_VPA_CODE        CHAR "rtrim(:LVEV_DEV_VPA_CODE)"
,LVEV_DEV_SEQNO           CHAR "rtrim(:LVEV_DEV_SEQNO)"
,LVEV_SYS_UPDATED         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LVEV_SYS_UPDATED=BLANKS
,LVEV_USERNAME     	      CHAR "rtrim(:LVEV_USERNAME)"
,LVEV_SOURCE_TYPE         CHAR "rtrim(:LVEV_SOURCE_TYPE)"
,LVEV_OFF_REFNO           CHAR "rtrim(:LVEV_OFF_REFNO)"
,LVEV_VIN_EFFECTIVE_DATE  DATE "DD-MON-YYYY" NULLIF LVEV_VIN_EFFECTIVE_DATE=BLANKS
,LVEV_TEXT                CHAR "rtrim(:LVEV_TEXT)"
)
 