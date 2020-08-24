-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.1          AJ          21-Mar-15  Change Control added as legacy ref
--                                     and new vin ref removed and ordered
--                                     to match load file order in doc and
--                                     data load table
-- 1.2          AJ          08-Aug-16  load data inadvertently put in twice
--                                      removed
--
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HEM_VOID_INSTANCES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LVIN_DLB_BATCH_ID     CONSTANT "$batch_no"
,LVIN_DL_SEQNO         RECNUM
,LVIN_DL_LOAD_STATUS   CONSTANT "L"
,LVIN_PRO_PROPREF      CHAR "RTRIM(:LVIN_PRO_PROPREF)"
,LVIN_STATUS_START     DATE "DD-MON-YYYY" NULLIF LVIN_STATUS_START=BLANKS
,LVIN_DEC_ALLOWANCE    CHAR NULLIF LVIN_DEC_ALLOWANCE=BLANKS "TO_NUMBER(:LVIN_DEC_ALLOWANCE)"
,LVIN_MAN_CREATED      CHAR "RTRIM(:LVIN_MAN_CREATED)"
,LVIN_VST_CODE         CHAR "RTRIM(:LVIN_VST_CODE)"
,LVIN_TGT_DATE         DATE "DD-MON-YYYY" NULLIF LVIN_TGT_DATE=BLANKS
,LVIN_APT_CODE         CHAR "RTRIM(:LVIN_APT_CODE)"
,LVIN_VGR_CODE         CHAR "RTRIM(:LVIN_VGR_CODE)"
,LVIN_VPA_CURR_CODE    CHAR "RTRIM(:LVIN_VPA_CURR_CODE)"
,LVIN_SCO_CODE         CHAR "RTRIM(:LVIN_SCO_CODE)"
,LVIN_HRV_RFV_CODE     CHAR "RTRIM(:LVIN_HRV_RFV_CODE)"
,LVIN_HRV_VCL_CODE     CHAR "RTRIM(:LVIN_HRV_VCL_CODE)"
,LVIN_EFFECTIVE_DATE   DATE "DD-MON-YYYY" NULLIF LVIN_EFFECTIVE_DATE=BLANKS
,LVIN_TEXT             CHAR "RTRIM(:LVIN_TEXT)"
,LVIN_CREATED_DATE     DATE "DD-MON-YYYY" NULLIF LVIN_CREATED_DATE=BLANKS
)

