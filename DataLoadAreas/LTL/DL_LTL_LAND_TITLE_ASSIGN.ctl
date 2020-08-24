-- *********************************************************************
--
-- Version    DBase  Who         Date       Why
-- 1.00              Ian Rowell  25-Sep-09  Initial Creation
-- 1.1       6.14    AJ          21-Mar-15  Date Time added to create and modified
--                                          dates request of Queensland
--
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_LTL_LAND_TITLE_ASSIGN
fields terminated by "," optionally enclosed by '"'
trailing nullcols
( LLTA_DLB_BATCH_ID                CONSTANT "$batch_no",              
  LLTA_DL_SEQNO                    RECNUM,
  LLTA_DL_LOAD_STATUS              CONSTANT "L",
  LLTA_LLTL_PLAN_NUMBER            CHAR "rtrim(:LLTA_LLTL_PLAN_NUMBER)",
  LLTA_LLTL_LOT_NUMBER             CHAR "rtrim(:LLTA_LLTL_LOT_NUMBER)",
  LLTA_LLTL_LTT_CODE               CHAR "rtrim(:LLTA_LLTL_LTT_CODE)",
  LLTA_START_DATE                  DATE "DD-MON-YYYY" NULLIF LLTA_START_DATE=blanks,
  LLTA_PRO_REFNO                   CHAR "rtrim(:LLTA_PRO_REFNO)",
  LLTA_AUN_CODE                    CHAR "rtrim(:LLTA_AUN_CODE)",
  LLTA_END_DATE                    DATE "DD-MON-YYYY" NULLIF LLTA_END_DATE=blanks,
  LLTA_HRV_FACR_CODE               CHAR "rtrim(:LLTA_HRV_FACR_CODE)",
  LLTA_COMMENTS                    CHAR "rtrim(:LLTA_COMMENTS)",
  LLTA_CREATED_BY                  CHAR "rtrim(:LLTA_CREATED_BY)",
  LLTA_CREATED_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLTA_CREATED_DATE=blanks,
  LLTA_MODIFIED_BY                 CHAR "rtrim(:LLTA_MODIFIED_BY)",
  LLTA_MODIFIED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLTA_MODIFIED_DATE=blanks,
  LLTA_SECTION_NUMBER              CHAR "rtrim(:LLTA_SECTION_NUMBER)"
)



