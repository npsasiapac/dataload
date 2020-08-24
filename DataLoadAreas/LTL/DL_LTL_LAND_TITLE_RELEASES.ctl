-- ********************************************************************************
--
-- Version   DBase   Who         Date       Why
-- 1.0               Ian Rowell  25-Sep-09  Initial Creation
-- 1.1       6.14    AJ          21-Mar-15  Date Time added to create and modified
--                                          dates request of Queensland  
--
-- ********************************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_LTL_LAND_TITLE_RELEASES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
( LLTR_DLB_BATCH_ID                CONSTANT "$batch_no",              
  LLTR_DL_SEQNO                    RECNUM,
  LLTR_DL_LOAD_STATUS              CONSTANT "L",
  LLTR_LLTL_PLAN_NUMBER            CHAR "rtrim(:LLTR_LLTL_PLAN_NUMBER)",
  LLTR_LLTL_LOT_NUMBER             CHAR "rtrim(:LLTR_LLTL_LOT_NUMBER)",
  LLTR_LLTL_LTT_CODE               CHAR "rtrim(:LLTR_LLTL_LTT_CODE)",
  LLTR_RELEASED_DATE               DATE "DD-MON-YYYY" NULLIF LLTR_RELEASED_DATE=blanks,
  LLTR_RELEASED_TO                 CHAR "rtrim(:LLTR_RELEASED_TO)",
  LLTR_HRV_FTRR_CODE               CHAR "rtrim(:LLTR_HRV_FTRR_CODE)",
  LLTR_MATTER_NUMBER               CHAR "rtrim(:LLTR_MATTER_NUMBER)",
  LLTR_RETURNED_DATE               DATE "DD-MON-YYYY" NULLIF LLTR_RETURNED_DATE=blanks,
  LLTR_COMMENTS                    CHAR "rtrim(:LLTR_COMMENTS)",
  LLTR_CREATED_BY                  CHAR "rtrim(:LLTR_CREATED_BY)",
  LLTR_CREATED_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLTR_CREATED_DATE=blanks,
  LLTR_MODIFIED_BY                 CHAR "rtrim(:LLTR_MODIFIED_BY)",
  LLTR_MODIFIED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLTR_MODIFIED_DATE=blanks,
  LLTR_SECTION_NUMBER              CHAR "rtrim(:LLTR_SECTION_NUMBER)"
)


