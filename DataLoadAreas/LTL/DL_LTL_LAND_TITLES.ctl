-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.00         Ian Rowell  25-Sep-09  Initial Creation
-- 1.10         MJK         12-Nov-15  Changed to use par_per_alt_ref and par_org_frv_oty_code
-- 1.2          AJ          21-Mar-15  Time stamp added to created_date and modified_date for
--                                     Queensland 
--
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_LTL_LAND_TITLES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
( LLTL_DLB_BATCH_ID                CONSTANT "$batch_no",              
  LLTL_DL_SEQNO                    RECNUM,
  LLTL_DL_LOAD_STATUS              CONSTANT "L", 
  LLTL_PLAN_NUMBER                 CHAR "rtrim(:LLTL_PLAN_NUMBER)",
  LLTL_LOT_NUMBER                  CHAR "rtrim(:LLTL_LOT_NUMBER)",
  LLTL_LTT_CODE                    CHAR "rtrim(:LLTL_LTT_CODE)",
  LLTL_PAR_PER_ALT_REF             CHAR "rtrim(:LLTL_PAR_PER_ALT_REF)",
  LLTL_PAR_ORG_SHORT_NAME          CHAR "rtrim(:LLTL_PAR_ORG_SHORT_NAME)",
  LLTL_PAR_ORG_FRV_OTY_CODE        CHAR "rtrim(:LLTL_PAR_ORG_FRV_OTY_CODE)",
  LLTL_AREA_MEASUREMENT            CHAR "rtrim(:LLTL_AREA_MEASUREMENT)",
  LLTL_START_DATE                  DATE "DD-MON-YYYY" NULLIF LLTL_START_DATE=blanks,
  LLTL_DATE_TYPE_IND               CHAR "rtrim(:LLTL_DATE_TYPE_IND)",
  LLTL_AFFECT_EASE_IND             CHAR "rtrim(:LLTL_AFFECT_EASE_IND)",
  LLTL_APPURT_EASE_IND             CHAR "rtrim(:LLTL_APPURT_EASE_IND)",
  LLTL_RESIDUAL_IND                CHAR "rtrim(:LLTL_RESIDUAL_IND)",
  LLTL_VOLUME_NUMBER               CHAR "rtrim(:LLTL_VOLUME_NUMBER)",
  LLTL_CREATED_BY                  CHAR "rtrim(:LLTL_CREATED_BY)",
  LLTL_CREATED_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLTL_CREATED_DATE=blanks,
  LLTL_SECTION_NUMBER              CHAR "rtrim(:LLTL_SECTION_NUMBER)",
  LLTL_CONSOLIDATION_NUMBER        CHAR "rtrim(:LLTL_CONSOLIDATION_NUMBER)",
  LLTL_FOLIO_NUMBER                CHAR "rtrim(:LLTL_FOLIO_NUMBER)",
  LLTL_BOOK_NUMBER                 CHAR "rtrim(:LLTL_BOOK_NUMBER)",
  LLTL_BOOK_SEQUENCE_NUMBER        CHAR "rtrim(:LLTL_BOOK_SEQUENCE_NUMBER)",
  LLTL_CLOSED_DATE                 DATE "DD-MON-YYYY" NULLIF LLTL_CLOSED_DATE=blanks,
  LLTL_HRV_FLTC_CODE               CHAR "rtrim(:LLTL_HRV_FLTC_CODE)",
  LLTL_CLOSED_BY                   CHAR "rtrim(:LLTL_CLOSED_BY)",
  LLTL_NUM_PROPERTIES              CHAR "rtrim(:LLTL_NUM_PROPERTIES)",
  LLTL_NUM_PROPERTIES_OWNED        CHAR "rtrim(:LLTL_NUM_PROPERTIES_OWNED)",
  LLTL_LLTL_PLAN_NUMBER            CHAR "rtrim(:LLTL_LLTL_PLAN_NUMBER)",
  LLTL_LLTL_LOT_NUMBER             CHAR "rtrim(:LLTL_LLTL_LOT_NUMBER)",
  LLTL_LLTL_LTT_CODE               CHAR "rtrim(:LLTL_LLTL_LTT_CODE)",
  LLTL_COMMENTS                    CHAR "rtrim(:LLTL_COMMENTS)",
  LLTL_MODIFIED_BY                 CHAR "rtrim(:LLTL_MODIFIED_BY)",
  LLTL_MODIFIED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLTL_MODIFIED_DATE=blanks
)



