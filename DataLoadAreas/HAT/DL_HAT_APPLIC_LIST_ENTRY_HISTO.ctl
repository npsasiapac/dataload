--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    PH   02-MAR-2004  Initial Creation.
--  1.1     6.13      AJ   29-FEB-2016  Control comments section added
--                                      time stamp added to LLEH_CREATED_DATE
--                                      LLEH_MODIFED_DATE
--                                      
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_APPLIC_LIST_ENTRY_HISTO
fields terminated by "," optionally enclosed by '"'
trailing nullcols
( LLEH_DLB_BATCH_ID                CONSTANT "$batch_no",              
  LLEH_DL_SEQNO                    RECNUM,
  LLEH_DL_LOAD_STATUS              CONSTANT "L", 
  LLEH_ALT_REF                     CHAR "rtrim(:LLEH_ALT_REF)",
  LLEH_RLI_CODE                    CHAR "rtrim(:LLEH_RLI_CODE)",
  LLEH_TYPE_IND                    CHAR "rtrim(:LLEH_TYPE_IND)",
  LLEH_LST_CODE                    CHAR "rtrim(:LLEH_LST_CODE)",
  LLEH_CREATED_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLEH_CREATED_DATE =blanks,
  LLEH_CREATED_BY                  CHAR "rtrim(:LLEH_CREATED_BY)",
  LLEH_MODIFED_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLEH_MODIFED_DATE =blanks,
  LLEH_MODIFIED_BY                 CHAR "rtrim(:LLEH_MODIFIED_BY)",
  LLEH_ACTION_IND                  CHAR "rtrim(:LLEH_ACTION_IND)",
  LLEH_ALS_ACTIVE_IND              CHAR "rtrim(:LLEH_ALS_ACTIVE_IND)",
  LLEH_REGISTERED_DATE             DATE "DD-MON-YYYY" NULLIF LLEH_REGISTERED_DATE =blanks,
  LLEH_HTY_CODE                    CHAR "rtrim(:LLEH_HTY_CODE)",
  LLEH_MODEL_HTY_CODE              CHAR "rtrim(:LLEH_MODEL_HTY_CODE)",
  LLEH_STATUS_START_DATE           DATE "DD-MON-YYYY" NULLIF LLEH_STATUS_START_DATE =blanks,
  LLEH_STATUS_REVIEW_DATE          DATE "DD-MON-YYYY" NULLIF LLEH_STATUS_REVIEW_DATE =blanks,
  LLEH_REREG_BY_DATE               DATE "DD-MON-YYYY" NULLIF LLEH_REREG_BY_DATE =blanks,
  LLEH_CPR_PRI                     CHAR "rtrim(:LLEH_CPR_PRI)",
  LLEH_BECAME_ACTIVE_DATE          DATE "DD-MON-YYYY" NULLIF LLEH_BECAME_ACTIVE_DATE =blanks,
  LLEH_APPLICATION_CATEGORY        CHAR "rtrim(:LLEH_APPLICATION_CATEGORY)",
  LLEH_LIST_REASON_QUALIFICATION   CHAR "rtrim(:LLEH_LIST_REASON_QUALIFICATION)",
  LLEH_APPLICATION_STATUS_REASON   CHAR "rtrim(:LLEH_APPLICATION_STATUS_REASON)")
