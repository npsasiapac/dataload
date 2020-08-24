-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 2.0          V.Shah      13-May-10  Defect 4517 addition of Modified By/Date
--
-- 3.0          P.Hearty    29-JUN-11  New field LSUAP_NEXT_SCHED_REVIEW_DATE
--                                     and now selects the suap_reference
--                                     sequence number.
--
-- 4.0          V.Shah      17-OCT-11  Added missing LSUAP_APP_LEGACY_REF
-- 4.1          A.Jones     12-OCT-17  Amended SUAP_START_DATE as date only
--                                     Amended LSUAP_RECEIVED_DATE as date only
--                                     Amended LSUAP_END_DATE as date only
--                                     Amended LSUAP_CHECKED_DATE as date only
--                                     Amended LSUAP_NEXT_SCHED_REVIEW_DATE as date only
--                                     Added LSUAP_AUN_CODE
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_SUBSIDY_APPLICATIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LSUAP_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LSUAP_DL_SEQNO                   RECNUM
,LSUAP_DL_LOAD_STATUS             CONSTANT "L"
,LSUAP_LEGACY_REF                 CHAR "rtrim(UPPER(:LSUAP_LEGACY_REF))"
,LSUAP_TCY_ALT_REF                CHAR "rtrim(UPPER(:LSUAP_TCY_ALT_REF))"
,LSUAP_START_DATE                 DATE "DD-MON-YYYY" NULLIF LSUAP_START_DATE=blanks
,LSUAP_RECEIVED_DATE              DATE "DD-MON-YYYY" NULLIF LSUAP_RECEIVED_DATE=blanks
,LSUAP_SCO_CODE                   CHAR "rtrim(UPPER(:LSUAP_SCO_CODE))"
,LSUAP_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSUAP_CREATED_DATE=blanks
,LSUAP_CREATED_BY                 CHAR "rtrim(UPPER(:LSUAP_CREATED_BY))"
,LSUAP_END_DATE                   DATE "DD-MON-YYYY" NULLIF LSUAP_END_DATE=blanks
,LSUAP_CHECKED_DATE               DATE "DD-MON-YYYY" NULLIF LSUAP_CHECKED_DATE=blanks
,LSUAP_CHECKED_BY                 CHAR "rtrim(UPPER(:LSUAP_CHECKED_BY))"
,LSUAP_MODIFIED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSUAP_MODIFIED_DATE=blanks
,LSUAP_MODIFIED_BY                CHAR "rtrim(UPPER(:LSUAP_MODIFIED_BY))"
,LSUAP_HRV_ASCA_CODE              CHAR "rtrim(UPPER(:LSUAP_HRV_ASCA_CODE))"
,LSUAP_HRV_HSTR_CODE              CHAR "rtrim(UPPER(:LSUAP_HRV_HSTR_CODE))"
,LSUAP_APP_LEGACY_REF             CHAR "rtrim(UPPER(:LSUAP_APP_LEGACY_REF))"
,LSUAP_NEXT_SCHED_REVIEW_DATE     DATE "DD-MON-YYYY" NULLIF LSUAP_NEXT_SCHED_REVIEW_DATE=blanks
,LSUAP_AUN_CODE                   CHAR "rtrim(UPPER(:LSUAP_AUN_CODE))"
,LSUAP_REFERENCE                  INTEGER EXTERNAL "SUAP_REFERENCE_SEQ.nextval"
)
