-- *********************************************************************
--
-- Version      Who   date         Why
-- 01.00        AJ    10-OCT-2016  Initial Creation WITH Change Control
--                                 as format tied up and dates checked
--                                 Amended the following as no time requiredLGRSR_EFFECTIVE_DATE
--                                 against them LGRSR_EFFECTIVE_DATE,
--                                 LGRSR_ASSESSMENT_DATE,LGRSR_LATEST_LAST_REVIEW_DATE,
--                                 LGRSR_GENERATED_DATE
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_GROUP_SUBSIDY_REVIEWS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LGRSR_DLB_BATCH_ID             CONSTANT "$batch_no",
LGRSR_DL_SEQNO                 RECNUM,
LGRSR_DL_LOAD_STATUS           CONSTANT "L",
LGRSR_USER_REFERENCE           CHAR "rtrim(:LGRSR_USER_REFERENCE)",
LGRSR_EFFECTIVE_DATE           DATE "DD-MON-YYYY" NULLIF LGRSR_EFFECTIVE_DATE=blanks,
LGRSR_ASSESSMENT_DATE          DATE "DD-MON-YYYY" NULLIF LGRSR_ASSESSMENT_DATE=blanks,
LGRSR_SCO_CODE                 CHAR "rtrim(:LGRSR_SCO_CODE)",
LGRSR_ISSUE_ICS_REQUESTS_IND   CHAR "rtrim(:LGRSR_ISSUE_ICS_REQUESTS_IND)",
LGRSR_ISSUE_INCOME_CERTIF_IND  CHAR "rtrim(:LGRSR_ISSUE_INCOME_CERTIF_IND)",
LGRSR_CREATED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LGRSR_CREATED_DATE=blanks,
LGRSR_CREATED_BY               CHAR "rtrim(:LGRSR_CREATED_BY)",
LGRSR_REMINDER_NUM_DAYS        CHAR "rtrim(:LGRSR_REMINDER_NUM_DAYS)",
LGRSR_TERMINATE_NUM_DAYS       CHAR "rtrim(:LGRSR_TERMINATE_NUM_DAYS)",
LGRSR_LATEST_LAST_REVIEW_DATE  DATE "DD-MON-YYYY" NULLIF LGRSR_LATEST_LAST_REVIEW_DATE=blanks,
LGRSR_HRV_ASCA_CODE            CHAR "rtrim(:LGRSR_HRV_ASCA_CODE)",
LGRSR_AUN_CODE                 CHAR "rtrim(:LGRSR_AUN_CODE)",
LGRSR_TTY_CODE                 CHAR "rtrim(:LGRSR_TTY_CODE)",
LGRSR_MODIFIED_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LGRSR_MODIFIED_DATE=blanks,
LGRSR_MODIFIED_BY              CHAR "rtrim(:LGRSR_MODIFIED_BY)",
LGRSR_GENERATED_DATE           DATE "DD-MON-YYYY" NULLIF LGRSR_GENERATED_DATE=blanks,
LGRSR_REFNO                    INTEGER EXTERNAL "GRSR_REFNO_SEQ.nextval"
)
