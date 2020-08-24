--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     6.16      AJ   10-OCT-2018  Change Control added with original created
--                                      date for QAus/SAHT (DB)
--  1.1     6.16      AJ   26-MAR-2019  (2000) added to both comments fields
--                                      otherwise length is limited to 255
--
--***********************************************************************
--
load data
APPEND
into table DL_HCS_CONTACTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LCCT_DLB_BATCH_ID          	CONSTANT "$batch_no",
 LCCT_DL_SEQNO              	RECNUM,
 LCCT_DL_LOAD_STATUS        	CONSTANT "L",
 LCCT_OBJ_LEGACY_REF            CHAR "rtrim(:LCCT_OBJ_LEGACY_REF)",
 LCCT_SECONDARY_REF             CHAR "rtrim(:LCCT_SECONDARY_REF)",
 LCCT_SECONDARY_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCCT_SECONDARY_DATE=blanks,
 LCCT_LEGACY_TYPE               CHAR "rtrim(:LCCT_LEGACY_TYPE)",
 LCCT_RECEIVED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCCT_RECEIVED_DATE=blanks,
 LCCT_CCM_CODE                  CHAR "rtrim(:LCCT_CCM_CODE)",
 LCCT_CNY_CODE                  CHAR "rtrim(:LCCT_CNY_CODE)",
 LCCT_SUB_LEGACY_REF            CHAR "rtrim(:LCCT_SUB_LEGACY_REF)",
 LCCT_SUB_SEC_REF               CHAR "rtrim(:LCCT_SUB_SEC_REF)",
 LCCT_SUB_SEC_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCCT_SUB_SEC_DATE=blanks,
 LCCT_SUBJECT_TYPE              CHAR "rtrim(:LCCT_SUBJECT_TYPE)",
 LCCT_SCN_BRO_CODE              CHAR "rtrim(:LCCT_SCN_BRO_CODE)",
 LCCT_SCN_BRC_CODE              CHAR "rtrim(:LCCT_SCN_BRC_CODE)",
 LCCT_SCN_TARGET_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCCT_SCN_TARGET_DATE=blanks,
 LCCT_DURATION                  CHAR "rtrim(:LCCT_DURATION)",
 LCCT_AUN_CODE_RESPONSIBLE      CHAR "rtrim(:LCCT_AUN_CODE_RESPONSIBLE)",
 LCCT_SCO_CODE                  CHAR "rtrim(:LCCT_SCO_CODE)",
 LCCT_STATUS_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCCT_STATUS_DATE=blanks,
 LCCT_CREATED_BY                CHAR "rtrim(:LCCT_CREATED_BY)",
 LCCT_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCCT_CREATED_DATE=blanks,
 LCCT_COMMENTS                  CHAR(2000) "rtrim(:LCCT_COMMENTS)",
 LCCT_USR_JRO_CODE              CHAR "rtrim(:LCCT_USR_JRO_CODE)",
 LCCT_USR_USERNAME              CHAR "rtrim(:LCCT_USR_USERNAME)",
 LCCT_CORRESPOND_REFERENCE      CHAR "rtrim(:LCCT_CORRESPOND_REFERENCE)",
 LCCT_ANSWERED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCCT_ANSWERED_DATE=blanks,
 LCCT_ANO_CODE                  CHAR "rtrim(:LCCT_ANO_CODE)",
 LCCT_OUTCOME_COMMENTS          CHAR(2000) "rtrim(:LCCT_OUTCOME_COMMENTS)",
 LCCT_BAN_ALT_REF               CHAR "rtrim(:LCCT_BAN_ALT_REF)"
)
