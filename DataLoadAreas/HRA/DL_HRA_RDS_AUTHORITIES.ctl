-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
--
-- 2.0          V.Shah      28-Jun-10  Defect 5159 Fix. Addition of
--                                     Modified By/Date
-- 2.1          T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--                                     Added LRDSA_HRV_BSRC_CODE
--
-- 2.2          V.Shah      19-Dec-18  Added LRDSA_DVA_UIN column for 6.18 Change
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_AUTHORITIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LRDSA_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LRDSA_DL_SEQNO                   RECNUM
,LRDSA_DL_LOAD_STATUS             CONSTANT "L"
,LRDSA_HA_REFERENCE               CHAR "rtrim(UPPER(:LRDSA_HA_REFERENCE))"
,LRDSA_PAR_PER_ALT_REF            CHAR "rtrim(UPPER(:LRDSA_PAR_PER_ALT_REF))"
,LRDSA_HRV_RPAG_CODE              CHAR "rtrim(UPPER(:LRDSA_HRV_RPAG_CODE))"
,LRDSA_PAY_AGENCY_CRN             CHAR "rtrim(UPPER(:LRDSA_PAY_AGENCY_CRN))"
,LRDSA_START_DATE                 DATE "DD-MON-YYYY" NULLIF LRDSA_START_DATE=blanks
,LRDSA_STATUS_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDSA_STATUS_DATE=blanks
,LRDSA_SCO_CODE                   CHAR "rtrim(UPPER(:LRDSA_SCO_CODE))"
,LRDSA_CREATED_BY                 CHAR "rtrim(UPPER(:LRDSA_CREATED_BY))"
,LRDSA_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDSA_CREATED_DATE=blanks
,LRDSA_PENDING_SCO_CODE           CHAR "rtrim(UPPER(:LRDSA_PENDING_SCO_CODE))"
,LRDSA_END_DATE                   DATE "DD-MON-YYYY" NULLIF LRDSA_END_DATE=blanks
,LRDSA_SUSPEND_FROM_DATE          DATE "DD-MON-YYYY" NULLIF LRDSA_SUSPEND_FROM_DATE=blanks
,LRDSA_SUSPEND_TO_DATE            DATE "DD-MON-YYYY" NULLIF LRDSA_SUSPEND_TO_DATE=blanks
,LRDSA_ACTION_SENT_DATETIME       DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDSA_ACTION_SENT_DATETIME=blanks
,LRDSA_HRV_SUSR_CODE              CHAR "rtrim(UPPER(:LRDSA_HRV_SUSR_CODE))"
,LRDSA_HRV_TERR_CODE              CHAR "rtrim(UPPER(:LRDSA_HRV_TERR_CODE))"
,LRDSA_MODIFIED_BY                CHAR "rtrim(UPPER(:LRDSA_MODIFIED_BY))"
,LRDSA_MODIFIED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDSA_MODIFIED_DATE=blanks
,LRDSA_HRV_BSRC_CODE			  CHAR "rtrim(UPPER(:LRDSA_HRV_BSRC_CODE))"
,LRDSA_DVA_UIN                    CHAR "rtrim(UPPER(:LRDSA_DVA_UIN))"
,LRDSA_REFNO                      INTEGER EXTERNAL "rdsa_seq.nextval"
)


