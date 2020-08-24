-- *********************************************************************
--
-- Version      Who         Date       Why
-- 1.0          Ian Rowell  25-Sep-09  Initial Creation
--
-- 1.1          V. Shah     20-Oct-09  Added LSURV_GRSR_USER_REFERENCE
--
-- 1.2          P. Hearty   29-JUN-11  Added new fields and
--                                     now gets surv_refno_seq
--
-- 1.3          V. Shah     17-OCT-11  Corrected LSURV_TCY_APP_ALT_REF
--
-- 1.4          M.Tapsell   01-NOV-11  Added LSURV_CAP_AMOUNT and TYPE for v19 spec
-- 1.5          AJ          11-OCT-17  Added LSURV_PAY_MARKET_RENT_IND after review
--                                     done for MB
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_SUBSIDY_REVIEWS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LSURV_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LSURV_DL_SEQNO                   RECNUM
,LSURV_DL_LOAD_STATUS             CONSTANT "L"
,LSURV_SUAP_LEGACY_REF            CHAR "rtrim(UPPER(:LSURV_SUAP_LEGACY_REF))"
,LSURV_LEGACY_REF                 CHAR "rtrim(UPPER(:LSURV_LEGACY_REF))"
,LSURV_TCY_APP_ALT_REF            CHAR "rtrim(UPPER(:LSURV_TCY_APP_ALT_REF))"
,LSURV_SUAP_START_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSURV_SUAP_START_DATE=blanks
,LSURV_CLASS_CODE                 CHAR "rtrim(UPPER(:LSURV_CLASS_CODE))"
,LSURV_EFFECTIVE_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSURV_EFFECTIVE_DATE=blanks
,LSURV_ASSESSMENT_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSURV_ASSESSMENT_DATE=blanks
,LSURV_DEN_ELIGIBLE_IND           CHAR "rtrim(UPPER(:LSURV_DEN_ELIGIBLE_IND))"
,LSURV_SUBP_ASCA_CODE             CHAR "rtrim(UPPER(:LSURV_SUBP_ASCA_CODE))"
,LSURV_SUBP_SEQ                   CHAR "rtrim(UPPER(:LSURV_SUBP_SEQ))"
,LSURV_SCO_CODE                   CHAR "rtrim(UPPER(:LSURV_SCO_CODE))"
,LSURV_HSRR_CODE                  CHAR "rtrim(UPPER(:LSURV_HSRR_CODE))"
,LSURV_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSURV_CREATED_DATE=blanks
,LSURV_CREATED_BY                 CHAR "rtrim(UPPER(:LSURV_CREATED_BY))"
,LSURV_END_DATE                   DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSURV_END_DATE=blanks
,LSURV_AUTHORISED_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSURV_AUTHORISED_DATE=blanks
,LSURV_AUTHORISED_BY              CHAR "rtrim(UPPER(:LSURV_AUTHORISED_BY))"
,LSURV_DEFAULT_PERCENTAGE         CHAR "rtrim(UPPER(:LSURV_DEFAULT_PERCENTAGE))"
,LSURV_DEN_SUB_UNROUNDED_AMT      CHAR "rtrim(UPPER(:LSURV_DEN_SUB_UNROUNDED_AMT))"
,LSURV_DEN_HHOLD_ASSESS_INC       CHAR "rtrim(UPPER(:LSURV_DEN_HHOLD_ASSESS_INC))"
,LSURV_DEN_SUBSIDY_AMOUNT         CHAR "rtrim(UPPER(:LSURV_DEN_SUBSIDY_AMOUNT))"
,LSURV_DEN_CALC_RENT_PAYABLE      CHAR "rtrim(UPPER(:LSURV_DEN_CALC_RENT_PAYABLE))"
,LSURV_DEN_TCY_MARKET_RENT        CHAR "rtrim(UPPER(:LSURV_DEN_TCY_MARKET_RENT))"
,LSURV_ASSESSED_SELB_CODE         CHAR "rtrim(UPPER(:LSURV_ASSESSED_SELB_CODE))"
,LSURV_HSCR_CODE                  CHAR "rtrim(UPPER(:LSURV_HSCR_CODE))"
,LSURV_ACHO_LEGACY_REF            CHAR "rtrim(UPPER(:LSURV_ACHO_LEGACY_REF))"
,LSURV_GRSR_USER_REFERENCE        CHAR "rtrim(:LSURV_GRSR_USER_REFERENCE)"
,LSURV_HSRS_CODE                  CHAR "rtrim(UPPER(:LSURV_HSRS_CODE))"
,LSURV_HTY_CODE                   CHAR "rtrim(UPPER(:LSURV_HTY_CODE))"
,LSURV_REVIEW_INITIATED_DATE      DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSURV_REVIEW_INITIATED_DATE=blanks
,LSURV_DETAILS_RECEIVED_DATE      DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSURV_DETAILS_RECEIVED_DATE=blanks
,LSURV_SUDA_LEGACY_REF            CHAR "rtrim(:LSURV_SUDA_LEGACY_REF)"
,LSURV_CAP_AMOUNT                 CHAR "rtrim(UPPER(:LSURV_CAP_AMOUNT))"
,LSURV_CAP_TYPE                   CHAR "rtrim(UPPER(:LSURV_CAP_TYPE))"
,LSURV_PAY_MARKET_RENT_IND        CHAR "rtrim(UPPER(:LSURV_PAY_MARKET_RENT_IND))"
,LSURV_REFNO                      INTEGER EXTERNAL "SURV_REFNO_SEQ.nextval"
)
