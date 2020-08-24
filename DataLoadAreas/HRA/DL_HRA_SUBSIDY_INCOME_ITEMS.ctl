-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
-- 01.01        M Tapsell   18-0ay-2011 Added par per alt ref
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_SUBSIDY_INCOME_ITEMS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LSUIT_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LSUIT_DL_SEQNO                   RECNUM
,LSUIT_DL_LOAD_STATUS             CONSTANT "L"
,LSUIT_SURV_LEGACY_REF            CHAR "rtrim(UPPER(:LSUIT_SURV_LEGACY_REF))"
,LSUIT_PAR_REFNO                  CHAR "rtrim(UPPER(:LSUIT_PAR_REFNO))"
,LSUIT_PAR_PER_ALT_REF            CHAR "rtrim(UPPER(:LSUIT_PAR_PER_ALT_REF))"
,LSUIT_HSIT_CODE                  CHAR "rtrim(UPPER(:LSUIT_HSIT_CODE))"
,LSUIT_ELIGIBILITY_AMOUNT         CHAR "rtrim(UPPER(:LSUIT_ELIGIBILITY_AMOUNT))"
,LSUIT_SUBSIDY_CALC_AMOUNT        CHAR "rtrim(UPPER(:LSUIT_SUBSIDY_CALC_AMOUNT))"
,LSUIT_OVERRIDDEN_INCOME_IND      CHAR "rtrim(UPPER(:LSUIT_OVERRIDDEN_INCOME_IND))"
,LSUIT_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSUIT_CREATED_DATE=blanks
,LSUIT_CREATED_BY                 CHAR "rtrim(UPPER(:LSUIT_CREATED_BY))"
,LSUIT_RENT_PAYABLE_CONTRIB       CHAR "rtrim(UPPER(:LSUIT_RENT_PAYABLE_CONTRIB))"
,LSUIT_SUAR_CODE                  CHAR "rtrim(UPPER(:LSUIT_SUAR_CODE))"
,LSUIT_PERCENTAGE                 CHAR "rtrim(UPPER(:LSUIT_PERCENTAGE))"
,LSUIT_SUAR_SUBP_SEQ              CHAR "rtrim(UPPER(:LSUIT_SUAR_SUBP_SEQ))"
,LSUIT_SUAR_SUBP_ASCA_CODE        CHAR "rtrim(UPPER(:LSUIT_SUAR_SUBP_ASCA_CODE))"
,LSUIT_SIOR_CODE                  CHAR "rtrim(UPPER(:LSUIT_SIOR_CODE))"
)



