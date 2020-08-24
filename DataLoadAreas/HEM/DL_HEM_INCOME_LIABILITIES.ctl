--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.11      AJ   22-Dec-2015  Initial Creation as new functionality
--                                      released at v611 income liabilities
--  1.1     6.13      AJ   22-Mar-2016  LINLI_PAR_REF_TYPE and LINLI_PAR_REF_VALUE
--                                      removed as not required
--
--***********************************************************************
--
-- Bespoke Income Data Load NonICS v6.11
--
load data
APPEND
into table DL_HEM_INCOME_LIABILITIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LINLI_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LINLI_DL_SEQNO                   RECNUM
,LINLI_DL_LOAD_STATUS             CONSTANT "L"
,LINLI_LEGACY_REF                 CHAR "rtrim(UPPER(:LINLI_LEGACY_REF))"
,LINLI_INH_LEGACY_REF             CHAR "rtrim(UPPER(:LINLI_INH_LEGACY_REF))"
,LINLI_ILR_CODE                   CHAR "rtrim(UPPER(:LINLI_ILR_CODE))"
,LINLI_LIABLE_PERCENT             CHAR "rtrim(UPPER(:LINLI_LIABLE_PERCENT))"
,LINLI_CREATED_BY                 CHAR "rtrim(UPPER(:LINLI_CREATED_BY))"
,LINLI_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINLI_CREATED_DATE=blanks
,LINLI_PAYMENT_AMOUNT             CHAR "rtrim(UPPER(:LINLI_PAYMENT_AMOUNT))"
,LINLI_INF_CODE                   CHAR "rtrim(UPPER(:LINLI_INF_CODE))"
,LINLI_REGULAR_AMOUNT             CHAR "rtrim(UPPER(:LINLI_REGULAR_AMOUNT))"
,LINLI_HRV_VETY_CODE              CHAR "rtrim(UPPER(:LINLI_HRV_VETY_CODE))"
,LINLI_CREDITOR                   CHAR "rtrim(UPPER(:LINLI_CREDITOR))"
,LINLI_SECURED_IND                CHAR "rtrim(UPPER(:LINLI_SECURED_IND))"
,LINLI_BALANCE                    CHAR "rtrim(UPPER(:LINLI_BALANCE))"
,LINLI_MATURITY_DATE              DATE "DD-MON-YYYY" NULLIF LINLI_MATURITY_DATE=blanks
,LINLI_COMMENTS                   CHAR(2000) "rtrim(:LINLI_COMMENTS)"
,LINLI_MODIFIED_BY                CHAR "rtrim(UPPER(:LINDT_MODIFIED_BY))"
,LINLI_MODIFIED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LINDT_MODIFIED_DATE=blanks
,LINLI_REFNO                      INTEGER EXTERNAL "inli_refno_seq.nextval"
)


