--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   14-DEC-2015  Control comments section added
--  1.2     6.13      AJ   24-FEB-2015  1)added LPPYT_PAR_PER_ALT_IND
--                                      and LPPYT_LAND_PAR_PER_ALT_IND
--                                      2)removed time stamp from payment
--                                      tra_effective and review dates
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_PREVENTION_PAYMENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPPYT_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LPPYT_DL_SEQNO                  RECNUM
,LPPYT_DL_LOAD_STATUS            CONSTANT "L"
,LPPYT_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LPPYT_ACAS_ALTERNATE_REF ))"
,LPPYT_PAYMENT_AMOUNT            CHAR "rtrim(UPPER(:LPPYT_PAYMENT_AMOUNT ))"
,LPPYT_PAYMENT_DATE              DATE "DD-MON-YYYY" NULLIF LPPYT_PAYMENT_DATE=blanks
,LPPYT_PPTP_CODE                 CHAR "rtrim(UPPER(:LPPYT_PPTP_CODE ))"
,LPPYT_PAYEE_TYPE                CHAR "rtrim(UPPER(:LPPYT_PAYEE_TYPE ))"
,LPPYT_HRV_HHPF_CODE             CHAR "rtrim(UPPER(:LPPYT_HRV_HHPF_CODE ))"
,LPPYT_HRV_HPPM_CODE             CHAR "rtrim(UPPER(:LPPYT_HRV_HPPM_CODE ))"
,LPPYT_SCO_CODE                  CHAR "rtrim(UPPER(:LPPYT_SCO_CODE ))"
,LPPYT_CREATED_BY                CHAR "rtrim(UPPER(:LPPYT_CREATED_BY ))"
,LPPYT_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPPYT_CREATED_DATE=blanks
,LPPYT_STATUS_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPPYT_STATUS_DATE=blanks
,LPPYT_PAR_PER_ALT_REF           CHAR "rtrim(UPPER(:LPPYT_PAR_PER_ALT_REF ))"
,LPPYT_PAR_ORG_ALT_REF           CHAR "rtrim(UPPER(:LPPYT_PAR_ORG_ALT_REF ))"
,LPPYT_ACPE_PAR_PER_ALT_REF      CHAR "rtrim(UPPER(:LPPYT_ACPE_PAR_PER_ALT_REF ))"
,LPPYT_LAND_PAR_PER_ALT_REF      CHAR "rtrim(UPPER(:LPPYT_LAND_PAR_PER_ALT_REF ))"
,LPPYT_COMMENTS                  CHAR(2000) "rtrim(UPPER(:LPPYT_COMMENTS ))"
,LPPYT_ALTERNATIVE_REFERENCE     CHAR "rtrim(UPPER(:LPPYT_ALTERNATIVE_REFERENCE ))"
,LPPYT_PAY_REF                   CHAR "rtrim(UPPER(:LPPYT_PAY_REF ))"
,LPPYT_TRA_EFFECTIVE_DATE        DATE "DD-MON-YYYY" NULLIF LPPYT_TRA_EFFECTIVE_DATE=blanks
,LPPYT_TRA_TRT_CODE              CHAR "rtrim(UPPER(:LPPYT_TRA_TRT_CODE ))"
,LPPYT_TRA_TRS_CODE              CHAR "rtrim(UPPER(:LPPYT_TRA_TRS_CODE ))"
,LPPYT_AUTHORISED_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPPYT_AUTHORISED_DATE=blanks
,LPPYT_AUTHORISED_BY             CHAR "rtrim(UPPER(:LPPYT_AUTHORISED_BY ))"
,LPPYT_HRV_HPCR_CODE             CHAR "rtrim(UPPER(:LPPYT_HRV_HPCR_CODE ))"
,LPPYT_REVIEW_DATE               DATE "DD-MON-YYYY" NULLIF LPPYT_REVIEW_DATE=blanks
,LPPYT_ACHO_LEGACY_REF           CHAR "rtrim(UPPER(:LPPYT_ACHO_LEGACY_REF ))"
,LPPYT_AUN_CODE                  CHAR "rtrim(UPPER(:LPPYT_AUN_CODE ))"
,LPPYT_IPP_SHORTNAME             CHAR "rtrim(UPPER(:LPPYT_IPP_SHORTNAME ))"
,LPPYT_PAR_PER_ALT_IND           CHAR "rtrim(UPPER(:LPPYT_PAR_PER_ALT_IND ))"
,LPPYT_LAND_PAR_PER_ALT_IND      CHAR "rtrim(UPPER(:LPPYT_LAND_PAR_PER_ALT_IND ))"
,LPPYT_REFNO                     INTEGER EXTERNAL "ppyt_refno_seq.nextval"
)






