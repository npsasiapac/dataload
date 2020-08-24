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
--  1.2     6.13      AJ   25-FEB-2016  Removed time stamp from LACQR_DATE_VALUE
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_ADV_CASE_QUESTN_RESP
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LACQR_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LACQR_DL_SEQNO                  RECNUM
,LACQR_DL_LOAD_STATUS            CONSTANT "L"
,LACQR_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LACQR_ACAS_ALTERNATE_REF))"
,LACQR_CAQU_REFERENCE            CHAR "rtrim(UPPER(:LACQR_CAQU_REFERENCE))"
,LACQR_TYPE                      CHAR "rtrim(UPPER(:LACQR_TYPE))"
,LACQR_CQRS_CODE                 CHAR "rtrim(UPPER(:LACQR_CQRS_CODE))"
,LACQR_BOOLEAN_VALUE             CHAR "rtrim(UPPER(:LACQR_BOOLEAN_VALUE))"
,LACQR_TEXT_VALUE                CHAR "rtrim(UPPER(:LACQR_TEXT_VALUE))"
,LACQR_DATE_VALUE                DATE "DD-MON-YYYY" NULLIF LACQR_DATE_VALUE=blanks
,LACQR_NUMBER_VALUE              CHAR "rtrim(UPPER(:LACQR_NUMBER_VALUE))"
,LACQR_CREATED_BY                CHAR "rtrim(UPPER(:LACQR_CREATED_BY))"
,LACQR_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACQR_CREATED_DATE=blanks
,LACQR_ADDITIONAL_RESPONSE       CHAR "rtrim(UPPER(:LACQR_ADDITIONAL_RESPONSE))"
,LACQR_ACHO_LEGACY_REF           CHAR "rtrim(UPPER(:LACQR_ACHO_LEGACY_REF))"
,LACQR_REFNO                     INTEGER EXTERNAL "acqr_refno_seq.nextval"
)


