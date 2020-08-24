--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   11-DEC-2015  Control comments section added
--  1.2     6.13      AJ   24-FEB-2016  removed time-stamp option from both
--                                      start dates as no applicable
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_ADVICE_CASE_HSG_OPTN
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LACHO_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LACHO_DL_SEQNO                  RECNUM
,LACHO_DL_LOAD_STATUS            CONSTANT "L"
,LACHO_LEGACY_REF                CHAR "rtrim(UPPER(:LACHO_LEGACY_REF))"
,LACHO_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LACHO_ACAS_ALTERNATE_REF))"
,LACHO_ARCS_ARSN_CODE            CHAR "rtrim(UPPER(:LACHO_ARCS_ARSN_CODE))"
,LACHO_HOOP_CODE                 CHAR "rtrim(UPPER(:LACHO_HOOP_CODE))"
,LACHO_HODS_HRV_DEST_CODE        CHAR "rtrim(UPPER(:LACHO_HODS_HRV_DEST_CODE))"
,LACHO_STATUS_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACHO_STATUS_DATE=blanks
,LACHO_HOAU_AUN_CODE             CHAR "rtrim(UPPER(:LACHO_HOAU_AUN_CODE))"
,LACHO_CREATED_BY                CHAR "rtrim(UPPER(:LACHO_CREATED_BY))"
,LACHO_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACHO_CREATED_DATE=blanks
,LACHO_COMMENTS                  CHAR(2000) "rtrim(:LACHO_COMMENTS)"
,LACHO_SAS_IND                   CHAR "rtrim(:LACHO_SAS_IND)"
,LACHO_RAC_PAY_REF               CHAR "rtrim(:LACHO_RAC_PAY_REF)"
,LACHO_START_DATE                DATE "DD-MON-YYYY" NULLIF LACHO_START_DATE=blanks
,LACHO_NEXT_PAY_PERIOD_START     DATE "DD-MON-YYYY" NULLIF LACHO_NEXT_PAY_PERIOD_START=blanks
,LACHO_REFERENCE                 INTEGER EXTERNAL "acho_reference_seq.nextval"
)
