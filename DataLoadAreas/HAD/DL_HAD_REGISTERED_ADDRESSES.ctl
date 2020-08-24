--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO          WHEN         WHY
--  1.0               MK           08/08/13     Initial Creation
--  1.1               M Millington 19/12/2013   Corrected syntax error with all Address variables
--  1.2               PJD          31/01/14     Renamed to DL_HAD_REGISTERED_ADDRESSES.ctl
--  1.3     6.10      AJ           14/12/15     tied up change control v610 HAD data load
--  1.4     6.13      AJ           25/02/16     Amended to all datetime for LREGA_CREATED_DATE
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_REGISTERED_ADDRESSES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LREGA_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LREGA_DL_SEQNO                  RECNUM
,LREGA_DL_LOAD_STATUS            CONSTANT "L"
,LREGA_LEGACY_REF                CHAR "rtrim(UPPER(:LREGA_LEGACY_REF))"
,LREGA_ADRE_CODE                 CHAR "rtrim(UPPER(:LREGA_ADRE_CODE))"
,LREGA_START_DATE                DATE "DD-MON-YYYY" NULLIF LREGA_START_DATE=blanks
,LREGA_END_DATE                  DATE "DD-MON-YYYY" NULLIF LREGA_END_DATE=blanks
,LREGA_HRV_RAE_CODE              CHAR "rtrim(UPPER(:LREGA_HRV_RAE_CODE))"
,LREGA_PROPOSED_END_DATE         DATE "DD-MON-YYYY" NULLIF LREGA_PROPOSED_END_DATE=blanks
,LREGA_AUN_CODE                  CHAR "rtrim(UPPER(:LREGA_AUN_CODE))"
,LREGA_COMMENTS                  CHAR "rtrim(:LREGA_COMMENTS)"
,LREGA_CREATED_BY                CHAR "rtrim(UPPER(:LREGA_CREATED_BY))"
,LREGA_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LREGA_CREATED_DATE=blanks
,LREGA_ADR_FLAT                  CHAR "rtrim(upper(:LREGA_ADR_FLAT))"
,LREGA_ADR_BUILDING              CHAR "rtrim(upper(:LREGA_ADR_BUILDING))"
,LREGA_ADR_STREET_NUMBER         CHAR "rtrim(upper(:LREGA_ADR_STREET_NUMBER))"
,LREGA_AEL_STREET_INDEX_CODE     CHAR "rtrim(upper(:LREGA_AEL_STREET_INDEX_CODE))"
,LREGA_AEL_STREET                CHAR "rtrim(upper(:LREGA_AEL_STREET))"
,LREGA_AEL_AREA                  CHAR "rtrim(upper(:LREGA_AEL_AREA))"
,LREGA_AEL_TOWN                  CHAR "rtrim(upper(:LREGA_AEL_TOWN))"
,LREGA_AEL_COUNTY                CHAR "rtrim(upper(:LREGA_AEL_COUNTY))"
,LREGA_AEL_COUNTRY               CHAR "rtrim(upper(:LREGA_AEL_COUNTRY))"
,LREGA_AEL_POSTCODE              CHAR "rtrim(upper(:LREGA_AEL_POSTCODE))"
,LREGA_AEL_LOCAL_IND             CHAR "rtrim(upper(:LREGA_AEL_LOCAL_IND))"
,LREGA_AEL_ABROAD_IND            CHAR "rtrim(upper(:LREGA_AEL_ABROAD_IND))"
,LREGA_ADR_EASTINGS              CHAR "rtrim(upper(:LREGA_ADR_EASTINGS))"
,LREGA_ADR_NORTHINGS             CHAR "rtrim(upper(:LREGA_ADR_NORTHINGS))"
,LREGA_ADR_UPRN                  CHAR "rtrim(upper(:LREGA_ADR_UPRN))"
)