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
--
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_ADV_CASE_HSG_OPTN_HIS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LACHH_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LACHH_DL_SEQNO                  RECNUM
,LACHH_DL_LOAD_STATUS            CONSTANT "L"
,LACHH_ACHO_LEGACY_REF           CHAR "rtrim(UPPER(:LACHH_ACHO_LEGACY_REF))"
,LACHH_HOAU_AUN_CODE             CHAR "rtrim(UPPER(:LACHH_HOAU_AUN_CODE ))"
,LACHH_HODS_HRV_DEST_CODE        CHAR "rtrim(UPPER(:LACHH_HODS_HRV_DEST_CODE))"
,LACHH_STATUS_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACHH_STATUS_DATE=blanks
,LACHH_CREATED_BY                CHAR "rtrim(UPPER(:LACHH_CREATED_BY))"
,LACHH_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACHH_CREATED_DATE=blanks
,LACHH_REFNO                     INTEGER EXTERNAL "achh_refno_seq.nextval"
)



