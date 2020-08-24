--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.0     6.8       MK   08-AUG-2015  amended removed real_ipp_shortname and
--                                      real_ipt_code
--  1.1     6.10      AJ   14-DEC-2015  Control comments section added
--  1.2     6.13      AJ   25-FEB-2016  amended to remove time stamp options from
--                                      LREAL_REGA_START_DATE LREAL_STATUS_DATE
--                                      LREAL_START_DATE LREAL_END_DATE
--                                      LREAL_PROPOSED_END_DATE
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_REG_ADDRESS_LETTINGS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LREAL_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LREAL_DL_SEQNO                  RECNUM
,LREAL_DL_LOAD_STATUS            CONSTANT "L"
,LREAL_REGA_LEGACY_REF           CHAR "rtrim(UPPER(:LREAL_REGA_LEGACY_REF))"
,LREAL_REGA_ADRE_CODE            CHAR "rtrim(UPPER(:LREAL_REGA_ADRE_CODE))"
,LREAL_REGA_START_DATE           DATE "DD-MON-YYYY" NULLIF LREAL_REGA_START_DATE=blanks
,LREAL_REFERENCE                 CHAR "rtrim(UPPER(:LREAL_REFERENCE))"
,LREAL_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LREAL_ACAS_ALTERNATE_REF))"
,LREAL_SCO_CODE                  CHAR "rtrim(UPPER(:LREAL_SCO_CODE))"
,LREAL_STATUS_DATE               DATE "DD-MON-YYYY" NULLIF LREAL_STATUS_DATE=blanks
,LREAL_CREATED_BY                CHAR "rtrim(UPPER(:LREAL_CREATED_BY))"
,LREAL_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LREAL_CREATED_DATE=blanks
,LREAL_COMMENTS                  CHAR(2000) "rtrim(:LREAL_COMMENTS)"
,LREAL_START_DATE                DATE "DD-MON-YYYY" NULLIF LREAL_START_DATE=blanks
,LREAL_END_DATE                  DATE "DD-MON-YYYY" NULLIF LREAL_END_DATE=blanks
,LREAL_PROPOSED_END_DATE         DATE "DD-MON-YYYY" NULLIF LREAL_PROPOSED_END_DATE=blanks
,LREAL_VISIT_DATETIME            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LREAL_VISIT_DATETIME=blanks
,LREAL_ACHO_LEGACY_REF           CHAR "rtrim(UPPER(:LREAL_ACHO_LEGACY_REF))"
,LREAL_REFNO                     INTEGER EXTERNAL "real_refno_seq.nextval"
)



