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
--  1.2     6.13      AJ   23-FEB-2016  added LHOP_HRV_HPSR_CODE and
--                                      LHOP_HRV_HPER_CODE
--  1.3     6.13      AJ   24-FEB-2016  amended date fields to remove
--                                      time stamp as should be date only
--  1.4     6.14      MJK  05-DEC-2017  Addded household ind and household group
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_HOUSEHOLD_PEOPLE
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LHOP_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LHOP_DL_SEQNO                  RECNUM
,LHOP_DL_LOAD_STATUS            CONSTANT "L"
,LHOP_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LHOP_ACAS_ALTERNATE_REF))"
,LHOP_ACHO_ALTERNATE_REF        CHAR "rtrim(UPPER(:LHOP_ACHO_ALTERNATE_REF))"
,LHOP_PAR_PER_ALT_REF           CHAR "rtrim(UPPER(:LHOP_PAR_PER_ALT_REF))"
,LHOP_START_DATE                DATE "DD-MON-YYYY" NULLIF LHOP_START_DATE=blanks
,LHOP_END_DATE                  DATE "DD-MON-YYYY" NULLIF LHOP_END_DATE=blanks
,LHOP_HRV_FRL_CODE              CHAR "rtrim(UPPER(:LHOP_HRV_FRL_CODE))"
,LHOP_REFNO                     INTEGER EXTERNAL "HOP_REFNO_SEQ.NEXTVAL"
,LHOP_HRV_HPSR_CODE             CHAR "rtrim(UPPER(:LHOP_HRV_HPSR_CODE))"
,LHOP_HRV_HPER_CODE             CHAR "rtrim(UPPER(:LHOP_HRV_HPER_CODE))"
,LHOP_HEAD_HHOLD_IND            CHAR "rtrim(UPPER(:LHOP_HEAD_HHOLD_IND))"
,LHOP_HHOLD_GROUP_NO            CHAR "rtrim(UPPER(:LHOP_HHOLD_GROUP_NO))"
)


