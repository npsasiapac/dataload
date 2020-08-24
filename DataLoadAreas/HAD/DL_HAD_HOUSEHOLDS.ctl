--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   11=DEC-2015  Control comments section added
--
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_HOUSEHOLDS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LHOU_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LHOU_DL_SEQNO                  RECNUM
,LHOU_DL_LOAD_STATUS            CONSTANT "L"
,LHOU_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LHOU_ACAS_ALTERNATE_REF))"
,LHOU_ACHO_ALTERNATE_REF        CHAR "rtrim(UPPER(:LHOU_ACHO_ALTERNATE_REF))"
,LHOU_REFNO                     INTEGER EXTERNAL "HOU_REFNO_SEQ.NEXTVAL"
)


