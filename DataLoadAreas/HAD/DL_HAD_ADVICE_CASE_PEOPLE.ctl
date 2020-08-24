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
--  1.2     6.13      AJ   24-FEB-2016  time option removed from start and
--                                      end dates
--  1.3     6.14      MJK  14-NOV-2017  Added LACPE_HEAD_HHOLD_IND and
--                                      LACPE_HHOLD_GROUP_NO
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_ADVICE_CASE_PEOPLE
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LACPE_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LACPE_DL_SEQNO                  RECNUM
,LACPE_DL_LOAD_STATUS            CONSTANT "L"
,LACPE_ACAS_ALTERNATE_REF        CHAR "rtrim(UPPER(:LACPE_ACAS_ALTERNATE_REF))"
,LACPE_PAR_PER_ALT_REF           CHAR "rtrim(UPPER(:LACPE_PAR_PER_ALT_REF))"
,LACPE_CLIENT_IND                CHAR "rtrim(UPPER(:LACPE_CLIENT_IND))"
,LACPE_JOINT_CLIENT_IND          CHAR "rtrim(UPPER(:LACPE_JOINT_CLIENT_IND))"
,LACPE_START_DATE                DATE "DD-MON-YYYY" NULLIF LACPE_START_DATE=blanks
,LACPE_CREATED_BY                CHAR "rtrim(UPPER(:LACPE_CREATED_BY))"
,LACPE_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACPE_CREATED_DATE=blanks
,LACPE_HRV_FRL_CODE              CHAR "rtrim(:LACPE_HRV_FRL_CODE)"
,LACPE_END_DATE                  DATE "DD-MON-YYYY" NULLIF LACPE_END_DATE=blanks
,LACPE_COMMENT                   CHAR(2000) "rtrim(UPPER(:LACPE_COMMENT))"
,LACPE_HEAD_HHOLD_IND            CHAR "rtrim(UPPER(:LACPE_HEAD_HHOLD_IND))"
,LACPE_HHOLD_GROUP_NO            CHAR "rtrim(UPPER(:LACPE_HHOLD_GROUP_NO))"
)




