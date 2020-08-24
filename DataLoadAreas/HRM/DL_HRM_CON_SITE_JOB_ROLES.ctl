--
-- ***********************************************************************
--  DESCRIPTION:
--  CHANGE CONTROL
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.13      AJ   08-JUL-2016  Initial Creation for new data loader
--                                      for Contractors repairs Contractors
--                                      Sites Job Roles
--
--***********************************************************************
--
load data
APPEND
into table DL_HRM_CON_SITE_JOB_ROLES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LCSJ_DLB_BATCH_ID CONSTANT "$batch_no"
,LCSJ_DL_SEQNO RECNUM
,LCSJ_DL_LOAD_STATUS CONSTANT "L"
,LCSJ_JRB_JRO_CODE CHAR "rtrim(:LCSJ_JRB_JRO_CODE)"
,LCSJ_JRB_READ_WRITE_IND CHAR "rtrim(:LCSJ_JRB_READ_WRITE_IND)"
,LCSJ_JRB_PK_CODE1 CHAR "rtrim(:LCSJ_JRB_PK_CODE1)"
)

