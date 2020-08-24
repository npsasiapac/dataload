--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     6.18      AJ   08-FEB-2019  Created For new SAHT loader
--                                      Support Services Referrals
--  1.1     6.18      AJ   11-FEB-2019  Completed
--  1.2     6.18      AJ   17-FEB-2019  Fields amended
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HSS_REFERRALS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LREF_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LREF_DL_SEQNO                  RECNUM
,LREF_DL_LOAD_STATUS            CONSTANT "L"
,LREF_ALTERNATE_REF             CHAR "rtrim(UPPER(:LREF_ALTERNATE_REF))"
,LREF_TYPE                      CHAR "rtrim(UPPER(:LREF_TYPE))"
,LREF_CLNT_PAR_PER_FORENAME     CHAR "rtrim(UPPER(:LREF_CLNT_PAR_PER_FORENAME))"
,LREF_CLNT_PAR_PER_SURNAME      CHAR "rtrim(UPPER(:LREF_CLNT_PAR_PER_SURNAME))"
,LREF_PAR_PER_DATE_OF_BIRTH     DATE "DD-MON-YYYY" NULLIF LREF_PAR_PER_DATE_OF_BIRTH=blanks
,LREF_PAR_PER_ALT_REF           CHAR "rtrim(UPPER(:LREF_PAR_PER_ALT_REF))"
,LREF_PAR_REFNO                 CHAR "rtrim(:LREF_PAR_REFNO)"
,LREF_SCO_CODE                  CHAR "rtrim(UPPER(:LREF_SCO_CODE))"
,LREF_STATUS_DATE               DATE "DD-MON-YYYY" NULLIF LREF_STATUS_DATE=blanks
,LREF_RECEIVED_DATE             DATE "DD-MON-YYYY" NULLIF LREF_RECEIVED_DATE=blanks
,LREF_CREATED_BY                CHAR "rtrim(UPPER(:LREF_CREATED_BY))"
,LREF_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LREF_CREATED_DATE=blanks
,LREF_CSVC_CODE                 CHAR "rtrim(UPPER(:LREF_CSVC_CODE))"
,LREF_TO_SUPR_CODE              CHAR "rtrim(UPPER(:LREF_TO_SUPR_CODE))"
,LREF_DEN_TO_SUPR_CODE          CHAR "rtrim(UPPER(:LREF_DEN_TO_SUPR_CODE))"
,LREF_AUN_CODE                  CHAR "rtrim(UPPER(:LREF_AUN_CODE))"
,LREF_COMMENTS                  CHAR(2000) "rtrim(:LREF_COMMENTS)"
)

