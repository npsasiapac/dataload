--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.18      JT   22-JAN-2019  Initial creation for SAHT
--
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HEM_PERSON_ALSO_KNOWN_AS
fields terminated by "," optionally enclosed by '"'
trailing nullcols       
( LPAKA_DLB_BATCH_ID             CONSTANT "$batch_no",             
  LPAKA_DL_SEQNO                 RECNUM,  
  LPAKA_DL_LOAD_STATUS           CONSTANT "L",
  LPAKA_PAR_REFNO                CHAR "rtrim(:LPAKA_PAR_REFNO)",
  LPAKA_PER_ALT_REF              CHAR "rtrim(:LPAKA_PER_ALT_REF)",
  LPAKA_PER_FORENAME             CHAR "upper(rtrim(:LPAKA_PER_FORENAME))",
  LPAKA_PER_SURNAME              CHAR "upper(rtrim(:LPAKA_PER_SURNAME))",
  LPAKA_FRV_AKAR_CODE            CHAR "upper(rtrim(:LPAKA_FRV_AKAR_CODE))",
  LPAKA_START_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPAKA_START_DATE=blanks,
  LPAKA_CREATED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPAKA_CREATED_DATE=blanks,
  LPAKA_CREATED_BY               CHAR "upper(rtrim(:LPAKA_CREATED_BY))",
  LPAKA_FORENAME                 CHAR "upper(rtrim(:LPAKA_FORENAME))",
  LPAKA_SURNAME                  CHAR "upper(rtrim(:LPAKA_SURNAME))",
  LPAKA_END_DATE                 DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPAKA_END_DATE=blanks,
  LPAKA_COMMENTS                 CHAR(2000) "rtrim(:LPAKA_COMMENTS)"
)

