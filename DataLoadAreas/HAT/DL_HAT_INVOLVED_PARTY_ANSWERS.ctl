--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0                    06-FEB-2008  latest version used as base.
--  1.1     6.13      AJ   29-FEB-2016  Control comments section added
--                                      time stamp added to LIPN_CREATED_DATE
--                                      
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_INVOLVED_PARTY_ANSWERS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LIPN_DLB_BATCH_ID       CONSTANT "$batch_no",  
 LIPN_DL_SEQNO           RECNUM,  
 LIPN_DL_LOAD_STATUS     CONSTANT "L",
 LPAR_PER_ALT_REF        CHAR "rtrim(:LPAR_PER_ALT_REF)",
 LAPP_LEGACY_REF         CHAR "rtrim(:LAPP_LEGACY_REF)",
 LIPN_QUE_REFNO          CHAR "rtrim(:LIPN_QUE_REFNO)",
 LIPN_CREATED_BY         CHAR "rtrim(:LIPN_CREATED_BY)",
 LIPN_CREATED_DATE       DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIPN_CREATED_DATE=blanks,
 LIPN_DATE_VALUE         DATE "DD-MON-YYYY" NULLIF LIPN_DATE_VALUE=blanks,
 LIPN_NUMBER_VALUE       CHAR "rtrim(:LIPN_NUMBER_VALUE)",
 LIPN_CHAR_VALUE         CHAR "rtrim(:LIPN_CHAR_VALUE)",
 LIPN_OTHER_CODE         CHAR "rtrim(:LIPN_OTHER_CODE)",
 LIPN_OTHER_DATE         DATE "DD-MON-YYYY" NULLIF LIPN_OTHER_DATE=blanks,
 LIPN_QOR_CODE           CHAR "rtrim(:LIPN_QOR_CODE)",
 LIPN_COMMENTS           CHAR(2000) "rtrim(:LIPN_COMMENTS)"  
)


