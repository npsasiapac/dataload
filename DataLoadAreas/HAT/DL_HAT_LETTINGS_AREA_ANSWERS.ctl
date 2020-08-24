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
--                                      time stamp added to LLAA_CREATED_DATE
--                                      
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_LETTINGS_AREA_ANSWERS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LLAA_DLB_BATCH_ID       CONSTANT "$batch_no",  
 LLAA_DL_SEQNO           RECNUM,
 LLAA_DL_LOAD_STATUS     CONSTANT "L",
 LLAA_LAR_CODE           CHAR "rtrim(:LLAA_LAR_CODE)",
 LAPP_LEGACY_REF         CHAR "rtrim(:LAPP_LEGACY_REF)",
 LLAA_QUE_REFNO          CHAR "rtrim(:LLAA_QUE_REFNO)",
 LLAA_CREATED_BY         CHAR "rtrim(:LLAA_CREATED_BY)",
 LLAA_CREATED_DATE       DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LLAA_CREATED_DATE=blanks,
 LLAA_OTHER_CODE         CHAR "rtrim(:LLAA_OTHER_CODE)",
 LLAA_OTHER_DATE         DATE "DD-MON-YYYY" NULLIF LLAA_OTHER_DATE =blanks,
 LLAA_QOR_CODE           CHAR "rtrim(:LLAA_QOR_CODE)",
 LLAA_COMMENTS           CHAR(2000) "rtrim(:LLAA_COMMENTS)",
 LLAA_DATE_VALUE         DATE "DD-MON-YYYY" NULLIF LLAA_DATE_VALUE=blanks,
 LLAA_NUMBER_VALUE       CHAR "rtrim(:LLAA_NUMBER_VALUE)",
 LLAA_CHAR_VALUE         CHAR "rtrim(:LLAA_CHAR_VALUE)"
)



