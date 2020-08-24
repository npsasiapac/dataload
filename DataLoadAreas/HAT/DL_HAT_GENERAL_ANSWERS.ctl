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
--                                      time stamp added to LGAN_CREATED_DATE
--                                      
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_GENERAL_ANSWERS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LGAN_DLB_BATCH_ID       CONSTANT "$batch_no",    
LGAN_DL_SEQNO            RECNUM,
LGAN_DL_LOAD_STATUS      CONSTANT "L", 
LAPP_LEGACY_REF          CHAR "rtrim(:LAPP_LEGACY_REF)",
LGAN_QUE_REFNO           CHAR "rtrim(:LGAN_QUE_REFNO)",
LGAN_CREATED_BY          CHAR "rtrim(:LGAN_CREATED_BY)",
LGAN_CREATED_DATE        DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LGAN_CREATED_DATE=blanks,
LGAN_DATE_VALUE          DATE "DD-MON-YYYY" NULLIF LGAN_DATE_VALUE=blanks,
LGAN_NUMBER_VALUE        CHAR "rtrim(:LGAN_NUMBER_VALUE)",
LGAN_CHAR_VALUE          CHAR "rtrim(:LGAN_CHAR_VALUE)",
LGAN_OTHER_CODE          CHAR "rtrim(:LGAN_OTHER_CODE)",
LGAN_OTHER_DATE          DATE "DD-MON-YYYY" NULLIF LGAN_OTHER_DATE=blanks,
LGAN_QOR_CODE            CHAR "rtrim(:LGAN_QOR_CODE)",
LGAN_COMMENTS            CHAR(2000) "rtrim(:LGAN_COMMENTS)"
)




   
