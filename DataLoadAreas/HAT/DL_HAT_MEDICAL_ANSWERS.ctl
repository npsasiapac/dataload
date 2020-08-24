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
--                                      time stamp added to LMAN_CREATED_DATE
--                                      
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_MEDICAL_ANSWERS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LMAN_DLB_BATCH_ID           CONSTANT "$batch_no",              
 LMAN_DL_SEQNO               RECNUM,      
 LMAN_DL_LOAD_STATUS         CONSTANT "L",
 LMAN_MRF_ASSESSMENT_REFNO   CHAR "rtrim(:LMAN_MRF_ASSESSMENT_REFNO)",
 LAPP_LEGACY_REF             CHAR "rtrim(:LAPP_LEGACY_REF)",      
 LMAN_QUE_REFNO              CHAR "rtrim(:LMAN_QUE_REFNO)",
 LMAN_CREATED_BY             CHAR "rtrim(:LMAN_CREATED_BY)",   
 LMAN_CREATED_DATE           DATE "DD-MON-YYYY" NULLIF LMAN_CREATED_DATE=blanks,
 LMAN_DATE_VALUE             DATE "DD-MON-YYYY" NULLIF LMAN_DATE_VALUE=blanks, 
 LMAN_NUMBER_VALUE           CHAR "rtrim(:LMAN_NUMBER_VALUE)",
 LMAN_CHAR_VALUE             CHAR "rtrim(:LMAN_CHAR_VALUE)",   
 LMAN_OTHER_CODE             CHAR "rtrim(:LMAN_OTHER_CODE)",   
 LMAN_OTHER_DATE             DATE "DD-MON-YYYY" NULLIF LMAN_OTHER_DATE=blanks,
 LMAN_QOR_CODE               CHAR "rtrim(:LMAN_QOR_CODE)",   
 LMAN_COMMENTS               CHAR(2000) "rtrim(:LMAN_COMMENTS)",
 LMRF_REFERRAL_DATE          DATE "DD-MON-YYYY" NULLIF LMRF_REFERRAL_DATE=blanks
)


