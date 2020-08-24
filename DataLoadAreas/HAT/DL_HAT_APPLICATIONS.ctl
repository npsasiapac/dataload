--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    PH   17-JUL-2007  Initial Creation.
--  1.1     6.13      AJ   26-FEB-2016  Control comments section added
--                                      time stamp added to LAPP_STATUS_DATE
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_APPLICATIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
( LAPP_DLB_BATCH_ID         CONSTANT "$batch_no",             
 LAPP_DL_SEQNO              RECNUM,     
 LAPP_DL_LOAD_STATUS        CONSTANT "L",    
 LAPP_LEGACY_REF            CHAR "rtrim(:LAPP_LEGACY_REF)",    
 LAPP_OFFER_FLAG            CHAR "rtrim(:LAPP_OFFER_FLAG)",   
 LAPP_NOMINATION_FLAG       CHAR "rtrim(:LAPP_NOMINATION_FLAG)",    
 LAPP_RECEIVED_DATE         DATE "DD-MON-YYYY" NULLIF lapp_received_date=blanks,  
 LAPP_CORR_NAME             CHAR "rtrim(:LAPP_CORR_NAME)",   
 LAPP_SCO_CODE              CHAR "rtrim(:LAPP_SCO_CODE)",   
 LAPP_STATUS_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF lapp_status_date=blanks,     
 LAPP_RENT_ACCOUNT_DETAILS  CHAR "rtrim(:LAPP_RENT_ACCOUNT_DETAILS )",
 LTCY_ALT_REF               CHAR "rtrim(:LTCY_ALT_REF)",
 LAPP_REFNO                 CHAR "rtrim(:LAPP_REFNO)",
 LAPP_HRV_FSSA_CODE         CHAR "rtrim(:LAPP_HRV_FSSA_CODE)",
 LAPP_ACAS_ALT_REF          CHAR "rtrim(:LAPP_ACAS_ALT_REF)"
)     
