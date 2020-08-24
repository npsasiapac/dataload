load data
infile $gri_datafile
APPEND
into table DL_HAT_MEDICAL_REFERRALS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LMRF_DLB_BATCH_ID           CONSTANT "$batch_no",         
 LMRF_DL_SEQNO               RECNUM,       
 LMRF_DL_LOAD_STATUS         CONSTANT "L", 
 LAPP_LEGACY_REF             CHAR "rtrim(:LAPP_LEGACY_REF)",       
 LMRF_REFERRAL_DATE          DATE "DD-MON-YYYY" NULLIF LMRF_REFERRAL_DATE=blanks,       
 LMRF_STATUS_CODE            CHAR "rtrim(:LMRF_STATUS_CODE)",      
 LMRF_STATUS_DATE            DATE "DD-MON-YYYY" NULLIF LMRF_STATUS_DATE=blanks,
 LPAR_PER_ALT_REF            CHAR "rtrim(:LPAR_PER_ALT_REF)",       
 LMRF_ASSESSMENT_DATE        DATE "DD-MON-YYYY" NULLIF LMRF_ASSESSMENT_DATE=blanks,      
 LMRF_HRV_MRR_CODE           CHAR "rtrim(:LMRF_HRV_MRR_CODE)",
 LMRF_AWARD_DATE             DATE "DD-MON-YYYY" NULLIF LMRF_AWARD_DATE=blanks,     
 LMRF_COMMENTS               CHAR "rtrim(:LMRF_COMMENTS)",         
  LMRF_ASSESMENT_LEGACY_REF   CHAR "rtrim(:LMRF_ASSESMENT_LEGACY_REF)")

