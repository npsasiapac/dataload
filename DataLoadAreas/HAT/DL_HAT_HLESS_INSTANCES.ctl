load data
infile $gri_datafile
APPEND
into table DL_HAT_HLESS_INSTANCES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LHIN_DLB_BATCH_ID          CONSTANT "$batch_no",             
 LHIN_DL_SEQNO              RECNUM,     
 LHIN_DL_LOAD_STATUS        CONSTANT "L",
 LAPP_LEGACY_REF            CHAR "rtrim(:LAPP_LEGACY_REF)",       
 LHIN_ALE_RLI_CODE	    CHAR "rtrim(:LHIN_ALE_RLI_CODE)",   
 LHIN_INSTANCE_REFNO	    CHAR "rtrim(:LHIN_INSTANCE_REFNO)",
 LHIN_EXPECTED_HLESS_DATE   DATE "DD-MON-YYYY" NULLIF LHIN_EXPECTED_HLESS_DATE=blanks,    
 LHIN_PRESENTED_DATE        DATE "DD-MON-YYYY" NULLIF LHIN_PRESENTED_DATE=blanks,  
 LHIN_ACCEPTED_HLESS_DATE   DATE "DD-MON-YYYY" NULLIF LHIN_ACCEPTED_HLESS_DATE=blanks,    
 LIPT_CODE                  CHAR "rtrim(:LIPT_CODE)",     
 LHIN_HRV_HCR_CODE          CHAR "rtrim(:LHIN_HRV_HCR_CODE )",    
 LHIN_HRV_HOR_CODE          CHAR "rtrim(:LHIN_HRV_HOR_CODE)", 
 LHIN_COMMENTS 		    CHAR(2000) "rtrim(:LHIN_COMMENTS)",
 LHIN_CREATED_BY		    CHAR "rtrim(:LHIN_CREATED_BY)",
 LHIN_CREATED_DATE	    DATE "DD-MON-YYYY" NULLIF LHIN_CREATED_DATE=blanks
)

   
