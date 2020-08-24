--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    PH   01-NOV-2004  Initial Creation.
--  1.1     6.13      AJ   29-FEB-2016  Control comments section added
--                                      time stamp added to LALE_CREATED_DATE
--  1.2     6.14      MOK  30-AUG-2017  NB extra fields added
--  1.3     6.14      AJ   05-SEP-2017  NB extra fields amended order                                    
--  1.4     6.14      AJ   06-SEP-2017  LALE_DEFINED_HTY_CODE removed as calculated
--                                      by HAT004
--  1.5     6.14      AJ   19-SEP-2017  Format slightly amended as load error
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_APPLIC_LIST_ENTRIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
( LALE_DLB_BATCH_ID            CONSTANT "$batch_no",          
 LALE_DL_SEQNO                 RECNUM,     
 LALE_DL_LOAD_STATUS           CONSTANT "L",
 LALE_ALT_REF                  CHAR "rtrim(:LALE_ALT_REF)",          
 LALE_RLI_CODE                 CHAR "rtrim(:LALE_RLI_CODE)",     
 LALE_LST_CODE                 CHAR "rtrim(:LALE_LST_CODE)",      
 LALE_AUN_CODE                 CHAR "rtrim(:LALE_AUN_CODE)",      
 LALE_CREATED_BY               CHAR "rtrim(:LALE_CREATED_BY)",      
 LALE_CREATED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF lale_created_date=blanks,      
 LALE_ALS_ACTIVE_IND           CHAR "rtrim(:LALE_ALS_ACTIVE_IND)",      
 LALE_REGISTERED_DATE          DATE "DD-MON-YYYY" NULLIF lale_registered_date=blanks,    
 LALE_STATUS_START_DATE        DATE "DD-MON-YYYY" NULLIF lale_status_start_date=blanks,      
 LALE_STATUS_REVIEW_DATE       DATE "DD-MON-YYYY" NULLIF lale_status_review_date=blanks,     
 LALE_REREG_BY_DATE            DATE "DD-MON-YYYY" NULLIF lale_rereg_by_date=blanks,      
 LALE_BECAME_ACTIVE_DATE       DATE "DD-MON-YYYY" NULLIF lale_became_active_date=blanks,     
 LALE_REFUSALS_COUNT           CHAR NULLIF lale_refusals_count=blanks "TO_NUMBER(:lale_refusals_count)",      
 LALE_ALA_HRV_APC_CODE         CHAR "rtrim(:LALE_ALA_HRV_APC_CODE)",     
 LALE_HRV_LRQ_CODE             CHAR "rtrim(:LALE_HRV_LRQ_CODE)",     
 LALE_HRV_APS_CODE             CHAR "rtrim(:LALE_HRV_APS_CODE)",
 LALE_CURRENT_NOMINATION_COUNT CHAR "rtrim(:LALE_CURRENT_NOMINATION_COUNT)", 
 LALE_CURRENT_OFFER_COUNT      CHAR "rtrim(:LALE_CURRENT_OFFER_COUNT)", 
 LALE_CATEGORY_START_DATE      DATE "DD-MON-YYYY" NULLIF lale_category_start_date=blanks,
 LALE_APP_REFNO                CHAR "rtrim(:LALE_APP_REFNO)", 
 LALE_CHANGED_IND              CHAR "rtrim(:LALE_CHANGED_IND)", 
 LALE_MODIFIED_DATE            DATE "DD-MON-YYYY" NULLIF lale_modified_date=blanks,
 LALE_MODIFIED_BY              CHAR "rtrim(:LALE_MODIFIED_BY)", 
 LALE_AMENDED_ONLINE_IND       CHAR "rtrim(:LALE_AMENDED_ONLINE_IND)"
)

      
