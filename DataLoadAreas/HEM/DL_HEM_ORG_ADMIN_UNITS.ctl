--
-- *******************************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.14      AJ   13-JAN-2017  Initial Creation for Org Admin Unit Loader
--  1.1     6.14      AJ   07-MAR-2017  amended short_code to sort_code and amended
--                                      format also added LORAU_PAR_ORG_CURRENT_IND
--  1.2     6.14      AJ   05-APR-2017  removed sort_code and par_org_current_ind                                 
--
-- *******************************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HEM_ORG_ADMIN_UNITS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LORAU_DLB_BATCH_ID          CONSTANT "$batch_no",  
 LORAU_DL_SEQNO              RECNUM,
 LORAU_DL_LOAD_STATUS        CONSTANT "L",
 LORAU_PAR_ORG_NAME          CHAR "rtrim(:LORAU_PAR_ORG_NAME)",
 LORAU_PAR_ORG_SHORT_NAME    CHAR "rtrim(:LORAU_PAR_ORG_SHORT_NAME)",
 LORAU_PAR_ORG_FRV_OTY_CODE  CHAR "rtrim(:LORAU_PAR_ORG_FRV_OTY_CODE)",
 LORAU_PAR_REFNO             CHAR "rtrim(:LORAU_PAR_REFNO)",
 LORAU_AUN_CODE              CHAR "rtrim(:LORAU_AUN_CODE)",
 LORAU_START_DATE            DATE "DD-MON-YYYY" NULLIF LORAU_START_DATE=blanks,
 LORAU_FRV_OAR_CODE          CHAR "rtrim(:LORAU_FRV_OAR_CODE)",
 LORAU_CREATED_DATE          DATE "DD-MON-YYYY" NULLIF LORAU_CREATED_DATE=blanks, 
 LORAU_CREATED_BY            CHAR "rtrim(:LORAU_CREATED_BY)",
 LORAU_END_DATE              DATE "DD-MON-YYYY" NULLIF LORAU_END_DATE=blanks,  
 LORAU_COMMENTS              CHAR "rtrim(:LORAU_COMMENTS)"
)

