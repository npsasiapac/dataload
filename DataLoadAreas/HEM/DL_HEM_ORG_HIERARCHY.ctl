--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.13      AJ   02-MAR-2016  Initial Creation for Organisation
--                                      Hierarchy Data Loader
--  1.1     6.14      AJ   05-APR-2017  Data Load completed under CR502 for Queensland
--                                      
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HEM_ORG_HIERARCHY
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LORHI_DLB_BATCH_ID           CONSTANT "$batch_no",  
 LORHI_DL_SEQNO               RECNUM,
 LORHI_DL_LOAD_STATUS         CONSTANT "L",
 LORHI_PAR_ORG_NAME           CHAR "rtrim(:LORHI_PAR_ORG_NAME)",
 LORHI_PAR_ORG_SHORT_NAME     CHAR "rtrim(:LORHI_PAR_ORG_SHORT_NAME)",
 LORHI_PAR_ORG_FRV_OTY_CODE   CHAR "rtrim(:LORHI_PAR_ORG_FRV_OTY_CODE)",
 LORHI_PAR_REFNO              CHAR "rtrim(:LORHI_PAR_REFNO)", 
 LORHI_PAR_ORG_NAME_C         CHAR "rtrim(:LORHI_PAR_ORG_NAME_C)",
 LORHI_PAR_ORG_SHORT_NAME_C   CHAR "rtrim(:LORHI_PAR_ORG_SHORT_NAME_C)",
 LORHI_PAR_ORG_FRV_OTY_CODE_C CHAR "rtrim(:LORHI_PAR_ORG_FRV_OTY_CODE_C)",
 LORHI_PAR_REFNO_C            CHAR "rtrim(:LORHI_PAR_REFNO_C)", 
 LORHI_START_DATE             DATE "DD-MON-YYYY" NULLIF LORHI_START_DATE=blanks,
 LORHI_FRV_ORT_CODE           CHAR "rtrim(:LORHI_FRV_ORT_CODE)",  
 LORHI_CREATED_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LORHI_CREATED_DATE=blanks,
 LORHI_CREATED_BY             CHAR "rtrim(:LORHI_CREATED_BY)",  
 LORHI_END_DATE               DATE "DD-MON-YYYY" NULLIF LORHI_END_DATE=blanks,
 LORHI_COMMENTS               CHAR(2000) "rtrim(:LORHI_COMMENTS)", 
 LORHI_REFNO                  CHAR "rtrim(:LORHI_REFNO)" 
)

