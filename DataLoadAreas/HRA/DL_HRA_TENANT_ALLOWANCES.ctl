--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.18      RH   14-FEB-2019  Initial creation for SAHT
--
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HRA_TENANT_ALLOWANCES
fields terminated by "," optionally enclosed by '"'
trailing nullcols       
( LTALL_DLB_BATCH_ID             CONSTANT "$batch_no",             
  LTALL_DL_SEQNO                 RECNUM,  
  LTALL_DL_LOAD_STATUS           CONSTANT "L", 
  LTALL_TCY_REFNO                CHAR "upper(rtrim(:LTALL_TCY_REFNO))", 
  LTALL_TALT_CODE                CHAR "upper(rtrim(:LTALL_TALT_CODE))", 
  LTALL_START_DATE               DATE "DD-MON-YYYY" NULLIF LTALL_START_DATE=blanks, 
  LTALL_CREATED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LTALL_CREATED_DATE=blanks, 
  LTALL_CREATED_BY               CHAR "upper(rtrim(:LTALL_CREATED_BY))", 
  LTALL_AMOUNT                   CHAR "upper(rtrim(:LTALL_AMOUNT))", 
  LTALL_END_DATE                 DATE "DD-MON-YYYY" NULLIF LTALL_END_DATE=blanks, 
  LTALL_APPROVED_DATE            DATE "DD-MON-YYYY" NULLIF LTALL_APPROVED_DATE=blanks,  
  LTALL_NEXT_PAYMENT_DUE_DATE    DATE "DD-MON-YYYY" NULLIF LTALL_NEXT_PAYMENT_DUE_DATE=blanks 
)


