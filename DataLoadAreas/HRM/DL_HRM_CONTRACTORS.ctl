--
-- ***********************************************************************
--  DESCRIPTION:
--  CHANGE CONTROL
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.13      AJ   28-SEP-2016  Initial Creation of control record
--                                      to detail correction of lines for
--                                      LCON_COS_AUTO_INV_DELAY_DAYS
--                                      LCON_COS_AUTO_REC_DELAY_DAYS
--                                      LCON_COS_AUTO_JOB_COMP_DELAY
--
--***********************************************************************
--
load data
APPEND
into table DL_HRM_CONTRACTORS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LCON_DLB_BATCH_ID CONSTANT "$batch_no"
,LCON_DL_SEQNO RECNUM
,LCON_DL_LOAD_STATUS CONSTANT "L"
,LCON_CODE CHAR "rtrim(:LCON_CODE)"
,LCON_NAME CHAR "rtrim(:LCON_NAME)"
,LCON_HRV_CTY_CODE CHAR "rtrim(:LCON_HRV_CTY_CODE)"
,LCON_CREATED_BY CHAR "rtrim(:LCON_CREATED_BY)"
,LCON_CREATED_DATE DATE "DD-MON-YYYY" NULLIF LCON_CREATED_DATE=blanks
,LCON_TAX_REG_IND CHAR "rtrim(:LCON_TAX_REG_IND)"
,LCON_EQUAL_OP_IND CHAR "rtrim(:LCON_EQUAL_OP_IND)"
,LCON_VATNO CHAR "rtrim(:LCON_VATNO)"
,LCON_TEXT CHAR "rtrim(:LCON_TEXT)"
,LCON_BUSINESS_NO CHAR "rtrim(:LCON_BUSINESS_NO)"
,LCON_BN_START_DATE DATE "DD-MON-YYYY" NULLIF LCON_BN_START_DATE=blanks
,LCON_BN_END_DATE DATE "DD-MON-YYYY" NULLIF LCON_BN_END_DATE=blanks
,LCON_TAX_START_DATE DATE "DD-MON-YYYY" NULLIF LCON_TAX_START_DATE=blanks
,LCON_TAX_END_DATE DATE "DD-MON-YYYY" NULLIF LCON_TAX_END_DATE=blanks
,LCON_HRM_RCPT_START_DATE DATE "DD-MON-YYYY" NULLIF LCON_HRM_RCPT_START_DATE=blanks
,LCON_HRM_RCPT_END_DATE DATE "DD-MON-YYYY" NULLIF LCON_HRM_RCPT_END_DATE=blanks
,LCON_HPM_RCPT_START_DATE DATE "DD-MON-YYYY" NULLIF LCON_HPM_RCPT_START_DATE=blanks
,LCON_HPM_RCPT_END_DATE DATE "DD-MON-YYYY" NULLIF LCON_HPM_RCPT_END_DATE=blanks
,LCON_FK_VENDOR_ID CHAR NULLIF LCON_FK_VENDOR_ID=blanks "TO_NUMBER(:LCON_FK_VENDOR_ID)"
,LCON_COS_CODE CHAR "rtrim(:LCON_COS_CODE)"
,LCON_COS_NAME CHAR "rtrim(:LCON_COS_NAME)"
,LCON_COS_MAX_WO_NO CHAR NULLIF LCON_COS_MAX_WO_NO=blanks "TO_NUMBER(:LCON_COS_MAX_WO_NO)" 
,LCON_COS_MAX_WOS_TOTAL_VALUE CHAR NULLIF LCON_COS_MAX_WOS_TOTAL_VALUE=blanks "TO_NUMBER(:LCON_COS_MAX_WOS_TOTAL_VALUE)"
,LCON_COS_MAX_WO_VALUE CHAR NULLIF LCON_COS_MAX_WO_VALUE=blanks "TO_NUMBER(:LCON_COS_MAX_WO_VALUE)"
,LCON_COS_HS_CERT_IND CHAR "rtrim(:LCON_COS_HS_CERT_IND)"
,LCON_COS_QUALITY_ASSURED_IND CHAR "rtrim(:LCON_COS_QUALITY_ASSURED_IND)"
,LCON_COS_ASBESTOS_APPROVED_IND CHAR "rtrim(:LCON_COS_ASBESTOS_APPROVED_IND)"
,LCON_COS_HPM_AUTHORISED_IND CHAR "rtrim(:LCON_COS_HPM_AUTHORISED_IND)"
,LCON_COS_SCO_CODE CHAR "rtrim(:LCON_COS_SCO_CODE)"
,LCON_COS_FCA_CODE CHAR "rtrim(:LCON_COS_FCA_CODE)"
,LCON_COS_PRE_INSPECT_LIMIT CHAR NULLIF LCON_COS_PRE_INSPECT_LIMIT=blanks "TO_NUMBER(:LCON_COS_PRE_INSPECT_LIMIT)"
,LCON_COS_POST_INSPECT_LIMIT CHAR NULLIF LCON_COS_POST_INSPECT_LIMIT=blanks "TO_NUMBER(:LCON_COS_POST_INSPECT_LIMIT)"
,LCON_COS_YEAR_REGD CHAR NULLIF LCON_COS_YEAR_REGD=blanks "TO_NUMBER(:LCON_COS_YEAR_REGD)"
,LCON_COS_MIN_WO_VALUE CHAR NULLIF LCON_COS_MIN_WO_VALUE=blanks "TO_NUMBER(:LCON_COS_MIN_WO_VALUE)"
,LCON_COS_MAX_JOB_EST_COST_VAR CHAR NULLIF LCON_COS_MAX_JOB_EST_COST_VAR=blanks "TO_NUMBER(:LCON_COS_MAX_JOB_EST_COST_VAR)"
,LCON_COS_MAX_JOB_EST_TAX_VAR CHAR NULLIF LCON_COS_MAX_JOB_EST_TAX_VAR=blanks "TO_NUMBER(:LCON_COS_MAX_JOB_EST_TAX_VAR)"
,LCON_COS_NO_CURRENT_WOS CHAR NULLIF LCON_COS_NO_CURRENT_WOS=blanks "TO_NUMBER(:LCON_COS_NO_CURRENT_WOS)"
,LCON_COS_VALUE_CURRENT_WOS CHAR NULLIF LCON_COS_VALUE_CURRENT_WOS=blanks "TO_NUMBER(:LCON_COS_VALUE_CURRENT_WOS)"
,LCON_COS_FK_VENDOR_SITE_ID CHAR NULLIF LCON_COS_FK_VENDOR_SITE_ID=blanks "TO_NUMBER(:LCON_COS_FK_VENDOR_SITE_ID)"
,LCON_COS_SPR_PRINTER_NAME CHAR "rtrim(:LCON_COS_SPR_PRINTER_NAME)"
,LCON_COS_COS_CODE CHAR "rtrim(:LCON_COS_COS_CODE)"
,LCON_COS_PAYMENT_INTERVAL CHAR "rtrim(:LCON_COS_PAYMENT_INTERVAL)"
,LCON_JRB_JRO_CODE CHAR "rtrim(:LCON_JRB_JRO_CODE)"
,LCON_JRB_OBJ_NAME CHAR "rtrim(:LCON_JRB_OBJ_NAME)"
,LCON_JRB_READ_WRITE_IND CHAR "rtrim(:LCON_JRB_READ_WRITE_IND)"
,LCON_JRB_PK_CODE1 CHAR "rtrim(:LCON_JRB_PK_CODE1)"
,LCON_COS_PHONE CHAR "rtrim(:LCON_COS_PHONE)"
,LCON_COS_APPT_TYPE_IND CHAR "rtrim(:LCON_COS_APPT_TYPE_IND)"
,LCON_COS_TP_APPT_IND CHAR "rtrim(:LCON_COS_TP_APPT_IND)"
,LCON_COS_AUTO_INV_MIN_DELAY CHAR "rtrim(:LCON_COS_AUTO_INV_MIN_DELAY)"
,LCON_COS_AUTO_INV_DELAY_DAYS CHAR NULLIF LCON_COS_AUTO_INV_DELAY_DAYS=blanks "TO_NUMBER(:LCON_COS_AUTO_INV_DELAY_DAYS)"
,LCON_COS_AUTO_REC_DELAY_DAYS CHAR NULLIF LCON_COS_AUTO_REC_DELAY_DAYS=blanks "TO_NUMBER(:LCON_COS_AUTO_REC_DELAY_DAYS)"
,LCON_COS_AUTO_JOB_COMP_IND  CHAR "rtrim(:LCON_COS_AUTO_JOB_COMP_IND)"
,LCON_COS_AUTO_JOB_COMP_DELAY CHAR NULLIF LCON_COS_AUTO_JOB_COMP_DELAY=blanks "TO_NUMBER(:LCON_COS_AUTO_JOB_COMP_DELAY)"
,LCON_CODE_MLANG CHAR "rtrim(:LCON_CODE_MLANG)"
,LCON_NAME_MLANG CHAR "rtrim(:LCON_NAME_MLANG)"
,LCON_COS_CODE_MLANG CHAR "rtrim(:LCON_COS_CODE_MLANG)"
,LCON_COS_NAME_MLANG CHAR "rtrim(:LCON_COS_NAME_MLANG)"
)

