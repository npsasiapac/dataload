-- *****************************************************************************
-- Version  Who           Date       Why
-- 01.00    D Bessell     25-Aug-15  Added end of line to file.
load data
-- *********************************************************************
--
-- Version      Who   date         Why
--  1.0         ??    21-JUN-2012  Initial Creation
--  1.1         AJ    01-MAR-2016  added change control and time stamp to 
--                                 both created dates
--
-- *********************************************************************
--
infile $gri_datafile
APPEND
into table DL_HRM_BUDGETS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LBUD_DLB_BATCH_ID       CONSTANT "$batch_no",
 LBUD_DL_SEQNO           RECNUM,
 LBUD_DL_LOAD_STATUS     CONSTANT "L",
 LBUD_BHE_CODE           CHAR "rtrim(:LBUD_BHE_CODE)",
 LBUD_BHE_DESCRIPTION    CHAR "rtrim(:LBUD_BHE_DESCRIPTION)",
 LBUD_BHE_CREATED_BY     CHAR "rtrim(:LBUD_BHE_CREATED_BY)",
 LBUD_BHE_CREATED_DATE   DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBUD_BHE_CREATED_DATE=BLANKS,
 LBUD_BCA_YEAR           CHAR NULLIF LBUD_BCA_YEAR=BLANKS,
 LBUD_TYPE               CHAR "rtrim(:LBUD_TYPE)",
 LBUD_AUN_CODE           CHAR "rtrim(:LBUD_AUN_CODE)",
 LBUD_AMOUNT             CHAR NULLIF LBUD_AMOUNT=BLANKS,
 LBUD_ALLOW_NEGATIVE_IND CHAR "rtrim(:LBUD_ALLOW_NEGATIVE_IND)",
 LBUD_REPEAT_WARNING_IND CHAR "rtrim(:LBUD_REPEAT_WARNING_IND)",
 LBUD_WARNING_ISSUED_IND CHAR "rtrim(:LBUD_WARNING_ISSUED_IND)",
 LBUD_SCO_CODE           CHAR "rtrim(:LBUD_SCO_CODE)",
 LBUD_CREATED_BY         CHAR "rtrim(:LBUD_CREATED_BY)",
 LBUD_CREATED_DATE       DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBUD_CREATED_DATE=BLANKS,
 LBUD_BUD_BHE_CODE       CHAR "rtrim(:LBUD_BUD_BHE_CODE)",
 LBUD_BPCA_BPR_CODE      CHAR "rtrim(:LBUD_BPCA_BPR_CODE)",
 LBUD_WARNING_PERCENT    CHAR NULLIF LBUD_WARNING_PERCENT=BLANKS,
 LBUD_COMMENTS           CHAR "rtrim(:LBUD_COMMENTS)",
 LBUD_COMMITTED          CHAR NULLIF LBUD_COMMITTED=BLANKS,
 LBUD_ACCRUED            CHAR NULLIF LBUD_ACCRUED=BLANKS,
 LBUD_INVOICED           CHAR NULLIF LBUD_INVOICED=BLANKS,
 LBUD_EXPENDED           CHAR NULLIF LBUD_EXPENDED=BLANKS,
 LBUD_CREDITED           CHAR NULLIF LBUD_CREDITED=BLANKS,
 LBUD_TAX_COMMITTED      CHAR NULLIF LBUD_TAX_COMMITTED=BLANKS,
 LBUD_TAX_ACCRUED        CHAR NULLIF LBUD_TAX_ACCRUED=BLANKS,
 LBUD_TAX_INVOICED       CHAR NULLIF LBUD_TAX_INVOICED=BLANKS,
 LBUD_TAX_EXPENDED       CHAR NULLIF LBUD_TAX_EXPENDED=BLANKS,
 LBUD_TAX_CREDITED       CHAR NULLIF LBUD_TAX_CREDITED=BLANKS,
 LBUD_ARC_CODE           CHAR NULLIF LBUD_ARC_CODE=BLANKS
)
