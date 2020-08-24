-- *********************************************************************
--
-- Version    Who   Date         Why
-- 1.0        AJ    28-FEB-2017  Initial Creation for Queensland Bespoke
--                               for CR462
-- 1.1        AJ    01-MAR-2017  Added further fields bca_year and bhe_code
-- 1.2        AJ    02-MAR-2017  Amended valued_datetime to date and also
--                               added the remaining deliverables columns
-- 1.3        AJ    03-MAR-2017  Removed LDVL_DVE_HRV_LOC_CODE as not required
-- 1.4        AJ    12-APR-2017  Added LDVL_HRV_VTY_CODE (valuation type)
--
--
-- *********************************************************************
--
load data
infile $GRI_DATAFILE
APPEND
into table DL_HPM_DELIVERABLE_VAL
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LDVL_DLB_BATCH_ID              CONSTANT "$batch_no",
 LDVL_DL_SEQNO                  RECNUM,
 LDVL_DL_LOAD_STATUS            CONSTANT "L",
 LDVL_TSK_TKG_SRC_TYPE 		    CONSTANT "CNT",
 LDVL_TPY_PAY_TYPE_IND          CONSTANT "P",
 LDVL_TSK_ALT_REFERENCE         CHAR "rtrim(:LDVL_TSK_ALT_REFERENCE)",
 LDVL_TSK_TKG_SRC_REFERENCE     CHAR "rtrim(:LDVL_TSK_TKG_SRC_REFERENCE)",
 LDVL_TSK_TKG_CODE              CHAR "rtrim(:LDVL_TSK_TKG_CODE)",
 LDVL_TSK_STK_CODE              CHAR "rtrim(:LDVL_TSK_STK_CODE)",
 LDVL_TPY_SCO_CODE              CHAR "rtrim(:LDVL_TPY_SCO_CODE)",
 LDVL_VALUED_DATE               DATE "DD-MON-YYYY" NULLIF LDVL_VALUED_DATE = BLANKS,
 LDVL_TPY_TASK_NET_AMOUNT       CHAR "rtrim(:LDVL_TPY_TASK_NET_AMOUNT)",
 LDVL_TPY_TASK_TAX_AMOUNT       CHAR "rtrim(:LDVL_TPY_TASK_TAX_AMOUNT)",
 LDVL_CAD_PRO_AUN_CODE          CHAR "rtrim(:LDVL_CAD_PRO_AUN_CODE)",
 LDVL_CAD_TYPE_IND              CHAR "rtrim(:LDVL_CAD_TYPE_IND)",
 LDVL_DVE_DISPLAY_SEQUENCE      CHAR "rtrim(:LDVL_DVE_DISPLAY_SEQUENCE)",
 LDVL_DVE_STD_CODE              CHAR "rtrim(:LDVL_DVE_STD_CODE)",
 LDVL_DVE_ESTIMATED_COST        CHAR "rtrim(:LDVL_DVE_ESTIMATED_COST)",
 LDVL_DLV_BHE_CODE              CHAR "rtrim(:LDVL_DLV_BHE_CODE)",
 LDVL_BUD_BCA_YEAR              CHAR "rtrim(:LDVL_BUD_BCA_YEAR)",
 LDVL_DVE_QUANTITY              CHAR "rtrim(:LDVL_DVE_QUANTITY)",
 LDVL_DVE_HRV_PMU_CODE_QUANTITY CHAR "rtrim(:LDVL_DVE_HRV_PMU_CODE_QUANTITY)",
 LDVL_DVE_UNIT_COST             CHAR "rtrim(:LDVL_DVE_UNIT_COST)",
 LDVL_DVE_PROJECTED_COST        CHAR "rtrim(:LDVL_DVE_PROJECTED_COST)",
 LDVL_COMMENTS                  CHAR(2000) "rtrim(:LDVL_COMMENTS)",
 LDVL_HRV_VTY_CODE              CHAR "rtrim(:LDVL_HRV_VTY_CODE)"
)


