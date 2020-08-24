-- *********************************************************************
--
-- Version    Who   Date         Why
-- 1.0        AJ    22-FEB-2017  Initial Creation for Queensland Bespoke
--                               for CR462
-- 1.1        AJ    27-FEB-2017  Added LTPY_TSK_STK_CODE and amended others
--
-- *********************************************************************
--
load data
infile $GRI_DATAFILE
APPEND
into table DL_HPM_TASK_PAYMENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LTPY_DLB_BATCH_ID 		      CONSTANT "$batch_no",
 LTPY_DL_SEQNO 			      RECNUM,
 LTPY_DL_LOAD_STATUS 		  CONSTANT "L",
 LTPY_TSK_ALT_REFERENCE       CHAR "rtrim(:LTPY_TSK_ALT_REFERENCE)",
 LTPY_TSK_TKG_SRC_REFERENCE   CHAR "rtrim(:LTPY_TSK_TKG_SRC_REFERENCE)",
 LTPY_TSK_TKG_CODE            CHAR "rtrim(:LTPY_TSK_TKG_CODE)",
 LTPY_TSK_STK_CODE            CHAR "rtrim(:LTPY_TSK_STK_CODE)",
 LTPY_SCO_CODE                CHAR "rtrim(:LTPY_SCO_CODE)",
 LTPY_STATUS_DATE             DATE "DD-MON-YYYY" NULLIF LTPY_STATUS_DATE = BLANKS,
 LTPY_DUE_DATE                DATE "DD-MON-YYYY" NULLIF LTPY_DUE_DATE = BLANKS,
 LTPY_TASK_NET_AMOUNT         CHAR "rtrim(:LTPY_TASK_NET_AMOUNT)",
 LTPY_TASK_TAX_AMOUNT         CHAR "rtrim(:LTPY_TASK_TAX_AMOUNT)",
 LTPY_PAID_DATE               DATE "DD-MON-YYYY" NULLIF LTPY_PAID_DATE = BLANKS,
 LTPY_PAYMENT_ID              CHAR "rtrim(:LTPY_PAYMENT_ID)",
 LTPY_PAYMENT_DATE            DATE "DD-MON-YYYY" NULLIF LTPY_PAYMENT_DATE = BLANKS
)


