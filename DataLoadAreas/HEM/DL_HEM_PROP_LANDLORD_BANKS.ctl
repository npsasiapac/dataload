--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  	WHEN       		WHY
--      1.0     MB  	15-Jul-2016   	Initial Dataload Version
--      1.1     AJ      26-Jul-2016     Added further fields for more detail
--                                      for bank_details and to help delete
--                                      process
--      1.2     AJ      29-Jul-2016     Amended delete the 3 indicator fields
--                                      added "_IND" to make them stand out more
--
--***********************************************************************
--
load data
APPEND
into table DL_HEM_PROP_LANDLORD_BANKS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPLB_DLB_BATCH_ID          CONSTANT "$batch_no",
 LPLB_DL_SEQNO              RECNUM,
 LPLB_DL_LOAD_STATUS        CONSTANT "L",
 LPLB_DEL_BDE_IND           CONSTANT "N",
 LPLB_DEL_BAD_IND           CONSTANT "N",
 LPLB_DEL_PBA_IND           CONSTANT "N",
 LPLB_PRO_PROPREF           CHAR "rtrim(:LPLB_PRO_PROPREF)",
 LPLB_PLD_START_DATE        DATE "DD-MON-YYYY" NULLIF LPLB_PLD_START_DATE=BLANKS,
 LPLB_PAR_REFNO             CHAR "rtrim(:LPLB_PAR_REFNO)",
 LPLB_ALT_PAR_REF           CHAR "rtrim(:LPLB_ALT_PAR_REF)",
 LPLB_START_DATE            DATE "DD-MON-YYYY" NULLIF LPLB_START_DATE=BLANKS,
 LPLB_BAD_ACCOUNT_NO        CHAR "rtrim(:LPLB_BAD_ACCOUNT_NO)",
 LPLB_BAD_ACCOUNT_NAME      CHAR "rtrim(:LPLB_BAD_ACCOUNT_NAME)",
 LPLB_BAD_SORT_CODE         CHAR "rtrim(:LPLB_BAD_SORT_CODE)",
 LPLB_BAD_START_DATE        DATE "DD-MON-YYYY" NULLIF LPLB_BAD_START_DATE=BLANKS,
 LPLB_BDE_BANK_NAME         CHAR "rtrim(:LPLB_BDE_BANK_NAME)",
 LPLB_BDE_BRANCH_NAME       CHAR "rtrim(:LPLB_BDE_BRANCH_NAME)",
 LPLB_BDE_BTY_CODE          CHAR "rtrim(:LPLB_BDE_BTY_CODE)",
 LPLB_BDE_BANK_CODE         CHAR "rtrim(:LPLB_BDE_BANK_CODE)",
 LPLB_BDE_BRANCH_CODE       CHAR "rtrim(:LPLB_BDE_BRANCH_CODE)",
 LPLB_BDE_BANK_NAME_MLANG   CHAR "rtrim(:LPLB_BDE_BANK_NAME_MLANG)",
 LPLB_BDE_BRANCH_NAME_MLANG CHAR "rtrim(:LPLB_BDE_BRANCH_NAME_MLANG)"
)
