-- ***********************************************************************
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   18-MAR-2013  Initial Version.
--
-- ***********************************************************************
load data
APPEND
into table DL_HEM_ICS_PAYMENT_CMPTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LICPC_DLB_BATCH_ID               CONSTANT "$BATCH_NO",
 LICPC_DL_SEQNO                   RECNUM,
 LICPC_DL_LOAD_STATUS             CONSTANT "L",
 LICPC_IBP_INDR_REFERENCE         CHAR "rtrim(UPPER(:LICPC_IBP_INDR_REFERENCE))",
 LICPC_IBP_HRV_IBEN_CODE          CHAR "rtrim(UPPER(:LICPC_IBP_HRV_IBEN_CODE))",
 LICPC_IBP_HRV_IPS_CODE           CHAR "rtrim(UPPER(:LICPC_IBP_HRV_IPS_CODE))",
 LICPC_IBP_HRV_ICPT_CODE          CHAR "rtrim(UPPER(:LICPC_IBP_HRV_ICPT_CODE))",
 LICPC_AMOUNT                     CHAR "rtrim(UPPER(:LICPC_AMOUNT))",
 LICPC_HRV_ICT_CODE               CHAR "rtrim(UPPER(:LICPC_HRV_ICT_CODE))",
 LICPC_CREATED_BY                 CHAR "rtrim(UPPER(:LICPC_CREATED_BY))",
 LICPC_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LICPC_CREATED_DATE=blanks,
 LICPC_COMPONENT_PAYMENT_CODE     CHAR "rtrim(UPPER(:LICPC_COMPONENT_PAYMENT_CODE))",
 LICPC_REFNO                      INTEGER EXTERNAL "icpc_refno_seq.nextval"
)
