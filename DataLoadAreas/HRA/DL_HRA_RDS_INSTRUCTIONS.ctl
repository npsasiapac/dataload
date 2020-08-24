-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
-- 01.01        T.Goodley   05-Feb-18  Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_INSTRUCTIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LRDIN_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LRDIN_DL_SEQNO                   RECNUM
,LRDIN_DL_LOAD_STATUS             CONSTANT "L"
,LRDIN_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LRDIN_RDSA_HA_REFERENCE))"
,LRDIN_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LRDIN_HRV_DEDT_CODE))"
,LRDIN_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LRDIN_HRV_RBEG_CODE))"
,LRDIN_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LRDIN_RAUD_START_DATE=blanks
,LRDIN_EFFECTIVE_DATE             DATE "DD-MON-YYYY" NULLIF LRDIN_EFFECTIVE_DATE=blanks
,LRDIN_INSTRUCTION_AMOUNT         CHAR "rtrim(:LRDIN_INSTRUCTION_AMOUNT)"
,LRDIN_CREATED_BY                 CHAR "rtrim(UPPER(:LRDIN_CREATED_BY))"
,LRDIN_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDIN_CREATED_DATE=blanks
,LRDIN_END_DATE                   DATE "DD-MON-YYYY" NULLIF LRDIN_END_DATE=blanks
,LRDIN_REFNO                      INTEGER EXTERNAL "rdin_seq.nextval"
)



