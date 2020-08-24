-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
-- 01.01        T.Goodley   05-Feb-18  Remove LRDAL_PAR_PER_ALT_REF as
--                                     it is not used anywhere.
--                                     Ensure date only columns only accept date.
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_RDS_ALLOCATIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LRDAL_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LRDAL_DL_SEQNO                   RECNUM
,LRDAL_DL_LOAD_STATUS             CONSTANT "L"
,LRDAL_RDSA_HA_REFERENCE          CHAR "rtrim(UPPER(:LRDAL_RDSA_HA_REFERENCE))"
,LRDAL_RAUD_START_DATE            DATE "DD-MON-YYYY" NULLIF LRDAL_RAUD_START_DATE=blanks
,LRDAL_HRV_DEDT_CODE              CHAR "rtrim(UPPER(:LRDAL_HRV_DEDT_CODE))"
,LRDAL_HRV_RBEG_CODE              CHAR "rtrim(UPPER(:LRDAL_HRV_RBEG_CODE))"
,LRDAL_RDIN_EFFECTIVE_DATE        DATE "DD-MON-YYYY" NULLIF LRDAL_RDIN_EFFECTIVE_DATE=blanks
,LRDAL_EFFECTIVE_DATE             DATE "DD-MON-YYYY" NULLIF LRDAL_EFFECTIVE_DATE=blanks
,LRDAL_ALLOCATED_AMOUNT           CHAR "rtrim(:LRDAL_ALLOCATED_AMOUNT)"
,LRDAL_DEDUCTION_ACTION_TYPE      CHAR "rtrim(UPPER(:LRDAL_DEDUCTION_ACTION_TYPE))"
,LRDAL_CREATED_BY                 CHAR "rtrim(UPPER(:LRDAL_CREATED_BY))"
,LRDAL_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LRDAL_CREATED_DATE=blanks
,LRDAL_REFNO                      INTEGER EXTERNAL "rdal_seq.nextval"
)


