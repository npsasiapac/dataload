load data
APPEND
into table DL_HPM_CONTRACT_SORS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LCVS_DLB_BATCH_ID         CONSTANT "$batch_no"
,LCVS_DL_SEQNO             RECNUM
,LCVS_DL_LOAD_STATUS       CONSTANT "L"
,LCVS_CNT_REFERENCE        CHAR "rtrim(:LCVS_CNT_REFERENCE)"
,LCVS_CVE_VERSION_NUMBER   CHAR "rtrim(:LCVS_CVE_VERSION_NUMBER)"
,LCVS_SOR_CODE             CHAR "rtrim(:LCVS_SOR_CODE)"
,LCVS_CURRENT_IND          CHAR "rtrim(:LCVS_CURRENT_IND)"
,LCVS_REPEAT_UNIT          CHAR "rtrim(:LCVS_REPEAT_UNIT)"
,LCVS_REPEAT_PERIOD_IND    CHAR "rtrim(:LCVS_REPEAT_PERIOD_IND)"
,LCPC_START_DATE           DATE "DD-MON-YYYY" NULLIF LCPC_START_DATE=blanks
,LCPC_PRICE                CHAR "rtrim(:LCPC_PRICE)"
,LCPC_END_DATE             DATE "DD-MON-YYYY" NULLIF LCPC_END_DATE=blanks
)


