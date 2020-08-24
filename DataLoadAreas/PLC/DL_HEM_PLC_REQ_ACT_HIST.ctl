load data
APPEND
into table DL_HEM_PLC_REQ_ACT_HIST
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPRAH_DLB_BATCH_ID                CONSTANT "$BATCH_NO"
,LPRAH_DL_SEQNO                    RECNUM
,LPRAH_DL_LOAD_STATUS              CONSTANT "L"
,LPRAH_PLPR_REFNO                  CHAR "rtrim(UPPER(:LPRAH_PLPR_REFNO))"
,LPRAH_PLAC_CODE                   CHAR "rtrim(UPPER(:LPRAH_PLAC_CODE))"
,LPRAH_COMPLETED_IND               CHAR "rtrim(UPPER(:LPRAH_COMPLETED_IND))"
,LPRAH_SCO_CODE_FROM               CHAR "rtrim(UPPER(:LPRAH_SCO_CODE_FROM))"
,LPRAH_CREATED_BY                  CHAR "rtrim(UPPER(:LPRAH_CREATED_BY))"
,LPRAH_CREATED_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPRAH_CREATED_DATE=blanks
,LPRAH_COMPLETED_BY                CHAR "rtrim(UPPER(:LPRAH_COMPLETED_BY))"
,LPRAH_COMPLETED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPRAH_COMPLETED_DATE=blanks
,LPRAH_SCO_CODE_TO                 CHAR "rtrim(UPPER(:LPRAH_SCO_CODE_TO))"
)






