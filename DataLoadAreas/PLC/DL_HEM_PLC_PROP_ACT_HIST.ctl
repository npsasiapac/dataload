load data
APPEND
into table DL_HEM_PLC_PROP_ACT_HIST
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPAH_DLB_BATCH_ID                CONSTANT "$BATCH_NO"
,LPAH_DL_SEQNO                    RECNUM
,LPAH_DL_LOAD_STATUS              CONSTANT "L"
,LPAH_PLPR_REFNO                  CHAR "rtrim(UPPER(:LPAH_PLPR_REFNO))"
,LPAH_PRO_SEQ_REFERENCE           CHAR "rtrim(UPPER(:LPAH_PRO_SEQ_REFERENCE))"
,LPAH_PENDING_PROP_IND            CHAR "rtrim(UPPER(:LPAH_PENDING_PROP_IND))"
,LPAH_PLAC_CODE                   CHAR "rtrim(UPPER(:LPAH_PLAC_CODE))"
,LPAH_COMPLETED_IND               CHAR "rtrim(UPPER(:LPAH_COMPLETED_IND))"
,LPAH_CREATED_BY                  CHAR "rtrim(UPPER(:LPAH_CREATED_BY))"
,LPAH_CREATED_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPAH_CREATED_DATE=blanks
,LPAH_COMPLETED_BY                CHAR "rtrim(UPPER(:LPAH_COMPLETED_BY))"
,LPAH_COMPLETED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPAH_COMPLETED_DATE=blanks
)






