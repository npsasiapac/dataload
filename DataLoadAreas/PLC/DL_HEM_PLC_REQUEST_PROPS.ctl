load data
APPEND
into table DL_HEM_PLC_REQUEST_PROPS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPLRP_DLB_BATCH_ID                CONSTANT "$BATCH_NO"
,LPLRP_DL_SEQNO                    RECNUM
,LPLRP_DL_LOAD_STATUS              CONSTANT "L"
,LPLRP_PLPR_REFNO                  CHAR "rtrim(UPPER(:LPLRP_PLPR_REFNO))"
,LPLRP_PENDING_PROP_IND            CHAR "rtrim(UPPER(:LPLRP_PENDING_PROP_IND))"
,LPLRP_CREATED_BY                  CHAR "rtrim(UPPER(:LPLRP_CREATED_BY))"
,LPLRP_CREATED_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPLRP_CREATED_DATE=blanks
,LPLRP_SEQUENCE                    CHAR "rtrim(UPPER(:LPLRP_SEQUENCE))"
,LPLRP_PROP_REFERENCE              CHAR "rtrim(UPPER(:LPLRP_PROP_REFERENCE))"
,LPLRP_MODIFIED_BY                 CHAR "rtrim(UPPER(:LPLRP_MODIFIED_BY ))"
,LPLRP_MODIFIED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPLRP_MODIFIED_DATE=blanks
,LPLRP_REFNO                       INTEGER EXTERNAL "PLRP_REFNO_SEQ.NEXTVAL"
)






