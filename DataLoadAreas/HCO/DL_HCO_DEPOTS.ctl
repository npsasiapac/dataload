load data
APPEND
into table DL_HCO_DEPOTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LDEP_DLB_BATCH_ID      CONSTANT "$BATCH_NO"
,LDEP_DL_SEQNO          RECNUM
,LDEP_DL_LOAD_STATUS    CONSTANT "L"
,LDEP_CODE              CHAR "rtrim(:LDEP_CODE)"
,LDEP_DESCRIPTION       CHAR "rtrim(:LDEP_DESCRIPTION)"
,LDEP_CURRENT_IND       CHAR "rtrim(:LDEP_CURRENT_IND)"
,LDEP_DEP_CODE          CHAR "rtrim(:LDEP_DEP_CODE)"
,LDEP_CODE_MLANG        CHAR "rtrim(:LDEP_CODE_MLANG)"
,LDEP_DESCRIPTION_MLANG CHAR "rtrim(:LDEP_DESCRIPTION_MLANG)"
) 
