load data
APPEND
into table DL_HCO_COS_DEPOTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LCDEP_DLB_BATCH_ID     CONSTANT "$BATCH_NO"
,LCDEP_DL_SEQNO         RECNUM
,LCDEP_DL_LOAD_STATUS   CONSTANT "L"
,LCDEP_COS_CODE         CHAR "rtrim(:LCDEP_COS_CODE)"
,LCDEP_DEP_CODE         CHAR "rtrim(:LCDEP_DEP_CODE)"
,LCDEP_CURRENT_IND      CHAR "rtrim(:LCDEP_CURRENT_IND)"
) 
