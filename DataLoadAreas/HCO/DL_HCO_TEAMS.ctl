load data
APPEND
into table DL_HCO_TEAMS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LTEA_DLB_BATCH_ID                CONSTANT "$BATCH_NO"
,LTEA_DL_SEQNO                    RECNUM
,LTEA_DL_LOAD_STATUS              CONSTANT "L"
,LTEA_CODE                        CHAR "rtrim(:LTEA_CODE)"
,LTEA_NAME                        CHAR "rtrim(:LTEA_NAME)"
,LTEA_TYPE_IND                    CHAR "rtrim(:LTEA_TYPE_IND)"
,LTEA_LEVEL_IND                   CHAR "rtrim(:LTEA_LEVEL_IND)"
,LTEA_CURRENT_IND                 CHAR "rtrim(:LTEA_CURRENT_IND)"
,LTEA_CDEP_COS_CODE               CHAR "rtrim(:LTEA_CDEP_COS_CODE)"
,LTEA_CDEP_DEP_CODE               CHAR "rtrim(:LTEA_CDEP_DEP_CODE)"
,LTEA_TEA_CODE                    CHAR "rtrim(:LTEA_TEA_CODE)"
,LTEA_DEFAULT_UTILISATION_PCT     NULLIF LTEA_DEFAULT_UTILISATION_PCT=BLANKS
,LTEA_COMMENTS                    CHAR "rtrim(:LTEA_COMMENTS)"
)
