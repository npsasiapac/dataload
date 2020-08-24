load data
APPEND
into table DL_HCS_PEOPLE_GROUP_ROLES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LPGO_DLB_BATCH_ID                 CONSTANT "$batch_no"
,LPGO_DL_SEQNO                     RECNUM
,LPGO_DL_LOAD_STATUS               CONSTANT "L"
,LPGO_PGE_PEG_CODE                 CHAR "rtrim(upper(:LPGO_PGE_PEG_CODE))"
,LPGO_PGE_PAR_REFNO                CHAR "rtrim(:LPGO_PGE_PAR_REFNO)"
,LPGO_GRO_CODE                     CHAR "rtrim(upper(:LPGO_GRO_CODE))"
,LPGO_START_DATE                   DATE "DD-MON-YYYY" NULLIF LPGO_START_DATE=blanks
,LPGO_END_DATE                     DATE "DD-MON-YYYY" NULLIF LPGO_END_DATE=blanks
,LPGO_COMMENTS                     CHAR "rtrim(:LPGO_COMMENTS)"
,LPGO_CREATED_DATE                 DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPGO_CREATED_DATE=blanks
,LPGO_CREATED_BY                   CHAR "upper(rtrim(:LPGO_CREATED_BY))"
)


