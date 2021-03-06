load data
infile $GRI_DATAFILE
APPEND
into table DL_HRM_INSPECTIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LINS_DLB_BATCH_ID CONSTANT "$batch_no"
,LINS_DL_SEQNO RECNUM
,LINS_DL_LOAD_STATUS CONSTANT "L"
,LINS_SRQ_LEGACY_REFNO CHAR "rtrim(:LINS_SRQ_LEGACY_REFNO)"
,LINS_LEGACY_REFNO CHAR "rtrim(:LINS_LEGACY_REFNO)"
,LINS_SEQNO CHAR NULLIF LINS_SEQNO=blanks "TO_NUMBER(:LINS_SEQNO)"
,LINS_RAISED_DATE DATE "DD-MON-YYYY" NULLIF LINS_RAISED_DATE=blanks
,LINS_TARGET_DATE DATE "DD-MON-YYYY" NULLIF LINS_TARGET_DATE=blanks
,LINS_PRINTED_IND CHAR "rtrim(:LINS_PRINTED_IND)"
,LINS_HRV_ITY_CODE CHAR "rtrim(:LINS_HRV_ITY_CODE)"
,LINS_HRV_IRN_CODE CHAR "rtrim(:LINS_HRV_IRN_CODE)"
,LINS_PRI_CODE CHAR "rtrim(:LINS_PRI_CODE)"
,LINS_SCO_CODE CHAR "rtrim(:LINS_SCO_CODE)"
,LINS_AUN_CODE CHAR "rtrim(:LINS_AUN_CODE)"
,LINS_PRO_PROPREF CHAR "rtrim(:LINS_PRO_PROPREF)"
,LINS_DESCRIPTION CHAR "rtrim(:LINS_DESCRIPTION)"
,LINS_ISSUED_DATE DATE "DD-MON-YYYY" NULLIF LINS_ISSUED_DATE=blanks
,LINS_COMPLETED_DATE DATE "DD-MON-YYYY" NULLIF LINS_COMPLETED_DATE=blanks
,LINS_STATUS_DATE DATE "DD-MON-YYYY" NULLIF LINS_STATUS_DATE=blanks
,LINS_ALTERNATIVE_REFNO CHAR "rtrim(:LINS_ALTERNATIVE_REFNO)"
,LIVI_SHORTNAME CHAR "rtrim(:LIVI_SHORTNAME)"
,LIVI_SCO_CODE CHAR "rtrim(:LIVI_SCO_CODE)"
,LIVI_STATUS_DATE DATE "DD-MON-YYYY" NULLIF LIVI_STATUS_DATE=blanks
,LIVI_VISIT_DATE DATE "DD-MON-YYYY" NULLIF LIVI_VISIT_DATE=blanks
,LIVI_VISIT_DESCRIPTION CHAR "rtrim(:LIVI_VISIT_DESCRIPTION)"
,LIVI_IRE_CODE CHAR "rtrim(:LIVI_IRE_CODE)"
,LIVI_RESULT_DESCRIPTION CHAR "rtrim(:LIVI_RESULT_DESCRIPTION)"
)
