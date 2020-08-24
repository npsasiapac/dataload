-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
--
-- *********************************************************************
--
load data
APPEND
into table DL_HRA_SUBSIDY_LETTERS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LSULE_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LSULE_DL_SEQNO                   RECNUM
,LSULE_DL_LOAD_STATUS             CONSTANT "L"
,LSULE_SURV_LEGACY_REF            CHAR "rtrim(UPPER(:LSULE_SURV_LEGACY_REF))"
,LSULE_SEQ		          CHAR "rtrim(UPPER(:LSULE_SEQ))"
,LSULE_HSLT_CODE                  CHAR "rtrim(UPPER(:LSULE_HSLT_CODE))"
,LSULE_SCO_CODE    	          CHAR "rtrim(UPPER(:LSULE_SCO_CODE))"
,LSULE_CREATED_DATE	          DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSULE_CREATED_DATE=blanks
,LSULE_CREATED_BY	 	  CHAR "rtrim(UPPER(:LSULE_CREATED_BY))"
,LSULE_COMMENTS                   CHAR "rtrim(UPPER(:LSULE_COMMENTS))"
,LSULE_PRINTED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSULE_PRINTED_DATE=blanks
,LSULE_CANCELLED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSULE_CANCELLED_DATE=blanks
,LSULE_CANCELLED_BY	 	  CHAR "rtrim(UPPER(:LSULE_CANCELLED_BY))"
,LSULE_MODIFIED_DATE	          DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LSULE_MODIFIED_DATE=blanks
,LSULE_MODIFIED_BY	          CHAR "rtrim(UPPER(:LSULE_MODIFIED_BY))"
,LSULE_HLCR_CODE                  CHAR "rtrim(UPPER(:LSULE_HLCR_CODE))"
)



