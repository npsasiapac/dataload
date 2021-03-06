load data
APPEND
into table DL_HEM_CONSENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LCNS_DLB_BATCH_ID               CONSTANT "$BATCH_NO"
,LCNS_DL_SEQNO                   RECNUM
,LCNS_DL_LOAD_STATUS             CONSTANT "L"
,LCNS_PAR_REFERENCE_TYPE         CHAR "rtrim(UPPER(:LCNS_PAR_REFERENCE_TYPE))"
,LCNS_PAR_REFERENCE              CHAR "rtrim(UPPER(:LCNS_PAR_REFERENCE))"
,LCNS_GRANTED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCNS_GRANTED_DATE=blanks
,LCNS_START_DATE                 DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCNS_START_DATE=blanks
,LCNS_PAR_REFERENCE_GRANTED      CHAR "rtrim(UPPER(:LCNS_PAR_REFERENCE_GRANTED))"
,LCNS_HRV_CTYP_CODE              CHAR "rtrim(UPPER(:LCNS_HRV_CTYP_CODE))"
,LCNS_HRV_CSO_CODE               CHAR "rtrim(UPPER(:LCNS_HRV_CSO_CODE))"
,LCNS_CREATED_BY                 CHAR "rtrim(UPPER(:LCNS_CREATED_BY))"
,LCNS_CREATED_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCNS_CREATED_DATE=blanks
,LCNS_HRV_CER_CODE               CHAR "rtrim(UPPER(:LCNS_HRV_CER_CODE))"
,LCNS_END_DATE                   DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCNS_END_DATE=blanks
,LCNS_ALTERNATIVE_REFERENCE      CHAR "rtrim(UPPER(:LCNS_ALTERNATIVE_REFERENCE))"
,LCNS_REVIEW_DATE                DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LCNS_REVIEW_DATE=blanks
,LCNS_COMMENTS                   CHAR(2000) "rtrim(:LCNS_COMMENTS)"
,LCNS_REFNO                      INTEGER EXTERNAL "cns_refno_seq.nextval"
)
