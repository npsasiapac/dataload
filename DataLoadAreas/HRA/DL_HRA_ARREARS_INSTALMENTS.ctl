load data
infile $gri_datafile
APPEND
into table DL_HRA_ARREARS_INSTALMENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LAIN_DLB_BATCH_ID        CONSTANT "$batch_no"
,LAIN_DL_SEQNO            RECNUM
,LAIN_DL_LOAD_STATUS      CONSTANT "L"
,LAIN_PAY_REF             CHAR "rtrim(:LAIN_PAY_REF)"
,LAIN_START_DATE          DATE "DD-MON-YYYY" NULLIF LAIN_START_DATE=BLANKS
,LAIN_ARA_CODE            CHAR "rtrim(:LAIN_ARA_CODE)" 
,LAIN_SEQNO               CHAR "rtrim(:LAIN_SEQNO)"
,LAIN_AMOUNT              CHAR "rtrim(:LAIN_AMOUNT)"
,LAIN_DUE_DATE            DATE "DD-MON-YYYY" NULLIF LAIN_DUE_DATE=BLANKS
,LAIN_AMEND_IND           CHAR "rtrim(:LAIN_AMEND_IND)"
,LAIN_EXT_PROCESSED_DATE  DATE "DD-MON-YYYY" NULLIF LAIN_EXT_PROCESSED_DATE=BLANKS
,LAIN_COMMENTS            CHAR "rtrim(:LAIN_COMMENTS)"
)
