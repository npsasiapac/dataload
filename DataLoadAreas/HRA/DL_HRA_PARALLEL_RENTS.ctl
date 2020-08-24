load data
APPEND
into table DL_HRA_PARALLEL_RENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
( LPRE_DLB_BATCH_ID       CONSTANT "$batch_no",
 LPRE_DL_SEQNO            RECNUM,
 LPRE_DL_LOAD_STATUS      CONSTANT "L",
 LPRE_PAY_REF             CHAR "rtrim(:LPRE_PAY_REF)",
 LPRE_GROSS_RENT          CHAR "rtrim(:LPRE_GROSS_RENT)",
 LPRE_BALANCE             CHAR "rtrim(:LPRE_BALANCE)",
 LPRE_DATE                DATE "DD-MON-YYYY" NULLIF lpre_date=blanks)

