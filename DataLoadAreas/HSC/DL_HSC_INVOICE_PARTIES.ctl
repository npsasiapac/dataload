load data
infile $gri_datafile
APPEND
into table DL_HSC_INVOICE_PARTIES
fields terminated by "," optionally enclosed by '"'
trailing nullcols    
(LSCIP_DLB_BATCH_ID       CONSTANT "$batch_no",
 LSCIP_DL_SEQNO           RECNUM, 
 LSCIP_DL_LOAD_STATUS     CONSTANT "L",
 LSCIP_PAR_ALT_REF        CHAR "rtrim(:LSCIP_PAR_ALT_REF)",
 LSCIP_PAY_REF            CHAR "rtrim(:LSCIP_PAY_REF)",
 LSCIP_START_DATE         DATE "DD-MON-YYYY" NULLIF LSCIP_START_DATE=blanks,
 LSCIP_END_DATE           DATE "DD-MON-YYYY" NULLIF LSCIP_END_DATE=blanks
)

