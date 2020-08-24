load data
infile $gri_datafile
APPEND
into table DL_CUS_DATALOAD_BATCHES
fields terminated by "," optionally enclosed by '"'
trailing nullcols    
(LDL_DLB_BATCH_ID          CONSTANT "$batch_no",
 LDL_DL_SEQNO              RECNUM, 
 LDL_DL_LOAD_STATUS        CONSTANT "L",
 LDL_BATCH_SEQNO           CHAR "rtrim(upper(:LDL_BATCH_SEQNO))",
 LDL_BATCH_ID              CHAR "rtrim(upper(:LDL_BATCH_ID))",
 LDL_PRODUCT_AREA          CHAR "rtrim(upper(:LDL_PRODUCT_AREA))",
 LDL_DATALOAD_AREA         CHAR "rtrim(upper(:LDL_DATALOAD_AREA))",
 LDL_QUESTION_ANSWER1      CHAR "rtrim(upper(:LDL_QUESTION_ANSWER1))",
 LDL_QUESTION_ANSWER2      CHAR "rtrim(upper(:LDL_QUESTION_ANSWER2))",
 LDL_REFNO                 SEQUENCE (MAX)
)
