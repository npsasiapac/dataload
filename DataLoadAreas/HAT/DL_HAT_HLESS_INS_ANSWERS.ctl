load data
infile $gri_datafile
APPEND
into table DL_HAT_HLESS_INS_ANSWERS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LHIA_DLB_BATCH_ID       CONSTANT "$batch_no",    
LHIA_DL_SEQNO            RECNUM,
LHIA_DL_LOAD_STATUS      CONSTANT "L", 
LHIN_INSTANCE_REFNO      CHAR "rtrim(:LHIN_INSTANCE_REFNO)",
LHIA_QUE_REFNO           CHAR "rtrim(:LHIA_QUE_REFNO)",
LHIA_CREATED_BY          CHAR "rtrim(:LHIA_CREATED_BY)",
LHIA_CREATED_DATE        DATE "DD-MON-YYYY" NULLIF LHIA_CREATED_DATE=blanks,
LHIA_DATE_VALUE          DATE "DD-MON-YYYY" NULLIF LHIA_DATE_VALUE=blanks,
LHIA_NUMBER_VALUE        CHAR "rtrim(:LHIA_NUMBER_VALUE)",
LHIA_CHAR_VALUE          CHAR "rtrim(:LHIA_CHAR_VALUE)",
LHIA_OTHER_CODE          CHAR "rtrim(:LHIA_OTHER_CODE)",
LHIA_OTHER_DATE          DATE "DD-MON-YYYY" NULLIF LHIA_OTHER_DATE=blanks,
LHIA_QOR_CODE            CHAR "rtrim(:LHIA_QOR_CODE)",
LHIA_COMMENTS            CHAR(2000) "rtrim(:LHIA_COMMENTS)"
)




   
