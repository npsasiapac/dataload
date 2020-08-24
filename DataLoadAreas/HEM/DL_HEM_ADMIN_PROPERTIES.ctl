load data
APPEND
into table DL_HEM_ADMIN_PROPERTIES


fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LAPR_DLB_BATCH_ID CONSTANT "$batch_no",
LAPR_DL_SEQNO RECNUM ,
LAPR_DL_LOAD_STATUS CONSTANT "L",
LAPR_PRO_PROPREF CHAR "rtrim(:LAPR_PRO_PROPREF)",
LAPR_AUN_CODE CHAR "rtrim(:LAPR_AUN_CODE)")
