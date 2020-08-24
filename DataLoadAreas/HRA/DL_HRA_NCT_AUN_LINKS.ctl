load data
APPEND
into table DL_HRA_NCT_AUN_LINKS


fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LNAL_DLB_BATCH_ID      CONSTANT "$batch_no",
LNAL_DL_SEQNO          RECNUM ,
LNAL_DL_LOAD_STATUS    CONSTANT "L",
LNAL_PAY_REF CHAR      "rtrim(:LNAL_PAY_REF)",
LNAL_AUN_CODE CHAR     "rtrim(:LNAL_AUN_CODE)"
)
