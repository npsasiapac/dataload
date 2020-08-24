load data
APPEND
into table DL_HEM_PROPERTY_ELEMENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LPEL_DLB_BATCH_ID   CONSTANT "$batch_no",
LPEL_DL_SEQNO       RECNUM ,
LPEL_DL_LOAD_STATUS CONSTANT "L",
LPEL_PRO_PROPREF    CHAR "rtrim(:LPEL_PRO_PROPREF)",
LPEL_ETY_CODE       CHAR "rtrim(:LPEL_ETY_CODE)",
LPEL_START          DATE "DD-MON-YYYY" NULLIF lpel_start=blanks,
LPEL_ATTY_CODE      CHAR "rtrim(:LPEL_ATTY_CODE)",
LPEL_DATE           DATE "DD-MON-YYYY" NULLIF lpel_date=blanks,
LPEL_VALUE          CHAR NULLIF lpel_value=blanks "TO_NUMBER(:lpel_value)",
LPEL_HRV_REPCAT     CHAR "rtrim(:LPEL_HRV_REPCAT)",
LPEL_END            DATE "DD-MON-YYYY" NULLIF lpel_end=blanks,
LPEL_HRV_ELO_CODE   CHAR "rtrim(:LPEL_HRV_ELO_CODE)",
LPEL_FAT_CODE       CHAR "rtrim(:LPEL_FAT_CODE)",
LPEL_TEXT           CHAR "rtrim(:LPEL_TEXT)",
LPEL_AUN_IND        CHAR "rtrim(:LPEL_AUN_IND)",
LPEL_HRV_RCO_CODE   CHAR "rtrim(:LPEL_HRV_RCO_CODE)",
LPEL_QUANTITY       CHAR NULLIF lpel_quantity=blanks "TO_NUMBER(:lpel_quantity)",
LPEL_ATTR_TYPE CHAR "decode(rtrim(:lpel_atty_code),null,decode(:lpel_date,null,'N','D'),'C')")








