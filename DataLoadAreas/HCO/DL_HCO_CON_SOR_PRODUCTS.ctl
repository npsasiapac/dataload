load data
APPEND
into table DL_HCO_CON_SOR_PRODUCTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LCSPH_DLB_BATCH_ID          CONSTANT "$BATCH_NO"
,LCSPH_DL_SEQNO              RECNUM
,LCSPH_DL_LOAD_STATUS        CONSTANT "L"
,LCSPH_COS_CODE              CHAR "rtrim(:LCSPH_COS_CODE)"
,LCSPH_SOR_CODE              CHAR "rtrim(:LCSPH_SOR_CODE)"
,LCSPS_START_DATE            DATE NULLIF LCSPS_START_DATE=blanks
,LCSPS_END_DATE              DATE NULLIF LCSPS_END_DATE=blanks
,LCSPR_PROD_CODE             CHAR "rtrim(:LCSPR_PROD_CODE)"
,LCSPR_DEFAULT_QUANTITY      CHAR "rtrim(:LCSPR_DEFAULT_QUANTITY)"
,LCSPR_HRV_UOM_CODE          CHAR "rtrim(:LCSPR_HRV_UOM_CODE)"
)
