load data
APPEND
into table DL_HSC_SERVICE_CHARGE_BASES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LSCB_DLB_BATCH_ID              CONSTANT "$batch_no"
,LSCB_DL_LOAD_STATUS            CONSTANT "L"
,LSCB_DL_SEQNO                  RECNUM
,LSCB_SCP_CODE                  CHAR "rtrim(:LSCB_SCP_CODE)"
,LSCB_SCP_START_DATE            DATE "DD-MON-YYYY" NULLIF LSCB_SCP_START_DATE=blanks
,LSCB_SVC_ATT_ELE_CODE          CHAR "rtrim(:LSCB_SVC_ATT_ELE_CODE)"
,LSCB_SVC_ATT_CODE              CHAR "rtrim(:LSCB_SVC_ATT_CODE)"
,LSCB_DESCRIPTION               CHAR "rtrim(:LSCB_DESCRIPTION)"
,LSCB_COST_BASIS                CHAR "rtrim(:LSCB_COST_BASIS)"
,LSCB_APPORTION_IND             CHAR "rtrim(:LSCB_APPORTION_IND)"
,LSCB_APPLY_CAP_IND             CHAR "rtrim(:LSCB_APPLY_CAP_IND)"
,LSCB_INCREASE_TYPE             CHAR "rtrim(:LSCB_INCREASE_TYPE)"
,LSCB_TAX_IND                   CHAR "rtrim(:LSCB_TAX_IND)"
,LSCB_CHARGE_APPLICABLE_TO      CHAR "rtrim(:LSCB_CHARGE_APPLICABLE_TO)"
,LSCB_COMPLETE_IND              CHAR "rtrim(:LSCB_COMPLETE_IND)"
,LSCB_REBATEABLE_IND            CHAR "rtrim(:LSCB_REBATEABLE_IND)"
,LSCB_ADJUSTMENT_METHOD         CHAR "rtrim(:LSCB_ADJUSTMENT_METHOD)"
,LSCB_NOM_ADMIN_PERIOD          CHAR "rtrim(:LSCB_NOM_ADMIN_PERIOD)"
,LSCB_ELE_CODE                  CHAR "rtrim(:LSCB_ELE_CODE)"
,LSCB_VCA_CODE                  CHAR "rtrim(:LSCB_VCA_CODE)"
,LSCB_REAPPORTION_ACTUALS_IND   CHAR "rtrim(:LSCB_REAPPORTION_ACTUALS_IND)"
,LSCB_DEFAULT_PERCENT_INCREASE  CHAR "rtrim(:LSCB_DEFAULT_PERCENT_INCREASE)"
,LSCB_EXTRACT_FROM_REPAIRS_IND  CHAR "rtrim(:LSCB_EXTRACT_FROM_REPAIRS_IND)"
,LSCB_INCLUDE_PROP_REPAIRS_IND  CHAR "rtrim(:LSCB_INCLUDE_PROP_REPAIRS_IND)"
,LSCB_COMPONENT_LEVEL           CHAR "rtrim(:LSCB_COMPONENT_LEVEL)"
,LSCB_AUY_CODE                  CHAR "rtrim(:LSCB_AUY_CODE)"
)



