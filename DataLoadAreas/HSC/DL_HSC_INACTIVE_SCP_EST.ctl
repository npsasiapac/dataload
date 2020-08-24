load data                                                                       
infile $GRI_DATAFILE                                                            
APPEND                                                                          
into table DL_HSC_INACTIVE_SCP_EST                                                     
fields terminated by "," optionally enclosed by '"'                             
trailing nullcols
(LISE_DLB_BATCH_ID        CONSTANT "$BATCH_NO"
,LISE_DL_SEQNO            RECNUM
,LISE_DL_LOAD_STATUS      CONSTANT "L",
LISE_PROREFNO_AUNCODE     CHAR "rtrim(:LISE_PROREFNO_AUNCODE)",   
LISE_PRO_AUN_TYPE         CHAR "rtrim(:LISE_PRO_AUN_TYPE)",   
LISE_SCB_SCP_CODE         CHAR "rtrim(:LISE_SCB_SCP_CODE)",   
LISE_SCB_SCP_START_DATE   DATE "DD-MON-YYYY",  
LISE_SCB_SVC_ATT_ELE_CODE CHAR "rtrim(:LISE_SCB_SVC_ATT_ELE_CODE)",  
LISE_SCB_SVC_ATT_CODE     CHAR "rtrim(:LISE_SCB_SVC_ATT_CODE)",   
LISE_AMOUNT               CHAR NULLIF LISE_AMOUNT=blanks               "(TO_NUMBER(:LISE_AMOUNT))",  
LISE_ORIDE_WEIGHTING_TOT  CHAR NULLIF LISE_ORIDE_WEIGHTING_TOT=blanks  "(TO_NUMBER(:LISE_ORIDE_WEIGHTING_TOT))",  
LISE_RECONCILED_IND       CHAR "rtrim(:LISE_RECONCILED_IND)" )  

