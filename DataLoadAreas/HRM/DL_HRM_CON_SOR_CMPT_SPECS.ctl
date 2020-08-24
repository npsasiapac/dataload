load data                                                                       
infile $gri_datafile                                                                                                            
APPEND                                                                          
into table DL_HRM_CON_SOR_CMPT_SPECS                                                   
fields terminated by "," optionally enclosed by '"'                             
trailing nullcols
(
LCSCS_DLB_BATCH_ID          CONSTANT "$BATCH_NO",
LCSCS_DL_SEQNO              RECNUM,
LCSCS_DL_LOAD_STATUS        CONSTANT "L",
LCSPG_PPC_COS_CODE          CHAR "rtrim(:LCSPG_PPC_COS_CODE)",
LCSPG_PPC_PPP_PPG_CODE      CHAR "rtrim(:LCSPG_PPC_PPP_PPG_CODE)",
LCSPG_PPC_PPP_WPR_CODE      CHAR "rtrim(:LCSPG_PPC_PPP_WPR_CODE)",
LCSPG_PPC_PPP_START_DATE    DATE "DD-MON-YYYY" NULLIF LCSPG_PPC_PPP_START_DATE=BLANKS,
LCSPG_START_DATE	    DATE "DD-MON-YYYY" NULLIF LCSPG_START_DATE=BLANKS,
LCSCS_CSP_SOR_CODE          CHAR "rtrim(:LCSCS_CSP_SOR_CODE)",
LCSCS_START_DATE            DATE "DD-MON-YYYY" NULLIF LCSCS_START_DATE=BLANKS,
LCSCS_END_DATE              DATE "DD-MON-YYYY" NULLIF LCSCS_END_DATE=BLANKS
)

