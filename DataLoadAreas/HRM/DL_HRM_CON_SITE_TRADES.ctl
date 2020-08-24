load data
infile $GRI_DATAFILE
APPEND
into table DL_HRM_CON_SITE_TRADES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LCSR_DLB_BATCH_ID      	        CONSTANT "$batch_no"
,LCSR_DL_SEQNO          	        RECNUM
,LCSR_DL_LOAD_STATUS    	        CONSTANT "L"
,LCSR_COS_CODE                        CHAR "rtrim(:LCSR_COS_CODE)"
,LCSR_HRV_TRD_CODE                    CHAR "rtrim(:LCSR_HRV_TRD_CODE)"
,LCSR_LABOUR_DAY_RATE                 CHAR NULLIF LCSR_LABOUR_DAY_RATE=blanks
,LCSR_LABOUR_OVERTIME                 CHAR NULLIF LCSR_LABOUR_OVERTIME=blanks
,LCSR_TRADE_DAY_RATE                  CHAR NULLIF LCSR_TRADE_DAY_RATE=blanks
,LCSR_TRADE_OVERTIME                  CHAR NULLIF LCSR_TRADE_OVERTIME=blanks
,LCSR_MILEAGE_RATE                    CHAR NULLIF LCSR_MILEAGE_RATE=blanks
,LCSR_CALL_OUT_CHARGE                 CHAR NULLIF LCSR_CALL_OUT_CHARGE=blanks
)
