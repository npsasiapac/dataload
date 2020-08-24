load data
APPEND
into table DL_HCO_VEHICLE_OPERATIVES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LVOP_DLB_BATCH_ID         CONSTANT "$BATCH_NO"
,LVOP_DL_SEQNO             RECNUM
,LVOP_DL_LOAD_STATUS       CONSTANT "L"
,LVOP_STO_LOCATION         CHAR "rtrim(:LVOP_STO_LOCATION)"
,LVOP_IPP_SHORTNAME        CHAR "rtrim(:LVOP_IPP_SHORTNAME)"
,LVOP_IPT_CODE             CHAR "rtrim(:LVOP_IPT_CODE)"
,LVOP_START_DATE           DATE "DD-MON-YYYY" NULLIF LVOP_START_DATE=blanks
,LVOP_END_DATE             DATE "DD-MON-YYYY" NULLIF LVOP_END_DATE=blanks
,LVOP_COMMENTS             CHAR "rtrim(:LVOP_COMMENTS)"
,LVOP_REFNO                INTEGER EXTERNAL "vop_refno_seq.nextval"
)

