--
-- *********************************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.13.0    MJK  21-APR-16    Version Control Commenced 
--  1.1     6.13.0    MJK  22-APR-16    NULLIF LDLV_DVE_ACTUAL_END_DATE 
--                                      Corrected to read
--                                      NULLIF LDLV_DLV_ACTUAL_END_DATE
--**********************************************************************************
--
load data
infile $GRI_DATAFILE
APPEND
into table DL_HPM_DELIVERABLES 
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
 LDLV_DLB_BATCH_ID 			CONSTANT "$batch_no",
 LDLV_DL_SEQNO 				RECNUM,
 LDLV_DL_LOAD_STATUS 			CONSTANT "L",
 LDLV_DLV_CNT_REFERENCE			CHAR "rtrim(:LDLV_DLV_CNT_REFERENCE)",
 LDLV_DLV_SCO_CODE                      CHAR "rtrim(:LDLV_DLV_SCO_CODE)",
 LDLV_DLV_STATUS_DATE			DATE "DD-MON-YYYY"  NULLIF LDLV_DLV_STATUS_DATE=blanks,
 LDLV_DLV_AUTHORISED_BY                 CHAR "rtrim(:LDLV_DLV_AUTHORISED_BY)",
 LDLV_DLV_ACTUAL_END_DATE               DATE "DD-MON-YYYY"  NULLIF LDLV_DLV_ACTUAL_END_DATE=blanks,
 LDLV_DLV_CAD_PRO_AUN_CODE		CHAR "rtrim(:LDLV_DLV_CAD_PRO_AUN_CODE)",
 LDLV_DLV_CAD_TYPE_IND			CHAR "rtrim(:LDLV_DLV_CAD_TYPE_IND)",
 LDLV_DVE_DISPLAY_SEQUENCE		CHAR "rtrim(:LDLV_DVE_DISPLAY_SEQUENCE)",
 LDLV_DVE_STD_CODE			CHAR "rtrim(:LDLV_DVE_STD_CODE)",
 LDLV_DVE_PLANNED_START_DATE		DATE "DD-MON-YYYY"  NULLIF LDLV_DVE_PLANNED_START_DATE=blanks,
 LDLV_DVE_ESTIMATED_COST                CHAR "rtrim(:LDLV_DVE_ESTIMATED_COST)",
 LDLV_DVE_SOR_CODE			CHAR "rtrim(:LDLV_DVE_SOR_CODE)",
 LDLV_BHE_CODE				CHAR "rtrim(:LDLV_BHE_CODE)",
 LDLV_BCA_YEAR				CHAR "rtrim(:LDLV_BCA_YEAR)",
 LDLV_DVE_VCA_CODE			CHAR "rtrim(:LDLV_DVE_VCA_CODE)",
 LDLV_DVE_DESCRIPTION			CHAR(4000) "rtrim(:LDLV_DVE_DESCRIPTION)",
 LDLV_DVE_QUANTITY                      CHAR "rtrim(:LDLV_DVE_QUANTITY)",
 LDLV_DVE_HRV_PMU_CODE_QUANTITY		CHAR "rtrim(:LDLV_DVE_HRV_PMU_CODE_QUANTITY)",
 LDLV_DVE_UNIT_COST                     CHAR "rtrim(:LDLV_DVE_UNIT_COST)",
 LDLV_DVE_PROJECTED_COST                CHAR "rtrim(:LDLV_DVE_PROJECTED_COST)",
 LDLV_DVE_HRV_LOC_CODE			CHAR "rtrim(:LDLV_DVE_HRV_LOC_CODE)",
 LDLV_DLV_REFNO				INTEGER EXTERNAL "to_char(dlv_refno_seq.nextval)"
)
