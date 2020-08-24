-- *********************************************************************
-- Multi Area Dataload for Other Field Values History
-- Version DBver   Who   date         Why
--  1.0    6.13    AJ    21-JUN-2017  Change Control Added
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_MAD_OTHER_FIELD_VAL_HIST
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPVH_DLB_BATCH_ID       	    CONSTANT "$batch_no",
 LPVH_DL_SEQNO           	    RECNUM,
 LPVH_DL_LOAD_STATUS     	    CONSTANT "L",
 LPVH_LEGACY_REF         	    CHAR "rtrim(:LPVH_LEGACY_REF)",
 LPVH_PVA_PDU_PDF_NAME   	    CHAR "rtrim(:LPVH_PVA_PDU_PDF_NAME)",
 LPVH_PVA_DATE_VALUE         	DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPVH_PVA_DATE_VALUE=blanks,
 LPVH_PVA_NUMBER_VALUE       	CHAR "rtrim(:LPVH_PVA_NUMBER_VALUE)",
 LPVH_PVA_CHAR_VALUE         	CHAR "rtrim(:LPVH_PVA_CHAR_VALUE)",
 LPVH_SECONDARY_REF      	    CHAR "rtrim(:LPVH_SECONDARY_REF)",
 LPVH_SECONDARY_DATE     	    DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPVH_SECONDARY_DATE=blanks,
 LPVH_PVA_PDU_POB_TABLE_NAME	CHAR "rtrim(:LPVH_PVA_PDU_POB_TABLE_NAME)",
 LPVH_CREATED_BY                CHAR "rtrim(:LPVH_CREATED_BY)",
 LPVH_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPVH_CREATED_DATE=blanks,
 LPVH_MODIFIED_BY               CHAR "rtrim(:LPVH_MODIFIED_BY)",
 LPVH_MODIFIED_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPVH_MODIFIED_DATE=blanks,
 LPVH_PVA_FURTHER_REF           CHAR "rtrim(:LPVH_PVA_FURTHER_REF)",
 LPVH_PVA_HRV_LOC_CODE          CHAR "rtrim(:LPVH_PVA_HRV_LOC_CODE)",
 LPVH_PVA_FURTHER_REF2          CHAR "rtrim(:LPVH_PVA_FURTHER_REF2)",
 LPVH_PVA_FURTHER_REF3          CHAR "rtrim(:LPVH_PVA_FURTHER_REF3)",
 LPVH_DESC                      CHAR "rtrim(:LPVH_DESC)"
)

