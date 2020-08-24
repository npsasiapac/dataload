-- *********************************************************************
-- Multi Area Data loader for Other Fields
-- Version DBver   Who   date         Why
--  1.0    6.10    AJ    21-JUN-2017  Change Control Added only
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_MAD_OTHER_FIELD_VALUES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPVA_DLB_BATCH_ID       CONSTANT "$batch_no",
 LPVA_DL_SEQNO           RECNUM,
 LPVA_DL_LOAD_STATUS     CONSTANT "L",
 LPVA_LEGACY_REF         CHAR "rtrim(:LPVA_LEGACY_REF)",
 LPVA_PDF_NAME           CHAR "rtrim(:LPVA_PDF_NAME)",
 LPVA_DATE_VALUE         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPVA_DATE_VALUE=blanks,
 LPVA_NUMBER_VALUE       NULLIF LPVA_NUMBER_VALUE=blanks,
 LPVA_CHAR_VALUE         CHAR "rtrim(:LPVA_CHAR_VALUE)",
 LPVA_SECONDARY_REF      CHAR "rtrim(:LPVA_SECONDARY_REF)",
 LPVA_SECONDARY_DATE     DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPVA_SECONDARY_DATE=blanks,
 LPVA_PDU_POB_TABLE_NAME CHAR "rtrim(:LPVA_PDU_POB_TABLE_NAME)",
 LPVA_CREATED_BY         CHAR "rtrim(:LPVA_CREATED_BY)",
 LPVA_CREATED_DATE       DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPVA_CREATED_DATE=blanks,
 LPVA_MODIFIED_BY        CHAR "rtrim(:LPVA_MODIFIED_BY)",
 LPVA_MODIFIED_DATE      DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LPVA_MODIFIED_DATE=blanks,
 LPVA_FURTHER_REF        CHAR "rtrim(:LPVA_FURTHER_REF)",
 LPVA_HRV_LOC_CODE	     CHAR "rtrim(:LPVA_HRV_LOC_CODE)",
 LPVA_FURTHER_REF2       CHAR "rtrim(:LPVA_FURTHER_REF2)",
 LPVA_FURTHER_REF3       CHAR "rtrim(:LPVA_FURTHER_REF3)",
 LPVA_DESC               CHAR "rtrim(:LPVA_DESC)",
 LPVA_BM_GRP_SEQ         CHAR "rtrim(:LPVA_BM_GRP_SEQ)"
)

