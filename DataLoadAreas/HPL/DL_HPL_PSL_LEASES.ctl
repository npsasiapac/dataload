-- *********************************************************************
--
-- Version      Who   date         Why
--   1.0        AJ    23-FEB-2016  Initial Creation WITH Change Control
--                                 as added LPSL_RENT_COMMENTS
--   1.1        AJ    23-FEB-2016  Getting of psl_refno removed and put
--                                 into create in package
--
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HPL_PSL_LEASES
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LPSL_DLB_BATCH_ID              CONSTANT "$batch_no",
LPSL_DL_SEQNO                  RECNUM,
LPSL_DL_LOAD_STATUS            CONSTANT "L",
LPSL_LEGACY_REF                CHAR "rtrim(:LPSL_LEGACY_REF)",	
LPSL_PRO_PROPREF               CHAR "rtrim(:LPSL_PRO_PROPREF)",
LPSL_LEASE_START_DATE          DATE "DD-MON-YYYY" NULLIF LPSL_LEASE_START_DATE=blanks,	
LPSL_LEASE_END_DATE            DATE "DD-MON-YYYY" NULLIF LPSL_LEASE_END_DATE=blanks,	
LPSL_PSY_CODE                  CHAR "rtrim(:LPSL_PSY_CODE)",	
LPSL_SCO_CODE                  CHAR "rtrim(:LPSL_SCO_CODE)",
LPSL_STATUS_DATE               DATE "DD-MON-YYYY" NULLIF LPSL_STATUS_DATE=blanks,	
LPSL_SCL_PSLS_CODE             CHAR "rtrim(:LPSL_SCL_PSLS_CODE)",
LPSL_RENT_START_DATE           DATE "DD-MON-YYYY" NULLIF LPSL_RENT_START_DATE=blanks,	
LPSL_RENT_END_DATE             DATE "DD-MON-YYYY" NULLIF LPSL_RENT_END_DATE=blanks,	
LPSL_EXTENSION_END_DATE        DATE "DD-MON-YYYY" NULLIF LPSL_EXTENSION_END_DATE=blanks,	
LPSL_PROPOSED_HANDBACK_DATE    DATE "DD-MON-YYYY" NULLIF LPSL_PROPOSED_HANDBACK_DATE=blanks,	
LPSL_EXT_NOTICE_SERVED_DATE    DATE "DD-MON-YYYY" NULLIF LPSL_EXT_NOTICE_SERVED_DATE=blanks,	
LPSL_HBACK_NOT_SERVE_DATE      DATE "DD-MON-YYYY" NULLIF LPSL_HBACK_NOT_SERVE_DATE=blanks,	
LPSL_ACTUAL_HANDBACK_DATE      DATE "DD-MON-YYYY" NULLIF LPSL_ACTUAL_HANDBACK_DATE=blanks,	
LPSL_PSLR_ANNUAL_RENT          CHAR "rtrim(:LPSL_PSLR_ANNUAL_RENT)",	
LPSL_COMMENTS                  CHAR(2000) "rtrim(:LPSL_COMMENTS)",	
LPSL_SCL_START_DATE            DATE "DD-MON-YYYY" NULLIF LPSL_SCL_START_DATE=blanks,	
LPSL_SCL_END_DATE              DATE "DD-MON-YYYY" NULLIF LPSL_SCL_END_DATE=blanks,	
LPSL_PSLR_START_DATE           DATE "DD-MON-YYYY" NULLIF LPSL_PSLR_START_DATE=blanks,	
LPSL_PSLR_END_DATE             DATE "DD-MON-YYYY" NULLIF LPSL_PSLR_END_DATE=blanks,	
LPSL_PSLR_REVIEW_DATE          DATE "DD-MON-YYYY" NULLIF LPSL_PSLR_REVIEW_DATE=blanks,	
LPSL_LLORD_PAID_IN_ADVANCE_IND CHAR "rtrim(:LPSL_LLORD_PAID_IN_ADVANCE_IND)",	
LPSL_RENT_COMMENTS             CHAR(2000) "rtrim(:LPSL_COMMENTS)"
)

