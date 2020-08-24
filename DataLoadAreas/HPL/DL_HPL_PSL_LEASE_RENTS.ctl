-- *********************************************************************
--
-- Version      Who   date         Why
-- 01.00        AJ    23-FEB-2016  Initial Creation WITH Change Control
--                                 as added LPSLR_COMMENTS
-- *********************************************************************
--
load data
APPEND
into table DL_HPL_PSL_LEASE_RENTS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LPSLR_DLB_BATCH_ID CONSTANT "$batch_no"
,LPSLR_DL_SEQNO RECNUM
,LPSLR_DL_LOAD_STATUS CONSTANT "L"
,LPSLR_PSL_REF_TYPE CHAR "rtrim(:LPSLR_PSL_REF_TYPE)"
,LPSLR_PSL_REF CHAR "rtrim(:LPSLR_PSL_REF)"
,LPSLR_ANNUAL_RENT CHAR NULLIF LPSLR_ANNUAL_RENT=blanks "TO_NUMBER(:LPSLR_ANNUAL_RENT)"
,LPSLR_START_DATE DATE "DD-MON-YYYY" NULLIF LPSLR_START_DATE=blanks
,LPSLR_END_DATE DATE "DD-MON-YYYY" NULLIF LPSLR_END_DATE=blanks
,LPSLR_REVIEW_DATE DATE "DD-MON-YYYY" NULLIF LPSLR_REVIEW_DATE=blanks
,LPSLR_COMMENTS CHAR(2000) "rtrim(:LPSL_COMMENTS)"
)
