-- *********************************************************************
--
-- Version DBver   Who   date         Why
--  1.0    6.13    AJ    11-MAY-2016  Initial Creation WITH Change Control
--                                    Amended LNOP_TEXT from 2000 to 4000
-- *********************************************************************
--
load data
APPEND
into table DL_MAD_NOTEPADS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(
LNOP_DLB_BATCH_ID 	    CONSTANT "$batch_no",
LNOP_DL_SEQNO 		    RECNUM,
LNOP_DL_LOAD_STATUS 	CONSTANT "L",
LNOP_TYPE 		        CHAR "rtrim(:LNOP_TYPE)",
LNOP_LEGACY_REF 	    CHAR "rtrim(:LNOP_LEGACY_REF)",
LNOP_SECONDARY_REF 	    CHAR "rtrim(:LNOP_SECONDARY_REF)",
LNOP_SECONDARY_DATE     DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LNOP_SECONDARY_DATE=blanks,
LNOP_CREATED_BY         CHAR "rtrim(:LNOP_CREATED_BY)",
LNOP_CREATED_DATE       DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LNOP_CREATED_DATE=blanks,
LNOP_CURRENT_IND        CHAR "rtrim(:LNOP_CURRENT_IND)",
LNOP_HIGHLIGHT_IND      CHAR "rtrim(:LNOP_HIGHLIGHT_IND)",
LNOP_TEXT               CHAR(4000) "rtrim(:LNOP_TEXT)",
LNOP_NTT_CODE           CHAR "rtrim(:LNOP_NTT_CODE)",
LNOP_APPLICATION_TYPE   CHAR "rtrim(:LNOP_APPLICATION_TYPE)",
LNOP_MODIFIED_BY        CHAR "rtrim(:LNOP_MODIFIED_BY)",
LNOP_MODIFIED_DATE      DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LNOP_MODIFIED_DATE=blanks
)
