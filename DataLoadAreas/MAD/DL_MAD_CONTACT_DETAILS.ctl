-- *********************************************************************
--
-- Version DBver   Who   date         Why
--  1.0            AJ    13-MAY-2016  Initial Creation WITH Change Control
--                                    Added LCDE_OCO_FORENAME and LCDE_OCO_SURNAME
--                                    for Organisation Contacts
--  1.1            AJ    09-MAR-2017  Added Organisation Contacts extra fields
--  1.2    6.14    AJ    28-MAR-2017  Added Organisation Contacts lcde_oco_update
--                                    and lcde_oco_create Y/N marker fields
--
-- *********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_MAD_CONTACT_DETAILS
fields terminated by "," optionally enclosed by '"'
trailing nullcols    
(LCDE_DLB_BATCH_ID       CONSTANT   "$batch_no",
 LCDE_DL_SEQNO           RECNUM, 
 LCDE_DL_LOAD_STATUS     CONSTANT   "L",
 LCDE_OCO_CREATED        CONSTANT   "N",
 LCDE_LEGACY_REF         CHAR       "rtrim(:LCDE_LEGACY_REF)",
 LCDE_LEGACY_TYPE        CHAR       "rtrim(:LCDE_LEGACY_TYPE)",
 LCDE_START_DATE         DATE       "DD-MON-YYYY HH24:MI:SS" NULLIF LCDE_START_DATE=blanks,
 LCDE_CREATED_DATE       DATE       "DD-MON-YYYY HH24:MI:SS" NULLIF LCDE_CREATED_DATE=blanks,
 LCDE_CREATED_BY         CHAR       "rtrim(:LCDE_CREATED_BY)",
 LCDE_CONTACT_VALUE      CHAR       "rtrim(:LCDE_CONTACT_VALUE)",
 LCDE_FRV_CME_CODE       CHAR       "rtrim(:LCDE_FRV_CME_CODE)",
 LCDE_CONTACT_NAME       CHAR       "rtrim(:LCDE_CONTACT_NAME)",
 LCDE_END_DATE           DATE       "DD-MON-YYYY HH24:MI:SS" NULLIF LCDE_END_DATE=blanks,
 LCDE_PRECEDENCE         CHAR       "rtrim(:LCDE_PRECEDENCE)",
 LCDE_FRV_COMM_PREF_CODE CHAR       "rtrim(:LCDE_FRV_COMM_PREF_CODE)",
 LCDE_ALLOW_TEXTS        CHAR       "rtrim(:LCDE_ALLOW_TEXTS)",
 LCDE_SECONDARY_REF      CHAR       "rtrim(:LCDE_SECONDARY_REF)",
 LCDE_COMMENTS           CHAR(2000) "rtrim(:LCDE_COMMENTS)",
 LCDE_OCO_FORENAME       CHAR       "rtrim(:LCDE_OCO_FORENAME)",
 LCDE_OCO_SURNAME        CHAR       "rtrim(:LCDE_OCO_SURNAME)",
 LCDE_OCO_FRV_TITLE      CHAR       "rtrim(:LCDE_OCO_FRV_TITLE)",
 LCDE_OCO_UPDATE         CHAR       "rtrim(:LCDE_OCO_UPDATE)",
 LCDE_OCO_CREATE         CHAR       "rtrim(:LCDE_OCO_CREATE)",
 LCDE_OCO_START_DATE     DATE       "DD-MON-YYYY" NULLIF LCDE_OCO_START_DATE=blanks,
 LCDE_OCO_END_DATE       DATE       "DD-MON-YYYY" NULLIF LCDE_OCO_END_DATE=blanks,
 LCDE_OCO_SIGNATORY_IND  CHAR       "rtrim(:LCDE_OCO_SIGNATORY_IND)",
 LCDE_OCO_FRV_OCR_CODE   CHAR       "rtrim(:LCDE_OCO_FRV_OCR_CODE)",
 LCDE_OCO_FRV_OPL_CODE   CHAR       "rtrim(:LCDE_OCO_FRV_OPL_CODE)",
 LCDE_OCO_COMMENTS       CHAR(2000) "rtrim(:LCDE_OCO_COMMENTS)"
)

