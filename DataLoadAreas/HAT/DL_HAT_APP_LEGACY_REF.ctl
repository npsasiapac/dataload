--
-- ***********************************************************************
--  DESCRIPTION: This is to replace the app_legacy_ref used in the data load
--               which was actually the app_refno with the actual legacy_ref
--               in the pahse1 data base for GNB Migration of Allocations
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.14      AJ   19-SEP-2017  Initial Creation.
--  1.1     6.14      AJ   03-OCT-2017  1) Added LALR_APP_TYPE to allow for
--                                      Other legacy refs other than Applications
--                                      2) Added 2 fields for applic list entries
--  1.2     6.14      AJ   05-OCT-2017  Added LALR_UPDATE_REQ Default to N on load
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_APP_LEGACY_REF
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LALR_DLB_BATCH_ID        CONSTANT "$batch_no",
 LALR_DL_SEQNO            RECNUM,
 LALR_DL_LOAD_STATUS      CONSTANT "L",
 LALR_UPDATE_REQ          CONSTANT "N",
 LALR_APP_TYPE            CHAR "rtrim(:LALR_APP_TYPE)",
 LALR_APP_REFNO           CHAR "rtrim(:LALR_APP_REFNO)",
 LALR_APP_LEGACY_REF      CHAR "rtrim(:LALR_APP_LEGACY_REF)",
 LALR_ALE_RLI_CODE        CHAR "rtrim(:LALR_ALE_RLI_CODE)",
 LALR_ALE_ALT_REF         CHAR "rtrim(:LALR_ALE_ALT_REF)"
)

      
