CREATE OR REPLACE PACKAGE s_dl_hat_app_legacy_ref
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN        WHY
--      1.0  6.14     AJ   02/10/2017  Bespoke Dataload for GNB HAT
--                                     Migration
--
--  declare package variables AND constants
--
--
--***********************************************************************
--
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE dataload_create    (p_batch_id          IN VARCHAR2
                             ,p_date              IN DATE);
--
PROCEDURE dataload_validate  (p_batch_id          IN VARCHAR2
                             ,p_date              IN DATE);
--
PROCEDURE dataload_delete    (p_batch_id          IN VARCHAR2
                             ,p_date              IN DATE);
--
END  s_dl_hat_app_legacy_ref;
/

