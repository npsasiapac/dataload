CREATE OR REPLACE PACKAGE s_dl_hat_involved_party_hist
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0  6.14     AJ   13/10/2017 Initial Creation for GNB Migration Project
--
--  declare package variables AND constants
--
--
--***********************************************************************
--
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE dataload_create    (p_batch_id           IN VARCHAR2
                             ,p_date               IN DATE);
--
PROCEDURE dataload_validate  (p_batch_id           IN VARCHAR2
                             ,p_date               IN DATE);
--
PROCEDURE dataload_delete    (p_batch_id           IN VARCHAR2
                             ,p_date               IN DATE);
--
END  s_dl_hat_involved_party_hist;
/

