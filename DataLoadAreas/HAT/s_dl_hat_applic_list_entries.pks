CREATE OR REPLACE PACKAGE s_dl_hat_applic_list_entries
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     RJ   3/4/2001   Dataload
--      1.1     PJD  31/10/2000 Include latest revisions to dataload process
--      2.0     PH   06-FEB-2008  Now includes its own set record status
--                                procedure
--                              
--
--
--  declare package variables AND constants
--
--
--***********************************************************************
--  DESCRIPTION
--
--  1:  ...
--  2:  ...
--  REFERENCES FUNCTION
--
--
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
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
END  s_dl_hat_applic_list_entries;
/

