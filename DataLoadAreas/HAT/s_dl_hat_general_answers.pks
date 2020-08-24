CREATE OR REPLACE PACKAGE s_dl_hat_general_answers
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     RJ   3/4/2001   Dataload
--      1.1     PJD  31/10/2000 Include latest revisions to dataload process
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  2.0     5.13.0    PH   06-FEB-2008  Now includes its own set
--                                      record status procedure
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
END  s_dl_hat_general_answers;
/

