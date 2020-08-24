CREATE OR REPLACE PACKAGE s_dl_hrm_works_orders
AS
-- ***********************************************************************
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     PH  12/07/01   Dataload
--      2.0     PJD 28/12/04   Now includes its own 
--                             set record status procedure
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
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END  s_dl_hrm_works_orders;
--
/
