CREATE OR REPLACE PACKAGE s_dl_hem_void_status_hist
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN         WHY
--      1.0     AJ   06-FEB-2017  Created for Queensland Migration CR461
--   
--
--  declare package variables AND constants
--***********************************************************************
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
END  s_dl_hem_void_status_hist;
/
