CREATE OR REPLACE PACKAGE s_dl_hra_arrears_arrangements
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION     WHO  WHEN         WHY
--      3.0  5.15.1    MB  09-Jun-2009   Bespoked for Midland Heart - end existing arrangements question, 
--                                                     set record status flag procedure
--      3.1  6.9.0     AJ  11-NOV-2013  Checked for 6.9.0
--      5.0  6.13.0    JS  23-MAR-2016  Checked for 6.13.0
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
END  s_dl_hra_arrears_arrangements;
--
/
