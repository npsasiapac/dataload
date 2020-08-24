CREATE OR REPLACE PACKAGE s_dl_hco_sor_prdt_specificatn
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN         WHY
--      1.0     PH   16-AUG-2006  Initial Creation
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  2.0     5.13.0    PH   06-FEB-2008  Now includes its own set
--                                      record status procedure
--                              
--
--
--  declare package variables AND constants
--
--
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
END  s_dl_hco_sor_prdt_specificatn;
--
/

