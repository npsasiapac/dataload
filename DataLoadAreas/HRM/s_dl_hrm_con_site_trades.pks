CREATE OR REPLACE PACKAGE s_dl_hrm_con_site_trades
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN         WHY
--      1.0     SB   13-SEP-2002  Initial Creation
--      1.1     PJD  27-AUG-2012  Renamed to con_site_trades
--      
--
--  declare package variables AND constants
--***********************************************************************
--  DESCRIPTION
--
--  1:  ...
--  2:  ...
--  REFERENCES FUNCTION
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
END  s_dl_hrm_con_site_trades;
/

show errors

