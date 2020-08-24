CREATE OR REPLACE PACKAGE s_dl_hsc_lease_summaries
AS
-- ***********************************************************************
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     PH   07/09/03   Initial Creation
--
--  declare package variables AND constants
--
--
--***********************************************************************
--
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);

--
END  s_dl_hsc_lease_summaries;
--
/

