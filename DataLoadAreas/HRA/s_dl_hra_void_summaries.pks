CREATE OR REPLACE PACKAGE s_dl_hra_void_summaries
AS
-- ***********************************************************************
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     PJD  23/10/01   Dataload
--
--
--  declare package variables AND constants
--
--
--***********************************************************************
--
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);

--
END  s_dl_hra_void_summaries;
--
/

