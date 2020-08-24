CREATE OR REPLACE PACKAGE  s_dl_ltl_land_title_releases
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     IR  29/10/2007  Initial Create
--
--  declare package variables AND constants
--
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END s_dl_ltl_land_title_releases;
/

