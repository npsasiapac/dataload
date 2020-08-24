CREATE OR REPLACE PACKAGE  s_dl_hem_org_admin_units
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     PJD  05/9/2000  Dataload
--
--  VERSION DB Vers   WHO  WHEN          WHY
--  1.0     6.14      AJ   22-DEC-2016   Initial creation of new organisations
--                                       admin units data loader
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
END s_dl_hem_org_admin_units;
/

