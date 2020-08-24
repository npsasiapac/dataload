CREATE OR REPLACE PACKAGE s_dl_hem_object_admin_units
AS
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION   WHO  WHEN        WHY
--      1.0   AJ   25-OCT-2013 Admin Unit Security Dataload for HEM 
--
--  declare package variables AND constants
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
END s_dl_hem_object_admin_units;
--
/
