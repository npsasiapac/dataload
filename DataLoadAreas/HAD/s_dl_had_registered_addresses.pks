CREATE OR REPLACE PACKAGE  s_dl_had_registered_addresses
AS
-- ***********************************************************************
--  DESCRIPTION:
-- adresses
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     MK   08/8/2013  Dataload
--
--***********************************************************************
--
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END s_dl_had_registered_addresses;
/
