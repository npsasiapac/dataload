CREATE OR REPLACE PACKAGE s_dl_hpl_psl_leases
AS
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     DRH  29/04/05   Dataload
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  2.0     5.13.0    PH   06-FEB-2008  Tidy up of code
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
END  s_dl_hpl_psl_leases;
--
/
