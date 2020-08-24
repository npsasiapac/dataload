CREATE OR REPLACE PACKAGE s_dl_hpl_psl_lease_rents
AS
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION   WHO  WHEN        WHY
--      1.0   PJD  04-MAR-2013 YE Dataload for LBN
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
END  s_dl_hpl_psl_lease_rents;
--
/
