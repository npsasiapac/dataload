CREATE OR REPLACE PACKAGE  s_dl_hpm_task_payments
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION		WHO	WHEN       		WHY
--      1.0		AJ  22-FEB-2017     New addition to HPM Bespoke Dataload for
--                                  Queensland CR462
--
--  declare package variables AND constants
--
--
PROCEDURE set_record_status_flag (p_rowid           IN ROWID,
                                  p_status          IN VARCHAR2);
PROCEDURE dataload_create        (p_batch_id        IN VARCHAR2,
                                  p_date            IN DATE);
PROCEDURE dataload_validate      (p_batch_id        IN VARCHAR2,
                                  p_date            IN DATE);
PROCEDURE dataload_delete        (p_batch_id        IN VARCHAR2,
                                  p_date            IN DATE);
--
END s_dl_hpm_task_payments;
/
