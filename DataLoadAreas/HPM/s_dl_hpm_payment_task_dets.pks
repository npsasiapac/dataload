CREATE OR REPLACE PACKAGE  s_dl_hpm_payment_task_dets	
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION		WHO	WHEN       		WHY
--      1.0		VRS	28-SEP-2007		Dataload
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
END s_dl_hpm_payment_task_dets;
/
