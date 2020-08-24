CREATE OR REPLACE PACKAGE  S_DL_CUS_DATALOAD_BATCHES
AS
-- ***********************************************************************
--  DESCRIPTION:
-- dl batches
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     PLL  1/6/18     Dataload
--
--  VERSION DB Vers   WHO  WHEN         WHY
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
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
-- Called from trigger dps_after_u
PROCEDURE run_next ( p_status VARCHAR2
                   , p_process VARCHAR2
                   , p_run_id VARCHAR2);
                   
END S_DL_CUS_DATALOAD_BATCHES;
/
