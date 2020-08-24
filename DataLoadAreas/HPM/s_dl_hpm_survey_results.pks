CREATE OR REPLACE PACKAGE  s_dl_hpm_survey_results
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN       WHY
--      1.0     PJD  02/03/2004  Dataload
--      2.0     PJD  12/04/2005  Added set_record_status_flag
--      3.0     VRS  02/03/2006  Removed set_record_status_flag and
--                               replaced with call to s_dl_utils.set_record_status_flag
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  4.0     5.13.0    PH   06-FEB-2008  Now includes its own set
--                                      record status procedure
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
END s_dl_hpm_survey_results;
/
