CREATE OR REPLACE PACKAGE s_dl_hat_hless_ins_stage_decis
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    MB 	03-DEC-2009	Initial version
--
--
--  declare package variables AND constants
--
--
--***********************************************************************
--
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE dataload_create    (p_batch_id          IN VARCHAR2
                             ,p_date              IN DATE);
--
PROCEDURE dataload_validate  (p_batch_id          IN VARCHAR2
                             ,p_date              IN DATE);
--
PROCEDURE dataload_delete    (p_batch_id          IN VARCHAR2
                             ,p_date              IN DATE);
--
END  s_dl_hat_hless_ins_stage_decis;
/