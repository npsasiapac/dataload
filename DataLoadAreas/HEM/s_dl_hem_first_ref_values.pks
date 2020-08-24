CREATE OR REPLACE PACKAGE s_dl_hem_first_ref_values
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0  6.16     AJ   28/03/2018 Initial Creation for OHMS Migration Project
--                                    New Data Load
--
--  declare package variables AND constants
--
--
--***********************************************************************
--
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE dataload_create    (p_batch_id           IN VARCHAR2
                             ,p_date               IN DATE);
--
PROCEDURE dataload_validate  (p_batch_id           IN VARCHAR2
                             ,p_date               IN DATE);
--
PROCEDURE dataload_delete    (p_batch_id           IN VARCHAR2
                             ,p_date               IN DATE);
--
END  s_dl_hem_first_ref_values;
/

