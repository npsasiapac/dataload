CREATE OR REPLACE PACKAGE s_dl_hem_prop_landlord_banks
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  	WHEN       		WHY
--      1.0     MB  	15-Jul-2016   	Initial Dataload Version
--
--
--  declare package variables AND constants
--***********************************************************************
--  DESCRIPTION
--
--  1:  ...
--  2:  ...
--  REFERENCES FUNCTION
--
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
END  s_dl_hem_prop_landlord_banks;
/

