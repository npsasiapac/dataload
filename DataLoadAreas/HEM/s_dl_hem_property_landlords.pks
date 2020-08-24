CREATE OR REPLACE PACKAGE s_dl_hem_property_landlords
AS
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  	WHEN       		WHY
--      1.0     VRS  	21-MAY-2006   	Initial Dataload Version
--		1.2		MB		29-SEP-2011		Added set_record_status_flag
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
END  s_dl_hem_property_landlords;
/