CREATE OR REPLACE PACKAGE s_dl_mad_contact_details
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  RELEASE    WHO  WHEN        WHY
--
--      1.0             PH   02-NOV-2004 Initial Creation
--      2.0  6.10/11    AJ   04-MAR-2015 Amended to MAD dataload Area
--      3.0  6.10/11    AJ   01-JUN-2015 Show errors added to bottom
--      4.0  6.10/11    AJ   05-JUN-2015 Altered Release column in description bit
--
--  declare package variables AND constants
--
-- ***********************************************************************
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
END s_dl_mad_contact_details;
/
show errors
