CREATE OR REPLACE PACKAGE  s_dl_hem_income_liabilities
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.11      AJ   22-Dec-2015  Initial Creation new functionality
--                                      released at v611 income liabilities
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
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END s_dl_hem_income_liabilities;
/
