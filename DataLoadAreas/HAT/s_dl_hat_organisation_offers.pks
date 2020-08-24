CREATE OR REPLACE PACKAGE  s_dl_hat_organisation_offers
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.16      MJK  26-MAR-2018  Initial Creation.
--
--***********************************************************************
--
PROCEDURE set_record_status_flag
  (p_rowid           IN ROWID
  ,p_status          IN VARCHAR2
  );
PROCEDURE dataload_create       
  (p_batch_id        IN VARCHAR2
  ,p_date            IN DATE
  );
PROCEDURE dataload_validate     
  (p_batch_id        IN VARCHAR2
  ,p_date            IN DATE
  );
PROCEDURE dataload_delete       
  (p_batch_id        IN VARCHAR2
  ,p_date            IN DATE
  );
END s_dl_hat_organisation_offers;
/

