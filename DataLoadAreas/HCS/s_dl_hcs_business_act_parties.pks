CREATE OR REPLACE PACKAGE s_dl_hcs_business_act_parties
AS
--  DESCRIPTION:
--
--  VERSION  DB Ver   WHO            WHEN         WHY
--      1.0  6.18     Rob Heath      05-FEB-2019  Initial Creation for SAHT
--                                                New Data Load
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
END s_dl_hcs_business_act_parties;
/
