CREATE OR REPLACE PACKAGE s_dl_hra_transactions
AS
-- ***********************************************************************
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN      WHY
--      1.0          PH  08/12/01   Dataload
--      2.0	       PJD 13/10/04   Now includes its own set record
--                                  status procedure
--      3.0  5.12    PH  19/06/07   Added DB Vers to Change Control
--                                  Included allocate_payment_to_account
--                                  procedure.
--
--
--  declare package variables AND constants


--***********************************************************************
--
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE allocate_payment_to_account( p_tra_refno      IN  NUMBER
                                     , p_rac_accno      IN  NUMBER
                                     , p_phe_batch_ref  IN  VARCHAR2
                                     , p_pos_seqno      IN  NUMBER 
                                     , p_external_ref   IN  VARCHAR2
                                     , p_cr_amount      IN  NUMBER);
--
PROCEDURE allocate_credit_to_invoice( p_clin_refno       IN  NUMBER
                                    , p_tra_refno        IN  NUMBER
                                    , p_amount           IN  NUMBER );

PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END  s_dl_hra_transactions;
--
/
