CREATE OR REPLACE PACKAGE  s_dl_hpm_contracts
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION	DB VER	WHO	WHEN         WHY
--      1.0	5.12	VRS	10-SEP-2007  Dataload
--      1.1     5.12.0  PH      16-NOV-2007  Added in validate_variation_amt   
--
--  declare package variables AND constants
--
PROCEDURE validate_variation_amt 
      ( p_prj_reference             IN projects.prj_reference%TYPE,
        p_cnt_reference             IN contracts.cnt_reference%TYPE,
        p_cve_max_variation_amount  IN contract_versions.cve_max_variation_amount%TYPE,
        p_rec_status                IN OUT VARCHAR2 );
--
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END s_dl_hpm_contracts;
/
