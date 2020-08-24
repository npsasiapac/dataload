CREATE OR REPLACE PACKAGE s_dl_hra_utils
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN       WHY
--      1.0          PJD  26/11/02   Product Dataload
--
-- ***********************************************************************
--
FUNCTION overlapping_contract
         (p_rac_accno  IN NUMBER
         ,p_par_refno  IN NUMBER
         ,p_start_date IN DATE
         ,p_end_date   IN DATE) RETURN BOOLEAN;
--
 FUNCTION pct_refno
         (p_rac_accno  IN NUMBER
         ,p_par_refno  IN NUMBER
         ,p_start_date IN DATE
         ,p_end_date   IN DATE) RETURN INTEGER;
--
FUNCTION f_bru_run_no
(
 p_bru_aun_code VARCHAR2
,p_effective_date DATE
)
RETURN NUMBER;
PROCEDURE insert_bank_details
    (p_bde_bank_name         IN varchar2,
     p_bde_branch_name       IN varchar2,
     p_bad_account_no        IN varchar2,
     p_bad_account_name      IN varchar2,
     p_bad_sort_code         IN varchar2,
     p_bad_start_date        IN DATE,
     p_bde_refno             OUT integer,
     p_bad_refno             OUT integer
     );
--
END s_dl_hra_utils;

/


