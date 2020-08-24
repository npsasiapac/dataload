CREATE OR REPLACE PACKAGE BODY s_dl_hpm_deliverable_val
AS
-- **************************************************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER     DB version  WHO  WHEN         WHY
--  1.0		6.15        AJ   28-FEB-2017  Initial creation Bespoke for
--                                        Queensland CR462
--  1.1		6.15        AJ   03-MAR-2017  Scripting complete ready for testing
--  1.2		6.15        AJ   06-MAR-2017  Amended validation cursors referencing alt ref instead of
--                                        tsk_src_ref
--  1.3		6.15        AJ   07-MAR-2017  Amended validation..ldvl_amount not being held for create
--
--
-- **************************************************************************************************
--
PROCEDURE set_record_status_flag
  (p_rowid  IN ROWID
  ,p_status IN VARCHAR2
  )
AS
BEGIN
  UPDATE dl_hpm_deliverable_val
  SET    ldvl_dl_load_status = p_status
  WHERE  rowid = p_rowid;
 EXCEPTION
 WHEN OTHERS 
  THEN
   dbms_output.put_line('Error updating status of dl_hpm_deliverable_val');
    RAISE;
END set_record_status_flag;
--
-- **************************************************************************************************
--
PROCEDURE dataload_create
  (p_batch_id IN VARCHAR2
  ,p_date     IN DATE
  )
AS
CURSOR c1 
  (p_batch_id VARCHAR2) IS
SELECT ROWID rec_rowid
,ldvl_dlb_batch_id
,ldvl_dl_seqno
,ldvl_dl_load_status
,ldvl_tsk_tkg_src_type
,ldvl_tpy_pay_type_ind
,ldvl_tsk_alt_reference
,ldvl_tsk_tkg_src_reference
,ldvl_tsk_tkg_code
,ldvl_tsk_stk_code
,ldvl_tpy_sco_code
,ldvl_valued_date
,ldvl_tpy_task_net_amount
,ldvl_tpy_task_tax_amount
,ldvl_cad_pro_aun_code
,ldvl_cad_type_ind
,ldvl_dve_display_sequence
,ldvl_dve_std_code
,ldvl_dve_estimated_cost
,ldvl_dlv_bhe_code
,ldvl_bud_bca_year
,ldvl_dve_quantity
,ldvl_dve_hrv_pmu_code_quantity
,ldvl_dve_unit_cost
,ldvl_dve_projected_cost
,ldvl_comments
,ldvl_hrv_vty_code
,ldvl_tsk_id
,NVL(ldvl_created_by,'DATALOAD') ldvl_created_by
,NVL(ldvl_created_date, SYSDATE) ldvl_created_date
,ldvl_bud_refno
,ldvl_refno
,ldvl_dlv_dcp_refno
,ldvl_amount
,ldvl_pro_refno_aun_code
FROM   dl_hpm_deliverable_val
WHERE  ldvl_dlb_batch_id = p_batch_id
AND    ldvl_dl_load_status = 'V';
-- *********************************
--
CURSOR chk_deliv_val
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      ,p_tsk_id            NUMBER
      ,p_bud_refno         NUMBER
      ,p_dvl_type_ind      VARCHAR2
      ,p_dvl_dlv_dcp_refno NUMBER)
IS
SELECT 'X'
FROM deliverable_valuations
WHERE dvl_tsk_tkg_src_reference = p_tkg_src_reference
AND   dvl_tsk_tkg_code = p_tkg_code
AND   dvl_tsk_tkg_src_type = p_tkg_src_type
AND   dvl_tsk_id = p_tsk_id
AND   dvl_bud_refno = p_bud_refno
AND   dvl_type_ind = p_dvl_type_ind
AND   dvl_dlv_dcp_refno = p_dvl_dlv_dcp_refno
AND   dvl_current_ind = 'Y';
--
CURSOR chk_deliv_val2
      (p_bud_refno         NUMBER
      ,p_dvl_type_ind      VARCHAR2
      ,p_dvl_dlv_dcp_refno NUMBER)
IS
SELECT 'X'
FROM deliverable_valuations
WHERE dvl_bud_refno = p_bud_refno
AND   dvl_type_ind = p_dvl_type_ind
AND   dvl_dlv_dcp_refno = p_dvl_dlv_dcp_refno
AND   dvl_current_ind ='Y';
--
CURSOR chk_deliv_val3
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      ,p_tsk_id            NUMBER)
IS
SELECT 'X'
FROM deliverable_valuations
WHERE dvl_tsk_tkg_src_reference = p_tkg_src_reference
AND   dvl_tsk_tkg_code = p_tkg_code
AND   dvl_tsk_tkg_src_type = p_tkg_src_type
AND   dvl_tsk_id = p_tsk_id
AND   dvl_current_ind = 'Y';
--
CURSOR get_dvl_refno
IS
SELECT dvl_refno_seq.nextval
FROM dual;
--
--********************************
-- Constants for process_summary
--
cb         VARCHAR2(30);
cd         DATE;
cp         VARCHAR2(30) := 'CREATE';
ct         VARCHAR2(30) := 'DL_HPM_DELIVERABLE_VAL';
cs         INTEGER;
ce         VARCHAR2(200);
--
-- Other variables
--
l_id       ROWID;
i          INTEGER := 0;
l_an_tab   VARCHAR2(1);
--
l_exits                 VARCHAR2(1);
l_dvl_type_ind          VARCHAR2(1):= 'D';
l_dvl_refno             NUMBER(10,0);
l_perc_comp             NUMBER(5,2):= 100;
l_dvl_current_ind       VARCHAR2(1):= 'Y';
--
--*****************************
--
BEGIN
 fsc_utils.proc_start('s_dl_hpm_deliverable_val.dataload_create');
 fsc_utils.debug_message( 's_dl_hpm_deliverable_val.dataload_create',3);
--
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 in c1(p_batch_id) 
  LOOP
   BEGIN
    cs := p1.ldvl_dl_seqno;
    l_id := p1.rec_rowid;
    SAVEPOINT SP1;
-- ********************
    l_exits  := NULL;
    l_dvl_refno  := NULL;
--
-- Check that deliverable valuation has not been created since validation ran
-- checks are for linked task payments or just a current valuation exists for deliverable
--     
    OPEN chk_deliv_val(p1.ldvl_tsk_tkg_src_reference
                      ,p1.ldvl_tsk_tkg_code
                      ,p1.ldvl_tsk_tkg_src_type
                      ,p1.ldvl_tsk_id
                      ,p1.ldvl_bud_refno
                      ,l_dvl_type_ind
                      ,p1.ldvl_dlv_dcp_refno);
    FETCH chk_deliv_val INTO l_exits;
    CLOSE chk_deliv_val;
--     
    OPEN chk_deliv_val2(p1.ldvl_bud_refno
                       ,l_dvl_type_ind
                       ,p1.ldvl_dlv_dcp_refno);
    FETCH chk_deliv_val2 INTO l_exits;
    CLOSE chk_deliv_val2;
--
    OPEN chk_deliv_val3(p1.ldvl_tsk_tkg_src_reference
                       ,p1.ldvl_tsk_tkg_code
                       ,p1.ldvl_tsk_tkg_src_type
                       ,p1.ldvl_tsk_id);
    FETCH chk_deliv_val3 INTO l_exits;
    CLOSE chk_deliv_val3;
--  
    IF (l_exits               IS NULL      AND
        p1.ldvl_tsk_id        IS NOT NULL  AND
        p1.ldvl_bud_refno     IS NOT NULL  AND
        p1.ldvl_dlv_dcp_refno IS NOT NULL     )
     THEN
--
-- Firstly create the deliverable valuation then store the dvl_refno for the delete
--
      OPEN get_dvl_refno;
      FETCH get_dvl_refno INTO l_dvl_refno;
      CLOSE get_dvl_refno;
--
      INSERT INTO deliverable_valuations
          (dvl_refno
          ,dvl_valued_datetime
          ,dvl_percent_complete
          ,dvl_amount
          ,dvl_hrv_vty_code
          ,dvl_current_ind
          ,dvl_comments
          ,dvl_created_by
          ,dvl_created_date
          ,dvl_dlv_dcp_refno
          ,dvl_type_ind
          ,dvl_bud_refno
          ,dvl_tsk_id
          ,dvl_tsk_tkg_code
          ,dvl_tsk_tkg_src_reference
          ,dvl_tsk_tkg_src_type
          )
          VALUES
          (l_dvl_refno
          ,p1.ldvl_valued_date
          ,l_perc_comp
          ,p1.ldvl_amount
          ,p1.ldvl_hrv_vty_code
          ,l_dvl_current_ind
          ,p1.ldvl_comments
          ,p1.ldvl_created_by
          ,p1.ldvl_created_date
          ,p1.ldvl_dlv_dcp_refno
          ,l_dvl_type_ind
          ,p1.ldvl_bud_refno
          ,p1.ldvl_tsk_id
          ,p1.ldvl_tsk_tkg_code
          ,p1.ldvl_tsk_tkg_src_reference
          ,p1.ldvl_tsk_tkg_src_type
          );
--
        UPDATE dl_hpm_deliverable_val
        SET    ldvl_refno = l_dvl_refno
        WHERE  ldvl_dlb_batch_id = p_batch_id
        AND    rowid = p1.rec_rowid;
--
    END IF;
--
-- ********************
--
-- keep a count of the rows processed and commit after every 5000
--
    i := i + 1;
    IF MOD(i,5000) = 0
     THEN
      COMMIT;
    END IF;
--
   s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
   set_record_status_flag(l_id,'C');
   EXCEPTION
    WHEN OTHERS 
     THEN
      ROLLBACK TO SP1;
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
   END;
  END LOOP;
 COMMIT;
--
-- Section to analyse the table(s) populated by this data load
--
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('DELIVERABLE_VALUATIONS');
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HPM_DELIVERABLE_VAL');
 fsc_utils.proc_end;
 COMMIT; 
  EXCEPTION
   WHEN OTHERS 
    THEN
     s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
END dataload_create;
--
-- **************************************************************************************************
--
PROCEDURE dataload_validate
  (p_batch_id IN VARCHAR2
  ,p_date     IN DATE
  )
AS
CURSOR c1 
    (p_batch_id VARCHAR2)
IS
SELECT ROWID rec_rowid
,ldvl_dlb_batch_id
,ldvl_dl_seqno
,ldvl_dl_load_status
,ldvl_tsk_tkg_src_type
,ldvl_tpy_pay_type_ind
,ldvl_tsk_alt_reference
,ldvl_tsk_tkg_src_reference
,ldvl_tsk_tkg_code
,ldvl_tsk_stk_code
,ldvl_tpy_sco_code
,ldvl_valued_date
,ldvl_tpy_task_net_amount
,ldvl_tpy_task_tax_amount
,ldvl_cad_pro_aun_code
,ldvl_cad_type_ind
,ldvl_dve_display_sequence
,ldvl_dve_std_code
,ldvl_dve_estimated_cost
,ldvl_dlv_bhe_code
,ldvl_bud_bca_year
,ldvl_dve_quantity
,ldvl_dve_hrv_pmu_code_quantity
,ldvl_dve_unit_cost
,ldvl_dve_projected_cost
,ldvl_comments
,ldvl_hrv_vty_code
,ldvl_tsk_id
,NVL(ldvl_created_by,'DATALOAD') ldvl_created_by
,NVL(ldvl_created_date, SYSDATE) ldvl_created_date
,ldvl_bud_refno
,ldvl_refno
,ldvl_dlv_dcp_refno
,ldvl_amount
,ldvl_pro_refno_aun_code
FROM   dl_hpm_deliverable_val
WHERE  ldvl_dlb_batch_id = p_batch_id
AND    ldvl_dl_load_status IN ('L','F','O');
-- *********************************
--
CURSOR chk_tsk_altref
   (p_ldvl_tsk_alt_ref VARCHAR2)
IS
SELECT count(*)
FROM   tasks
WHERE  tsk_alt_reference = p_ldvl_tsk_alt_ref;
--
CURSOR chk_ltsk_altref
      (p_ldvl_tsk_alt_ref   VARCHAR2
      ,p_ldvl_dlb_batch_id  VARCHAR2)
IS
SELECT count(*)
FROM   dl_hpm_deliverable_val
WHERE  ldvl_tsk_alt_reference = p_ldvl_tsk_alt_ref
and    ldvl_dlb_batch_id = p_ldvl_dlb_batch_id;
--
CURSOR chk_contract
      (p_cnt_reference VARCHAR2)
IS
SELECT 'X',cnt_sco_code
FROM   contracts
WHERE  cnt_reference = p_cnt_reference;
--
CURSOR chk_task_groups
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      ,p_tkg_group_type    VARCHAR2)
IS
SELECT 'X'
FROM   task_groups
WHERE  tkg_src_reference = p_tkg_src_reference
AND    tkg_code = p_tkg_code
AND    tkg_src_type = p_tkg_src_type
AND    tkg_group_type = p_tkg_group_type;
--
CURSOR chk_stk_code
      (p_stk_code VARCHAR2)
IS
SELECT 'X'
FROM   standard_tasks
WHERE  stk_code = p_stk_code;
--
CURSOR get_tsk_detail
      (p_tsk_alt_ref VARCHAR2)
IS
SELECT tsk_tkg_src_reference
      ,tsk_tkg_code
      ,tsk_tkg_src_type
      ,tsk_id
      ,tsk_sco_code
      ,tsk_type_ind
      ,tsk_stk_code
FROM tasks
WHERE tsk_alt_reference = p_tsk_alt_ref;
--
CURSOR get_tve_max_ver
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      ,p_tsk_id            NUMBER  )
IS
SELECT max(tve_version_number)
FROM task_versions
WHERE tve_tsk_tkg_src_reference = p_tkg_src_reference
AND   tve_tsk_tkg_code = p_tkg_code
AND   tve_tsk_tkg_src_type = p_tkg_src_type
AND   tve_tsk_id = p_tsk_id
AND   tve_current_ind ='Y';
--
CURSOR chk_tsk_paymt
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      ,p_tsk_id            NUMBER
      ,p_pay_type_ind      VARCHAR2)
IS
SELECT 'X'
FROM task_payments
WHERE tpy_tsk_tkg_src_reference = p_tkg_src_reference
AND   tpy_tsk_tkg_code = p_tkg_code
AND   tpy_tsk_tkg_src_type = p_tkg_src_type
AND   tpy_tsk_id = p_tsk_id
AND   tpy_pay_type_ind = p_pay_type_ind;
--
CURSOR get_tve
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      ,p_tsk_id            NUMBER
      ,p_tve_verno         NUMBER )
IS
SELECT tve_bca_year
      ,tve_net_amount
      ,tve_tax_amount
FROM task_versions
WHERE tve_tsk_tkg_src_reference = p_tkg_src_reference
AND   tve_tsk_tkg_code = p_tkg_code
AND   tve_tsk_tkg_src_type = p_tkg_src_type
AND   tve_tsk_id = p_tsk_id
AND   tve_version_number = p_tve_verno
AND   tve_current_ind ='Y';
--
CURSOR get_tba
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      ,p_tsk_id            NUMBER
      ,p_tve_verno         NUMBER )
IS
SELECT tba_bud_refno
      ,tba_net_amount
      ,tba_tax_amount
FROM task_budget_amounts
WHERE tba_tve_tsk_tkg_src_ref = p_tkg_src_reference
AND   tba_tve_tsk_tkg_code = p_tkg_code
AND   tba_tve_tsk_tkg_src_type = p_tkg_src_type
AND   tba_tve_tsk_id = p_tsk_id
AND   tba_tve_version_number = p_tve_verno
AND   tba_current_ind ='Y';
--
CURSOR chk_get_prop
      (p_pro_propref VARCHAR2)
IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_pro_propref;
--
CURSOR chk_get_aun
      (p_aun_code VARCHAR2)
IS
SELECT aun_code
FROM   admin_units
WHERE  aun_code = p_aun_code;
--
CURSOR chk_cad
      (p_tkg_src_reference VARCHAR2
      ,p_pro_aun           VARCHAR2
      ,p_type_ind          VARCHAR2)
IS
SELECT 'X'
FROM contract_addresses
WHERE cad_cnt_reference = p_tkg_src_reference
AND   cad_pro_aun_code = p_pro_aun
AND   cad_type_ind = p_type_ind;
--
CURSOR chk_stk_del_code
      (p_stk_code VARCHAR2)
IS
SELECT 'X'
FROM   standard_deliverables
WHERE  std_code = p_stk_code;
--
CURSOR chk_bhe_code
      (p_bhe_code VARCHAR2)
IS
SELECT bhe_refno
FROM   budget_heads
WHERE  bhe_code = p_bhe_code;
--
CURSOR chk_bca_year
      (p_bca_year NUMBER)
IS
SELECT 'X', bca_start_date, bca_end_date
FROM   budget_calendars
WHERE  bca_year = p_bca_year;
--
CURSOR chk_get_dve
      (p_cnt_reference VARCHAR2
      ,p_pro_aun_code  VARCHAR2
      ,p_type_ind      VARCHAR2
      ,p_display_seq   NUMBER
      ,p_std_code      VARCHAR2)
IS
SELECT dve_dlv_refno
      ,dlv_sco_code
      ,dve_estimated_cost
FROM   deliverables
      ,deliverable_versions
WHERE  dlv_refno = dve_dlv_refno
AND    dlv_cnt_reference = p_cnt_reference
AND    dlv_cad_pro_aun_code = p_pro_aun_code
AND    dlv_cad_type_ind = p_type_ind
AND    dve_display_sequence = p_display_seq
AND    dve_std_code = p_std_code
AND    dve_current_ind = 'Y';
--
CURSOR chk_get_dve2
      (p_cnt_reference  VARCHAR2
      ,p_pro_aun_code   VARCHAR2
      ,p_type_ind       VARCHAR2
      ,p_display_seq    NUMBER
      ,p_std_code       VARCHAR2
      ,p_dve_quantity   NUMBER
      ,p_code_quantity  VARCHAR2
      ,p_unit_cost      NUMBER
      ,p_projected_cost NUMBER)
IS
SELECT dve_dlv_refno
      ,dlv_sco_code
      ,dve_estimated_cost
FROM   deliverables
      ,deliverable_versions
WHERE  dlv_refno = dve_dlv_refno
AND    dlv_cnt_reference = p_cnt_reference
AND    dlv_cad_pro_aun_code = p_pro_aun_code
AND    dlv_cad_type_ind = p_type_ind
AND    dve_display_sequence = p_display_seq
AND    dve_std_code = p_std_code
AND    dve_quantity = p_dve_quantity
AND    dve_hrv_pmu_code_quantity = p_code_quantity
AND    dve_unit_cost = p_unit_cost
AND    dve_projected_cost = p_projected_cost
AND    dve_current_ind = 'Y';
--
CURSOR chk_deliv_val4
      (p_dvl_dlv_dcp_refno NUMBER)
IS
SELECT 'X'
FROM deliverable_valuations
WHERE dvl_dlv_dcp_refno = p_dvl_dlv_dcp_refno
AND   dvl_current_ind ='Y';
--
CURSOR chk_deliv_val3
      (p_tkg_src_reference VARCHAR2
      ,p_tkg_code          VARCHAR2
      ,p_tkg_src_type      VARCHAR2
      ,p_tsk_id            NUMBER)
IS
SELECT 'X'
FROM deliverable_valuations
WHERE dvl_tsk_tkg_src_reference = p_tkg_src_reference
AND   dvl_tsk_tkg_code = p_tkg_code
AND   dvl_tsk_tkg_src_type = p_tkg_src_type
AND   dvl_tsk_id = p_tsk_id
AND   dvl_current_ind = 'Y';
--
CURSOR chk_budget
      (p_bhe_refno NUMBER
      ,p_bca_year  NUMBER)
IS
SELECT bud_refno
FROM   budgets
WHERE bud_bhe_refno = p_bhe_refno
AND   bud_bca_year  = p_bca_year;
--
CURSOR chk_vty_code
      (p_vty_code  VARCHAR)
IS
SELECT 'X'
FROM  first_ref_values
WHERE frv_code = p_vty_code
AND   frv_frd_domain  = 'VALCODE';
--
-- *********************************
--
-- Constants for process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'VALIDATE';
ct          VARCHAR2(30) := 'DL_HPM_DELIVERABLE_VAL';
cs          INTEGER;
ce          VARCHAR2(200);
--
-- Other variables
--
l_id               ROWID;
i                  INTEGER := 0;
l_an_tab           VARCHAR2(1);
l_pro_aun          VARCHAR2(20);
l_errors           VARCHAR2(10);
l_error_ind        VARCHAR2(10);
l_chk_tskaltref    INTEGER;
l_chk_ltskaltref   INTEGER;
l_cnt_exists       VARCHAR2(1);
l_cnt_status       VARCHAR2(3);
l_tkg_exists       VARCHAR2(1);
l_stk_exists       VARCHAR2(1);
l_tsk_tkg_src_ref  VARCHAR2(15);
l_tsk_tkg_code     VARCHAR2(10);
l_tsk_tkg_src_type VARCHAR2(3);
l_tsk_id           NUMBER(8);
l_tsk_sco_code     VARCHAR2(3);
l_tsk_type_ind     VARCHAR2(1);
l_tsk_stk_code     VARCHAR2(8);
l_tve_max          NUMBER(8);
l_tsk_exists       VARCHAR2(1);
l_tve_bca_year     NUMBER(4);
l_tve_net_amt      NUMBER(14,2);
l_tve_tax_amt      NUMBER(14,2);
l_tba_bud_refno    NUMBER(10);
l_budget           NUMBER(10);
l_tba_net_amt      NUMBER(14,2);
l_tba_tax_amt      NUMBER(14,2);
l_chk_cad          VARCHAR2(1);
l_stk_del_exists   VARCHAR2(1);
l_bhe_refno        NUMBER(10);
l_bca_exists       VARCHAR2(1);
l_bca_start        DATE;
l_bca_end          DATE;
l_dlv_refno        NUMBER(10);
l_dlv_sco_code     VARCHAR2(3);
l_dve_est_cost     NUMBER(14,2);
l_amount           NUMBER(14,2);
l_dve_exits        VARCHAR2(1);
l_dve2_exits       VARCHAR2(1);
l_chk_vty_code     VARCHAR2(1);
--
--*****************************
--
BEGIN
 fsc_utils.proc_start('s_dl_hpm_deliverable_val.dataload_validate');
 fsc_utils.debug_message( 's_dl_hpm_deliverable_val.dataload_validate',3);
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR p1 IN c1(p_batch_id) 
  LOOP
   BEGIN
    cs := p1.ldvl_dl_seqno;
    l_id := p1.rec_rowid;
    l_errors := 'V';
    l_error_ind := 'N';
    l_pro_aun := NULL;
    l_chk_tskaltref := NULL;
    l_chk_ltskaltref := NULL;
    l_cnt_exists := NULL;
    l_cnt_status := NULL;
    l_tkg_exists := NULL;
    l_stk_exists := NULL;
    l_tsk_tkg_src_ref := NULL;
    l_tsk_tkg_code := NULL;
    l_tsk_tkg_src_type := NULL;
    l_tsk_id := NULL;
    l_tsk_sco_code := NULL;
    l_tsk_type_ind := NULL;
    l_tsk_stk_code := NULL;
    l_tve_max := NULL;
    l_tsk_exists := NULL;
    l_tve_bca_year := NULL;
    l_tve_net_amt := NULL;
    l_tve_tax_amt := NULL;
    l_tba_bud_refno := NULL;
    l_budget := NULL;
    l_tba_net_amt := NULL;
    l_tba_tax_amt := NULL;
    l_chk_cad := NULL;
    l_stk_del_exists := NULL;
    l_bhe_refno := NULL;
    l_bca_exists := NULL;
    l_bca_start := NULL;
    l_bca_end := NULL;
    l_dlv_refno := NULL;
    l_dlv_sco_code := NULL;
    l_dve_est_cost := NULL;
    l_amount := NULL;
    l_dve_exits := NULL;
    l_dve2_exits := NULL;
    l_chk_vty_code := NULL;
-- ****************************
-- check mandatory fields supplied
--
-- task alternative reference
--
   IF (p1.ldvl_tsk_alt_reference IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',134);
   ELSE 
    OPEN chk_tsk_altref(p1.ldvl_tsk_alt_reference);
    FETCH chk_tsk_altref INTO l_chk_tskaltref;
    CLOSE chk_tsk_altref;
    IF (l_chk_tskaltref = 0) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',137);
    END IF;
    IF (l_chk_tskaltref > 1) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',138);
    END IF;
--
    OPEN chk_ltsk_altref(p1.ldvl_tsk_alt_reference,p_batch_id);
    FETCH chk_ltsk_altref INTO l_chk_ltskaltref;
    CLOSE chk_ltsk_altref;
     IF (l_chk_ltskaltref > 1) 
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',136);
     END IF;
   END IF;
--   
-- Contract reference
--
   IF (p1.ldvl_tsk_tkg_src_reference IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',139);
   ELSE 
    OPEN chk_contract(p1.ldvl_tsk_tkg_src_reference);
    FETCH chk_contract
    INTO l_cnt_exists,l_cnt_status;
    CLOSE chk_contract;
    IF (l_cnt_exists IS NULL) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',333);
    END IF;
    IF (l_cnt_status !='AUT') 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',160);
    END IF;
   END IF;
--
-- Task Group Code
--
   IF (p1.ldvl_tsk_tkg_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',357);
   ELSE 
     OPEN chk_task_groups(p1.ldvl_tsk_tkg_src_reference
                         ,p1.ldvl_tsk_tkg_code
                         ,p1.ldvl_tsk_tkg_src_type
                         ,'PAYT');
     FETCH chk_task_groups INTO l_tkg_exists;
     CLOSE chk_task_groups;
     IF (l_tkg_exists IS NULL) 
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',140);
      END IF;
   END IF;
--   
-- Check Standard Task Code
--
   IF (p1.ldvl_tsk_stk_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',147);
   ELSE 
    OPEN chk_stk_code(p1.ldvl_tsk_stk_code);
    FETCH chk_stk_code INTO l_stk_exists;
    CLOSE chk_stk_code;
    IF (l_stk_exists IS NULL) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',148);
    END IF;
   END IF;
--
-- Task Payment Status Code must be either RAI or CLO
--
   IF (p1.ldvl_tpy_sco_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',141);
   ELSIF (p1.ldvl_tpy_sco_code NOT IN ('RAI','CLO')) 
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',142);
   END IF;	
--
-- Deliverable Valuations valued date
--
   IF (p1.ldvl_valued_date IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',161);
   END IF;
--
-- Task Payment Amounts are supplied
--
   IF (p1.ldvl_tpy_task_net_amount IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',143);
   END IF;
--
   IF (p1.ldvl_tpy_task_tax_amount IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',144);
   END IF;
--
-- Property reference or Admin Unit and Indicator must be supplied and exist
--
   IF (p1.ldvl_cad_pro_aun_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',163);
   END IF;
--
   IF (p1.ldvl_cad_type_ind IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',164);
   END IF;
--
   IF (p1.ldvl_cad_type_ind IS NOT NULL)
    THEN
     IF (p1.ldvl_cad_type_ind NOT IN ('P','A'))
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',165);
     END IF;
   END IF;
--
   IF (p1.ldvl_cad_pro_aun_code IS NOT NULL  AND  p1.ldvl_cad_type_ind = 'P')
    THEN
     OPEN chk_get_prop(p1.ldvl_cad_pro_aun_code);
     FETCH chk_get_prop INTO l_pro_aun;
     CLOSE chk_get_prop;
     IF (l_pro_aun IS NULL) 
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',166);
     END IF;
   END IF;
--
   IF (p1.ldvl_cad_pro_aun_code IS NOT NULL  AND p1.ldvl_cad_type_ind = 'A')
    THEN
     OPEN chk_get_aun(p1.ldvl_cad_pro_aun_code);
     FETCH chk_get_aun INTO l_pro_aun;
     CLOSE chk_get_aun;
     IF (l_pro_aun IS NULL) 
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',167);
     END IF;
   END IF;
--	
   IF (p1.ldvl_cad_pro_aun_code IS NOT NULL      AND
       p1.ldvl_tsk_tkg_src_reference IS NOT NULL AND
       p1.ldvl_cad_type_ind IN ('P','A')         AND
       l_pro_aun IS NOT NULL                         )
    THEN
     OPEN chk_cad(p1.ldvl_tsk_tkg_src_reference
                 ,l_pro_aun
                 ,p1.ldvl_cad_type_ind);
     FETCH chk_cad INTO l_chk_cad;
     CLOSE chk_cad;
     IF (l_chk_cad IS NULL) 
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',168);
     END IF;
   END IF;
--
-- Deliverable Version display Sequence required
--
   IF (p1.ldvl_dve_display_sequence IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',169);
   END IF;
--
-- Deliverable Stand Code is required exists and is current
--   
   IF (p1.ldvl_dve_std_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',170);
   ELSE 
    OPEN chk_stk_del_code(p1.ldvl_dve_std_code);
    FETCH chk_stk_del_code INTO l_stk_del_exists;
    CLOSE chk_stk_del_code;
    IF (l_stk_del_exists IS NULL) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',171);
    END IF;
   END IF;
--
-- Deliverables estimated cost is required
--
   IF (p1.ldvl_dve_estimated_cost IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',172);
   END IF;
--
-- The budget head code and calendar year must be valid
--
   IF (p1.ldvl_dlv_bhe_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',173);
   ELSE 
    OPEN chk_bhe_code(p1.ldvl_dlv_bhe_code);
    FETCH chk_bhe_code INTO l_bhe_refno;
    CLOSE chk_bhe_code;
    IF (l_bhe_refno IS NULL) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',175);
    END IF;
   END IF;
--
   IF (p1.ldvl_bud_bca_year IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',174);
   ELSE 
    OPEN chk_bca_year(p1.ldvl_bud_bca_year);
    FETCH chk_bca_year INTO l_bca_exists, l_bca_start, l_bca_end ;
    CLOSE chk_bca_year;
    IF (l_bca_exists IS NULL) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',176);
    END IF;
   END IF;
--
-- Deliverables Version Quantities optional fields all or none needed
--
   IF (p1.ldvl_dve_quantity              IS NOT NULL   OR
       p1.ldvl_dve_hrv_pmu_code_quantity IS NOT NULL   OR
       p1.ldvl_dve_unit_cost             IS NOT NULL   OR
       p1.ldvl_dve_projected_cost        IS NOT NULL     )
    THEN
     IF (p1.ldvl_dve_quantity              IS NULL   OR
         p1.ldvl_dve_hrv_pmu_code_quantity IS NULL   OR
         p1.ldvl_dve_unit_cost             IS NULL   OR
         p1.ldvl_dve_projected_cost        IS NULL     )
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',177);
     END IF;
   END IF;
--  
-- Check and get task amounts budgets driver task alternative reference
-- so only get detail if tsk_alt_reference is unique
--
   IF (l_chk_tskaltref = 1)
    THEN
     OPEN get_tsk_detail(p1.ldvl_tsk_alt_reference);
     FETCH get_tsk_detail INTO l_tsk_tkg_src_ref
                              ,l_tsk_tkg_code
                              ,l_tsk_tkg_src_type
                              ,l_tsk_id
                              ,l_tsk_sco_code
                              ,l_tsk_type_ind
                              ,l_tsk_stk_code;
     CLOSE get_tsk_detail;
--
     IF (l_tsk_tkg_src_ref != p1.ldvl_tsk_tkg_src_reference)
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',149);
     END IF;
--
     IF (l_tsk_tkg_code != p1.ldvl_tsk_tkg_code)
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',150);
     END IF;
--
     IF (l_tsk_tkg_src_type != 'CNT')
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',151);
     END IF;
--
     IF (l_tsk_sco_code != 'COM')
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',152);
     END IF;
--
     IF (l_tsk_type_ind != 'P')
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',153);
     END IF;
--
     IF (l_tsk_stk_code != p1.ldvl_tsk_stk_code)
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',154);
     END IF;
--
-- check to make sure the task payment exists
--     
     OPEN chk_tsk_paymt(p1.ldvl_tsk_tkg_src_reference
                       ,p1.ldvl_tsk_tkg_code
                       ,p1.ldvl_tsk_tkg_src_type
                       ,l_tsk_id
                       ,p1.ldvl_tpy_pay_type_ind);
     FETCH chk_tsk_paymt INTO l_tsk_exists;
     CLOSE chk_tsk_paymt;
--
     IF (l_tsk_exists IS NULL)
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',162);
     END IF;
--
-- check task version for amounts
-- get max version number first then detail then check amounts
--     
     OPEN get_tve_max_ver(p1.ldvl_tsk_tkg_src_reference
                         ,p1.ldvl_tsk_tkg_code
                         ,p1.ldvl_tsk_tkg_src_type
                         ,l_tsk_id);
     FETCH get_tve_max_ver INTO l_tve_max;
     CLOSE get_tve_max_ver;
--
-- get task version detail
--   
     OPEN get_tve(p1.ldvl_tsk_tkg_src_reference
                 ,p1.ldvl_tsk_tkg_code
                 ,p1.ldvl_tsk_tkg_src_type
                 ,l_tsk_id
                 ,l_tve_max);
     FETCH get_tve INTO l_tve_bca_year
                       ,l_tve_net_amt
                       ,l_tve_tax_amt;
     CLOSE get_tve;
--
-- get task budget amounts detail
--  
     OPEN get_tba(p1.ldvl_tsk_tkg_src_reference
                 ,p1.ldvl_tsk_tkg_code
                 ,p1.ldvl_tsk_tkg_src_type
                 ,l_tsk_id
                 ,l_tve_max);
     FETCH get_tba INTO l_tba_bud_refno
                       ,l_tba_net_amt
                       ,l_tba_tax_amt;
     CLOSE get_tba;
--
     IF (l_bca_exists IS NOT NULL  AND
         l_bhe_refno  IS NOT NULL  AND
         l_tba_bud_refno IS NOT NULL  )
      THEN
       OPEN chk_budget(l_bhe_refno
                      ,p1.ldvl_bud_bca_year);
       FETCH chk_budget INTO l_budget;
       CLOSE chk_budget;
       IF (l_budget != l_tba_bud_refno) 
        THEN
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',183);
       END IF;
     END IF;
--
-- Now check that amounts supplied match task version and budget amounts
--
     IF (l_tve_net_amt != p1.ldvl_tpy_task_net_amount OR
         l_tve_tax_amt != p1.ldvl_tpy_task_tax_amount   )
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',157);
     END IF;
--
     IF (l_tba_net_amt != p1.ldvl_tpy_task_net_amount OR
         l_tba_tax_amt != p1.ldvl_tpy_task_tax_amount   )
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',158);
     END IF;
--
     IF (l_tba_net_amt != l_tve_net_amt OR
         l_tba_tax_amt != l_tve_tax_amt   )
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',159);
     END IF;
--
   END IF; -- end of task details check
--
-- Now get and check deliverables details
--
   IF (p1.ldvl_tsk_tkg_src_reference IS NOT NULL  AND
       l_pro_aun                     IS NOT NULL  AND
       p1.ldvl_cad_type_ind          IS NOT NULL  AND
       p1.ldvl_dve_display_sequence  IS NOT NULL  AND
       p1.ldvl_dve_std_code          IS NOT NULL     )
    THEN
     IF (p1.ldvl_dve_quantity              IS NULL  AND
         p1.ldvl_dve_hrv_pmu_code_quantity IS NULL  AND
         p1.ldvl_dve_unit_cost             IS NULL  AND
         p1.ldvl_dve_projected_cost        IS NULL     )
      THEN     
       OPEN chk_get_dve(p1.ldvl_tsk_tkg_src_reference
                       ,l_pro_aun
                       ,p1.ldvl_cad_type_ind
                       ,p1.ldvl_dve_display_sequence
                       ,p1.ldvl_dve_std_code);
       FETCH chk_get_dve INTO l_dlv_refno
                             ,l_dlv_sco_code
                             ,l_dve_est_cost;
       CLOSE chk_get_dve;
	 ELSE
       OPEN chk_get_dve2(p1.ldvl_tsk_tkg_src_reference
                        ,l_pro_aun
                        ,p1.ldvl_cad_type_ind
                        ,p1.ldvl_dve_display_sequence
                        ,p1.ldvl_dve_std_code
                        ,p1.ldvl_dve_quantity
                        ,p1.ldvl_dve_hrv_pmu_code_quantity
                        ,p1.ldvl_dve_unit_cost
                        ,p1.ldvl_dve_projected_cost);
       FETCH chk_get_dve2 INTO l_dlv_refno
                             ,l_dlv_sco_code
                             ,l_dve_est_cost;
       CLOSE chk_get_dve2;
     END IF;
--
     IF (l_dlv_refno IS NULL)
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',178);
     END IF;
--
     IF (l_dlv_refno IS NOT NULL)
      THEN
       IF (l_dlv_sco_code  != 'COM')
        THEN
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',179);
       END IF;
--
       l_amount := p1.ldvl_tpy_task_net_amount + p1.ldvl_tpy_task_tax_amount;
--
       IF (l_dve_est_cost != l_amount)
        THEN
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',180);
       END IF;
--
-- Check that deliverable valuation does not already exist for the deliverable
--     
       OPEN chk_deliv_val4(l_dlv_refno);
       FETCH chk_deliv_val4 INTO l_dve_exits;
       CLOSE chk_deliv_val4;
       IF (l_dve_exits IS NOT NULL)
        THEN
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',181);
       END IF;
--
       OPEN chk_deliv_val3(p1.ldvl_tsk_tkg_src_reference
                          ,p1.ldvl_tsk_tkg_code
                          ,p1.ldvl_tsk_tkg_src_type
                          ,l_tsk_id);
       FETCH chk_deliv_val3 INTO l_dve2_exits;
       CLOSE chk_deliv_val3;
       IF (l_dve2_exits IS NOT NULL)
        THEN
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',182);
       END IF;
     END IF; -- l_dlv_refno IS NOT NULL
   END IF; -- check deliverables details
--   
-- Check deliverable valuation type
-- added 12-Apr-2017 (AJ)
--
   IF (p1.ldvl_hrv_vty_code IS NOT NULL)
    THEN
     OPEN chk_vty_code(p1.ldvl_hrv_vty_code);
     FETCH chk_vty_code INTO l_chk_vty_code;
     CLOSE chk_vty_code;
     IF (l_chk_vty_code IS NULL) 
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',203);
     END IF;
   END IF;
--
--
-- now update dl_hpm_deliverable_val with details needed for create
--
   IF (l_tsk_id IS NOT NULL)
    THEN
     UPDATE dl_hpm_deliverable_val
     SET    ldvl_tsk_id = l_tsk_id
     WHERE  ldvl_dlb_batch_id = p_batch_id
     AND    rowid = p1.rec_rowid;
   END IF;
--
   IF (l_tba_bud_refno IS NOT NULL)
    THEN
     UPDATE dl_hpm_deliverable_val
     SET    ldvl_bud_refno = l_tba_bud_refno
     WHERE  ldvl_dlb_batch_id = p_batch_id
     AND    rowid = p1.rec_rowid;
   END IF;
--
   IF (l_pro_aun IS NOT NULL)
    THEN
     UPDATE dl_hpm_deliverable_val
     SET    ldvl_pro_refno_aun_code = l_pro_aun
     WHERE  ldvl_dlb_batch_id = p_batch_id
     AND    rowid = p1.rec_rowid;
   END IF;
--
   IF (l_dlv_refno IS NOT NULL)
    THEN
     UPDATE dl_hpm_deliverable_val
     SET    ldvl_dlv_dcp_refno = l_dlv_refno
     WHERE  ldvl_dlb_batch_id = p_batch_id
     AND    rowid = p1.rec_rowid;
   END IF;
--
   IF (l_amount IS NOT NULL)
    THEN
     UPDATE dl_hpm_deliverable_val
     SET    ldvl_amount = l_amount
     WHERE  ldvl_dlb_batch_id = p_batch_id
     AND    rowid = p1.rec_rowid;
   END IF;
--
--********************************************
-- Now UPDATE the record count AND error code
--
    IF l_errors = 'F' 
     THEN
      l_error_ind := 'Y';
     ELSE
      l_error_ind := 'N';
    END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
    i := i + 1;
    IF MOD(i,1000) = 0
     THEN
      COMMIT;
    END IF;
--
    s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
    set_record_status_flag(l_id,l_errors);
    EXCEPTION
     WHEN OTHERS 
      THEN
       ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
       s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
       set_record_status_flag(l_id,'O');
   END;  
  END LOOP;
 COMMIT;
--
-- Section to analyse the table(s) populated by this dataload
--
 l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HPM_DELIVERABLE_VAL');
--
 fsc_utils.proc_END;
 COMMIT;
 EXCEPTION
  WHEN OTHERS 
   THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
END dataload_validate;
--
-- **************************************************************************************************
--
PROCEDURE dataload_delete 
  (p_batch_id IN VARCHAR2
  ,p_date     IN DATE
  ) 
IS
CURSOR c1 (p_batch_id VARCHAR2)
IS
SELECT ROWID rec_rowid
,ldvl_dlb_batch_id
,ldvl_dl_seqno
,ldvl_dl_load_status
,ldvl_tsk_tkg_src_type
,ldvl_tpy_pay_type_ind
,ldvl_tsk_alt_reference
,ldvl_tsk_tkg_src_reference
,ldvl_tsk_tkg_code
,ldvl_tsk_stk_code
,ldvl_tpy_sco_code
,ldvl_valued_date
,ldvl_tpy_task_net_amount
,ldvl_tpy_task_tax_amount
,ldvl_cad_pro_aun_code
,ldvl_cad_type_ind
,ldvl_dve_display_sequence
,ldvl_dve_std_code
,ldvl_dve_estimated_cost
,ldvl_dlv_bhe_code
,ldvl_bud_bca_year
,ldvl_dve_quantity
,ldvl_dve_hrv_pmu_code_quantity
,ldvl_dve_unit_cost
,ldvl_dve_projected_cost
,ldvl_comments
,ldvl_hrv_vty_code
,ldvl_tsk_id
,NVL(ldvl_created_by,'DATALOAD') ldvl_created_by
,NVL(ldvl_created_date, SYSDATE) ldvl_created_date
,ldvl_bud_refno
,ldvl_refno
,ldvl_dlv_dcp_refno
,ldvl_amount
,ldvl_pro_refno_aun_code
FROM   dl_hpm_deliverable_val
WHERE  ldvl_dlb_batch_id = p_batch_id
AND    ldvl_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HPM_DELIVERABLE_VAL';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- Other variables
--
l_id           ROWID;
i              INTEGER := 0;
l_an_tab       VARCHAR2(1);
--
--*****************************
--
 BEGIN
  fsc_utils.proc_start('s_dl_hpm_deliverable_val.dataload_delete');
  fsc_utils.debug_message( 's_dl_hpm_deliverable_val.dataload_delete',3 );
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 IN c1(p_batch_id) 
   LOOP
    BEGIN
     cs := p1.ldvl_dl_seqno;
     l_id := p1.rec_rowid;
     SAVEPOINT SP1;
-- ****************************************
--
-- First delete any records in deliverable valuation history if they exist
--
     DELETE
     FROM   deliverable_valuation_history
     WHERE  dvlh_dlv_dcp_refno = p1.ldvl_dlv_dcp_refno
     AND    dvlh_type = 'D';
--
-- Then delete from deliverable valuation amounts if they exist
--
     DELETE 
     FROM   deliv_valuation_amts
     WHERE  dam_dvl_dlv_dcp_refno = p1.ldvl_dlv_dcp_refno
     AND    dam_dvl_type_ind_src  = 'D';
--
-- Then lastly delete from deliverable valuations
--
     DELETE 
     FROM   deliverable_valuations
     WHERE  dvl_dlv_dcp_refno = p1.ldvl_dlv_dcp_refno
     AND    dvl_refno = p1.ldvl_refno;
--
-- ****************************************
-- keep a count of the rows processed and commit after every 1000
--
     i := i + 1;
     IF MOD(i,1000) = 0
      THEN
       COMMIT;
     END IF;
--
     s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
     set_record_status_flag(l_id,'V');
     EXCEPTION
      WHEN OTHERS 
       THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        set_record_status_flag(l_id,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    END;
  END LOOP;
 COMMIT;
--
-- Section to analyse the table(s) populated by this dataload
--
 l_an_tab := s_dl_hem_utils.dl_comp_stats('DELIVERABLE_VALUATIONS');
 l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HPM_DELIVERABLE_VAL');
 fsc_utils.proc_end;
 COMMIT;  
 EXCEPTION
  WHEN OTHERS 
   THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
   RAISE;
END dataload_delete;
--
END s_dl_hpm_deliverable_val;
/

show errors

