CREATE OR REPLACE PACKAGE BODY s_dl_hpm_task_payments
AS
-- **************************************************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER     DB version  WHO  WHEN         WHY
--  1.0		6.15        AJ   22-FEB-2017  Initial creation Bespoke for
--                                        Queensland CR462
--  1.1		6.15        AJ   28-FEB-2017  Completed script ready for testing
--  1.2		6.15        AJ   03-MAR-2017  Amendment to validate syntax
--  1.3		6.15        AJ   07-MAR-2017  Amendment to 4 cursor could not derive budget refno as
--                                        passing in alt ref should be src_ref
--
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
  UPDATE dl_hpm_task_payments
  SET    ltpy_dl_load_status = p_status
  WHERE  rowid = p_rowid;
 EXCEPTION
 WHEN OTHERS 
  THEN
   dbms_output.put_line('Error updating status of dl_hpm_task_payments');
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
,ltpy_dlb_batch_id
,ltpy_dl_seqno
,ltpy_dl_load_status
,ltpy_tsk_alt_reference
,ltpy_tsk_tkg_src_reference
,ltpy_tsk_tkg_code
,ltpy_tsk_stk_code
,NVL(ltpy_tsk_tkg_src_type,'CNT') ltpy_tsk_tkg_src_type
,ltpy_tsk_id
,NVL(ltpy_pay_type_ind,'P') ltpy_pay_type_ind
,ltpy_sco_code
,ltpy_status_date
,ltpy_due_date
,ltpy_task_net_amount
,ltpy_task_tax_amount
,NVL(ltpy_created_by,'DATALOAD') ltpy_created_by
,NVL(ltpy_created_date, SYSDATE) ltpy_created_date
,ltpy_paid_date
,ltpy_payment_id
,ltpy_payment_date
,ltpy_tpm_bud_refno
,ltpy_tpm_seqno
FROM   dl_hpm_task_payments
WHERE  ltpy_dlb_batch_id = p_batch_id
AND    ltpy_dl_load_status = 'V';	
-- *********************************
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
--********************************
-- Constants for process_summary
--
cb         VARCHAR2(30);
cd         DATE;
cp         VARCHAR2(30) := 'CREATE';
ct         VARCHAR2(30) := 'DL_HPM_TASK_PAYMENTS';
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
l_ren_perc              NUMBER(4,2):= 0;
l_ren_net               NUMBER(14,2):= 0;
l_ren_tax               NUMBER(14,2):= 0;
l_tpm_display_sequence  NUMBER(4,0);
l_tpm_current_ind       VARCHAR2(1):= 'Y';
l_tmp_seqno             NUMBER(4,0);
l_tpm_type_ind          VARCHAR2(4):= 'INIT';

--
--*****************************
--
BEGIN
 fsc_utils.proc_start('s_dl_hpm_task_payments.dataload_create');
 fsc_utils.debug_message( 's_dl_hpm_task_payments.dataload_create',3);
--
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 in c1(p_batch_id) 
  LOOP
   BEGIN
    cs := p1.ltpy_dl_seqno;
    l_id := p1.rec_rowid;
    SAVEPOINT SP1;
-- ********************
    l_exits  := NULL;
--
-- Check that task payment has not been created since validation ran
--     
    OPEN chk_tsk_paymt(p1.ltpy_tsk_alt_reference
                      ,p1.ltpy_tsk_tkg_code
                      ,p1.ltpy_tsk_tkg_src_type
                      ,p1.ltpy_tsk_id
                      ,p1.ltpy_pay_type_ind);
    FETCH chk_tsk_paymt INTO l_exits;
    CLOSE chk_tsk_paymt;
--
    IF (l_exits               IS NULL      AND
        p1.ltpy_tsk_id        IS NOT NULL  AND
        p1.ltpy_tpm_bud_refno IS NOT NULL     )
     THEN
--
-- Firstly create task payment record then create task payment budget amount
-- record this needs to be done for both Raised and Closed Payments
--
      IF p1.ltpy_sco_code = 'CLO'
       THEN
        INSERT INTO task_payments
          (tpy_tsk_tkg_src_reference
          ,tpy_tsk_tkg_code
          ,tpy_tsk_tkg_src_type
          ,tpy_tsk_id
          ,tpy_pay_type_ind
          ,tpy_sco_code
          ,tpy_status_date
          ,tpy_due_date
          ,tpy_task_net_amount
          ,tpy_task_tax_amount
          ,tpy_created_by
          ,tpy_created_date
          ,tpy_paid_date
          ,tpy_payment_id
          ,tpy_payment_date
          ,tpy_total_net_amount
          ,tpy_total_tax_amount
          ,tpy_retention_percentage
          ,tpy_retention_net_amount
          ,tpy_retention_tax_amount
          )
          VALUES
          (p1.ltpy_tsk_tkg_src_reference
          ,p1.ltpy_tsk_tkg_code
          ,p1.ltpy_tsk_tkg_src_type  -- always CNT
          ,p1.ltpy_tsk_id            -- found during validate
          ,p1.ltpy_pay_type_ind      -- always P
          ,p1.ltpy_sco_code          -- RAI or CLO
          ,p1.ltpy_status_date
          ,p1.ltpy_due_date
          ,p1.ltpy_task_net_amount
          ,p1.ltpy_task_tax_amount
          ,p1.ltpy_created_by
          ,p1.ltpy_created_date
          ,p1.ltpy_paid_date           -- ONLY IF (TPY_SCO_CODE = CLO)
          ,p1.ltpy_payment_id          -- ONLY IF (TPY_SCO_CODE = CLO)
          ,p1.ltpy_payment_date        -- ONLY IF (TPY_SCO_CODE = CLO)
          ,p1.ltpy_task_net_amount
          ,p1.ltpy_task_tax_amount
          ,l_ren_perc
          ,l_ren_net
          ,l_ren_tax
          );
--
      ELSIF p1.ltpy_sco_code = 'RIA'
       THEN
--
        INSERT INTO task_payments
          (tpy_tsk_tkg_src_reference
          ,tpy_tsk_tkg_code
          ,tpy_tsk_tkg_src_type
          ,tpy_tsk_id
          ,tpy_pay_type_ind
          ,tpy_sco_code
          ,tpy_status_date
          ,tpy_due_date
          ,tpy_task_net_amount
          ,tpy_task_tax_amount
          ,tpy_created_by
          ,tpy_created_date
          ,tpy_total_net_amount
          ,tpy_total_tax_amount
          ,tpy_retention_percentage
          ,tpy_retention_net_amount
          ,tpy_retention_tax_amount
          )
          VALUES
          (p1.ltpy_tsk_tkg_src_reference
          ,p1.ltpy_tsk_tkg_code
          ,p1.ltpy_tsk_tkg_src_type  -- always CNT
          ,p1.ltpy_tsk_id            -- found during validate
          ,p1.ltpy_pay_type_ind      -- always P
          ,p1.ltpy_sco_code          -- RAI or CLO
          ,p1.ltpy_status_date
          ,p1.ltpy_due_date
          ,p1.ltpy_task_net_amount
          ,p1.ltpy_task_tax_amount
          ,p1.ltpy_created_by
          ,p1.ltpy_created_date
          ,p1.ltpy_task_net_amount
          ,p1.ltpy_task_tax_amount
          ,l_ren_perc
          ,l_ren_net
          ,l_ren_tax
          );
--
      END IF;
--
-- now do the task payment budget amount record same for each record type
--
      l_tpm_display_sequence := (p1.ltpy_tsk_id * 10);
      l_tmp_seqno := (p1.ltpy_tsk_id * 10);
--
      INSERT INTO task_payment_budget_amounts
        (tpm_tpy_tsk_tkg_src_reference
        ,tpm_tpy_tsk_tkg_code
        ,tpm_tpy_tsk_tkg_src_type
        ,tpm_tpy_tsk_id
        ,tpm_tpy_pay_type_ind
        ,tpm_bud_refno
        ,tpm_display_sequence
        ,tpm_current_ind
        ,tpm_net_amount
        ,tpm_tax_amount
        ,tpm_created_by
        ,tpm_created_date
        ,tpm_seqno
        ,tpm_type_ind
        ,tpm_tpm_tpy_pay_type_ind
        )
        VALUES
        (p1.ltpy_tsk_tkg_src_reference
        ,p1.ltpy_tsk_tkg_code
        ,p1.ltpy_tsk_tkg_src_type  -- always CNT
        ,p1.ltpy_tsk_id            -- found during validate
        ,p1.ltpy_pay_type_ind      -- always P
        ,p1.ltpy_tpm_bud_refno     -- found during validate
        ,l_tpm_display_sequence    -- ltpy_tsk_id times 10
        ,l_tpm_current_ind         -- always Y
        ,p1.ltpy_task_net_amount
        ,p1.ltpy_task_tax_amount
        ,p1.ltpy_created_by
        ,p1.ltpy_created_date
        ,l_tmp_seqno               -- ltpy_tsk_id times 10
        ,l_tpm_type_ind            -- always INIT (interim)
        ,p1.ltpy_pay_type_ind      -- always P
        );
--
        UPDATE dl_hpm_task_payments
        SET    ltpy_tpm_seqno = l_tmp_seqno
        WHERE  ltpy_dlb_batch_id = p_batch_id
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
-- Section to analyse the table(s) populated by this dataload
--
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('TASK_PAYMENTS');
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('TASK_PAYMENT_BUDGET_AMOUNTS');
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HPM_TASK_PAYMENTS');
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
,ltpy_dlb_batch_id
,ltpy_dl_seqno
,ltpy_dl_load_status
,ltpy_tsk_alt_reference                                 --validate
,ltpy_tsk_tkg_src_reference                             --validate
,ltpy_tsk_tkg_code                                      --validate
,ltpy_tsk_stk_code                                      --validate
,NVL(ltpy_tsk_tkg_src_type,'CNT') ltpy_tsk_tkg_src_type --constant
,ltpy_tsk_id                                             --found in validate                                            
,NVL(ltpy_pay_type_ind,'P') ltpy_pay_type_ind           --constant
,ltpy_sco_code                                          --validate
,ltpy_status_date                                       --validate
,ltpy_due_date                                          --validate
,ltpy_task_net_amount                                   --validate
,ltpy_task_tax_amount                                   --validate
,NVL(ltpy_created_by,'DATALOAD') ltpy_created_by        --constant
,NVL(ltpy_created_date, SYSDATE) ltpy_created_date      --constant
,ltpy_paid_date                                         --validate
,ltpy_payment_id                                        --validate
,ltpy_payment_date                                      --validate
,ltpy_tpm_bud_refno                                     --found in validate
,ltpy_tpm_seqno                                         --found in validate
FROM   dl_hpm_task_payments
WHERE  ltpy_dlb_batch_id = p_batch_id
AND    ltpy_dl_load_status IN ('L','F','O');
-- *********************************
--
CURSOR chk_tsk_altref
   (p_ltpy_tsk_alt_ref VARCHAR2)
IS
SELECT count(*)
FROM   tasks
WHERE  tsk_alt_reference = p_ltpy_tsk_alt_ref;
--
CURSOR chk_ltsk_altref
      (p_ltpy_tsk_alt_ref VARCHAR2
      ,p_ltpy_dlb_batch_id  VARCHAR2)
IS
SELECT count(*)
FROM   dl_hpm_task_payments
WHERE  ltpy_tsk_alt_reference = p_ltpy_tsk_alt_ref
and    ltpy_dlb_batch_id = p_ltpy_dlb_batch_id;
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
-- *********************************
--
-- Constants for process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'VALIDATE';
ct          VARCHAR2(30) := 'DL_HPM_TASK_PAYMENTS';
cs          INTEGER;
ce          VARCHAR2(200);
--
-- Other variables
--
l_id               ROWID;
i                  INTEGER := 0;
l_an_tab           VARCHAR2(1);
l_exists           VARCHAR2(1);
l_pro_refno        NUMBER(10);
l_errors           VARCHAR2(10);
l_error_ind        VARCHAR2(10);
l_curr_void        VARCHAR2(1);
l_vin_refno        INTEGER := 0;
l_vpa_exists       VARCHAR2(1);
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
l_tba_net_amt      NUMBER(14,2);
l_tba_tax_amt      NUMBER(14,2);
--
--*****************************
--
BEGIN
 fsc_utils.proc_start('s_dl_hpm_task_payments.dataload_validate');
 fsc_utils.debug_message( 's_dl_hpm_task_payments.dataload_validate',3);
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR p1 IN c1(p_batch_id) 
  LOOP
   BEGIN
    cs := p1.ltpy_dl_seqno;
    l_id := p1.rec_rowid;
    l_errors := 'V';
    l_error_ind := 'N';
    l_exists := NULL;
    l_pro_refno := NULL;
    l_curr_void := NULL;
    l_vin_refno := NULL;
    l_vpa_exists := NULL;
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
    l_tba_net_amt := NULL;
    l_tba_tax_amt := NULL;
--
-- ****************************
-- check mandatory fields supplied
--
-- task alternative reference
--
   IF (p1.ltpy_tsk_alt_reference IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',134);
   ELSE 
    OPEN chk_tsk_altref(p1.ltpy_tsk_alt_reference);
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
    OPEN chk_ltsk_altref(p1.ltpy_tsk_alt_reference,p_batch_id);
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
   IF (p1.ltpy_tsk_tkg_src_reference IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',139);
   ELSE 
    OPEN chk_contract(p1.ltpy_tsk_tkg_src_reference);
    FETCH chk_contract
    INTO l_cnt_exists,l_cnt_status;
    CLOSE chk_contract;
    IF (l_cnt_exists IS NULL) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',333);
    END IF;
   END IF;
--
-- Task Group Code
--
   IF (p1.ltpy_tsk_tkg_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',357);
   ELSE 
     OPEN chk_task_groups(p1.ltpy_tsk_tkg_src_reference
                         ,p1.ltpy_tsk_tkg_code
                         ,p1.ltpy_tsk_tkg_src_type
                         ,'PAYT');
     FETCH chk_task_groups INTO l_tkg_exists;
     CLOSE chk_task_groups;
     IF (l_tkg_exists IS NULL) 
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',140);
      END IF;
   END IF;
--
-- Task Payment Status Code must be either RAI or CLO
--
   IF (p1.ltpy_sco_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',141);
   ELSIF (p1.ltpy_sco_code NOT IN ('RAI','CLO')) 
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',142);
   END IF;	
--
-- Task Payment Status Start Date
--
   IF (p1.ltpy_status_date IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',338);
   END IF;
--
-- Task Payment Due Date
--
   IF (p1.ltpy_due_date IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',774);
   END IF;
--
-- Task Payment Amounts are supplied
--
   IF (p1.ltpy_task_net_amount IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',143);
   END IF;
   IF (p1.ltpy_task_tax_amount IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',144);
   END IF;
--
-- Task Payment Paid Date and External Payment references checks if supplied
-- must only be supplied if status is CLO
--
   IF (p1.ltpy_sco_code = 'CLO')
    THEN
     IF (p1.ltpy_payment_id   IS NULL   OR
         p1.ltpy_payment_date IS NULL   OR
         p1.ltpy_paid_date    IS NULL     )
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',145);
     END IF;
   END IF;
--
   IF (p1.ltpy_sco_code = 'RAI')
    THEN
     IF (p1.ltpy_payment_id   IS NOT NULL   OR
         p1.ltpy_payment_date IS NOT NULL   OR
         p1.ltpy_paid_date    IS NOT NULL     )
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',146);
     END IF;
   END IF;
--
   IF (p1.ltpy_payment_date IS NOT NULL  AND
       p1.ltpy_paid_date    IS NOT NULL     )
    THEN
     IF (p1.ltpy_payment_date < p1.ltpy_paid_date)
      THEN	 
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',156);
     END IF;
   END IF;
--   
-- Check Standard Task Code
--
   IF (p1.ltpy_tsk_stk_code IS NULL)
    THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',147);
   ELSE 
    OPEN chk_stk_code(p1.ltpy_tsk_stk_code);
    FETCH chk_stk_code INTO l_stk_exists;
    CLOSE chk_stk_code;
    IF (l_stk_exists IS NULL) 
     THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',148);
    END IF;
   END IF;
--
-- Check and get task amounts budgets driver task alternative reference
-- so only get detail if tsk_alt_reference is unique
--
   IF (l_chk_tskaltref = 1)
    THEN
     OPEN get_tsk_detail(p1.ltpy_tsk_alt_reference);
     FETCH get_tsk_detail INTO l_tsk_tkg_src_ref
                              ,l_tsk_tkg_code
                              ,l_tsk_tkg_src_type
                              ,l_tsk_id
                              ,l_tsk_sco_code
                              ,l_tsk_type_ind
                              ,l_tsk_stk_code;
     CLOSE get_tsk_detail;
--
     IF (l_tsk_tkg_src_ref != p1.ltpy_tsk_tkg_src_reference)
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',149);
     END IF;
--
     IF (l_tsk_tkg_code != p1.ltpy_tsk_tkg_code)
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
     IF (l_tsk_stk_code != p1.ltpy_tsk_stk_code)
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',154);
     END IF;
--
-- check to see if task payment already exists
--     
     OPEN chk_tsk_paymt(p1.ltpy_tsk_tkg_src_reference
                       ,p1.ltpy_tsk_tkg_code
                       ,p1.ltpy_tsk_tkg_src_type
                       ,l_tsk_id
                       ,p1.ltpy_pay_type_ind);
     FETCH chk_tsk_paymt INTO l_tsk_exists;
     CLOSE chk_tsk_paymt;
--
     IF (l_tsk_exists IS NOT NULL)
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',155);
     END IF;
--
-- check task version for amounts
-- get max version number first then detail then check amounts
--     
     OPEN get_tve_max_ver(p1.ltpy_tsk_tkg_src_reference
                         ,p1.ltpy_tsk_tkg_code
                         ,p1.ltpy_tsk_tkg_src_type
                         ,l_tsk_id);
     FETCH get_tve_max_ver INTO l_tve_max;
     CLOSE get_tve_max_ver;
--
-- get task version detail
--   
     OPEN get_tve(p1.ltpy_tsk_tkg_src_reference
                 ,p1.ltpy_tsk_tkg_code
                 ,p1.ltpy_tsk_tkg_src_type
                 ,l_tsk_id
                 ,l_tve_max);
     FETCH get_tve INTO l_tve_bca_year
                       ,l_tve_net_amt
                       ,l_tve_tax_amt;
     CLOSE get_tve;
--
-- get task budget amounts detail
--  
     OPEN get_tba(p1.ltpy_tsk_tkg_src_reference
                 ,p1.ltpy_tsk_tkg_code
                 ,p1.ltpy_tsk_tkg_src_type
                 ,l_tsk_id
                 ,l_tve_max);
     FETCH get_tba INTO l_tba_bud_refno
                       ,l_tba_net_amt
                       ,l_tba_tax_amt;
     CLOSE get_tba;
--
-- Now check that amounts supplied match task version and budget amounts
--
     IF (l_tve_net_amt != p1.ltpy_task_net_amount OR
         l_tve_tax_amt != p1.ltpy_task_tax_amount   )
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',157);
     END IF;
--
     IF (l_tba_net_amt != p1.ltpy_task_net_amount OR
         l_tba_tax_amt != p1.ltpy_task_tax_amount   )
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
-- now update dl_hpm_task_payments with details needed for create
--
   IF (l_tsk_id IS NOT NULL)
    THEN
     UPDATE dl_hpm_task_payments
     SET    ltpy_tsk_id = l_tsk_id
     WHERE  ltpy_dlb_batch_id = p_batch_id
     AND    rowid = p1.rec_rowid;
   END IF;
--
   IF (l_tba_bud_refno IS NOT NULL)
    THEN
     UPDATE dl_hpm_task_payments
     SET    ltpy_tpm_bud_refno = l_tba_bud_refno
     WHERE  ltpy_dlb_batch_id = p_batch_id
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
 l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HPM_TASK_PAYMENTS');
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
,ltpy_dlb_batch_id
,ltpy_dl_seqno
,ltpy_dl_load_status
,ltpy_tsk_alt_reference
,ltpy_tsk_tkg_src_reference
,ltpy_tsk_tkg_code
,ltpy_tsk_stk_code
,NVL(ltpy_tsk_tkg_src_type,'CNT') ltpy_tsk_tkg_src_type
,ltpy_tsk_id
,NVL(ltpy_pay_type_ind,'P') ltpy_pay_type_ind
,ltpy_sco_code
,ltpy_status_date
,ltpy_due_date
,ltpy_task_net_amount
,ltpy_task_tax_amount
,NVL(ltpy_created_by,'DATALOAD') ltpy_created_by
,NVL(ltpy_created_date, SYSDATE) ltpy_created_date
,ltpy_paid_date
,ltpy_payment_id
,ltpy_payment_date
,ltpy_tpm_bud_refno
,ltpy_tpm_seqno
FROM   dl_hpm_task_payments
WHERE  ltpy_dlb_batch_id = p_batch_id
AND    ltpy_dl_load_status = 'C';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HPM_TASK_PAYMENTS';
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
  fsc_utils.proc_start('s_dl_hpm_task_payments.dataload_delete');
  fsc_utils.debug_message( 's_dl_hpm_task_payments.dataload_delete',3 );
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 IN c1(p_batch_id) 
   LOOP
    BEGIN
     cs := p1.ltpy_dl_seqno;
     l_id := p1.rec_rowid;
     SAVEPOINT SP1;
-- ****************************************
--
-- First delete the task budget amounts
--

     DELETE
     FROM   task_payment_budget_amounts	 
     WHERE  tpm_tpy_tsk_id = p1.ltpy_tsk_id
     AND    tpm_tpy_tsk_tkg_src_reference = p1.ltpy_tsk_tkg_src_reference
     AND    tpm_tpy_tsk_tkg_code = p1.ltpy_tsk_tkg_code
     AND    tpm_tpy_tsk_tkg_src_type = p1.ltpy_tsk_tkg_src_type
     AND    tpm_tpy_pay_type_ind = p1.ltpy_pay_type_ind
     AND    tpm_bud_refno = p1.ltpy_tpm_bud_refno
     AND    tpm_seqno = p1.ltpy_tpm_seqno
	 ;
--
-- Then delete the task payment
--
     DELETE 
     FROM   task_payments
     WHERE  tpy_tsk_id = p1.ltpy_tsk_id
     AND    tpy_tsk_tkg_src_reference = p1.ltpy_tsk_tkg_src_reference
     AND    tpy_tsk_tkg_code = p1.ltpy_tsk_tkg_code
     AND    tpy_tsk_tkg_src_type = p1.ltpy_tsk_tkg_src_type
     AND    tpy_pay_type_ind = p1.ltpy_pay_type_ind
     AND    tpy_sco_code = p1.ltpy_sco_code
	 ;
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
 l_an_tab := s_dl_hem_utils.dl_comp_stats('TASK_PAYMENTS');
 l_an_tab := s_dl_hem_utils.dl_comp_stats('TASK_PAYMENT_BUDGET_AMOUNTS');
 fsc_utils.proc_end;
 COMMIT;  
 EXCEPTION
  WHEN OTHERS 
   THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
   RAISE;
END dataload_delete;
--
END s_dl_hpm_task_payments;
/

show errors

