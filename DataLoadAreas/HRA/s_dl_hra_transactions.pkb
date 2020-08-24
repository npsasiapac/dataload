CREATE OR REPLACE PACKAGE BODY s_dl_hra_transactions
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER DB Ver WHO  WHEN       WHY
  --  1.0        MTR  27/11/00   Dataload
  --  1.1 5.1.4  PJD  27/02/02   Changed main cursors to set ltra_dr
  --                             =0 where both dr and cr are 0 or null
  --                             Add in code to create batch runs at 
  --                             start of the create process.
  --                             Allow tra_refno to be selected when not 
  --                             supplied
  --  1.2 5.1.4  PJD  18/03/02   Set tra_str_refno.
  --  2.0 5.2.0  PJD  11/07/02   Addition of tra_debit_effective_date in create proc.
  --  2.1 5.2.0  PJD  30/07/02   Altered validation of start date validation to exclude PAY and ADJ
  --  2.2 5.2.0  MH   30/10/02   Added extra validation to ensure correct tra_debit_eff 
  --  2.3 5.2.0  SB   08/10/01   Added cursor to assign to rac_hrv_ate_code when getting bru_run_no
  --                             so that transaction assigned correct batch_run_no
  --                             declare package variables AND constants
  --  2.4 5.3.0  IR   29/11/02   Added suspense account payment reference (ltra_susp_pay_ref)
  --  3.0 5.3.0  PJD  04/02/03   Default 'Created By' field to DATALOAD
  --  3.1 5.3.0  PJD  30/06/03   Changed insert into batch runs into a loop
  --  3.2 5.3.0  PH   29/07/03   Amended above cursor to correct compil'n error
  --  3.3 5.3.0  PJD  24/09/03   Amended to allow for Susp Trans where 
  --                             Admin Period checks will be inappropriate
  --  3.4 5.3.0  PH   17/06/03   Changed the way we do the analyse after the create. Check to see
  --                             if the number of records in the batch is greater than those in the 
  --                             target table - if so analyse.
  --  3.5 5.4.0  PH   11/12/03   Amended Create and Validate processes. Commit every 5000.
  --                             Also, changed to Process in chuncks of 30000 records.  
  --  3.6 5.5.0  PJD  17/02/04   Changed loop: to loop;  
  --  3.7 5.6.0  PJD  13/10/04   Now includes its own set_record_status_flag procedure 
  --  3.9 5.9.0  PH   17/07/06   Commented out validate on dates being prior to 
  --                             rac_start_date as this is allowed
  --  4.0 5.10.0 PH   28/07/06   Amended Effective Date check (HDL137) as different for
  --                             Accounts that have a Class Code of LIA.
  --  4.1 5.10.0 PH   09/10/06   Amended cursor c_hra069 to prevent very old balance 
  --                             periods being selected and inserted and then
  --                             subsequently deleted by void summaries.
  --  4.2 5.10.0 PH   02/11/06   Added validate on tra_effective_date. Cannot be
  --                             more than 20 years ago. This will prevent problems
  --                             when they need summarising.
  --  5.0 5.12.0 PH   06/11/06   New code to allocate payments to accounts
  --                             for Customer Liablility Invoices
  --  5.1 5.15.1 PH   20/11/09   Amended validate on tra_dr can be zero but not null
  --  5.2 6.1.1  MB   06/09/11   Added field TRA_EXT_DESCRIPTION at request of Southwark
  --  5.3 6.9    PJD  09/12/14   Added c_aye cursor into create proc as prev
  --                             call to s_admin_years liable to error.
  --  5.4 6.13   AJ   09/05/15   1)Amended to use customer liability invoice field (23) and allocate (24)
  --                             on guide so user can control if PAY or ADJ is allocated or if DRS or
  --                             ADC linked to invoice on transactions table (tra_clin_refno)
  --                             Validation added for fields 23 and 24 errors 891 - 893
  --  5.5 6.13   AJ   12/05/15   1)the p_name amended from hra168 to s_dl_hra_transactions
  --                             2)check of AND l_rac_hrv_ate_code = 'SER' remove from chice of account
  --                               when want to allocate PAY or ADJ  
  --  5.6 6.13   PJD  15/12/16   added NVL to 
  --                             IF ( NVL(l_tra_balance_ind,'Y')='N'
  --                             check in Create Proc
  --                             Change control added 21-Feb-2018 (AJ)
  --


PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)     
AS
-- 
BEGIN
  UPDATE dl_hra_transactions
  SET ltra_dl_load_status = p_status
  WHERE rowid = p_rowid;
  -- 
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_transactions');
     RAISE;
  --
END set_record_status_flag;
--
PROCEDURE allocate_payment_to_account( p_tra_refno      IN  NUMBER
                                     , p_rac_accno      IN  NUMBER
                                     , p_phe_batch_ref  IN  VARCHAR2
                                     , p_pos_seqno      IN  NUMBER 
                                     , p_external_ref   IN  VARCHAR2
                                     , p_cr_amount      IN  NUMBER)  IS
--
CURSOR c_undisputed( cp_rac_accno   IN  NUMBER )
IS
SELECT clin.*
FROM   customer_liability_invoices clin
     , invoice_balances            inba
     , invoice_categories          inca
WHERE  clin.clin_rac_accno  = cp_rac_accno
AND    inba.inba_clin_refno = clin.clin_refno
AND    inba.inba_seqno      = ( SELECT MAX(inba2.inba_seqno)
                                FROM invoice_balances inba2
                                WHERE inba2.inba_clin_refno = clin.clin_refno )
AND inba.inba_undisputed_balance > 0
AND inca.inca_code          = clin.clin_inca_code
ORDER BY NVL(inca.inca_pay_alloc_priority,99) ASC, clin.clin_created_date ASC;
--
CURSOR c_disputed( cp_rac_accno   IN  NUMBER ) IS
SELECT clin.*
FROM   customer_liability_invoices clin
     , invoice_balances inba
     , invoice_categories inca
WHERE  clin.clin_rac_accno  = cp_rac_accno
AND    inba.inba_clin_refno = clin.clin_refno
AND    inba.inba_seqno      = ( SELECT MAX(inba2.inba_seqno)
                                FROM invoice_balances inba2
                                WHERE inba2.inba_clin_refno = clin.clin_refno )
AND    inba.inba_total_balance > 0
AND    inca.inca_code       = clin.clin_inca_code
ORDER BY NVL(inca.inca_pay_alloc_priority,99) ASC, clin.clin_created_date ASC;
--
--
r_clin                    customer_liability_invoices%ROWTYPE;
r_inba                    invoice_balances%ROWTYPE;
r_inip                    invoice_instalment_plans%ROWTYPE;
r_trans                   transactions%ROWTYPE;
--
l_currentPaymentBalance   NUMBER;
l_remainingCredit         NUMBER;
l_amountToAllocate        NUMBER;
--
  BEGIN
--
-- If this needs to use a bespoke allocation process 
--
     s_hou_site.site_version(
--        p_name => 'hra168.allocate_payment_to_account'
        p_name => 's_dl_hra_transactions.allocate_payment_to_account'
     ,  p_param_1 => p_tra_refno
     ,  p_param_2 => p_rac_accno
     ,  p_param_3 => p_phe_batch_ref
     ,  p_param_4 => p_pos_seqno );
     IF hou_variables.get_g_overide_flag() = 'N'
     THEN
        r_trans := s_transactions.get_original_transaction( p_tra_refno
                                                          , p_rac_accno);
--
        l_currentPaymentBalance := s_payment_balances.create_payment_balances( p_tra_refno
                                                                             , p_cr_amount
                                                                             , TRUNC(SYSDATE) );
--
        l_remainingCredit := p_cr_amount;
--
        r_clin := s_customer_liability_invoices.get_rec_for_invoice_ref(p_external_ref);          
        r_inba := s_invoice_balances.get_record( r_clin.clin_refno );
-- 
        IF p_cr_amount < r_inba.inba_total_balance
        THEN
           l_amountToAllocate := p_cr_amount;
        ELSE
           l_amountToAllocate := r_inba.inba_total_balance;
        END IF;
-- 
        allocate_credit_to_invoice( r_clin.clin_refno
                                  , p_tra_refno
                                  , l_amountToAllocate );
 
        l_remainingCredit := l_remainingCredit - l_amountToAllocate;
-- 
        -- If there's a payment plan in action that can be recalc'd, recalc it.
--
        IF  s_invoice_instalment_plans.get_plan_status(r_clin.clin_refno) = 'CURRENT'
        THEN
           r_inip := s_invoice_instalment_plans.get_plan_for_clin(r_clin.clin_refno);
           s_invoice_instalment_plans.recalc_instalment_plan_proc(r_inip.inip_refno);
        END IF;
--
        IF l_remainingCredit = 0
        THEN
           RETURN;
        END IF;
--
        FOR r_rec IN c_undisputed(p_rac_accno)
        LOOP
           r_inba := s_invoice_balances.get_record( r_rec.clin_refno );
--
           IF l_remainingCredit < r_inba.inba_undisputed_balance
           THEN
              l_amountToAllocate := l_remainingCredit;
           ELSE
              l_amountToAllocate := r_inba.inba_undisputed_balance;
           END IF;
--
           allocate_credit_to_invoice( r_rec.clin_refno
                                     , p_tra_refno
                                     , l_amountToAllocate );
--
           l_remainingCredit := l_remainingCredit - l_amountToAllocate;
--
           -- If there's a payment plan in action that can be recalc'd, recalc it.
--
           IF  s_invoice_instalment_plans.get_plan_status(r_clin.clin_refno) = 'CURRENT'
           THEN
              r_inip := s_invoice_instalment_plans.get_plan_for_clin(r_rec.clin_refno);
              s_invoice_instalment_plans.recalc_instalment_plan_proc(r_inip.inip_refno);
           END IF;
--
           IF l_remainingCredit = 0
           THEN
              RETURN;
           END IF;
--
        END LOOP;
--
        FOR r_rec IN c_disputed(p_rac_accno)
        LOOP
           r_inba := s_invoice_balances.get_record( r_rec.clin_refno );
--
           IF l_remainingCredit < r_inba.inba_total_balance
           THEN
              l_amountToAllocate := l_remainingCredit;
           ELSE
              l_amountToAllocate := r_inba.inba_total_balance;
           END IF;
--
           allocate_credit_to_invoice( r_rec.clin_refno
                                     , p_tra_refno
                                     , l_amountToAllocate );
--
           l_remainingCredit := l_remainingCredit - l_amountToAllocate;
           IF l_remainingCredit = 0
           THEN
              RETURN;
           END IF;
--
        END LOOP;
--
     END IF;
--
     fsc_utils.proc_end;
--  
  EXCEPTION
  WHEN OTHERS
  THEN
     fsc_utils.handle_exception;
  END allocate_payment_to_account;
--
--
PROCEDURE allocate_credit_to_invoice( p_clin_refno       IN  NUMBER
                                    , p_tra_refno        IN  NUMBER
                                    , p_amount           IN  NUMBER )  IS
--
r_paba                     payment_balances%ROWTYPE;
r_inba                     invoice_balances%ROWTYPE;
--
l_cral_refno               NUMBER;
l_paba_refno               NUMBER;
l_inba_refno               NUMBER;
l_invTotalBal              NUMBER;
l_invUndisputedBal         NUMBER;
--
  BEGIN
--
     l_cral_refno := s_credit_allocations.create_credit_allocations( p_amount
                                                                   , p_tra_refno
                                                                   , NULL
                                                                   , NULL
                                                                   , NULL
                                                                   , p_clin_refno );
--
     r_paba := s_payment_balances.get_record(p_tra_refno);
     l_paba_refno := s_payment_balances.create_payment_balances( p_tra_refno 
                                                               , r_paba.paba_total_balance - p_amount
                                                               , TRUNC(SYSDATE) );
--
     r_inba := s_invoice_balances.get_record(p_clin_refno);
     l_invTotalBal        := r_inba.inba_total_balance - p_amount;
     l_invUndisputedBal   := r_inba.inba_undisputed_balance - p_amount;
     IF l_invUndisputedBal < 0
     THEN
        l_invUndisputedBal := 0;
     END IF;
--
     l_inba_refno := s_invoice_balances.create_invoice_balances( p_clin_refno
                                                               , l_invTotalBal
                                                               , TRUNC(SYSDATE)
                                                               , l_invUndisputedBal );
--
     fsc_utils.proc_end;
--
  EXCEPTION
  WHEN OTHERS
  THEN
     fsc_utils.handle_exception;
  END allocate_credit_to_invoice;
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c_max(p_batch_id VARCHAR2) is
SELECT MIN(ltra_dl_seqno) min_seqno
,      MAX(ltra_dl_seqno+1) max_seqno
FROM   dl_hra_transactions
WHERE  ltra_dlb_batch_id    = p_batch_id
AND    ltra_dl_load_status  = 'V';
--
CURSOR c1(p_batch_id VARCHAR2,p_min_seqno INTEGER, p_curr_seqno INTEGER) IS
SELECT
rowid rec_rowid,
ltra_dlb_batch_id,
ltra_dl_seqno,
ltra_refno,
ltra_date,
ltra_pay_ref,
ltra_effective_date,
ltra_statement_ind,
ltra_trt_code,
ltra_trs_code,
ltra_pmy_code,
ltra_balance_ind,
decode(ltra_dr,0,decode(ltra_cr,null,0,0,0,null),ltra_dr) ltra_dr,
decode(ltra_cr,0,null,ltra_cr) ltra_cr,
decode(ltra_vat_dr,0,null,ltra_vat_dr) ltra_vat_dr,
decode(ltra_vat_cr,0,null,ltra_vat_cr) ltra_vat_cr,
ltra_payment_date,
ltra_hde_claim_no,
ltra_external_ref,
ltra_text,
ltra_balance_year,
ltra_balance_period,
ltra_summarise_ind,
ltra_debit_effective_date,
ltra_susp_pay_ref,
ltra_ext_description,
ltra_clin_invoice_ref,
ltra_allocate_to_clin
FROM dl_hra_transactions
WHERE ltra_dlb_batch_id    = p_batch_id
AND   ltra_dl_seqno        BETWEEN   p_min_seqno AND p_curr_seqno
AND   ltra_dl_load_status  = 'V';
--
CURSOR c_bru_run_no(p_aun_code VARCHAR2,p_date DATE,p_rac_hrv_ate_code VARCHAR2) is
SELECT max(bru_run_no)
  FROM batch_runs
 WHERE bru_mod_name = 'HRA069'
   AND bru_aun_code = p_aun_code
   AND p_date       between bru_period_start_date and bru_period_end_date
   AND bru_hrv_ate_code = p_rac_hrv_ate_code;
--
CURSOR c_aye (p_aun_code VARCHAR2, p_date DATE) IS
SELECT 'X'
FROM   admin_years
WHERE  aye_aun_code          = p_aun_code
  AND  aye_start_date       <= p_date
  AND  aye_end_date         >= p_date;
--
CURSOR c_rac_hrv_ate_code (p_rac_accno NUMBER) IS
SELECT rac_hrv_ate_code
FROM   revenue_accounts
WHERE  rac_accno = p_rac_accno;
--
CURSOR c_tra_refno IS
SELECT tra_refno_seq.nextval 
FROM dual;
--
CURSOR c_hra069 IS
SELECT 
frv_code,ape_aye_aun_code,ape_start_date,ape_end_date
FROM first_ref_values,admin_periods
WHERE ape_end_date        < sysdate
AND ape_admin_period_type = 'BAL'
AND frv_frd_domain        = 'RAC_TYPE'
AND NOT EXISTS(SELECT NULL 
               FROM batch_runs
               WHERE bru_mod_name          = 'HRA069'
                 AND bru_hrv_ate_code      = frv_code
--                 AND bru_period_start_date = ape_start_date  -- Changed this line
                   AND bru_period_start_date >= ape_start_date  -- to this
                 AND bru_aun_code          = ape_aye_aun_code)
AND (frv_code IN ('REN','HBO')
     OR frv_usage = 'USR')
ORDER BY ape_aye_aun_code,ape_start_date;
--
CURSOR c_analyse IS
SELECT COUNT(*)
FROM   transactions;
--
CURSOR c_class_code (p_rac_accno NUMBER) IS
SELECT rac_class_code
FROM   revenue_accounts
WHERE  rac_accno = p_rac_accno;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_TRANSACTIONS';
cs       PLS_INTEGER;
ce         VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
i                   PLS_INTEGER := 0;
l_rac_accno         NUMBER;
l_aye_exists        VARCHAR2(1);
l_stran_seqno       NUMBER default null;
l_bru_run_no        NUMBER;
l_bru_period_end    DATE;
l_bru_period_start  DATE;
l_rac_aun_code      revenue_accounts.rac_aun_code%type;
r_ape               admin_periods%rowtype;
l_an_tab            VARCHAR2(1);
l_tra_refno         NUMBER;
l_tra_upd           VARCHAR(1);
l_rac_hrv_ate_code  VARCHAR2(10);
l_min_seqno         PLS_INTEGER := 0;
l_max_seqno         PLS_INTEGER := 0;
l_curr_seqno        PLS_INTEGER := 0;
l_tra_count         PLS_INTEGER := 0;
l_tra_balance_ind   VARCHAR2(1);
l_rac_class_code    VARCHAR2(10);
l_tra_clin_refno    NUMBER;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_transactions.dataload_create');
fsc_utils.debug_message( 's_dl_hra_transactions.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Create the HRA069 entries - so they are in the right sequence
-- 
FOR p_hra069 IN c_hra069 LOOP
--
INSERT INTO batch_runs
(bru_run_no,bru_mod_name,bru_run_start_date,bru_run_end_date,bru_hrv_ate_code,bru_aun_code,
 bru_period_start_date,bru_period_end_date,bru_created_by,bru_created_date)
values 
(
bru_run_no_seq.nextval,'HRA069',trunc(sysdate),trunc(sysdate),p_hra069.frv_code,
p_hra069.ape_aye_aun_code,p_hra069.ape_start_date,p_hra069.ape_end_date,'DATALOAD',sysdate);
--

END LOOP;
--
COMMIT;
--
-- Now check to see how many records exist in transactions
-- This will be used later to decide if we should analyse
--
  OPEN  c_analyse;
   FETCH c_analyse into l_tra_count;
  CLOSE c_analyse;
--
OPEN c_max(p_batch_id);
FETCH c_max INTO l_min_seqno, l_max_seqno;
CLOSE c_max;
--
l_curr_seqno := l_min_seqno + 30000;
--
WHILE l_min_seqno < l_max_seqno LOOP
--
  FOR p1 IN c1(p_batch_id,l_min_seqno,l_curr_seqno) LOOP
--
  BEGIN
--
cs   := p1.ltra_dl_seqno;
l_id := p1.rec_rowid;
--
IF p1.ltra_susp_pay_ref IS NOT NULL
THEN
  l_tra_balance_ind := 'N';
ELSE
  l_tra_balance_ind := p1.ltra_balance_ind;
END IF;
--
  -- Get Revenue Account number
     l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                    (p1.ltra_pay_ref);
  --
  -- Get AUN code
     l_rac_aun_code  := s_revenue_accounts.get_rac_aun_code
                       ( l_rac_accno);
--
--  Get RAC_HRV_ATE_CODE (Account Type)
     l_rac_hrv_ate_code := NULL;
     OPEN  c_rac_hrv_ate_code(l_rac_accno);
     FETCH c_rac_hrv_ate_code into l_rac_hrv_ate_code;
     CLOSE c_rac_hrv_ate_code;
     
  --
  -- If the summarise transaction indicator is set to 'Y' then
  -- set the summary transaction seqno to 0 otherwise set it to null
     l_stran_seqno := NULL;
     IF ( p1.ltra_summarise_ind = 'Y' )
     THEN
       l_stran_seqno := 0;
     END IF;
  --
  --
  -- If the transaction is balanced then get balance info
  -- Get the relevant balance run number from BATCH_RUNS. If none
  -- exists, insert a row into BATCH_RUNS using data from
  -- ADMIN_PERIODS
  -- however........
  -- This transaction could be so old that there is no matching admin year
  -- So first we need to check for a valid admin year
     l_aye_exists := NULL;
     OPEN  c_aye(l_rac_aun_code,p1.ltra_effective_date);
     FETCH c_aye into l_aye_exists;
     CLOSE c_aye;
     IF l_aye_exists IS NOT NULL
     THEN
     --
       IF ( NVL(l_tra_balance_ind,'Y') = 'N')
       THEN
         l_bru_run_no := NULL;
       ELSE
         IF (p1.ltra_balance_year IS NOT NULL AND
               p1.ltra_balance_period IS NOT NULL )
         THEN
           r_ape := s_admin_periods.get_record
                    (l_rac_aun_code,p1.ltra_balance_year
                    ,p1.ltra_balance_period,'BAL');
           l_bru_period_start := r_ape.ape_start_date;
           l_bru_period_end   := r_ape.ape_end_date;
        --
           OPEN  c_bru_run_no(l_rac_aun_code,l_bru_period_start,l_rac_hrv_ate_code);
           FETCH c_bru_run_no into l_bru_run_no;
           CLOSE c_bru_run_no;
        --
         ELSE
           OPEN  c_bru_run_no(l_rac_aun_code,p1.ltra_effective_date,l_rac_hrv_ate_code);
           FETCH c_bru_run_no into l_bru_run_no;
           CLOSE c_bru_run_no;
         END IF;
       --
         IF ( l_bru_run_no IS NULL )
         THEN
           SELECT bru_run_no_seq.nextval
           INTO l_bru_run_no
           FROM dual;
           -- If balance year and period information have been obtained
           -- then use them to get info from ADMIN_PERIODS, otherwise
           -- use the transaction effective date
         --
           IF (p1.ltra_balance_year IS NOT NULL AND
               p1.ltra_balance_period IS NOT NULL )
           THEN
             r_ape := s_admin_periods.get_record
                      (l_rac_aun_code,p1.ltra_balance_year
                      ,p1.ltra_balance_period,'BAL');
             l_bru_period_start := r_ape.ape_start_date;
             l_bru_period_end   := r_ape.ape_end_date;
           ELSE
             r_ape := s_admin_periods.get_record_effective
                      (l_rac_aun_code,p1.ltra_effective_date,'BAL');
             l_bru_period_start := r_ape.ape_start_date;
             l_bru_period_end   := r_ape.ape_end_date;
           END IF;
           -- Insert a row into BATCH_RUNS
           INSERT INTO batch_runs
           (
           bru_run_no,
           bru_mod_name,
           bru_run_start_date,
           bru_created_by,
           bru_created_date,
           bru_hrv_ate_code,
           bru_run_end_date,
           bru_period_start_date,
           bru_period_end_date,
           bru_gri_run_id,
           bru_aun_code,
           bru_modified_by,
           bru_modified_date
           )
           VALUES
           (
           l_bru_run_no,
           'HRA069',
           sysdate,
           'DATALOAD',
           sysdate,
           l_rac_hrv_ate_code,
           sysdate,
           l_bru_period_start,
           l_bru_period_end,
           null,
           l_rac_aun_code,
           null,
           null
           );
         END IF;
       END IF;
     ELSE
       l_bru_run_no := NULL;
     END IF;  -- l_aye_exists is null
     --
     l_tra_upd   := 'N';
     l_tra_refno := p1.ltra_refno;
     IF l_tra_refno is NULL 
     THEN 
       l_tra_upd := 'Y';
       --
       OPEN  c_tra_refno;
       FETCH c_tra_refno INTO l_tra_refno;
       CLOSE c_tra_refno;
     --
     END IF;
     --
     INSERT into transactions
     (
      tra_refno
     ,tra_rac_accno
     ,tra_date
     ,tra_effective_date
     ,tra_statement_ind
     ,tra_created_by
     ,tra_invoice_ind
     ,tra_susp_ind
     ,tra_sort_sequence_ind
     ,tra_trt_code
     ,tra_trs_code
     ,tra_pmy_code
     ,tra_balance_ind
     ,tra_dr
     ,tra_cr
     ,tra_vat_dr
     ,tra_vat_cr
     ,tra_payment_date
     ,tra_hde_claim_no
     ,tra_external_ref
     ,tra_text
     ,tra_bru_run_no
     ,tra_str_refno
     ,tra_debit_effective_date
     ,tra_susp_pay_ref
         ,tra_ext_description
     )
     VALUES
     (
      l_tra_refno
     ,l_rac_accno
     ,p1.ltra_date
     ,p1.ltra_effective_date
     ,p1.ltra_statement_ind
     ,'DATALOAD'
     ,'N'
     ,'N'
     ,'N'
     ,p1.ltra_trt_code
     ,p1.ltra_trs_code
     ,p1.ltra_pmy_code
     ,l_tra_balance_ind
     ,p1.ltra_dr
     ,p1.ltra_cr
     ,p1.ltra_vat_dr
     ,p1.ltra_vat_cr
     ,p1.ltra_payment_date
     ,p1.ltra_hde_claim_no
     ,p1.ltra_external_ref
     ,p1.ltra_text
     ,l_bru_run_no
     ,l_stran_seqno
     ,p1.ltra_debit_effective_date
     ,p1.ltra_susp_pay_ref
         ,p1.ltra_ext_description);
     --
         -- If DRS or ADC and LTRA_CLIN_INVOICE_REF has been
         -- supplied and LTRA_ALLOCATE_TO_CLIN set to "Y"
         -- then update the tra_clin_refno
         --      
         IF ( p1.ltra_clin_invoice_ref IS NOT NULL
            AND p1.ltra_allocate_to_clin ='Y'
            AND p1.ltra_trt_code IN ('DRS','ADC')
                )
          THEN
         --
       l_tra_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.ltra_clin_invoice_ref);
         --
       UPDATE transactions
           SET tra_clin_refno = l_tra_clin_refno
       WHERE tra_refno = l_tra_refno;
     --
     END IF;
     --  
     -- Insert a row into debit_details when tran_hrv_type is 'DRS'
     --
     IF p1.ltra_trt_code in ('DRS','DRA')
     THEN
        INSERT INTO debit_details
        (
        dde_tra_refno,
        dde_pro_refno,
        dde_ele_code,
        dde_att_code,
        dde_amount,
        dde_vat_amount,
        dde_vca_code,
        dde_str_refno
        )
        VALUES
        (
        l_tra_refno,
        null,
        'UNID',
        null,
        p1.ltra_dr,
        p1.ltra_vat_dr,
        null,
        null
        );
     END IF;
--
--
-- If the account is type SER and class is LIA and the 
-- transaction is a PAY type then we call the same code
-- as in allocate_payment_to_account. This will alleviate
-- the need for sites to do Invoice Balances, Payment Balances
-- and Credit Allocations dataloads
-- If the alt ref is populated with an invoice ref then
-- the payment will get allocated to that invoice, otherwise
-- it will use the rules.
--
-- Amended to use field 23 (ltra_clin_invoice_ref) instead of 
-- external reference (ltra_external_ref) and only to be done
-- when field 24 is Y(es) (ltra_allocate_to_clin). Also allowed
-- ADJ as well as PAY.  AJ 26Apr2016
--
--
   l_rac_class_code := NULL;
--
   IF ( p1.ltra_allocate_to_clin ='Y'
    AND p1.ltra_trt_code IN ('PAY','ADJ')
           )
     THEN
--
      OPEN c_class_code (l_rac_accno);
       FETCH c_class_code INTO l_rac_class_code;
      CLOSE c_class_code;
--
        IF l_rac_class_code = 'LIA'
         THEN
           allocate_payment_to_account( l_tra_refno
                                      , l_rac_accno
                                      , cb
                                      , cs
                                      , p1.ltra_clin_invoice_ref
                                      , p1.ltra_cr);
--
        END IF; -- Class Code = LIA
--
   END IF;    -- trt_code = PAY ADJ
--
--
-- keep a count of the rows processed and commit after every 5000
--
   i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END IF;
--
   s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
 -- May need to update the ltra_refno so
 --
 IF l_tra_upd = 'N' 
 THEN
   set_record_status_flag(l_id,'C');
 ELSE
   update dl_hra_transactions
   SET ltra_dl_load_status = 'C'
   ,   ltra_refno = l_tra_refno
   WHERE rowid = l_id;
   --
 END IF;
--
 EXCEPTION
 WHEN OTHERS THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
 END;
END LOOP;
l_min_seqno := l_curr_seqno +1;
l_curr_seqno := l_curr_seqno + 30000;
END LOOP;
--
-- Section to analyse the table(s) populated by this dataload
--
-- If the number of records in this batch is greater than the
-- number of records which were in the table to start off with
-- then do the analyse. Otherwise don't bother.
--
   IF i > l_tra_count
    THEN
     l_an_tab:=s_dl_hem_utils.dl_comp_stats('TRANSACTIONS');
     l_an_tab:=s_dl_hem_utils.dl_comp_stats('DEBIT_DETAILS');
   END IF;
--
fsc_utils.proc_end;
commit;
--
EXCEPTION
WHEN OTHERS THEN
  set_record_status_flag(l_id,'O');
  s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
--
END dataload_create;
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c_max(p_batch_id VARCHAR2) is
SELECT MIN(ltra_dl_seqno) min_seqno
,      MAX(ltra_dl_seqno+1) max_seqno
FROM   dl_hra_transactions
WHERE  ltra_dlb_batch_id    = p_batch_id
AND    ltra_dl_load_status   in ('L','F','O');
--
CURSOR c1(p_batch_id VARCHAR2,p_min_seqno INTEGER, p_curr_seqno INTEGER) IS
SELECT
rowid rec_rowid
,ltra_dlb_batch_id
,ltra_dl_seqno,
ltra_refno,
ltra_date,
ltra_pay_ref,
ltra_effective_date,
ltra_statement_ind,
ltra_trt_code,
ltra_trs_code,
ltra_pmy_code,
ltra_balance_ind,
decode(ltra_dr,0,decode(ltra_cr,null,0,0,0,null),ltra_dr) ltra_dr,
decode(ltra_cr,0,null,ltra_cr) ltra_cr,
decode(ltra_vat_dr,0,null,ltra_vat_dr) ltra_vat_dr,
decode(ltra_vat_cr,0,null,ltra_vat_cr) ltra_vat_cr,
ltra_payment_date,
ltra_hde_claim_no,
ltra_external_ref,
ltra_text,
ltra_balance_year,
ltra_balance_period,
ltra_summarise_ind,
ltra_debit_effective_date,
ltra_susp_pay_ref,
ltra_ext_description,
ltra_clin_invoice_ref,
ltra_allocate_to_clin
FROM dl_hra_transactions
WHERE ltra_dlb_batch_id      = p_batch_id
AND   ltra_dl_seqno          BETWEEN   p_min_seqno AND p_curr_seqno
AND   ltra_dl_load_status       in ('L','F','O');
--
CURSOR c_aye_start(p_aun_code VARCHAR2) IS
SELECT min(aye_start_date) 
FROM admin_years
WHERE aye_aun_code = p_aun_code;
--
CURSOR c_check_ate_code(p_pay_ref VARCHAR2) IS
SELECT 'X' 
FROM   revenue_accounts
WHERE  rac_hrv_ate_code  = 'SUS'
AND    rac_pay_ref       = p_pay_ref;
--
CURSOR c_chk_deb_date(p_aun_code          VARCHAR2,
                      p_admin_period_type VARCHAR2,
                      p_effective_date    DATE) IS
SELECT 'X'
  FROM admin_periods
 WHERE ape_aye_aun_code      = p_aun_code
   AND ape_admin_period_type = p_admin_period_type
   AND p_effective_date BETWEEN ape_start_date
                            AND ape_end_date;
--
CURSOR c_aye (p_aun_code VARCHAR2
             ,p_eff_date DATE) IS
SELECT aye_year
FROM   admin_years
WHERE  aye_aun_code    = p_aun_code
  AND  aye_start_date <= p_eff_date
  AND  aye_end_date   >= p_eff_date
;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_TRANSACTIONS';
cs       PLS_INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                PLS_INTEGER :=0;
l_min_seqno      PLS_INTEGER := 0;
l_max_seqno      PLS_INTEGER := 0;
l_curr_seqno     PLS_INTEGER := 0;
--
-- Other Variables
r_rac              revenue_accounts%ROWTYPE;
l_trt_subtype_required transaction_types.trt_subtype_required_ind%TYPE;
l_rac_accno        revenue_accounts.rac_accno%TYPE;
r_pmy              payment_method_types%ROWTYPE;
r_aye              admin_years%ROWTYPE;
r_ape              admin_periods%ROWTYPE;
r_ape_eff          admin_periods%ROWTYPE;
l_min_aye          date;
l_tra_balance_ind  VARCHAR2(1);
l_tra_deb_eff_date VARCHAR2(1);
l_aye_year         NUMBER(4);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_transactions.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_transactions.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
OPEN c_max(p_batch_id);
FETCH c_max INTO l_min_seqno, l_max_seqno;
CLOSE c_max;
--
l_curr_seqno := l_min_seqno + 30000;
--
WHILE l_min_seqno < l_max_seqno LOOP
--
  FOR p1 IN c1(p_batch_id,l_min_seqno,l_curr_seqno) LOOP
--
  BEGIN
  l_id := p1.rec_rowid;
  cs   := p1.ltra_dl_seqno;
--
  l_errors := 'V';
  l_error_ind := 'N';
  --
IF p1.ltra_susp_pay_ref IS NOT NULL
THEN
  l_tra_balance_ind := 'N';
ELSE
  l_tra_balance_ind := p1.ltra_balance_ind;
END IF;
  -- Check the payment reference exists on REVENUE_ACCOUNTS
  --
  l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref( p1.ltra_pay_ref );
  IF l_rac_accno IS NOT NULL
  THEN
    r_rac := s_revenue_accounts2.rec_rac ( l_rac_accno );
  ELSE
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
  END IF;
  --
  -- Check all the reference values
  --
  -- Transaction type (if valid get the subtype required indicator)
  --
  l_trt_subtype_required:= null;
  --
  IF (s_transaction_types.get_trt_name(p1.ltra_trt_code) ) IS NOT NULL
  THEN
    l_trt_subtype_required := s_transaction_types.get_trt_subtype_required_ind(p1.ltra_trt_code, 'Y');
  ELSE
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',130);
  END IF;
  --
  -- Transaction subtype
  --
  IF (l_trt_subtype_required = 'Y' and p1.ltra_trs_code IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',131);
  END IF;
  IF p1.ltra_trs_code IS NOT NULL
    THEN
      IF NOT (s_transaction_subtypes.validate_code
                           (p1.ltra_trt_code, p1.ltra_trs_code, 'Y'))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',132);
      END IF;
  END IF;
  --
  -- Payment method
  --
  IF (p1.ltra_pmy_code IS NOT NULL)
  THEN
    r_pmy := s_payment_method_types.get_record(p1.ltra_pmy_code);
    IF ( r_pmy.pmy_code ) IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',133);
    END IF;
  END IF;
  --
  -- If a revenue account start date has been obtained, check that
  -- none of the dates precede it
  --
-- This section of code was edited out for NCC
-- Re-activated at 515 MH 09/05/02
-- and then amended by PD on 30/07/02 to only check non PAY or ADJ transactions
--
-- Commented this out again as it's valid within the database. It's
-- possible to move the account start date and therefore have DRS and DRA
-- transactions with an effective date earlier than their rac_start_date
--
  --IF (p1.ltra_trt_code NOT IN ('PAY','ADJ'))
  --THEN
    --
    -- Transaction date
    --
    --IF (p1.ltra_date < r_rac.rac_start_date)
    --THEN
    --  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',134);
    --END IF;
      --
      -- Effective date
      --
      --IF (p1.ltra_effective_date < r_rac.rac_start_date)
      --THEN
      --  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',135);
      --END IF;
      --
      -- Payment date
      --
      --IF (p1.ltra_payment_date < r_rac.rac_start_date)
      --THEN
      --  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',136);
      --END IF;
    --END IF;
    --
      l_min_aye := null;
      OPEN c_aye_start(r_rac.rac_aun_code);
      FETCH c_aye_start into l_min_aye;
      CLOSE c_aye_start;
    --
    -- If an admin unit code has been obtained, check the transaction
    -- effective date is a valid effective debit date on ADMIN_PERIODS
    -- for that admin unit. This only applies to DRS and DRA
    -- transactions.
    -- PH 28/07/06 Amended this check as for accounts with a class_code
    -- of LIA as long as the effective date is within an Admin Period 
    -- of type DEB its' okay. It doesn't have to be a debit date.
    --
    IF (p1.ltra_trt_code IN ('DRS','DRA')
    AND r_rac.rac_aun_code IS NOT NULL
    AND p1.ltra_effective_date > l_min_aye
    AND p1.ltra_debit_effective_date IS NULL)
    THEN
     IF (r_rac.rac_class_code != 'LIA') THEN
      IF (NOT s_admin_periods.f_exists_ape_effective(
          r_rac.rac_aun_code,'DEB', p1.ltra_effective_date))
      THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',137);
      END IF;
--
     ELSE
--
          l_tra_deb_eff_date := NULL;
--
           OPEN c_chk_deb_date(r_rac.rac_aun_code,'DEB', p1.ltra_effective_date)
;
          FETCH c_chk_deb_date INTO l_tra_deb_eff_date;
          CLOSE c_chk_deb_date;
--
          IF (l_tra_deb_eff_date IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',029);
          END IF;
--
       END IF;
--   
    END IF;
    --
    -- If an admin unit code has been obtained, check the transaction
    -- DEBIT effective date is a valid effective debit date on ADMIN_PERIODS
    -- for that admin unit. This only applies to DRS and DRA
    -- transactions.
    --
    IF    (p1.ltra_trt_code NOT IN ('ADJ','HBS','HBU','PAY')
           AND
           p1.ltra_susp_pay_ref IS NOT NULL)
    THEN
           l_exists := NULL;
           open c_check_ate_code(p1.ltra_pay_ref);
           fetch c_check_ate_code into l_exists;
           close c_check_ate_code;
      IF l_exists = 'X'
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',175);
      END IF;
    -- 
    ELSIF (p1.ltra_trt_code IN ('DRS','DRA')
    AND r_rac.rac_aun_code IS NOT NULL
    AND p1.ltra_debit_effective_date > l_min_aye
    AND p1.ltra_debit_effective_date IS NOT NULL)
    THEN
      IF (NOT s_admin_periods.f_exists_ape_effective(
          r_rac.rac_aun_code,'DEB', p1.ltra_debit_effective_date))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',973);
      END IF;
    END IF;
    --
    -- Check that either a debit amount or a credit amount has been
    -- supplied. Amended this check 05-FEB-2008 as you can have 
    -- debit details that sum up to zero. Therefore for DRS, DRA
    -- you could have tra_dr of zero.
    --
    IF p1.ltra_trt_code IN ('DRS','DRA')
    THEN
      IF p1.ltra_dr IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',95);
      END IF;
    ELSIF (p1.ltra_dr IS NULL AND p1.ltra_cr IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',138);
    END IF;

    --
    -- Check that debit and credit amounts have not both been supplied
    --
    IF (p1.ltra_dr IS NOT NULL AND p1.ltra_cr IS NOT NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',139);
    END IF;
    --
    -- Check that VAT debit and credit amounts have not both been
    -- supplied
    --
    IF (p1.ltra_vat_dr IS NOT NULL AND p1.ltra_vat_cr IS NOT NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',140);
    END IF;
    --
    -- If a debit amount has been supplied, check the VAT credit amount
    -- is null
    --
    IF (p1.ltra_dr IS NOT NULL AND p1.ltra_vat_cr IS NOT NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',141);
    END IF;
    --
    -- If a credit amount has been supplied, check the VAT debit amount
    -- is null
    --
    IF (p1.ltra_cr IS NOT NULL AND p1.ltra_vat_dr IS NOT NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',142);
    END IF;
    --
    -- Check the balance indicator is 'N' or null
    --
    IF ( NVL(l_tra_balance_ind,'N') != 'N')
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',143);
    END IF;
    --
    -- Check that if a balance year has been supplied, a balance period
    -- has also been supplied
    --
    IF (p1.ltra_balance_year IS NOT NULL AND
        p1.ltra_balance_period IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',144);
    END IF;
    --
    -- Check that if a balance period has been supplied, a balance year
    -- has also been supplied
    --
    IF (p1.ltra_balance_period IS NOT NULL AND
        p1.ltra_balance_year IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',145);
    END IF;
    --
    -- If the admin unit code has been obtained then if balance year and
    -- period information has been supplied check they are valid,
    -- otherwise check that a balance period exists for the given
    -- effective date
    --
    -- ADDED at NCC 29 MAR 2001 - Check to see if admin year exists
    -- if not then do nothing
    -- MOVED this cursor further upfield as needed in another validation check
    -- 
    --l_min_aye := null;
    --OPEN c_aye_start(r_rac.rac_aun_code);
    --FETCH c_aye_start into l_min_aye;
    --CLOSE c_aye_start;
    --
    IF (r_rac.rac_aun_code IS NOT NULL
        AND l_min_aye < p1.ltra_effective_date
        AND p1.ltra_susp_pay_ref IS NULL)
    THEN
      IF p1.ltra_balance_year IS NOT NULL
      THEN
        l_aye_year := NULL;
        OPEN c_aye (r_rac.rac_aun_code
                   ,p1.ltra_effective_date 
                   );
        FETCH c_aye INTO l_aye_year;
        CLOSE c_aye;

--       r_aye := s_admin_years.rec_aye(
--                 r_rac.rac_aun_code,
--                 p1.ltra_effective_date,
--                 'Y');
        IF ( l_aye_year ) IS NOT NULL
        THEN
          r_ape := s_admin_periods.get_record(
                     r_rac.rac_aun_code,
                     p1.ltra_balance_year,
                     p1.ltra_balance_period,
                     'BAL');
          IF (r_ape.ape_aye_year) IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',147);
          END IF;
        ELSE
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',146);
        END IF;
      ELSE
        r_ape_eff := s_admin_periods.get_record_effective(
                       r_rac.rac_aun_code,
                       p1.ltra_effective_date,
                       'BAL');
        IF ( r_ape_eff.ape_aye_year) IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',171);
        END IF;
      END IF;
    END IF;
--
-- Additional validate on effective date, cannot be more
-- than 20 years old. This will prevent issues with summarisation.
-- Brought in as a result of year being loaded as 06 rather than
-- 2006 and this was inserted as 0006. Transaction then got
-- balanced but not summarised.
--
   IF p1.ltra_effective_date < add_months(trunc(sysdate), -240)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',223);
   END IF;
--
-- Validate Customer Liability Invoice (ltra_clin_invoice_ref) and
-- Allocate Indicator (ltra_allocate_to_clin) if supplied.
--
   IF ( p1.ltra_clin_invoice_ref IS NOT NULL
      AND p1.ltra_trt_code NOT IN ('DRS','ADC','PAY','ADJ')
          )
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',891);
   END IF;
--
   IF ( p1.ltra_allocate_to_clin IS NOT NULL
      AND p1.ltra_allocate_to_clin NOT IN ('Y','N')
          )
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',892);
   END IF;
--
   IF ( p1.ltra_clin_invoice_ref IS NOT NULL
      AND p1.ltra_allocate_to_clin IS NULL
          )
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',892);
   END IF;
--
-- Check Customer Liability Invoices Reference exists
--
   IF (p1.ltra_clin_invoice_ref IS NOT NULL)
    THEN
     IF (s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.ltra_clin_invoice_ref) IS NULL)
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',893);
     END IF;
   END IF;
--









--
-- Now UPDATE the record count AND error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
--
-- keep a count of the rows processed and commit after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END IF;
--
 s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
  set_record_status_flag(l_id,l_errors);
--
  EXCEPTION
     WHEN OTHERS THEN
     ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
     s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
     set_record_status_flag(l_id,'O');
  END;
--
commit;
--
  END LOOP;
l_min_seqno := l_curr_seqno +1;
l_curr_seqno := l_curr_seqno + 30000;
END LOOP;
--
COMMIT;
--
fsc_utils.proc_END;
--
  EXCEPTION
     WHEN OTHERS THEN
     s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT
 a1.rowid rec_rowid
,a1.ltra_dlb_batch_id
,a1.ltra_dl_seqno
,a1.ltra_dl_load_status
,a1.ltra_refno
,a1.ltra_trt_code
FROM transactions p1,  dl_hra_transactions a1
WHERE tra_refno             = ltra_refno
  AND ltra_dlb_batch_id     = p_batch_id
  AND ltra_dl_load_status   = 'C';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_TRANSACTIONS';
cs       PLS_INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
i        PLS_INTEGER := 0;
l_an_tab VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_transactions.dataload_delete');
fsc_utils.debug_message( 's_dl_hra_transactions.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.ltra_dl_seqno;
i := i +1;
l_id := p1.rec_rowid;
--
IF p1.ltra_trt_code IN ('DRS','DRA')
THEN
 DELETE from DEBIT_DETAILS
 WHERE  dde_tra_refno = p1.ltra_refno;
END IF;
--
DELETE FROM transactions
 WHERE tra_refno = p1.ltra_refno;
--
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
  EXCEPTION
  WHEN OTHERS THEN
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  -- set_record_status_flag(l_id,'C'); not needed as already C anyway 
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
END LOOP;
--
fsc_utils.proc_end;
--
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.SET_record_status_flag(ct,cb,cs,'O');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hra_transactions;
/

show errors

