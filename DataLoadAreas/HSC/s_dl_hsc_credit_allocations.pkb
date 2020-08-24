CREATE OR REPLACE PACKAGE BODY s_dl_hsc_credit_allocations
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION  DB Vers  WHO  WHEN       WHY
  --      1.0           MTR  23/11/01   Dataload
  --      2.0           PH   12/06/06   Added Delete Process
  --
  --      3.0           VRS  26/06/06   Correcting CREATE/VALIDATE Process
  --                                    to overcome trigger CRAL_AR_IU failures.
  --                                    General Code tidy up as well.
  --      3.1  5.10.0   PH   11/07/06   Added in DB Version to Change Control
  --                                    Amended code to derive transaction 
  --                                    rather than site supply tra_refno.
  --      3.2  5.10.0   PH   23/10/06   Amended DLO103 validation, only
  --                                    performed if clin_refno_credit_from
  --                                    is not supplied.
  --      4.0 5.13.0    PH  06-FEB-2008 Now includes its own 
--                                      set_record_status_flag procedure.
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hsc_credit_allocations
  SET lcral_dl_load_status  = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_credit_allocations');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
  --
  --  declare package variables AND constants
--
-- ***********************************************************************
--
PROCEDURE dataload_create (p_batch_id          IN VARCHAR2,
                           p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       lcral_dlb_batch_id,
       lcral_dl_seqno,
       lcral_dl_load_status,
       lcral_refno,  
       lcral_allocated_amount,
       lcral_pay_ref,
       lcral_tra_effective_date,
       lcral_tra_cr,
       lcral_tra_external_ref,
       lcral_ccme_refno_credit_from,
       lcral_clin_refno_credit_from,
       lcral_tra_refno_credit_to,
       lcral_clin_refno_credit_to
  FROM dl_hsc_credit_allocations
 WHERE lcral_dlb_batch_id    = p_batch_id
   AND lcral_dl_load_status  = 'V';
--
CURSOR c_get_tra_refno (p_rac_accno    number,
                        p_effect_date  date,
                        p_tra_cr       number,
                        p_external_ref varchar2) IS
SELECT  tra_refno
FROM    transactions
WHERE   tra_rac_accno                  = p_rac_accno
AND     tra_effective_date             = p_effect_date
AND     tra_cr                         = p_tra_cr
AND     tra_trt_code                   = 'PAY'
AND     nvl(tra_external_ref, '~XYZ~') = nvl(p_external_ref, '~XYZ~');
--
-- **********************************************************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HSC_CREDIT_ALLOCATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i            		INTEGER := 0;
l_clin_refno 		NUMBER(10);
l_clin_refno_to 	NUMBER(10);
l_clin_refno_from 	NUMBER(10);
l_ccme_refno_from	NUMBER(10);
l_rac_accno             NUMBER(8);
l_tra_refno             NUMBER(12);
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_credit_allocations.dataload_create');
    fsc_utils.debug_message( 's_dl_hsc_credit_allocations.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--      
          cs := p1.lcral_dl_seqno;
          l_id := p1.rec_rowid;
--
-- get clin_refno from/to
--
          l_clin_refno_from := NULL;
          l_clin_refno_to   := NULL;
--
          IF (p1.lcral_clin_refno_credit_from IS NOT NULL) THEN
           l_clin_refno_from := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lcral_clin_refno_credit_from);
          END IF;
--
          IF (p1.lcral_clin_refno_credit_to IS NOT NULL) THEN
           l_clin_refno_to := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lcral_clin_refno_credit_to);
          END IF;
--
--
-- get ccme_refno from
--
          l_ccme_refno_from := NULL;
--
          IF (p1.lcral_clin_refno_credit_from IS NOT NULL) THEN
           l_ccme_refno_from := s_customer_credit_memos.get_ccme_refno_for_memo_ref(p1.lcral_ccme_refno_credit_from);
          END IF;
--
-- Get the tra_refno
--
     l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lcral_pay_ref);
     l_tra_refno := null;
--
  OPEN c_get_tra_refno(l_rac_accno, p1.lcral_tra_effective_date,
                       p1.lcral_tra_cr, p1.lcral_tra_external_ref);
   FETCH c_get_tra_refno INTO l_tra_refno;
  CLOSE c_get_tra_refno;
--    
-- Create credit allocations record
--
          INSERT INTO credit_allocations(cral_refno,
                                         cral_allocated_amount,
                                         cral_tra_refno_credit_from,
                                         cral_ccme_refno_credit_from,
                                         cral_clin_refno_credit_from,
                                         cral_tra_refno_credit_to,
                                         cral_clin_refno_credit_to)
--
                                  VALUES(p1.lcral_refno,
                                         p1.lcral_allocated_amount,
                                         l_tra_refno,
                                         l_ccme_refno_from,
                                         l_clin_refno_from,
                                         p1.lcral_tra_refno_credit_to,             
                                         l_clin_refno_to);                                    
--
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'C');
--
          EXCEPTION
               WHEN OTHERS THEN
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
               set_record_status_flag(l_id,'O');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
    END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('credit_allocations');
--
--
   fsc_utils.proc_end;
   commit;
--
   EXCEPTION
        WHEN OTHERS THEN
        s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
        s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
END dataload_create;
--
-- ********************************************************************************************************
--
--
-- As defined in FUNCTION H400.60.10.40.10.20
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       lcral_dlb_batch_id,
       lcral_dl_seqno,
       lcral_dl_load_status,
       lcral_refno,  
       lcral_allocated_amount,
       lcral_pay_ref,
       lcral_tra_effective_date,
       lcral_tra_cr,
       lcral_tra_external_ref,
       lcral_ccme_refno_credit_from,
       lcral_clin_refno_credit_from,
       lcral_tra_refno_credit_to,
       lcral_clin_refno_credit_to
  FROM dl_hsc_credit_allocations
 WHERE lcral_dlb_batch_id    = p_batch_id
   AND lcral_dl_load_status in ('L','F','O');
--
CURSOR c_get_tra_refno (p_rac_accno    number,
                        p_effect_date  date,
                        p_tra_cr       number,
                        p_external_ref varchar2) IS
SELECT  tra_refno
FROM    transactions
WHERE   tra_rac_accno                  = p_rac_accno
AND     tra_effective_date             = p_effect_date
AND     tra_cr                         = p_tra_cr
AND     tra_trt_code                   = 'PAY'
AND     nvl(tra_external_ref, '~XYZ~') = nvl(p_external_ref, '~XYZ~');
--
-- **********************************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'VALIDATE';
ct       	VARCHAR2(30) := 'DL_HSC_CREDIT_ALLOCATIONS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     ROWID;
--
l_errors     	VARCHAR2(10);
l_error_ind     VARCHAR2(10);
i               INTEGER :=0;
--
-- Other variables
--
l_dummy         VARCHAR2(10);
l_rac_accno     NUMBER(8);
l_tra_refno     NUMBER(12);
l_exists        VARCHAR2(1);
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_credit_allocations.dataload_validate');
    fsc_utils.debug_message('s_dl_hsc_credit_allocations.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lcral_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- **********************************************************************
--
-- Validate the Allocated Amount (LCRAL_ALLOCATED_AMOUNT). Check to see that 
-- it has been supplied.
--
          IF (p1.lcral_allocated_amount IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',152);
          END IF;
--
-- **********************************************************************
--
-- Validate the Transaction Refno Credited From	(LCRAL_TRA_REFNO_CREDIT_FROM). 
-- Check to see that it has been supplied and valid.
-- Amended this code as sites will not know the tra_refno
-- 
--
   IF (p1.lcral_clin_refno_credit_from IS NULL) THEN
--
    l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lcral_pay_ref);
    --
      OPEN c_get_tra_refno(l_rac_accno, p1.lcral_tra_effective_date,
                           p1.lcral_tra_cr, p1.lcral_tra_external_ref);
       FETCH c_get_tra_refno INTO l_tra_refno;
        IF c_get_tra_refno%notfound
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',103);
        END IF;
      CLOSE c_get_tra_refno;
--
   END IF;
--
-- **********************************************************************
--
-- Validate the Customer Credit Memos from (lcral_ccme_refno_credit_from).
-- If supplied make sure that it is valid.
--
         IF (p1.lcral_ccme_refno_credit_from IS NOT NULL) THEN
--
          IF NOT s_customer_credit_memos.ccme_refno_exists(
                 s_customer_credit_memos.get_ccme_refno_for_memo_ref(p1.lcral_ccme_refno_credit_from)) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',104);
--
          END IF;
--
         END IF;
--
-- **********************************************************************
--         
-- Validate Customer Liabilty Invoice from / to (lcral_clin_refno_credit_from,
-- lcral_tra_refno_credit_to. If supplied make sure that it is valid.
--
         IF (p1.lcral_clin_refno_credit_from IS NOT NULL) THEN
--
          IF (s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lcral_clin_refno_credit_from) IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',105);
          END IF;
--
         END IF;
--
-- 
         IF (p1.lcral_clin_refno_credit_to IS NOT NULL) THEN
--
          IF (s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.lcral_clin_refno_credit_to) IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',107);
          END IF;
--
         END IF;        
--
-- **********************************************************************
--         
-- Validate the Transaction Refno Credited to (LCRAL_TRA_REFNO_CREDIT_TO). 
-- If supplied check to see that it is valid.
--     
         IF (p1.lcral_tra_refno_credit_to IS NOT NULL) THEN
--
          IF (NOT s_transactions2.tra_refno_exists( p1.lcral_tra_refno_credit_to)) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',106);
          END IF;
--
         END IF;
--
-- **********************************************************************
--         
-- Validate for trigger CRAL_AR_IU
-- 
--    
-- Check that the 'allocated to' object instance is not the same as the 
-- 'allocated from' instance.
-- Amended to use l_tra_refno
--
       --  IF (    p1.lcral_tra_refno_credit_from IS NOT NULL
         IF (    l_tra_refno                    IS NOT NULL
             AND p1.lcral_tra_refno_credit_to   IS NOT NULL) THEN

       --   IF (p1.lcral_tra_refno_credit_from = p1.lcral_tra_refno_credit_to) THEN
          IF ( l_tra_refno = p1.lcral_tra_refno_credit_to) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',154);
          END IF;
--
         ELSIF (    p1.lcral_clin_refno_credit_from IS NOT NULL
                AND p1.lcral_clin_refno_credit_to   IS NOT NULL) THEN
--
             IF (p1.lcral_clin_refno_credit_from  = p1.lcral_clin_refno_credit_to) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',155);
             END IF;
--
         END IF;
--
-- **********************
-- 
-- Check exclusivity and mandatory nature of the allocating credit from.
-- Allocating from can only be from one, either the transaction or
-- credit memo or Invoice.
-- Amended to use l_tra_refno
--
       --  IF (p1.lcral_tra_refno_credit_from IS NOT NULL) THEN
         IF (l_tra_refno IS NOT NULL) THEN
--
          IF (   p1.lcral_ccme_refno_credit_from IS NOT NULL
              OR p1.lcral_clin_refno_credit_from IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',156);
--
          END IF;
--
         ELSIF (p1.lcral_ccme_refno_credit_from IS NOT NULL) THEN
--
       --      IF (   p1.lcral_tra_refno_credit_from   IS NOT NULL
             IF (   l_tra_refno   IS NOT NULL
                 OR p1.lcral_clin_refno_credit_from  IS NOT NULL) THEN
-- 
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',156);
--
             END IF;
--
         ELSIF (p1.lcral_clin_refno_credit_from IS NOT NULL) THEN
--
             IF (   p1.lcral_ccme_refno_credit_from IS NOT NULL
       --          OR p1.lcral_tra_refno_credit_from  IS NOT NULL) THEN
                 OR l_tra_refno  IS NOT NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',156);
--
             END IF;
--
       --  ELSIF (    p1.lcral_tra_refno_credit_from  IS NULL
         ELSIF (    l_tra_refno  IS NULL
                AND p1.lcral_ccme_refno_credit_from IS NULL
                AND p1.lcral_clin_refno_credit_from IS NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',156);
--
         END IF;
--
-- **********************
-- 
-- Check exclusivity and mandatory nature of the allocating credit to.
-- The rule here is that you can only have 1 FROM going to 1 TO, not both.
--
         IF (    p1.lcral_tra_refno_credit_to  IS NOT NULL
             AND p1.lcral_clin_refno_credit_to IS NOT NULL)THEN              
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',157);
--
         ELSIF (    p1.lcral_tra_refno_credit_to  IS NULL
                AND p1.lcral_clin_refno_credit_to IS NULL) THEN
--
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',158);
--
         END IF;
--
-- **********************************************************************
--
-- Now UPDATE the record count and error code 
--
         IF l_errors = 'F' THEN
          l_error_ind := 'Y';
         ELSE
            l_error_ind := 'N';
         END IF;

--
-- keep a count of the rows processed and commit after every 1000
--
         i := i+1; 
--
         IF MOD(i,1000)=0 THEN 
          COMMIT; 
         END IF;
--
         s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
         set_record_status_flag(l_id,l_errors);
-- 
         EXCEPTION
              WHEN OTHERS THEN
              ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
              s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
              set_record_status_flag(l_id,'O');
--
      END;
--
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
-- **********************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       lcral_dlb_batch_id,
       lcral_dl_seqno,
       lcral_dl_load_status,
       lcral_refno,  
       lcral_allocated_amount,
       lcral_pay_ref,
       lcral_tra_effective_date,
       lcral_tra_cr,
       lcral_tra_external_ref,
       lcral_ccme_refno_credit_from,
       lcral_clin_refno_credit_from,
       lcral_tra_refno_credit_to,
       lcral_clin_refno_credit_to
  FROM dl_hsc_credit_allocations
 WHERE lcral_dlb_batch_id   = p_batch_id
   AND lcral_dl_load_status = 'C';
--
-- **********************************************************************
-- 
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
ce       VARCHAR2(200);
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HSC_CREDIT_ALLOCATIONS';
cs       INTEGER;
l_id     ROWID;
--
i        INTEGER := 0;

BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_credit_allocations.dataload_delete');
    fsc_utils.debug_message('s_dl_hsc_credit_allocations.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lcral_dl_seqno;
          l_id := p1.rec_rowid;
--
          DELETE 
            FROM credit_allocations
           WHERE cral_refno    = p1.lcral_refno;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
               set_record_status_flag(l_id,'C');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
    fsc_utils.proc_end;
    commit;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
         RAISE;
--
END dataload_delete;
--
--
END s_dl_hsc_credit_allocations;
/