CREATE OR REPLACE PACKAGE BODY s_dl_hsc_invoice_balances
AS
--
-- ******************************************************************************
--
  --
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION     WHO  WHEN       WHY
  --      1.0     MTR  23/11/01   Dataload
  --
  --      2.0     VRS  01/03/06   Correct the Create process and add more validation
  --				  checks. Also the DELETE process needed to be re-written as
  --                              the initial release didn't do anything. General tidy up
  --				  of code, removed the use of created by/date and modified by/date
  --                              as these are populated by triggers on the invoice_balances table.
  --
  --      3.0     VRS  19/06/06   Amended all ISG-DLO created error codes to HD1
--
--      4.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      4.1 5.13.0   PH   04-MAR-2008 Moved exception handler in delete to 
--                                    within loop.
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
  UPDATE dl_hsc_invoice_balances
  SET linba_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hsc_invoice_balances');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
  --
  --  declare package variables AND constants
--
-- **********************************************************************************************
--
PROCEDURE dataload_create(p_batch_id	IN VARCHAR2,
                          p_date	IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) IS
SELECT rowid rec_rowid,
       linba_dlb_batch_id,
       linba_dl_seqno,
       linba_dl_load_status,
       linba_clin_invoice_ref,
       linba_seqno,
       linba_balance_date,
       linba_total_balance,
       linba_undisputed_balance,
       linba_interest_charge_to_date
  FROM dl_hsc_invoice_balances
 WHERE linba_dlb_batch_id   = p_batch_id
   AND linba_dl_load_status = 'V';
--
-- **************************************************************
--
-- Constants for process_summary
--
cb		VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'CREATE';
ct       	VARCHAR2(30) := 'DL_HSC_INVOICE_BALANCES';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     ROWID;
l_an_tab 	VARCHAR2(1);
--
-- Other variables
--
i            INTEGER := 0;
l_clin_refno NUMBER(10);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_invoice_balances.dataload_create');
    fsc_utils.debug_message( 's_dl_hsc_invoice_balances.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1(p_batch_id) LOOP
--
      BEGIN
--
-- VRS 01-MAR-2006 tidy this up
--
          cs := p1.linba_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_clin_refno := NULL;
--  
          l_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.linba_clin_invoice_ref);
--
-- Create invoice_balance record
--
          INSERT INTO invoice_balances (inba_clin_refno,
           				inba_seqno,
           				inba_balance_date,
           				inba_total_balance,
           				inba_undisputed_balance,
           				inba_interest_charge_to_date
                                       )
--
      				 VALUES(l_clin_refno,
           				p1.linba_seqno,
           				p1.linba_balance_date,
           				p1.linba_total_balance,
           				p1.linba_undisputed_balance,
               				p1.linba_interest_charge_to_date
				       );
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
--
    END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INVOICE_BALANCE');
--
    fsc_utils.proc_end;
--
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
-- **********************************************************************************************
--
--
-- As defined in FUNCTION H400.60.10.40.10.20
--
PROCEDURE dataload_validate(p_batch_id	IN VARCHAR2,
                            p_date	IN DATE)
AS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       linba_dlb_batch_id,
       linba_dl_seqno,
       linba_dl_load_status,
       linba_clin_invoice_ref,
       linba_seqno,
       linba_balance_date,
       linba_total_balance,
       linba_undisputed_balance,
       linba_interest_charge_to_date
  FROM dl_hsc_invoice_balances
 WHERE linba_dlb_batch_id    = p_batch_id
   AND linba_dl_load_status IN ('L','F','O');
--
-- **************************************************************
--
CURSOR chk_for_dups(p_clin_invoice_ref	VARCHAR2,
                    p_seqno		NUMBER,
		    p_batch_id		VARCHAR2) IS
--
SELECT COUNT(*)
  FROM dl_hsc_invoice_balances
 WHERE linba_clin_invoice_ref = p_clin_invoice_ref
   AND linba_seqno            = p_seqno
   AND linba_dlb_batch_id     = p_batch_id;
--
-- **************************************************************
--
CURSOR chk_inv_bal_exists(p_clin_refno 	NUMBER,
                          p_seqno	NUMBER) IS
--
SELECT 'X'
  FROM invoice_balances
 WHERE inba_clin_refno = p_clin_refno
   AND inba_seqno      = p_seqno;
--
-- **************************************************************
--
-- Constants for process_summary
--
cb	VARCHAR2(30);
cd	DATE;
cp      VARCHAR2(30) := 'VALIDATE';
ct      VARCHAR2(30) := 'DL_HSC_INVOICE_BALANCES';
cs      INTEGER;
ce      VARCHAR2(200);
l_id     ROWID;
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_link1          VARCHAR2(1);
l_link2          VARCHAR2(1);
l_parent_type    VARCHAR2(1);
l_grandchild     VARCHAR2(1);
--
-- Other variables
--
l_dummy             	VARCHAR2(10);
l_clin_refno		NUMBER;
l_inv_bal_exists	VARCHAR2(1);
l_inv_bal_count 	INTEGER;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_invoice_balances.dataload_validate');
    fsc_utils.debug_message('s_dl_hsc_invoice_balances.dataload_validate',3);
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
          cs := p1.linba_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- **************************************************************
--
-- Check All Mandatory values have been supplies
--
-- LINBA_CLIN_INVOICE_REF
--
          IF (p1.linba_clin_invoice_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',125);
          END IF;
--
--
-- LINBA_SEQNO
--
          IF (p1.linba_seqno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',126);
          END IF;
--
--
-- LINBA_BALANCE_DATE
--
          IF (p1.linba_balance_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',127);
          END IF;
--
--
-- LINBA_TOTAL_BALANCE
--
          IF (p1.linba_total_balance IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',128);
          END IF;
--
--
-- LINBA_UNDISPUTED_BALANCE
--
          IF (p1.linba_undisputed_balance IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',129);
          END IF;
--
-- **************************************************************
--
-- Check Customer Liability Invoices Reference exist's
--
--
          IF (p1.linba_clin_invoice_ref IS NOT NULL) THEN
--
           IF (s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.linba_clin_invoice_ref) IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',102);
           END IF;
--
          END IF;
--
-- **************************************************************
--
-- Check to see if an entry already exists in invoice balance
--
--
          IF (    p1.linba_clin_invoice_ref IS NOT NULL
              AND p1.linba_seqno            IS NOT NULL) THEN
--
           l_clin_refno     := NULL;
           l_inv_bal_exists := NULL;
--
           l_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.linba_clin_invoice_ref);
--
            OPEN chk_inv_bal_exists(l_clin_refno,p1.linba_seqno);
           FETCH chk_inv_bal_exists INTO l_inv_bal_exists;
           CLOSE chk_inv_bal_exists;
--
           IF (l_inv_bal_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',130);
           END IF;
--
          END IF;
--
-- **************************************************************
--
-- Check to see if there are any duplicate records in the dataload file
--
--
          IF (    p1.linba_clin_invoice_ref IS NOT NULL
              AND p1.linba_seqno            IS NOT NULL) THEN
--
           l_inv_bal_count := NULL;
--
            OPEN chk_for_dups(p1.linba_clin_invoice_ref,p1.linba_seqno,p_batch_id);
           FETCH chk_for_dups INTO l_inv_bal_count;
           CLOSE chk_for_dups;
--
           IF (l_inv_bal_count > 1 ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',131);
           END IF;
--
          END IF;
--
-- **************************************************************
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
-- **********************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       linba_dlb_batch_id,
       linba_dl_seqno,
       linba_dl_load_status,
       linba_clin_invoice_ref,
       linba_seqno,
       linba_balance_date,
       linba_total_balance,
       linba_undisputed_balance,
       linba_interest_charge_to_date
  FROM dl_hsc_invoice_balances
 WHERE linba_dlb_batch_id   = p_batch_id
   AND linba_dl_load_status = 'C';
--
-- **************************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HSC_INVOICE_BALANCES';
cs       	INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i        	INTEGER := 0;
l_clin_refno	NUMBER(10);
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hsc_invoice_balances.dataload_delete');
    fsc_utils.debug_message('s_dl_hsc_invoice_balances.dataload_delete',3);
--
    cp := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.linba_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
          l_clin_refno     := NULL;
--
          l_clin_refno := s_customer_liability_invoices.get_clin_refno_for_invoice_ref(p1.linba_clin_invoice_ref);
--
          DELETE
            FROM invoice_balances
           WHERE inba_clin_refno = l_clin_refno
             AND inba_seqno      = p1.linba_seqno;
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
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          EXCEPTION
               WHEN OTHERS THEN
               ROLLBACK TO SP1;
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
               set_record_status_flag(l_id,'C');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab := s_dl_hem_utils.dl_comp_stats('INVOICE_BALANCES');
--
    COMMIT;
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
         RAISE;
--
END dataload_delete;
--
--
END s_dl_hsc_invoice_balances;
/
