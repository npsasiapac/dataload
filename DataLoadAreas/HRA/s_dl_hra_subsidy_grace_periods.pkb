--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_subsidy_grace_periods
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    KH   18-FEB-2009  Initial Creation.
--
--  1.1     5.15.0    VS   05-MAY-2009  Disable trigger SGPE_BR_IU on
--                                      CREATE process to stop the supplied
--                                      created_by and created_date from 
--                                      being over written.
--  1.2     6.4.0     PH   30-JUN-2010  Added cursors to return suap_ref
--                                      Removed enable/disable of triggers now
--                                      run update after inserted.
--  1.3     6.5.0     PH   24-FEB-2012  Legacy Ref now held in subsidy
--                                      applications, removed call to dl table
--  1.4     6.8.0    MM    10-Jul-2013 Amended IF (p1.lsgpe_start_date <
--                                     p1.lsgpe_end_date) THEN to be IF 
--                                    (p1.lsgpe_start_date > p1.lsgpe_end_date) 
--                                    THEN as Start Date can not be greater than
--                                    End Date which is what it was validating
--
--  1.5     6.18     PL    30-JUL-2019 Added link to Account Rent Limits    
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_hra_subsidy_grace_periods
  SET    lsgpe_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_subsidy_grace_periods');
      RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid REC_ROWID,
       LSGPE_DLB_BATCH_ID,
       LSGPE_DL_SEQNO,
       LSGPE_DL_LOAD_STATUS,
       LSGPE_SUAP_LEGACY_REF,
       LSGPE_SEQ,
       LSGPE_HGPR_CODE,
       LSGPE_START_DATE,
       LSGPE_RENT_PAYABLE,
       NVL(LSGPE_CREATED_DATE, SYSDATE)  LSGPE_CREATED_DATE,
       NVL(LSGPE_CREATED_BY, 'DATALOAD') LSGPE_CREATED_BY,
       LSGPE_END_DATE,
       LSGPE_MODIFIED_DATE,
       LSGPE_MODIFIED_BY
  FROM dl_hra_subsidy_grace_periods
 WHERE lsgpe_dlb_batch_id    = p_batch_id
   AND lsgpe_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- get subsidy review Refno, held on the DL table
--
CURSOR get_suap_refno(p_suap_legagcy_ref  VARCHAR2)
IS
SELECT suap_reference
  FROM subsidy_applications
 WHERE suap_legacy_ref = p_suap_legagcy_ref;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_SUBSIDY_GRACE_PERIODS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                          INTEGER := 0;
l_exists                   VARCHAR2(1);
l_tcy_refno                NUMBER(10);
l_suap_reference           NUMBER(10);
l_surv_refno               NUMBER(10);     
l_par_refno                NUMBER(8);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_grace_periods.dataload_create');
    fsc_utils.debug_message('s_dl_hra_subsidy_grace_periods.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lsgpe_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
          l_suap_reference := NULL;
--
-- get the survey application ref
--
            OPEN get_suap_refno(p1.lsgpe_suap_legacy_ref);
           FETCH get_suap_refno into l_suap_reference;
           CLOSE get_suap_refno;
--
-- Insert int relevent table
--
          INSERT /* +APPEND */ INTO subsidy_grace_periods(SGPE_SUAP_REFERENCE,
                                           SGPE_SEQ,
                                           SGPE_HGPR_CODE,
                                           SGPE_START_DATE,
                                           SGPE_RENT_PAYABLE,
                                           SGPE_CREATED_DATE,
                                           SGPE_CREATED_BY,
                                           SGPE_END_DATE,
                                           SGPE_MODIFIED_DATE,
                                           SGPE_MODIFIED_BY
                                          )
--
                                    VALUES(l_suap_reference,
                                           p1.lsgpe_seq,
                                           p1.lsgpe_hgpr_code,
                                           p1.lsgpe_start_date,
                                           p1.lsgpe_rent_payable,
                                           p1.lsgpe_created_date,
                                           p1.lsgpe_created_by,
                                           p1.lsgpe_end_date,
                                           p1.lsgpe_modified_date,
                                           p1.lsgpe_modified_by
                                          );
--
-- Now update the record to set the correct created by and created date
-- to ovecome the trigger
--
         UPDATE   subsidy_grace_periods
            SET   sgpe_created_date   = p1.lsgpe_created_date
                , sgpe_created_by     = p1.lsgpe_created_by
          WHERE   sgpe_suap_reference = l_suap_reference
            AND   sgpe_seq            = p1.lsgpe_seq;

-- Fix up account rent limits

         UPDATE   account_rent_limits
            SET   arli_sgpe_suap_reference = l_suap_reference
            ,     arli_sgpe_seq            = p1.lsgpe_seq
            ,     arli_surv_refno          = NULL
          WHERE   arli_refno = (SELECT arli_refno
                                FROM subsidy_applications
                                JOIN revenue_accounts ON rac_tcy_refno = suap_tcy_refno
                                JOIN account_rent_limits ON arli_rac_accno = rac_accno
                                WHERE arli_rlty_code = '==GRACE=='
                                AND arli_start_date = p1.lsgpe_start_date
                                AND arli_end_date = p1.lsgpe_end_date
                                AND arli_amount = p1.lsgpe_rent_payable
                                AND suap_reference = l_suap_reference);
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
   i := i+1; 
--
   IF MOD(i,500000)=0 THEN 
     COMMIT; 
   END IF;
--
   s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
   set_record_status_flag(l_id,'C');
--
    EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK TO SP1;
            ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
            set_record_status_flag(l_id,'O');
            s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
    END;
--
  END LOOP;
--   
  COMMIT;
--
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_GRACE_PERIODS');
--
   fsc_utils.proc_END;
--
   EXCEPTION
        WHEN OTHERS THEN
           s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
           RAISE;
--
END dataload_create;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid REC_ROWID,
       LSGPE_DLB_BATCH_ID,
       LSGPE_DL_SEQNO,
       LSGPE_DL_LOAD_STATUS,
       LSGPE_SUAP_LEGACY_REF,
       LSGPE_SEQ,
       LSGPE_HGPR_CODE,
       LSGPE_START_DATE,
       LSGPE_RENT_PAYABLE,
       LSGPE_CREATED_DATE,
       LSGPE_CREATED_BY,
       LSGPE_END_DATE,
       LSGPE_MODIFIED_DATE,
       LSGPE_MODIFIED_BY
  FROM dl_hra_subsidy_grace_periods
 WHERE lsgpe_dlb_batch_id    = p_batch_id
   AND lsgpe_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR chk_suap_exists(p_suap_legacy_ref    VARCHAR2)
IS
SELECT suap_reference
      ,suap_start_date
  FROM subsidy_applications
 WHERE suap_legacy_ref = p_suap_legacy_ref;
--
-- ***********************************************************************
--
CURSOR chk_sgpe_exists(p_suap_reference  NUMBER,
                       p_sgpe_seq        NUMBER) 
IS
SELECT 'X'
  FROM subsidy_grace_periods
 WHERE sgpe_suap_reference = p_suap_reference
   AND sgpe_seq            = p_sgpe_seq;
--
-- ***********************************************************************
--
CURSOR chk_arli_exists(p_suap_reference  NUMBER,
                       p_start_date      DATE,
                       p_end_date        DATE,
                       p_amount          NUMBER
                       ) 
IS
SELECT 'X'
  FROM subsidy_applications 
  JOIN revenue_accounts ON rac_tcy_refno = suap_tcy_refno
  JOIN account_rent_limits ON arli_rac_accno = rac_accno 
 WHERE sgpe_suap_reference = p_suap_reference
   AND sgpe_seq            = p_sgpe_seq
   AND rac_hrv_ate_code = 'REN'
   AND arli_rlty_code = '==GRACE=='
   AND arli_start_date = p_start_date
   AND arli_end_date = p_end_date
   AND arli_amount = p_amount
   AND arli_status = 'A';
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_GRACE_PERIODS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         	VARCHAR2(1);
l_sgpe_exists           VARCHAR2(1);
l_arli_exists     VARCHAR2(1);

l_suap_reference        NUMBER(10);
l_suap_start_date       DATE;

l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_grace_periods.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_subsidy_grace_periods.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_DATE;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs   := p1.lsgpe_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check subsidy Application legacy reference exists 
-- 
          IF (p1.lsgpe_suap_legacy_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',012);
--
          ELSE
--
             l_suap_reference  := NULL;
             l_suap_start_date := NULL;
--
              OPEN chk_suap_exists (p1.lsgpe_suap_legacy_ref);
             FETCH chk_suap_exists INTO l_suap_reference, l_suap_start_date;
             CLOSE chk_suap_exists;
--
             IF (l_suap_reference IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',52);
             END IF;
--
          END IF;
--
-- *************************************************************************
--
-- Check that a record doesn't already exist in subsidy grace periods for the
-- subsidy application legacy ref and sequence 
--
          IF (    l_suap_reference IS NOT NULL
              AND p1.lsgpe_seq     IS NOT NULL) THEN
--
           l_sgpe_exists := NULL;
--
            OPEN chk_sgpe_exists ( l_suap_reference , p1.lsgpe_seq);
           FETCH chk_sgpe_exists INTO l_sgpe_exists;
           CLOSE chk_sgpe_exists;
--
           IF (l_sgpe_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',288);
           END IF;
--
          END IF;
--
-- *************************************************************************
--
-- Check Subsidy Grace Period Sequence is supplied
--
          IF (p1.lsgpe_seq IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',283);
          END IF;
--
-- *************************************************************************
--
-- Check Subsidy Grace Period Reason
--
          IF (p1.lsgpe_hgpr_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',284);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('SUBGPRSN',p1.lsgpe_hgpr_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',243);
          END IF;
--
-- *************************************************************************
--
-- Check Subsidy Grace Period Start Date is supplied and not earlier than the application
-- start date
--
          IF (p1.lsgpe_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',285);
--
          ELSIF (p1.lsgpe_start_date < l_suap_start_date) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',286);
          END IF;
--
-- *************************************************************************
--
-- If supplied the Subsidy Grace Period End Date cannot be earlier than the start date
--
          IF (    p1.lsgpe_start_date IS NOT NULL
              AND p1.lsgpe_end_date is NOT NULL) THEN
--
           IF (p1.lsgpe_start_date > p1.lsgpe_end_date) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',3);
           END IF;
--
          END IF;
--
--
-- ***********************************************************************
--
-- Make sure an account rent limit exists
--
           OPEN chk_arli_exists ( l_suap_reference , p1.lsgpe_start_date, p1.lsgpe_end_date, p1.LSGPE_RENT_PAYABLE);
           FETCH chk_arli_exists INTO l_arli_exists;
           CLOSE chk_arli_exists;
           
           IF NVL(l_arli_exists,'N') = 'N' THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',3); -- need a dl error number for this.
           END IF;
--
-- ***********************************************************************
--           
--
-- Now UPDATE the record status and process count
--
          IF (l_errors = 'F') THEN
           l_error_ind := 'Y';
          ELSE
             l_error_ind := 'N';
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
          set_record_status_flag(l_id,l_errors);
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
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
               set_record_status_flag(l_id,'O');
--
      END;
--
    END LOOP;
--
    fsc_utils.proc_END;
--
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
-- 
END dataload_validate;
--
--
-- ***********************************************************************
--
PROCEDURE dataload_delete(p_batch_id       IN VARCHAR2,
                          p_date           IN date) IS
--
CURSOR c1 is
SELECT rowid REC_ROWID,
       LSGPE_DLB_BATCH_ID,
       LSGPE_DL_SEQNO,
       LSGPE_DL_LOAD_STATUS,
       LSGPE_SUAP_LEGACY_REF,
       LSGPE_SEQ
FROM dl_hra_subsidy_grace_periods
 WHERE lsgpe_dlb_batch_id   = p_batch_id
   AND lsgpe_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- get subsidy application refno
--
CURSOR get_suap_refno(p_suap_legagcy_ref  VARCHAR2)
IS
SELECT suap_reference
  FROM subsidy_applications
 WHERE suap_legacy_ref = p_suap_legagcy_ref;
--
--
-- ***********************************************************************


-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_GRACE_PERIODS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
l_suap_reference NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_grace_periods.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_subsidy_grace_periods.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lsgpe_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
          l_suap_reference := NULL;
--
-- get the survey application ref
--
            OPEN get_suap_refno(p1.lsgpe_suap_legacy_ref);
           FETCH get_suap_refno into l_suap_reference;
           CLOSE get_suap_refno;
--
-- Delete from table
--
          DELETE 
            FROM subsidy_grace_periods
           WHERE sgpe_suap_reference = l_suap_reference
             AND sgpe_seq            = p1.lsgpe_seq;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          IF MOD(i,5000) = 0 THEN 
           COMMIT; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
               set_record_status_flag(l_id,'C');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_GRACE_PERIODS');
--
    fsc_utils.proc_end;
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
         RAISE;
--
END dataload_delete;
--
END s_dl_hra_subsidy_grace_periods;
/