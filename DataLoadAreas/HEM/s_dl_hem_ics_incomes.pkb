--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_ics_incomes
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   21-MAR-2013  Initial Creation.
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
    UPDATE dl_hem_ics_incomes
       SET linc_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_ics_incomes');
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
SELECT rowid rec_rowid,
       LINC_DLB_BATCH_ID,
       LINC_DL_SEQNO,
       LINC_DL_LOAD_STATUS,
       LINC_INDR_REFERENCE,
       LINC_TYPE,
       LINC_HRV_IITY_CODE,
       LINC_AMOUNT,
       LINC_INCOME_DATE,
       NVL(LINC_CREATED_DATE,SYSDATE)  LINC_CREATED_DATE,
       NVL(LINC_CREATED_BY,'DATALOAD') LINC_CREATED_BY,
       LINC_INF_CODE,
       LINC_REFNO
  FROM dl_hem_ics_incomes
 WHERE linc_dlb_batch_id   = p_batch_id
   AND linc_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_ICS_INCOMES';
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
i                    INTEGER := 0;
l_exists             VARCHAR2(1);
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_incomes.dataload_create');
    fsc_utils.debug_message('s_dl_hem_ics_incomes.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.linc_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
-- Now insert into ics_request_statuses
--
          INSERT /* +APPEND */ INTO  ics_incomes(INC_REFNO,
                                                 INC_TYPE,
                                                 INC_INDR_REFNO,
                                                 INC_HRV_IITY_CODE,
                                                 INC_AMOUNT,
                                                 INC_INCOME_DATE,
                                                 INC_INF_CODE
                                                )
--
                                         VALUES (p1.LINC_REFNO,
                                                 p1.LINC_TYPE,
                                                 p1.LINC_INDR_REFERENCE,
                                                 p1.LINC_HRV_IITY_CODE,
                                                 p1.LINC_AMOUNT,
                                                 p1.LINC_INCOME_DATE,
                                                 p1.LINC_INF_CODE
                                                );
--
-- Maintain CREATED BY/DATE
--
          UPDATE ics_incomes
             SET inc_created_by   = p1.LINC_CREATED_BY,
                 inc_created_date = p1.LINC_CREATED_DATE
           WHERE inc_refno = p1.linc_refno;
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,50000) = 0 THEN 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_INCOMES');
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
PROCEDURE dataload_validate(p_batch_id       IN VARCHAR2,
                            p_date           IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LINC_DLB_BATCH_ID,
       LINC_DL_SEQNO,
       LINC_DL_LOAD_STATUS,
       LINC_INDR_REFERENCE,
       LINC_TYPE,
       LINC_HRV_IITY_CODE,
       LINC_AMOUNT,
       LINC_INCOME_DATE,
       NVL(LINC_CREATED_DATE,SYSDATE)  LINC_CREATED_DATE,
       NVL(LINC_CREATED_BY,'DATALOAD') LINC_CREATED_BY,
       LINC_INF_CODE,
       LINC_REFNO
  FROM dl_hem_ics_incomes
 WHERE linc_dlb_batch_id   = p_batch_id
   AND linc_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_chk_indr_ref_exists(p_indr_reference   NUMBER) 
IS
SELECT 'X'
  FROM income_detail_requests
 WHERE indr_refno = p_indr_reference;
--
-- ***********************************************************************
--
CURSOR c_chk_iity_code_exists(p_iity_code  VARCHAR2) 
IS
SELECT 'X'
  FROM first_ref_values
 WHERE frv_code        = p_iity_code
   AND frv_frd_domain  = 'ICSINCOMETYPE'
   AND frv_current_ind = 'Y';
--
-- ***********************************************************************
--
CURSOR c_chk_inf_code_exists(p_inf_code   VARCHAR2) 
IS
SELECT 'X'
  FROM income_frequencies
 WHERE inf_code = p_inf_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'VALIDATE';
ct               VARCHAR2(30) := 'DL_HEM_ICS_INCOMES';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_iity_exists      VARCHAR2(1);
l_indr_ref_exists  VARCHAR2(1);
l_inf_exists       VARCHAR2(1);
l_errors           VARCHAR2(10);
l_error_ind        VARCHAR2(10);
i                  INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_incomes.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_ics_incomes.dataload_validate',3);
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
          cs   := p1.linc_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';

--
-- ***********************************************************************
--
-- Income Detail Request Reference has been supplied and is valid
--
          IF (p1.linc_indr_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',327);
          ELSE
--
             l_indr_ref_exists := NULL;
--
              OPEN c_chk_indr_ref_exists(p1.linc_indr_reference);
             FETCH c_chk_indr_ref_exists INTO l_indr_ref_exists;
             CLOSE c_chk_indr_ref_exists;
--
             IF (l_indr_ref_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',333);
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Income Type Indicator must be supplied and valid
--
          IF (p1.linc_type IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',555);
--
          ELSIF (p1.linc_type NOT IN ('TINC','DINC')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',334);
--
          END IF;
--
-- ***********************************************************************
--
-- The Income Type Code must be supplied and exist in first ref values 
-- for domain ICSINCOMETYPE
--
          IF (p1.linc_hrv_iity_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',556);
--
          ELSE
--
             l_iity_exists := NULL;
--
              OPEN c_chk_iity_code_exists(p1.linc_hrv_iity_code);
             FETCH c_chk_iity_code_exists INTO l_iity_exists;
--
             IF (c_chk_iity_code_exists%NOTFOUND) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',335);
             END IF;
--
             CLOSE c_chk_iity_code_exists;
--
          END IF;
--
-- ***********************************************************************
--
-- Income Amount must be supplied
--
          IF (p1.linc_amount IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',557);
          END IF;
--
-- ***********************************************************************
--
-- Income Date must be supplied
--
          IF (p1.linc_income_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',558);
          END IF;

--
-- ***********************************************************************
--
-- Check income frequency is only supplied when the Type Indicator is 'DINC'
--
          IF (    p1.linc_type     != 'DINC'
              AND p1.linc_inf_code IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',336);
--
          END IF;
--
-- ***********************************************************************
--
-- Check income frequency is supplied when the Type Indicator is 'DINC'
--
          IF (    p1.linc_type      = 'DINC'
              AND p1.linc_inf_code IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',000);
--
          END IF;
--
-- ***********************************************************************
--
-- Check income frequency is valid if supplied when the Type Indicator is 'DINC'
--
          IF (    p1.linc_type      = 'DINC'
              AND p1.linc_inf_code IS NOT NULL) THEN
--
            OPEN c_chk_inf_code_exists(p1.linc_inf_code);
           FETCH c_chk_inf_code_exists INTO l_inf_exists;
--
           IF (c_chk_inf_code_exists%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',337);
           END IF;
--
           CLOSE c_chk_inf_code_exists;
--
          END IF;
--
-- ***********************************************************************
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
                          p_date           IN date) 
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LINC_DLB_BATCH_ID,
       LINC_DL_SEQNO,
       LINC_DL_LOAD_STATUS,
       LINC_REFNO
  FROM dl_hem_ics_incomes
 WHERE linc_dlb_batch_id   = p_batch_id
   AND linc_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'DELETE';
ct               VARCHAR2(30) := 'DL_HEM_ICS_INCOMES';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
l_an_tab         VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_ics_incomes.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_ics_incomes.dataload_delete',3 );
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
          cs   := p1.linc_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from tables
--
          DELETE 
            FROM ics_incomes
           WHERE inc_refno = p1.linc_refno;
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
          IF mod(i,5000) = 0 THEN 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ICS_INCOMES');
--
    fsc_utils.proc_end;
--
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_delete;
--
END s_dl_hem_ics_incomes;
/