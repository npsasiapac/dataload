--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_pyi100
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    IR   01-FEB-2009  Initial Creation.
--
--  2.0     5.15.0    VS   11-JUN-2009  Changed DL_HRA_RDS_TRANSMISSION_FILES
--                                      to DL_HRA_RDS_TRANS_FILES.
--
--  3.0     5.15.0    VS   23-FEB-2010  Defect Id 3611 Fix. Should be linked to 
--                                      Outgoing Transmission File.
--
--  4.0     5.15.0    VS   10-APR-2010  Defect Id 4162 Fix. Authorised Deduction 
--                                      Refno not derived correctly.
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
    UPDATE dl_hra_rds_pyi100
       SET lp100_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_rds_pyi100');
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
       lp100_dlb_batch_id,
       lp100_dl_seqno,
       lp100_dl_load_status,
       lp100_rdtf_alt_ref,
       lp100_rdsa_ha_reference,
       lp100_raud_start_date,
       lp100_hrv_dedt_code,
       lp100_hrv_rbeg_code,
       lp100_customer_birth_date,
       lp100_customer_surname,
       lp100_customer_postcode,
       lp100_trans_ref,
       lp100_crn,
       lp100_timestamp,
       lp100_request_action_code,
       lp100_instruction_amount,
       lp100_tp_id,
       lp100_start_date,
       lp100_end_date
  FROM dl_hra_rds_pyi100
 WHERE lp100_dlb_batch_id   = p_batch_id
   AND lp100_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Get RDS Authority Deduction Reference Number
--
CURSOR c_get_raud_refno(p_ha_reference  VARCHAR2,
                        p_dedt_code     VARCHAR2,
                        p_rbeg_code     VARCHAR2,
                        p_start_date    DATE) 
IS
SELECT raud_refno
  FROM rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno         = raud_rdsa_refno
   AND rdsa_ha_reference  = p_ha_reference
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_hrv_rbeg_code = p_rbeg_code
   AND raud_start_date    = p_start_date;
--
-- ***********************************************************************
--
-- Get RDS Transmission File Reference Number
--
CURSOR c_get_rdtf_ref(p_rdtf_ref    VARCHAR2) 
IS
SELECT rdtf_refno
  FROM rds_transmission_files, 
       dl_hra_rds_trans_files
 WHERE lrdtf_refno   = rdtf_refno
   AND lrdtf_alt_ref = p_rdtf_ref
   AND lrdtf_type    = 'O';
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_PYI100';
cs                   INTEGER;
ce	             VARCHAR2(200);
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
l_raud_refno         NUMBER(10);
l_rdtf_refno         NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_pyi100.dataload_create');
    fsc_utils.debug_message('s_dl_hra_rds_pyi100.dataload_create',3);
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
          cs   := p1.lp100_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
          l_raud_refno := NULL;
          l_rdtf_refno := NULL;
--
-- Main processing
--
-- Open any cursors
--
           OPEN c_get_raud_refno(p1.lp100_rdsa_ha_reference,
                                 p1.lp100_hrv_dedt_code,
                                 p1.lp100_hrv_rbeg_code,
                                 p1.lp100_raud_start_date);
--
          FETCH c_get_raud_refno INTO l_raud_refno;
          CLOSE c_get_raud_refno; 
--
           OPEN c_get_rdtf_ref(p1.lp100_rdtf_alt_ref);
          FETCH c_get_rdtf_ref INTO l_rdtf_refno;
          CLOSE c_get_rdtf_ref;
--
-- Insert into RDS PYI100 table
--
        INSERT /* +APPEND */ INTO  rds_pyi100s(p100_trans_ref,
                                               p100_rdtf_refno,  
                                               p100_raud_refno, 
                                               p100_crn,
                                               p100_ext_ref_id,
                                               p100_timestamp,
                                               p100_request_action_code,
                                               p100_instruction_amount,
                                               p100_tp_id,
                                               p100_deduction_code,
                                               p100_benefit_group,
                                               p100_start_date,
                                               p100_end_date,
                                               p100_customer_birth_date,
                                               p100_customer_surname,
                                               p100_customer_postcode
                                              )
--
                                       VALUES (p1.lp100_trans_ref,
                                               l_rdtf_refno,
                                               l_raud_refno,
                                               p1.lp100_crn,
                                               p1.lp100_rdsa_ha_reference,
                                               p1.lp100_timestamp,
                                               p1.lp100_request_action_code,
                                               p1.lp100_instruction_amount,
                                               p1.lp100_tp_id,
                                               p1.lp100_hrv_dedt_code,
                                               p1.lp100_hrv_rbeg_code,
                                               p1.lp100_start_date,
                                               p1.lp100_end_date,
                                               p1.lp100_customer_birth_date,
                                               p1.lp100_customer_surname,
                                               p1.lp100_customer_postcode
                                              );
--
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_PYI100S');
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
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       lp100_dlb_batch_id,
       lp100_dl_seqno,
       lp100_dl_load_status,
       lp100_rdtf_alt_ref,
       lp100_rdsa_ha_reference,
       lp100_raud_start_date,
       lp100_hrv_dedt_code,
       lp100_hrv_rbeg_code,
       lp100_customer_birth_date,
       lp100_customer_surname,
       lp100_customer_postcode,
       lp100_trans_ref,
       lp100_crn,
       lp100_timestamp,
       lp100_request_action_code,
       lp100_instruction_amount,
       lp100_tp_id,
       lp100_start_date,
       lp100_end_date
  FROM dl_hra_rds_pyi100
 WHERE lp100_dlb_batch_id    = p_batch_id
   AND lp100_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Check RDS Authority Deduction Reference Number Exists
--
CURSOR c_chk_raud_refno(p_ha_reference  VARCHAR2,
                        p_dedt_code     VARCHAR2,
                        p_rbeg_code     VARCHAR2,
                        p_start_date    DATE) 
IS
SELECT raud_refno
  FROM rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno         = raud_rdsa_refno
   AND rdsa_ha_reference  = p_ha_reference
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_hrv_rbeg_code = p_rbeg_code
   AND raud_start_date    = p_start_date;
--
-- ***********************************************************************
--
-- Check RDS Transmission File Reference Number Exists
--
CURSOR c_rdtf_ref(p_rdtf_ref    VARCHAR2) 
IS
SELECT 'X'
  FROM rds_transmission_files, 
       dl_hra_rds_trans_files
 WHERE lrdtf_refno   = rdtf_refno
   AND lrdtf_alt_ref = p_rdtf_ref
   AND lrdtf_type    = 'O';
--
-- ***********************************************************************
--
-- Check Reference does not exist
--
CURSOR c_p100_ref(p_p100_ref    VARCHAR2) 
IS
SELECT 'X'
  FROM rds_pyi100s
 WHERE p100_trans_ref = p_p100_ref;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_PYI100';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists             VARCHAR2(1);
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
i                    INTEGER :=0;
--
l_raud_refno         NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_pyi100.dataload_validate');
    fsc_utils.debug_message( 's_dl_hra_rds_pyi100.dataload_validate',3);
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
          cs   := p1.lp100_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check PYI100 reference is unique
--
           OPEN c_p100_ref(p1.lp100_trans_ref);
--
          FETCH c_p100_ref into l_exists;
--
          IF (c_p100_ref%FOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',229);
          END IF;
--
          CLOSE c_p100_ref;
--
-- ***********************************************************************
--
-- Check Authorised Deduction Exists
--
          l_raud_refno := NULL;
--
           OPEN c_chk_raud_refno(p1.lp100_rdsa_ha_reference,
                                 p1.lp100_hrv_dedt_code,
                                 p1.lp100_hrv_rbeg_code,
                                 p1.lp100_raud_start_date);
--
          FETCH c_chk_raud_refno INTO l_raud_refno;
--
          IF (c_chk_raud_refno%NOTFOUND)THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',230);
          END IF;
--
          CLOSE c_chk_raud_refno;
--
-- ***********************************************************************
--
-- Check Transmission File Reference exists
--
           OPEN c_rdtf_ref(p1.lp100_rdtf_alt_ref);
--
          FETCH c_rdtf_ref INTO l_exists;
--
          IF (c_rdtf_ref%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',231);
          END IF;
--
          CLOSE c_rdtf_ref;
--
-- ***********************************************************************
--
-- Mandatory Fields Not Already Checked
--
-- CRN Number
--
          IF (p1.lp100_crn IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',232);
          END IF;
--
-- ***********************************************************************
--
-- Timestamp
--
          IF (p1.lp100_timestamp IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',233);
          END IF;
--
-- ***********************************************************************
--
-- Instruction Amount
--
          IF (p1.lp100_instruction_amount IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',234);
          END IF;
--
-- ***********************************************************************
--
-- TP ID
--
          IF (p1.lp100_tp_id IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',235);
          END IF;
--
-- ***********************************************************************
--
-- Customer Surname
--
          IF (p1.lp100_customer_surname IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',236);
          END IF;
--
-- ***********************************************************************
--
/*
-- Check Authority Deduction Type Exists
--
          IF (NOT s_dl_hem_utils.exists_frv('RDS_DED_TYPE',p1.lp100_hrv_dedt_code,'Y')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',237);
          END IF;
--
-- ***********************************************************************
--
-- Check Benefit Group Code Exists
--
          IF (p1.lp100_hrv_rbeg_code is NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lp100_hrv_rbeg_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',238);
           END IF;
--
          END IF;
*/
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
                          p_date           IN date) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       lp100_dlb_batch_id,
       lp100_dl_seqno,
       lp100_dl_load_status,
       lp100_rdtf_alt_ref,
       lp100_rdsa_ha_reference,
       lp100_raud_start_date,
       lp100_hrv_dedt_code,
       lp100_hrv_rbeg_code,
       lp100_customer_birth_date,
       lp100_customer_surname,
       lp100_customer_postcode,
       lp100_trans_ref,
       lp100_crn,
       lp100_timestamp,
       lp100_request_action_code,
       lp100_instruction_amount,
       lp100_tp_id,
       lp100_start_date,
       lp100_end_date
  FROM dl_hra_rds_pyi100
 WHERE lp100_dlb_batch_id   = p_batch_id
   AND lp100_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Get RDS Authority Deduction Reference Number
--
CURSOR c_get_raud_refno(p_ha_reference  VARCHAR2,
                        p_dedt_code     VARCHAR2,
                        p_rbeg_code     VARCHAR2,
                        p_start_date    DATE) 
IS
SELECT raud_refno
  FROM rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno         = raud_rdsa_refno
   AND rdsa_ha_reference  = p_ha_reference
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_hrv_rbeg_code = p_rbeg_code
   AND raud_start_date    = p_start_date;
--
-- ***********************************************************************
--
-- Get RDS Transmission File Reference Number
--
CURSOR c_get_rdtf_ref( p_rdtf_ref    VARCHAR2) 
IS
SELECT rdtf_refno
  FROM rds_transmission_files, 
       dl_hra_rds_trans_files
 WHERE lrdtf_refno   = rdtf_refno
   AND lrdtf_alt_ref = p_rdtf_ref
   AND lrdtf_type    = 'O';
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_PYI100';
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
l_exists         VARCHAR2(1);
i                INTEGER :=0;
l_raud_refno     NUMBER(10);
l_rdtf_refno     NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_pyi100.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_rds_pyi100.dataload_delete',3 );
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
          cs   := p1.lp100_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--         
          l_raud_refno := NULL;
          l_rdtf_refno := NULL;
--
           OPEN c_get_raud_refno(p1.lp100_rdsa_ha_reference,
                                 p1.lp100_hrv_dedt_code,
                                 p1.lp100_hrv_rbeg_code,
                                 p1.lp100_raud_start_date);
--
          FETCH c_get_raud_refno INTO l_raud_refno;
          CLOSE c_get_raud_refno; 
--
--
           OPEN c_get_rdtf_ref(p1.lp100_rdtf_alt_ref);
          FETCH c_get_rdtf_ref INTO l_rdtf_refno;
          CLOSE c_get_rdtf_ref;
--
-- Delete from RDS PYI100 table
--
--
          DELETE 
            FROM rds_pyi100s
           WHERE p100_trans_ref  = p1.lp100_trans_ref
             AND p100_rdtf_refno = l_rdtf_refno
             AND p100_raud_Refno = l_raud_refno
             AND p100_crn        = p1.lp100_crn
             AND p100_ext_ref_id = p1.lp100_rdsa_ha_reference;
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
           commit; 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_PYI100S');
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
END s_dl_hra_rds_pyi100;
/
