CREATE OR REPLACE PACKAGE BODY s_dl_hem_plc_prop_requests
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.16.0    VRS  20-JUL-2009  Initial Creation.
--
--  2.0     5.16.0    VRS  20-NOV-2009  Defect 2610 Fix. Disable/enable
--                                      trigger PLPR_BR_I in the CREATE process
--  3.0     6.12.0    AJ   03-SEP-2015  LPLPR_AUN_CODE added in create and validate
--
--  4.0     6.12.0    VS   03-NOV-2015  Resolve compilation errors
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
  UPDATE dl_hem_plc_prop_requests
     SET lplpr_dl_load_status = p_status
   WHERE rowid                = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_plc_prop_requests');
          RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id IN VARCHAR2,
                          p_date     IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPLPR_DLB_BATCH_ID,
       LPLPR_DL_SEQNO,
       LPLPR_DL_LOAD_STATUS,
       LPLPR_REFNO,
       LPLPR_CREATE_IND,
       LPLPR_PPRT_CODE,
       LPLPR_STATUS_CODE,
       LPLPR_STATUS_DATE,
       NVL(LPLPR_CREATED_BY, 'DATALOAD') LPLPR_CREATED_BY,
       NVL(LPLPR_CREATED_DATE, SYSDATE)  LPLPR_CREATED_DATE,
       LPLPR_PROJECT_NUMBER,
       LPLPR_TOTAL_BEDROOMS,
       LPLPR_TOTAL_PROPERTIES,
       LPLPR_HRV_PLMI_CODE,
       LPLPR_MILESTONE_DATE,
       LPLPR_PLPR_REFNO,
       LPLPR_COMMENTS,
       LPLPR_MODIFIED_BY,
       LPLPR_MODIFIED_DATE,
       LPLPR_AUN_CODE
  FROM dl_hem_plc_prop_requests
 WHERE lplpr_dlb_batch_id   = p_batch_id  
   AND lplpr_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HEM_PLC_PROP_REQUESTS';
cs                   INTEGER;
ce	                 VARCHAR2(200);
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
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger PLPR_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_plc_prop_requests.dataload_create');
    fsc_utils.debug_message('s_dl_hem_plc_prop_requests.dataload_create',3);
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
          cs   := p1.lplpr_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
--
-- Insert into PLC_PROPERTY_REQUESTS table
--
          INSERT INTO  plc_property_requests(PLPR_REFNO,
                                             PLPR_CREATE_IND,
                                             PLPR_PPRT_CODE,
                                             PLPR_SCO_CODE,
                                             PLPR_STATUS_DATE,
                                             PLPR_CREATED_BY,
                                             PLPR_CREATED_DATE,
                                             PLPR_PROJECT_NUMBER,
                                             PLPR_TOTAL_BEDROOMS,
                                             PLPR_TOTAL_PROPERTIES,
                                             PLPR_HRV_PLMI_CODE,
                                             PLPR_MILESTONE_DATE,
                                             PLPR_PLPR_REFNO,
                                             PLPR_COMMENTS,
                                             PLPR_MODIFIED_BY,
                                             PLPR_MODIFIED_DATE,
                                             PLPR_AUN_CODE
                                            )
--
                                     VALUES (p1.LPLPR_REFNO,
                                             p1.LPLPR_CREATE_IND,
                                             p1.LPLPR_PPRT_CODE,
                                             p1.LPLPR_STATUS_CODE,
                                             p1.LPLPR_STATUS_DATE,
                                             p1.LPLPR_CREATED_BY,
                                             p1.LPLPR_CREATED_DATE,
                                             p1.LPLPR_PROJECT_NUMBER,
                                             p1.LPLPR_TOTAL_BEDROOMS,
                                             p1.LPLPR_TOTAL_PROPERTIES,
                                             p1.LPLPR_HRV_PLMI_CODE,
                                             p1.LPLPR_MILESTONE_DATE,
                                             p1.LPLPR_PLPR_REFNO,
                                             p1.LPLPR_COMMENTS,
                                             p1.LPLPR_MODIFIED_BY,
                                             p1.LPLPR_MODIFIED_DATE,
                                             p1.LPLPR_AUN_CODE
                                            );
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_PROPERTY_REQUESTS');
--
    execute immediate 'alter trigger PLPR_BR_I enable';
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
PROCEDURE dataload_validate(p_batch_id IN VARCHAR2,
                            p_date     IN DATE)
AS
--
CURSOR C1
IS
SELECT rowid rec_rowid,
       LPLPR_DLB_BATCH_ID,
       LPLPR_DL_SEQNO,
       LPLPR_DL_LOAD_STATUS,
       LPLPR_REFNO,
       LPLPR_CREATE_IND,
       LPLPR_PPRT_CODE,
       LPLPR_STATUS_CODE,
       LPLPR_STATUS_DATE,
       NVL(LPLPR_CREATED_BY, 'DATALOAD') LPLPR_CREATED_BY,
       NVL(LPLPR_CREATED_DATE, SYSDATE)  LPLPR_CREATED_DATE,
       LPLPR_PROJECT_NUMBER,
       LPLPR_TOTAL_BEDROOMS,
       LPLPR_TOTAL_PROPERTIES,
       LPLPR_HRV_PLMI_CODE,
       LPLPR_MILESTONE_DATE,
       LPLPR_PLPR_REFNO,
       LPLPR_COMMENTS,
       LPLPR_MODIFIED_BY,
       LPLPR_MODIFIED_DATE,
       LPLPR_AUN_CODE
  FROM dl_hem_plc_prop_requests
 WHERE lplpr_dlb_batch_id    = p_batch_id  
   AND lplpr_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Check PLC_PROPERTY_REQUESTS Reference Number Exists
--
CURSOR chk_plpr_refno(p_plpr_refno NUMBER) 
IS
SELECT 'X'
  FROM plc_property_requests
 WHERE plpr_refno = p_plpr_refno;
--
-- ***********************************************************************
--
-- Check PLC Property Type Code is Valid
--
CURSOR chk_pprt_code(p_pprt_code VARCHAR2) 
IS
SELECT 'X'
  FROM plc_property_request_types
 WHERE pprt_code = p_pprt_code;
--
-- ***********************************************************************
--
-- Check PLC Admin Unit Code is Valid
--
CURSOR chk_aun_code(p_aun_code VARCHAR2) 
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_PROP_REQUESTS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_plpr_exists        VARCHAR2(1);
l_pprt_exists        VARCHAR2(1);
l_aun_exists         VARCHAR2(1);

l_errors                   VARCHAR2(10);
l_error_ind                VARCHAR2(10);
i                          INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_plc_prop_requests.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_plc_prop_requests.dataload_validate',3);
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
          cs   := p1.lplpr_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Perform Validation Checks
--
-- Check that the PLC_PROPERTY_REQUESTS reference doesn't already exist
--
          l_plpr_exists := NULL;
--
           OPEN chk_plpr_refno(p1.lplpr_refno);
          FETCH chk_plpr_refno INTO l_plpr_exists;
          CLOSE chk_plpr_refno;
--
          IF l_plpr_exists IS NOT NULL THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',001);
          END IF;
--
-- ***********************************************************************
--
-- Check create Indicator is Y or N
--
          IF p1.lplpr_create_ind NOT IN ('Y','N') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',002);
          END IF;
--
-- ***********************************************************************
--
-- Check Property Request Type Code is Valid
--
          l_pprt_exists := NULL;
--
           OPEN chk_pprt_code(p1.lplpr_pprt_code);
          FETCH chk_pprt_code INTO l_pprt_exists;
          CLOSE chk_pprt_code;
--
          IF l_pprt_exists IS NULL THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',003);
          END IF;
--
-- ***********************************************************************
--
-- Check Status Code Supplied is Valid
--
          IF p1.lplpr_status_code NOT IN ('OPN','WAI','ABN','ACC','REJ','COM','APR') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',004);
          END IF;
--
-- ***********************************************************************
--
-- If Supplied Check PLC Milestone Code is valid
--
         IF (p1.lplpr_hrv_plmi_code IS NOT NULL) THEN
--
          IF (NOT s_dl_hem_utils.exists_frv('PLCMILESTONE',p1.lplpr_hrv_plmi_code,'Y')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',005);
          END IF;
--
         END IF;
--
-- ***********************************************************************
--
-- Check that the PLC_PROPERTY_REQUESTS Parent reference doesn't already exist
--
          l_plpr_exists := NULL;
--
         IF (p1.lplpr_plpr_refno IS NOT NULL) THEN
--
           OPEN chk_plpr_refno(p1.lplpr_plpr_refno);
          FETCH chk_plpr_refno INTO l_plpr_exists;
          CLOSE chk_plpr_refno;
--
          IF l_plpr_exists IS NULL THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',006);
          END IF;
--
         END IF;
--
-- ***********************************************************************
--
-- Check that the PLPR_AUN_CODE is valid
--
          l_aun_exists := NULL;
--
           OPEN chk_aun_code(p1.lplpr_aun_code);
          FETCH chk_aun_code INTO l_aun_exists;
          CLOSE chk_aun_code;
--
          IF l_aun_exists IS NULL THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',001);
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
                          p_date           IN date) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPLPR_DLB_BATCH_ID,
       LPLPR_DL_SEQNO,
       LPLPR_DL_LOAD_STATUS,
       LPLPR_REFNO
  FROM dl_hem_plc_prop_requests
 WHERE lplpr_dlb_batch_id   = p_batch_id  
   AND lplpr_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_PROP_REQUESTS';
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
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_plc_prop_requests.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_plc_prop_requests.dataload_delete',3 );
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
          cs   := p1.lplpr_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from PLC_PROPERTY_REQUESTS table
--
--
          DELETE 
            FROM plc_property_requests
           WHERE plpr_refno = p1.lplpr_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_PROPERTY_REQUESTS');
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
END s_dl_hem_plc_prop_requests;
/