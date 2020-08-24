--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_plc_prop_act_hist
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.16.0    VRS  24-JUL-2009  Initial Creation.
--
--  2.0     5.16.0    VRS  01-MAR-2010  Disable/Enable Trigger PAH_BR_I
--                                      in the CREATE Process.
--
--  3.0     5.16.0    VRS  03-MAR-2010  Remove all occurance of LPAH_HRV_PLMI_CODE.
--
--  4.0     5.16.0    VRS  09-MAR-2010  Apply same logic as PLC Data Item Load to
--                                      derive the plrp_refno.
--
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
  UPDATE dl_hem_plc_prop_act_hist
     SET lpah_dl_load_status = p_status
   WHERE rowid                = p_rowid;
  --
  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_plc_prop_act_hist');
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
       LPAH_DLB_BATCH_ID,
       LPAH_DL_SEQNO,
       LPAH_DL_LOAD_STATUS,
       LPAH_PLPR_REFNO,
       LPAH_PRO_SEQ_REFERENCE,
       LPAH_PENDING_PROP_IND,
       LPAH_PLAC_CODE,
       LPAH_COMPLETED_IND,
       NVL(LPAH_CREATED_BY,'DATALOAD') LPAH_CREATED_BY,
       NVL(LPAH_CREATED_DATE,SYSDATE)  LPAH_CREATED_DATE,
       LPAH_COMPLETED_BY,
       LPAH_COMPLETED_DATE
  FROM dl_hem_plc_prop_act_hist
 WHERE lpah_dlb_batch_id   = p_batch_id  
   AND lpah_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- Check PLC_REQUEST_PROPERTIES - Sequence Check
--
CURSOR get_plrp_dets_1(p_plpr_refno NUMBER,
                       p_sequence   VARCHAR2) 
IS
SELECT plrp_refno
  FROM plc_request_properties
 WHERE plrp_plpr_refno = p_plpr_refno
   AND plrp_sequence   = p_sequence;
--
-- ***********************************************************************
--
-- Check PLC_REQUEST_PROPERTIES - Property Check
--
CURSOR get_plrp_dets_2(p_plpr_refno NUMBER,
                       p_prop_ref   VARCHAR2) 
IS
SELECT plrp_refno
  FROM plc_request_properties,
       properties
 WHERE plrp_plpr_refno = p_plpr_refno
   AND plrp_pro_refno  = pro_refno
   AND pro_propref     = p_prop_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_PROP_ACT_HIST';
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
l_plrp_refno         NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger PAH_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_plc_prop_act_hist.dataload_create');
    fsc_utils.debug_message('s_dl_hem_plc_prop_act_hist.dataload_create',3);
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
          cs   := p1.lpah_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          
          l_plrp_refno := NULL;
--
          IF (p1.lpah_pending_prop_ind = 'Y') THEN
--
            OPEN get_plrp_dets_1(p1.lpah_plpr_refno, p1.lpah_pro_seq_reference);
           FETCH get_plrp_dets_1 INTO l_plrp_refno;
           CLOSE get_plrp_dets_1;
--
          ELSIF (p1.lpah_pending_prop_ind = 'N') THEN
--
               OPEN get_plrp_dets_2(p1.lpah_plpr_refno, p1.lpah_pro_seq_reference);
              FETCH get_plrp_dets_2 INTO l_plrp_refno;
              CLOSE get_plrp_dets_2;
--
          END IF;
--
--
-- Insert into plc_property_action_history table
--
          INSERT /* +APPEND */ INTO  plc_property_action_history(PAH_PLRP_REFNO,
                                                                 PAH_PLAC_CODE,
                                                                 PAH_COMPLETED_IND,
                                                                 PAH_CREATED_BY,
                                                                 PAH_CREATED_DATE,
                                                                 PAH_COMPLETED_BY,
                                                                 PAH_COMPLETED_DATE)
--  
                                                         VALUES (l_plrp_refno,
                                                                 p1.LPAH_PLAC_CODE,
                                                                 p1.LPAH_COMPLETED_IND,
                                                                 p1.LPAH_CREATED_BY,
                                                                 p1.LPAH_CREATED_DATE,
                                                                 p1.LPAH_COMPLETED_BY,
                                                                 p1.LPAH_COMPLETED_DATE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_PROPERTY_ACTION_HISTORY');
--
    execute immediate 'alter trigger PAH_BR_I enable';
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
CURSOR c1 is
SELECT rowid rec_rowid,
       LPAH_DLB_BATCH_ID,
       LPAH_DL_SEQNO,
       LPAH_DL_LOAD_STATUS,
       LPAH_PLPR_REFNO,
       LPAH_PRO_SEQ_REFERENCE,
       LPAH_PENDING_PROP_IND,
       LPAH_PLAC_CODE,
       LPAH_COMPLETED_IND,
       NVL(LPAH_CREATED_BY,'DATALOAD') LPAH_CREATED_BY,
       NVL(LPAH_CREATED_DATE,SYSDATE)  LPAH_CREATED_DATE,
       LPAH_COMPLETED_BY,
       LPAH_COMPLETED_DATE
  FROM dl_hem_plc_prop_act_hist
 WHERE lpah_dlb_batch_id   = p_batch_id  
   AND lpah_dl_load_status IN ('L','F','O');
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
-- Check Property Reference Exists in Properties
--
CURSOR chk_pro_refno(p_pro_ref VARCHAR2) 
IS
SELECT 'X'
  FROM properties
 WHERE pro_propref = p_pro_ref;
--
-- ***********************************************************************
--
-- Check PLC_REQUEST_PROPERTIES - Sequence Check
--
CURSOR chk_plrp_dets_1(p_plrp_refno NUMBER,
                       p_sequence   VARCHAR2) 
IS
SELECT 'X'
  FROM plc_request_properties
 WHERE plrp_plpr_refno = p_plrp_refno
   AND plrp_sequence   = p_sequence;
--
-- ***********************************************************************
--
-- Check PLC_REQUEST_PROPERTIES - Property Check
--
CURSOR chk_plrp_dets_2(p_plrp_refno NUMBER,
                       p_prop_ref   VARCHAR2) 
IS
SELECT 'X'
  FROM plc_request_properties,
       properties
 WHERE plrp_plpr_refno = p_plrp_refno
   AND plrp_pro_refno  = pro_refno
   AND pro_propref     = p_prop_ref;
--
-- ***********************************************************************
--
-- Check PLC_REQUEST_PROPERTIES Check
--
CURSOR chk_plrp_refno(p_plrp_refno NUMBER) 
IS
SELECT 'X'
  FROM plc_request_properties
 WHERE plrp_refno    = p_plrp_refno;
--
-- ***********************************************************************
--
-- Check PLC Property Request Action Code is Valid
--
CURSOR chk_plac_code(p_plac_code VARCHAR2) 
IS
SELECT 'X'
  FROM plc_actions
 WHERE plac_code        = p_plac_code
   AND plac_current_ind = 'Y';
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_PROP_ACT_HIST';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_plrp_exists        VARCHAR2(1);
l_plac_exists        VARCHAR2(1);
l_plpr_exists        VARCHAR2(1);
--
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
i                    INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_plc_prop_act_hist.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_plc_prop_act_hist.dataload_validate',3);
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
          cs   := p1.lpah_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Perform Validation Checks
--
-- Check that the PLC_REQUEST_PROPERTIES reference doesn't already exist
--
--          l_plrp_exists := NULL;
--
--           OPEN chk_plrp_refno(p1.lpah_plrp_refno);
--          FETCH chk_plrp_refno INTO l_plrp_exists;
--          CLOSE chk_plrp_refno;
--
--          IF (l_plrp_exists IS NULL) THEN
--           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',023);
--          END IF;
--
-- ***********************************************************************
--
-- Check that the PLC PROPERTY REQUEST reference is valid
--
--
          l_plpr_exists := NULL;
--
           OPEN chk_plpr_refno(p1.LPAH_PLPR_REFNO);
          FETCH chk_plpr_refno INTO l_plpr_exists;
          CLOSE chk_plpr_refno;
--
          IF (l_plpr_exists IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',007);
          END IF;
--
--
-- ***********************************************************************
--
--
-- Check that the PLC_ACTION Code is valid
--
          l_plac_exists := NULL;
--
           OPEN chk_plac_code(p1.lpah_plac_code);
          FETCH chk_plac_code INTO l_plac_exists;
          CLOSE chk_plac_code;
--
          IF (l_plac_exists IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',024);
          END IF;
--
-- ***********************************************************************
--
-- If Completed Indicator = Y then completed by and date must be supplied
--
          IF p1.lpah_completed_ind NOT IN ('Y','N') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',025);
          END IF;
--
--
          IF     (p1.lpah_completed_ind = 'Y'
             AND (   p1.lpah_completed_by   IS NULL
                  OR p1.lpah_completed_date IS NULL)) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',026);
--
          END IF;
--
-- ***********************************************************************
--
-- Check Pending Property Indicator = 'Y' then sequence exists on 
-- PLC_request_properties
--
-- Check Pending Property Indicator = 'N' then property exists on 
-- PLC_request_properties
--
--       
          l_plrp_exists := NULL;
--
          IF (p1.lpah_pending_prop_ind = 'Y') THEN
--
            OPEN chk_plrp_dets_1(p1.lpah_plpr_refno, p1.lpah_pro_seq_reference);
           FETCH chk_plrp_dets_1 INTO l_plrp_exists;
           CLOSE chk_plrp_dets_1;
--
           IF (l_plrp_exists IS NULL) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',021);
           END IF;
--
          END IF;
--
--
          IF (p1.lpah_pending_prop_ind = 'N') THEN
--
            OPEN chk_plrp_dets_2(p1.lpah_plpr_refno, p1.lpah_pro_seq_reference);
           FETCH chk_plrp_dets_2 INTO l_plrp_exists;
           CLOSE chk_plrp_dets_2;
--
           IF (l_plrp_exists IS NULL) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',022);
           END IF;
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
                          p_date           IN date) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPAH_DLB_BATCH_ID,
       LPAH_DL_SEQNO,
       LPAH_DL_LOAD_STATUS,
       LPAH_PLPR_REFNO,
       LPAH_PRO_SEQ_REFERENCE,
       LPAH_PENDING_PROP_IND,
       LPAH_PLAC_CODE
  FROM dl_hem_plc_prop_act_hist
 WHERE lpah_dlb_batch_id   = p_batch_id  
   AND lpah_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- Check PLC_REQUEST_PROPERTIES - Sequence Check
--
CURSOR get_plrp_dets_1(p_plrp_refno NUMBER,
                       p_sequence   VARCHAR2) 
IS
SELECT plrp_refno
  FROM plc_request_properties
 WHERE plrp_plpr_refno = p_plrp_refno
   AND plrp_sequence   = p_sequence;
--
-- ***********************************************************************
--
-- Check PLC_REQUEST_PROPERTIES - Property Check
--
CURSOR get_plrp_dets_2(p_plrp_refno NUMBER,
                       p_prop_ref   VARCHAR2) 
IS
SELECT plrp_refno
  FROM plc_request_properties,
       properties
 WHERE plrp_plpr_refno = p_plrp_refno
   AND plrp_pro_refno  = pro_refno
   AND pro_propref     = p_prop_ref;
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_PROP_ACT_HIST';
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
l_plrp_refno     NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_plc_prop_act_hist.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_plc_prop_act_hist.dataload_delete',3 );
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
          cs   := p1.lpah_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
         
          l_plrp_refno := NULL;
--
          IF (p1.lpah_pending_prop_ind = 'Y') THEN
--
            OPEN get_plrp_dets_1(p1.lpah_plpr_refno, p1.lpah_pro_seq_reference);
           FETCH get_plrp_dets_1 INTO l_plrp_refno;
           CLOSE get_plrp_dets_1;
--
          ELSIF (p1.lpah_pending_prop_ind = 'N') THEN
--
               OPEN get_plrp_dets_2(p1.lpah_plpr_refno, p1.lpah_pro_seq_reference);
              FETCH get_plrp_dets_2 INTO l_plrp_refno;
              CLOSE get_plrp_dets_2;
--
          END IF;
--
--
--
-- Delete from PLC_PROPERTY_ACTION_HISTORY table
--
--
          DELETE 
            FROM plc_property_action_history
           WHERE pah_plrp_refno = l_plrp_refno
             AND pah_plac_code  = p1.lpah_plac_code;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_PROPERTY_ACTION_HISTORY');
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
END s_dl_hem_plc_prop_act_hist;
/