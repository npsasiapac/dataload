--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_plc_req_act_hist
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.16.0    VRS  27-JUL-2009  Initial Creation.
--
--  2.0     5.16.0    VRS  20-NOV-2009  Defect 2610 Fix. Disable/enable
--                                      trigger PRAH_BR_I in the CREATE process
--
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
  UPDATE dl_hem_plc_req_act_hist
     SET lprah_dl_load_status = p_status
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
       LPRAH_DLB_BATCH_ID,
       LPRAH_DL_SEQNO,
       LPRAH_DL_LOAD_STATUS,
       LPRAH_PLPR_REFNO,
       LPRAH_PLAC_CODE,
       LPRAH_COMPLETED_IND,
       LPRAH_SCO_CODE_FROM,
       NVL(LPRAH_CREATED_BY, 'DATALOAD') LPRAH_CREATED_BY,
       NVL(LPRAH_CREATED_DATE, SYSDATE)  LPRAH_CREATED_DATE,
       LPRAH_COMPLETED_BY,
       LPRAH_COMPLETED_DATE,
       LPRAH_SCO_CODE_TO
  FROM dl_hem_plc_req_act_hist
 WHERE lprah_dlb_batch_id   = p_batch_id  
   AND lprah_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQ_ACT_HIST';
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
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger PRAH_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_plc_req_act_hist.dataload_create');
    fsc_utils.debug_message('s_dl_hem_plc_req_act_hist.dataload_create',3);
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
          cs   := p1.lprah_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
--
-- Insert into plc_request_action_history table
--
          INSERT /* +APPEND */ INTO  plc_request_action_history(PRAH_PLPR_REFNO,
                                                                PRAH_PLAC_CODE,
                                                                PRAH_COMPLETED_IND,
                                                                PRAH_SCO_CODE_FROM,
                                                                PRAH_CREATED_BY,
                                                                PRAH_CREATED_DATE,
                                                                PRAH_COMPLETED_BY,
                                                                PRAH_COMPLETED_DATE,
                                                                PRAH_SCO_CODE_TO
                                                               )
--  
                                                        VALUES (p1.LPRAH_PLPR_REFNO,
                                                                p1.LPRAH_PLAC_CODE,
                                                                p1.LPRAH_COMPLETED_IND,
                                                                p1.LPRAH_SCO_CODE_FROM,
                                                                p1.LPRAH_CREATED_BY,
                                                                p1.LPRAH_CREATED_DATE,
                                                                p1.LPRAH_COMPLETED_BY,
                                                                p1.LPRAH_COMPLETED_DATE,
                                                                p1.LPRAH_SCO_CODE_TO
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_REQUEST_ACTION_HISTORY');
--
    execute immediate 'alter trigger PRAH_BR_I enable';
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
       LPRAH_DLB_BATCH_ID,
       LPRAH_DL_SEQNO,
       LPRAH_DL_LOAD_STATUS,
       LPRAH_PLPR_REFNO,
       LPRAH_PLAC_CODE,
       LPRAH_COMPLETED_IND,
       LPRAH_SCO_CODE_FROM,
       NVL(LPRAH_CREATED_BY, 'DATALOAD') LPRAH_CREATED_BY,
       NVL(LPRAH_CREATED_DATE, SYSDATE)  LPRAH_CREATED_DATE,
       LPRAH_COMPLETED_BY,
       LPRAH_COMPLETED_DATE,
       LPRAH_SCO_CODE_TO
  FROM dl_hem_plc_req_act_hist
 WHERE lprah_dlb_batch_id   = p_batch_id  
   AND lprah_dl_load_status IN ('L','F','O');
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
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQ_ACT_HIST';
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
l_plac_exists        VARCHAR2(1);

l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
i                    INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_plc_req_act_hist.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_plc_req_act_hist.dataload_validate',3);
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
          cs   := p1.lprah_dl_seqno;
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
           OPEN chk_plpr_refno(p1.lprah_plpr_refno);
          FETCH chk_plpr_refno INTO l_plpr_exists;
          CLOSE chk_plpr_refno;
--
          IF (l_plpr_exists IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',007);
          END IF;
--
-- ***********************************************************************
--
--
-- Check that the PLC_REQUEST_PROPERTIES reference doesn't already exist
--
          l_plac_exists := NULL;
--
           OPEN chk_plac_code(p1.lprah_plac_code);
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
          IF p1.lprah_completed_ind NOT IN ('Y','N') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',025);
          END IF;
--
--
          IF     (p1.lprah_completed_ind = 'Y'
             AND (   p1.lprah_completed_by   IS NULL
                  OR p1.lprah_completed_date IS NULL)) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',026);
--
          END IF;
--
-- ***********************************************************************
--
-- Check Status Code From Supplied is Valid
--
          IF (p1.lprah_sco_code_from NOT IN ('APR','OPN','WAI','ABN','ACC','REJ','COM')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',027);
          END IF;
--
-- ***********************************************************************
--
-- Check Status Code To Supplied is Valid
--
          IF (p1.lprah_sco_code_to IS NOT NULL) THEN
--
           IF (p1.lprah_sco_code_to NOT IN ('APR','OPN','WAI','ABN','ACC','REJ','COM')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',028);
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
       LPRAH_DLB_BATCH_ID,
       LPRAH_DL_SEQNO,
       LPRAH_DL_LOAD_STATUS,
       LPRAH_PLPR_REFNO,
       LPRAH_PLAC_CODE
  FROM dl_hem_plc_req_act_hist
 WHERE lprah_dlb_batch_id   = p_batch_id  
   AND lprah_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQ_ACT_HIST';
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
    fsc_utils.proc_start('s_dl_hem_plc_req_act_hist.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_plc_req_act_hist.dataload_delete',3 );
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
          cs   := p1.lprah_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from PLC_REQUEST_ACTION_HISTORY table
--
--
          DELETE 
            FROM plc_request_action_history
           WHERE prah_plpr_refno = p1.lprah_plpr_refno
             AND prah_plac_code  = p1.lprah_plac_code;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_REQUEST_ACTION_HISTORY');
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
END s_dl_hem_plc_req_act_hist;
/