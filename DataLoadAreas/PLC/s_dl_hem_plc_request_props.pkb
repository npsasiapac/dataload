--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_plc_request_props
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
--                                      trigger PLRP_BR_I in the CREATE process
--
--                                      Defect 2632 Fix. Also validate property
--                                      reference in the VALIDATE process added.
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
  UPDATE dl_hem_plc_request_props
     SET lplrp_dl_load_status = p_status
   WHERE rowid                = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_plc_request_props');
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
       LPLRP_DLB_BATCH_ID,
       LPLRP_DL_SEQNO,
       LPLRP_DL_LOAD_STATUS,
       LPLRP_PLPR_REFNO,
       LPLRP_PENDING_PROP_IND,
       NVL(LPLRP_CREATED_BY,'DATALOAD') LPLRP_CREATED_BY,
       NVL(LPLRP_CREATED_DATE,SYSDATE)  LPLRP_CREATED_DATE,
       LPLRP_SEQUENCE,
       LPLRP_PROP_REFERENCE,
       LPLRP_MODIFIED_BY,
       LPLRP_MODIFIED_DATE,
       LPLRP_REFNO
  FROM dl_hem_plc_request_props
 WHERE lplrp_dlb_batch_id   = p_batch_id  
   AND lplrp_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR get_pro_refno(p_prop_ref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_prop_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQUEST_PROPS';
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
l_pro_refno          NUMBER(8);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger PLRP_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_plc_request_props.dataload_create');
    fsc_utils.debug_message('s_dl_hem_plc_request_props.dataload_create',3);
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
          cs   := p1.lplrp_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          l_pro_refno := NULL;
--
          IF (p1.LPLRP_PROP_REFERENCE IS NOT NULL) THEN
--
            OPEN get_pro_refno(p1.LPLRP_PROP_REFERENCE);
           FETCH get_pro_refno INTO l_pro_refno;
           CLOSE get_pro_refno;
--
          END IF;
--
--
-- Insert into plc_request_properties table
--
          INSERT /* +APPEND */ INTO  plc_request_properties(PLRP_REFNO,
                                                            PLRP_PLPR_REFNO,
                                                            PLRP_PENDING_PROP_IND,
                                                            PLRP_CREATED_BY,
                                                            PLRP_CREATED_DATE,
                                                            PLRP_SEQUENCE,
                                                            PLRP_PRO_REFNO,
                                                            PLRP_MODIFIED_BY,
                                                            PLRP_MODIFIED_DATE
                                                           )
--
                                                    VALUES (p1.lplrp_refno,
                                                            p1.LPLRP_PLPR_REFNO,
                                                            p1.LPLRP_PENDING_PROP_IND,
                                                            p1.LPLRP_CREATED_BY,
                                                            p1.LPLRP_CREATED_DATE,
                                                            p1.LPLRP_SEQUENCE,
                                                            l_pro_refno,
                                                            p1.LPLRP_MODIFIED_BY,
                                                            p1.LPLRP_MODIFIED_DATE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_REQUEST_PROPERTIES');
--
    execute immediate 'alter trigger PLRP_BR_I enable';
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
       LPLRP_DLB_BATCH_ID,
       LPLRP_DL_SEQNO,
       LPLRP_DL_LOAD_STATUS,
       LPLRP_PLPR_REFNO,
       LPLRP_PENDING_PROP_IND,
       NVL(LPLRP_CREATED_BY,'DATALOAD') LPLRP_CREATED_BY,
       NVL(LPLRP_CREATED_DATE,SYSDATE)  LPLRP_CREATED_DATE,
       LPLRP_SEQUENCE,
       LPLRP_PROP_REFERENCE,
       LPLRP_MODIFIED_BY,
       LPLRP_MODIFIED_DATE
  FROM dl_hem_plc_request_props
 WHERE lplrp_dlb_batch_id   = p_batch_id  
   AND lplrp_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Check PLC_PROPERTY_REQUESTS Reference Number Exists
--
CURSOR chk_lpr_refno(p_plpr_refno NUMBER) 
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
CURSOR get_pro_refno(p_prop_ref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_prop_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQUEST_PROPS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_lpr_exists         	VARCHAR2(1);
l_pprt_exists        	VARCHAR2(1);
l_pro_refno		NUMBER(10);
--
l_errors                VARCHAR2(10);
l_error_ind             VARCHAR2(10);
i                       INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_plc_request_props.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_plc_request_props.dataload_validate',3);
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
          cs   := p1.lplrp_dl_seqno;
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
          l_lpr_exists := NULL;
--
           OPEN chk_lpr_refno(p1.lplrp_plpr_refno);
          FETCH chk_lpr_refno INTO l_lpr_exists;
          CLOSE chk_lpr_refno;
--
          IF (l_lpr_exists IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',007);
          END IF;
--
-- ***********************************************************************
--
-- Check Pending Property Indicator is Y or N
--
-- If Pending Property Indicator = Y then Sequence Number must be supplied
-- and Property Rereference must not be supplied
--
-- If Pending Property Indicator = Y then Sequence Number must be supplied
-- and Property Rereference must not be supplied
--
--
          IF p1.lplrp_pending_prop_ind NOT IN ('Y','N') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',008);
          END IF;
--
-- *************
--
          IF (p1.lplrp_pending_prop_ind = 'Y') THEN
--
           IF (    p1.lplrp_sequence       IS NULL
               AND p1.lplrp_prop_reference IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',009);
--
           ELSIF (    p1.lplrp_sequence       IS NULL
                  AND p1.lplrp_prop_reference IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',010);
--
           ELSIF (    p1.lplrp_sequence       IS NOT NULL
                  AND p1.lplrp_prop_reference IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',011);
--
           END IF;
--
          END IF; -- p1.lplrp_pending_prop_ind = 'Y'
--
-- *************
--
          IF (p1.lplrp_pending_prop_ind = 'N') THEN
--
           IF (    p1.lplrp_sequence       IS NOT NULL
               AND p1.lplrp_prop_reference IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',012);
--
           ELSIF (    p1.lplrp_sequence       IS NULL
                  AND p1.lplrp_prop_reference IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',013);
--
           ELSIF (    p1.lplrp_sequence       IS NOT NULL
                  AND p1.lplrp_prop_reference IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',014);
--
           END IF;
--
          END IF; -- p1.lplrp_pending_prop_ind = 'N'
--
--
-- ***********************************************************************
--
          l_pro_refno := NULL;
--
          IF (p1.LPLRP_PROP_REFERENCE IS NOT NULL) THEN
--
            OPEN get_pro_refno(p1.LPLRP_PROP_REFERENCE);
           FETCH get_pro_refno INTO l_pro_refno;
           CLOSE get_pro_refno;
--
           IF (l_pro_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',029);
           END IF;

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
       LPLRP_DLB_BATCH_ID,
       LPLRP_DL_SEQNO,
       LPLRP_DL_LOAD_STATUS,
       LPLRP_REFNO
  FROM dl_hem_plc_request_props
 WHERE lplrp_dlb_batch_id   = p_batch_id  
   AND lplrp_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQUEST_PROPS';
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
    fsc_utils.proc_start('s_dl_hem_plc_request_props.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_plc_request_props.dataload_delete',3 );
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
          cs   := p1.lplrp_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from PLC_REQUEST_PROPERTIES table
--
--
          DELETE 
            FROM plc_request_properties
           WHERE plrp_refno = p1.lplrp_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_REQUEST_PROPERTIES');
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
END s_dl_hem_plc_request_props;
/