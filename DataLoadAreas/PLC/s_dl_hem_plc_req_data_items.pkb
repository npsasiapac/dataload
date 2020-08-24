--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_plc_req_data_items
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
--  2.0     5.16.0    IR   21-SEP-2009  Change Table structure to 
--                                      incorporate property reference
--                                      and property indicator
--
--  3.0     5.16.0    VS   09-OCT-2009  Comment out validation checks PLC023
--                                      and PLC017.
--
--  4.0     5.16.0    VRS  20-NOV-2009  Defect 2610 Fix. Disable/enable
--                                      trigger PRDI_BR_I in the CREATE process
--
--                                      Defect 2611 Fix. 
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
  UPDATE dl_hem_plc_req_data_items
     SET lprdi_dl_load_status = p_status
   WHERE rowid                = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_plc_req_data_items');
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
       LPRDI_DLB_BATCH_ID,
       LPRDI_DL_SEQNO,
       LPRDI_DL_LOAD_STATUS,
       LPRDI_TYPE,
       LPRDI_PDAI_REFNO,
       NVL(LPRDI_CREATED_BY,'DATALOAD') LPRDI_CREATED_BY,
       NVL(LPRDI_CREATED_DATE,SYSDATE)  LPRDI_CREATED_DATE,
       LPRDI_PLPR_REFNO,
       LPRDI_PRO_SEQ_REFERENCE,
       LPRDI_PENDING_PROP_IND,
       LPRDI_CHAR_VALUE,
       LPRDI_NUMBER_VALUE,
       LPRDI_DATE_VALUE,
       LPRDI_ESTATES_UPDATED_IND,
       LPRDI_MODIFIED_BY,
       LPRDI_MODIFIED_DATE,
       LPRDI_REFNO
  FROM dl_hem_plc_req_data_items
 WHERE lprdi_dlb_batch_id   = p_batch_id  
   AND lprdi_dl_load_status = 'V';
--
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
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_PROP_REQUESTS';
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
    execute immediate 'alter trigger PRDI_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_plc_req_data_items.dataload_create');
    fsc_utils.debug_message('s_dl_hem_plc_req_data_items.dataload_create',3);
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
          cs   := p1.lprdi_dl_seqno;
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
          IF (p1.lprdi_type = 'PRO') THEN
--
           IF (p1.lprdi_pending_prop_ind = 'Y') THEN
--
             OPEN get_plrp_dets_1(p1.lprdi_plpr_refno, p1.lprdi_pro_seq_reference);
            FETCH get_plrp_dets_1 INTO l_plrp_refno;
            CLOSE get_plrp_dets_1;
--
           ELSIF (p1.lprdi_pending_prop_ind = 'N') THEN
--
                OPEN get_plrp_dets_2(p1.lprdi_plpr_refno, p1.lprdi_pro_seq_reference);
               FETCH get_plrp_dets_2 INTO l_plrp_refno;
               CLOSE get_plrp_dets_2;
--
           END IF;
--
          END IF;
--

-- Insert into plc_request_data_items table
--
          INSERT /* +APPEND */ INTO  plc_request_data_items(PRDI_REFNO,
                                                            PRDI_TYPE,
                                                            PRDI_PDAI_REFNO,
                                                            PRDI_CREATED_BY,
                                                            PRDI_CREATED_DATE,
                                                            PRDI_PLPR_REFNO,
                                                            PRDI_PLRP_REFNO,
                                                            PRDI_CHAR_VALUE,
                                                            PRDI_NUMBER_VALUE,
                                                            PRDI_DATE_VALUE,
                                                            PRDI_ESTATES_UPDATED_IND,
                                                            PRDI_MODIFIED_BY,
                                                            PRDI_MODIFIED_DATE
                                                           )
--
                                                    VALUES (p1.LPRDI_REFNO,       
                                                            p1.LPRDI_TYPE,
                                                            p1.LPRDI_PDAI_REFNO,
                                                            p1.LPRDI_CREATED_BY,
                                                            p1.LPRDI_CREATED_DATE,
                                                            p1.LPRDI_PLPR_REFNO,
                                                            l_plrp_refno,
                                                            p1.LPRDI_CHAR_VALUE,
                                                            p1.LPRDI_NUMBER_VALUE,
                                                            p1.LPRDI_DATE_VALUE,
                                                            p1.LPRDI_ESTATES_UPDATED_IND,
                                                            p1.LPRDI_MODIFIED_BY,
                                                            p1.LPRDI_MODIFIED_DATE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_REQUEST_DATA_ITEMS');
--
    execute immediate 'alter trigger PRDI_BR_I enable';
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
       LPRDI_DLB_BATCH_ID,
       LPRDI_DL_SEQNO,
       LPRDI_DL_LOAD_STATUS,
       LPRDI_TYPE,
       LPRDI_PDAI_REFNO,
       NVL(LPRDI_CREATED_BY,'DATALOAD') LPRDI_CREATED_BY,
       NVL(LPRDI_CREATED_DATE,SYSDATE)  LPRDI_CREATED_DATE,
       LPRDI_PLPR_REFNO,
--       LPRDI_PLRP_REFNO,
       LPRDI_PRO_SEQ_REFERENCE,
       LPRDI_PENDING_PROP_IND,
       LPRDI_CHAR_VALUE,
       LPRDI_NUMBER_VALUE,
       LPRDI_DATE_VALUE,
       LPRDI_ESTATES_UPDATED_IND,
       LPRDI_MODIFIED_BY,
       LPRDI_MODIFIED_DATE,
       LPRDI_REFNO
  FROM dl_hem_plc_req_data_items
 WHERE lprdi_dlb_batch_id    = p_batch_id  
   AND lprdi_dl_load_status IN ('L','F','O');
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
-- Check Data Item Reference Number Exists
--
CURSOR chk_pdai_refno(p_pdai_refno NUMBER) 
IS
SELECT 'X'
  FROM plc_data_items
 WHERE pdai_refno = p_pdai_refno;
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
-- Check PLC_REQUEST_PROPERTIES Reference Number Exists
--
CURSOR chk_plrp_refno(p_plrp_refno NUMBER) 
IS
SELECT 'X'
  FROM plc_request_properties
 WHERE plrp_refno = p_plrp_refno;
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
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQ_DATA_ITEMS';
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
l_plrp_exists        VARCHAR2(1);
l_pdai_exists        VARCHAR2(1);
l_pro_exists         VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_hem_plc_req_data_items.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_plc_req_data_items.dataload_validate',3);
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
          cs   := p1.lprdi_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Perform Validation Checks
--
--
-- Check Requested Data Type Code Supplied is Valid
--
          IF p1.lprdi_type NOT IN ('PRO','REQ') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',015);
          END IF;
--
-- ***********************************************************************
--
-- Check that the Action Data Item reference is valid
--
          l_pdai_exists := NULL;
--
           OPEN chk_pdai_refno(p1.LPRDI_PDAI_REFNO);
          FETCH chk_pdai_refno INTO l_pdai_exists;
          CLOSE chk_pdai_refno;
--
          IF l_pdai_exists IS NULL THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',016);
          END IF;
--
-- ***********************************************************************
--
-- Check that the PLC PROPERTY REQUEST reference is valid if supplied
--
          IF (p1.lprdi_plpr_refno IS NOT NULL) THEN
--
           l_plpr_exists := NULL;
--
            OPEN chk_plpr_refno(p1.LPRDI_PLPR_REFNO);
           FETCH chk_plpr_refno INTO l_plpr_exists;
           CLOSE chk_plpr_refno;
--
           IF (l_plpr_exists IS NULL) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',007);
           END IF;
--
          END IF;
--
-- ***********************************************************************
/*
--
-- Check that the PLC REQUEST PROPERTIES reference is valid if supplied
--
          IF (p1.lprdi_pending_prop_ind = 'Y') THEN
--
           l_plrp1_exists := NULL;
--
            OPEN chk_plrp1_refno(p1.LPRDI_PLRP_REFNO);
           FETCH chk_plrp1_refno INTO l_plrp1_exists;
           CLOSE chk_plrp1_refno;
--
           IF (l_plrp1_exists IS NULL) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',023);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If Data Item = PRO then check to see that the property reference exists
--
          IF (p1.lprdi_type = 'PRO') THEN
--
           l_pro_exists := NULL;
--
            OPEN chk_pro_refno(p1.LPRDI_PRO_SEQ_REFERENCE);
           FETCH chk_pro_refno INTO l_pro_exists;
           CLOSE chk_pro_refno;
--
           IF (l_pro_exists IS NULL) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',017);
           END IF;         
--
          END IF;
--
*/
--
-- ***********************************************************************
--
-- Check to see that the LPRDI_PRO_SEQ_REFERENCE and lprdi_pending_prop_ind
-- have been supplied if the request data item is PRO
--
--
          IF (p1.lprdi_type = 'PRO') THEN
--
           IF (   p1.lprdi_pending_prop_ind  IS NULL
               OR p1.LPRDI_PRO_SEQ_REFERENCE IS NULL) THEN
--
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',000);
--
           END IF;
--
          END IF;
--
--
-- ***********************************************************************
--
-- Check Pending Property Indicator is Y or N
--
--
          IF (    p1.lprdi_type             = 'PRO'
              AND p1.lprdi_pending_prop_ind IS NOT NULL) THEN
--
           IF p1.lprdi_pending_prop_ind NOT IN ('Y','N') THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',018);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check that the char/Number/Date Value has been supplied
--
          IF (    p1.lprdi_char_value   IS NULL
              AND p1.lprdi_number_value IS NULL
              AND p1.lprdi_date_value   IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',019);
--
          END IF;
--
-- ***********************************************************************
--
-- Check Estates Updated Indicator is Y or N
--
          IF (p1.lprdi_estates_updated_ind IS NOT NULL) THEN
--
           IF p1.lprdi_estates_updated_ind NOT IN ('Y','N') THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',020);
           END IF;
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
          IF (p1.lprdi_type = 'PRO') THEN
--         
           l_plrp_exists := NULL;
--
           IF (p1.lprdi_pending_prop_ind = 'Y') THEN
--
             OPEN chk_plrp_dets_1(p1.lprdi_plpr_refno, p1.lprdi_pro_seq_reference);
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
           IF (p1.lprdi_pending_prop_ind = 'N') THEN
--
             OPEN chk_plrp_dets_2(p1.lprdi_plpr_refno, p1.lprdi_pro_seq_reference);
            FETCH chk_plrp_dets_2 INTO l_plrp_exists;
            CLOSE chk_plrp_dets_2;
--
            IF (l_plrp_exists IS NULL) THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',022);
            END IF;
--
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
       LPRDI_DLB_BATCH_ID,
       LPRDI_DL_SEQNO,
       LPRDI_DL_LOAD_STATUS,
       LPRDI_REFNO
  FROM dl_hem_plc_req_data_items
 WHERE lprdi_dlb_batch_id   = p_batch_id  
   AND lprdi_dl_load_status = 'C';
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
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQ_DATA_ITEMS';
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
    fsc_utils.proc_start('s_dl_hem_plc_req_data_items.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_plc_req_data_items.dataload_delete',3 );
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
          cs   := p1.lprdi_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from PLC_REQUEST_DATA_ITEMS table
--
--
          DELETE 
            FROM plc_request_data_items
           WHERE prdi_refno = p1.lprdi_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_REQUEST_DATA_ITEMS');
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
END s_dl_hem_plc_req_data_items;
/