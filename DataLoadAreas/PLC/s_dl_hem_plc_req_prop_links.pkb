--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_plc_req_prop_links
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.16.0    VRS  24-Nov-2009  Initial Creation.
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
  UPDATE dl_hem_plc_req_prop_links
     SET lprpl_dl_load_status = p_status
   WHERE rowid                = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_plc_req_prop_links');
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
       LPRPL_DLB_BATCH_ID,
       LPRPL_DL_SEQNO,
       LPRPL_DL_LOAD_STATUS,
       LPRPL_PRO_PROPREF_FROM,
       LPRPL_PLPR_REFNO_FROM,
       LPRPL_PRO_PROPREF_TO,
       LPRPL_PLPR_REFNO_TO,
       NVL(LPRPL_CREATED_BY,'DATALOAD') LPRPL_CREATED_BY,
       NVL(LPRPL_CREATED_DATE,SYSDATE)  LPRPL_CREATED_DATE
  FROM dl_hem_plc_req_prop_links
 WHERE lprpl_dlb_batch_id   = p_batch_id  
   AND lprpl_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Get PLC_REQUEST_PROPERTIES Refno
--
CURSOR get_plrp_refno(p_plpr_refno NUMBER,
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
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQ_PROP_LINKS';
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
i                    	INTEGER := 0;
l_exists             	VARCHAR2(1);
--
l_prpl_plrp_refno_from	NUMBER(10);
l_prpl_plrp_refno_to	NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger PRPL_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_plc_req_prop_links.dataload_create');
    fsc_utils.debug_message('s_dl_hem_plc_req_prop_links.dataload_create',3);
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
          cs   := p1.lprpl_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          l_prpl_plrp_refno_from := NULL;
          l_prpl_plrp_refno_to   := NULL;
--
--
           OPEN get_plrp_refno(p1.LPRPL_PLPR_REFNO_FROM, p1.LPRPL_PRO_PROPREF_FROM);
          FETCH get_plrp_refno INTO l_prpl_plrp_refno_from;
          CLOSE get_plrp_refno;
--
           OPEN get_plrp_refno(p1.LPRPL_PLPR_REFNO_TO, p1.LPRPL_PRO_PROPREF_TO);
          FETCH get_plrp_refno INTO l_prpl_plrp_refno_to;
          CLOSE get_plrp_refno;
--
--
--
-- Insert into plc_request_property_links table
--
          INSERT /* +APPEND */ INTO  plc_request_property_links(PRPL_PLRP_REFNO_FROM,
                                                                PRPL_PLRP_REFNO_TO,
                                                                PRPL_CREATED_BY,
                                                                PRPL_CREATED_DATE
                                                               )
--
                                                        VALUES (l_prpl_plrp_refno_from,
                                                                l_prpl_plrp_refno_to,
                                                                p1.LPRPL_CREATED_BY,
                                                                p1.LPRPL_CREATED_DATE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_REQUEST_PROPERTY_LINKS');
--
    execute immediate 'alter trigger PRPL_BR_I enable';
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
       LPRPL_DLB_BATCH_ID,
       LPRPL_DL_SEQNO,
       LPRPL_DL_LOAD_STATUS,
       LPRPL_PRO_PROPREF_FROM,
       LPRPL_PLPR_REFNO_FROM,
       LPRPL_PRO_PROPREF_TO,
       LPRPL_PLPR_REFNO_TO,
       NVL(LPRPL_CREATED_BY,'DATALOAD') LPRPL_CREATED_BY,
       NVL(LPRPL_CREATED_DATE,SYSDATE)  LPRPL_CREATED_DATE
  FROM dl_hem_plc_req_prop_links
 WHERE lprpl_dlb_batch_id   = p_batch_id  
   AND lprpl_dl_load_status IN ('L','F','O');
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
CURSOR get_pro_refno(p_prop_ref VARCHAR2)
IS
SELECT 'X'
  FROM properties
 WHERE pro_propref = p_prop_ref;
--
-- ***********************************************************************
--
-- Get PLC_REQUEST_PROPERTIES Refno
--
CURSOR get_plrp_refno(p_plpr_refno NUMBER,
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
-- Gheck PLC_REQUEST_PROPERTY_LINKS record exists
--
CURSOR chk_prpl_exists(p_prpl_refno_from NUMBER,
                       p_prpl_refno_to   NUMBER) 
IS
SELECT 'X'
  FROM plc_request_property_links
 WHERE prpl_plrp_refno_from = p_prpl_refno_from
   AND prpl_plrp_refno_to   = p_prpl_refno_to;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQ_PROP_LINKS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_plpr_exists         	VARCHAR2(1);
l_pro_exists		VARCHAR2(1);
--
l_prpl_plrp_refno_from	NUMBER(10);
l_prpl_plrp_refno_to	NUMBER(10);
l_prpl_exists         	VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_hem_plc_req_prop_links.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_plc_req_prop_links.dataload_validate',3);
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
          cs   := p1.lprpl_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Perform Validation Checks
--
-- Check that the PLC_PROPERTY_REQUESTS reference from/to 
-- doesn't already exist
--
          l_plpr_exists := NULL;
--
           OPEN chk_plpr_refno(p1.LPRPL_PLPR_REFNO_FROM);
          FETCH chk_plpr_refno INTO l_plpr_exists;
          CLOSE chk_plpr_refno;
--
          IF (l_plpr_exists IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',030);
          END IF;
--
--
          l_plpr_exists := NULL;
--
           OPEN chk_plpr_refno(p1.LPRPL_PLPR_REFNO_TO);
          FETCH chk_plpr_refno INTO l_plpr_exists;
          CLOSE chk_plpr_refno;
--
          IF (l_plpr_exists IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',031);
          END IF;
--
-- ***********************************************************************
--
-- Check the property reference from/to are valid
--
          l_pro_exists := NULL;
--
            OPEN get_pro_refno(p1.LPRPL_PRO_PROPREF_FROM);
           FETCH get_pro_refno INTO l_pro_exists;
           CLOSE get_pro_refno;
--
           IF (l_pro_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',032);
           END IF;
--
--
          l_pro_exists := NULL;
--
            OPEN get_pro_refno(p1.LPRPL_PRO_PROPREF_TO);
           FETCH get_pro_refno INTO l_pro_exists;
           CLOSE get_pro_refno;
--
           IF (l_pro_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',033);
           END IF;
--
-- ***********************************************************************
--
-- Check that a record doesn't already exist for FROM/TO PLRP_REFNO combination
-- in PLC_REQUEST_PROPERTY_LINKS table
--
--
          l_prpl_plrp_refno_from := NULL;
          l_prpl_plrp_refno_to   := NULL;
          l_prpl_exists          := NULL;
--
--
           OPEN get_plrp_refno(p1.LPRPL_PLPR_REFNO_FROM, p1.LPRPL_PRO_PROPREF_FROM);
          FETCH get_plrp_refno INTO l_prpl_plrp_refno_from;
          CLOSE get_plrp_refno;
--
           OPEN get_plrp_refno(p1.LPRPL_PLPR_REFNO_TO, p1.LPRPL_PRO_PROPREF_TO);
          FETCH get_plrp_refno INTO l_prpl_plrp_refno_to;
          CLOSE get_plrp_refno;
--
--
-- Now check if record exists in PLC_REQUEST_PROPERTY_LINKS
--
--
           OPEN chk_prpl_exists(l_prpl_plrp_refno_from, l_prpl_plrp_refno_to);
          FETCH chk_prpl_exists INTO l_prpl_exists;
          CLOSE chk_prpl_exists;
--
          IF (l_prpl_exists IS NOT NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'PLC',034);
          END IF;
--
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
       LPRPL_DLB_BATCH_ID,
       LPRPL_DL_SEQNO,
       LPRPL_DL_LOAD_STATUS,
       LPRPL_PRO_PROPREF_FROM,
       LPRPL_PLPR_REFNO_FROM,
       LPRPL_PRO_PROPREF_TO,
       LPRPL_PLPR_REFNO_TO,
       LPRPL_CREATED_BY,
       LPRPL_CREATED_DATE
  FROM dl_hem_plc_req_prop_links
 WHERE lprpl_dlb_batch_id   = p_batch_id  
   AND lprpl_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Get PLC_REQUEST_PROPERTIES Refno
--
CURSOR get_plrp_refno(p_plpr_refno NUMBER,
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
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HEM_PLC_REQ_PROP_LINKS';
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
l_exists         	VARCHAR2(1);
i                	INTEGER :=0;
--
l_prpl_plrp_refno_from	NUMBER(10);
l_prpl_plrp_refno_to	NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_plc_req_prop_links.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_plc_req_prop_links.dataload_delete',3 );
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
          cs   := p1.lprpl_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Main processing
--
-- Open any cursors
--
          l_prpl_plrp_refno_from := NULL;
          l_prpl_plrp_refno_to   := NULL;
--
--
           OPEN get_plrp_refno(p1.LPRPL_PLPR_REFNO_FROM, p1.LPRPL_PRO_PROPREF_FROM);
          FETCH get_plrp_refno INTO l_prpl_plrp_refno_from;
          CLOSE get_plrp_refno;
--
           OPEN get_plrp_refno(p1.LPRPL_PLPR_REFNO_TO, p1.LPRPL_PRO_PROPREF_TO);
          FETCH get_plrp_refno INTO l_prpl_plrp_refno_to;
          CLOSE get_plrp_refno;
--
--
-- Delete from PLC_REQUEST_PROPERTIES table
--
--
          DELETE 
            FROM plc_request_property_links
           WHERE prpl_plrp_refno_from = l_prpl_plrp_refno_from
             AND prpl_plrp_refno_to   = l_prpl_plrp_refno_to
             AND prpl_created_by      = NVL(p1.LPRPL_CREATED_BY, prpl_created_by)
             AND prpl_created_date    = NVL(p1.LPRPL_CREATED_DATE, prpl_created_date);
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PLC_REQUEST_PROPERTY_LINKS');
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
END s_dl_hem_plc_req_prop_links;
/