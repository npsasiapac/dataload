CREATE OR REPLACE PACKAGE BODY S_DL_CUS_DATALOAD_BATCHES
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver WHO  WHEN       WHY
--    1.0          PL   01/06/18   Initial Creation    
--    1.1          PL   13/06/18   Change Question 1
--    1.2          PL   03/08/18   Increased time delay between loads to 5 secs
--                                 This is to try and prevent false gpi failures.
--    1.3          PL   15/11/19   FAIL is not longer concidered still running.
--  declare package variables AND constants
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE DL_CUS_DATALOAD_BATCHES
  SET ldl_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of DL_CUS_DATALOAD_BATCHES');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ldl_dlb_batch_id
,ldl_dl_seqno
,ldl_dl_load_status
,ldl_batch_seqno
,ldl_batch_id
,ldl_product_area
,ldl_dataload_area
,ldl_question_answer1
,ldl_question_answer2
FROM  DL_CUS_DATALOAD_BATCHES
WHERE ldl_dlb_batch_id   = p_batch_id
AND   ldl_dl_load_status = 'V';
--
--
--CURSORS
--

--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_CUS_DATALOAD_BATCHES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
-- Other variables
--
l_an_tab VARCHAR2(1);
i        INTEGER :=0;
l_exists    VARCHAR2(1);
--
--
BEGIN
--
fsc_utils.proc_start('s_DL_CUS_DATALOAD_BATCHES.dataload_create');
fsc_utils.debug_message('s_DL_CUS_DATALOAD_BATCHES.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
fsc_variables.g_username := 'MIGRATION';

--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.ldl_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
--Reset all the variables            
--
INSERT INTO dl_batches ( DLB_BATCH_ID
                       , DLB_DLA_PRODUCT_AREA
					   , DLB_DLA_DATALOAD_AREA
					   , DLB_CREATED_BY
					   , DLB_CREATED_DATE
                       , DLB_QUESTION_ANSWER1
                       , DLB_QUESTION_ANSWER2)
VALUES                 ( p1.LDL_BATCH_ID
                       , p1.LDL_PRODUCT_AREA
					   , p1.LDL_DATALOAD_AREA
					   , 'MIGRATION'
					   , SYSDATE
					   , p1.LDL_QUESTION_ANSWER1
					   , p1.LDL_QUESTION_ANSWER2);
--
-- Set the dataload statuses
--
INSERT INTO DL_PROCESS_SUMMARY 
( DPS_DLB_BATCH_ID
, DPS_PROCESS
, DPS_DATE
, DPS_STATUS
, DPS_FAILURES_IND
, DPS_TOTAL_RECORDS
, DPS_FAILED_RECORDS
, DPS_PROCESSED_RECORDS
, DPS_CREATED_BY
, DPS_CREATED_DATE )
VALUES
( p1.LDL_BATCH_ID
, 'LOAD'
, SYSDATE
, 'CREATED'
, 'N'
, 0
, 0
, 0
, 'MIGRATION'
, SYSDATE);

s_batch_requests.submit_job_api
( p_module_name => 'FDL100'
, p_printer_name => ''
, param01 => p1.LDL_PRODUCT_AREA
, param02 => p1.LDL_DATALOAD_AREA
, param03 => p1.LDL_BATCH_ID
, param04 => TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS')
, param05 => NULL
, param06 => NULL
, param07 => NULL
, param08 => NULL
, param09 => NULL
, param10 => NULL);

dbms_lock.sleep(5);
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
 END;
--
 END LOOP;
--
  -- Section to analyze the table(s) populated by this dataload
  --
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_BATCHES');
    --
fsc_utils.proc_end;
COMMIT;
--
    EXCEPTION
       WHEN OTHERS THEN
       s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
       RAISE;
--
END dataload_create;
--
--
--
--
PROCEDURE dataload_validate
(p_batch_id          IN VARCHAR2
,p_date              IN DATE    )
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ldl_dlb_batch_id
,ldl_dl_seqno
,ldl_dl_load_status
,ldl_batch_seqno
,ldl_batch_id
,ldl_product_area
,ldl_dataload_area
,ldl_question_answer1
,ldl_question_answer2
FROM  DL_CUS_DATALOAD_BATCHES
WHERE ldl_dlb_batch_id    = p_batch_id
AND   ldl_dl_load_status IN ('L','F','O')
-- AND rownum < 20
;
--
-- VALIDATION CURSORS
--
 CURSOR c2 (cp_dl_area VARCHAR2) IS
 SELECT *
 FROM dl_load_areas 
 WHERE dla_dataload_area = cp_dl_area;
--
 l2 c2%rowtype;
 
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_CUS_DATALOAD_BATCHES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- Other Variables
l_exists             VARCHAR2(1);
l_obj_ref            VARCHAR2(30);
l_car_exists         VARCHAR2(1);
l_aut_format_ind     VARCHAR2(3);
i                    INTEGER:=0;
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
l_answer             VARCHAR2(1);
l_llord_par_refno    NUMBER(10);
--
--
BEGIN
--
fsc_utils.proc_start('s_DL_CUS_DATALOAD_BATCHES.dataload_validate');
fsc_utils.debug_message('s_DL_CUS_DATALOAD_BATCHES.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.ldl_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_errors := 'V';
l_error_ind := 'N';
--
--
OPEN c2(p1.ldl_dataload_area);
FETCH c2 INTO l2;
IF c2%NOTFOUND THEN
	l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',1); -- Invalid dataload area
END IF;
CLOSE c2;

IF l2.dla_product_area IS NOT NULL
THEN
	IF l2.dla_product_area != p1.LDL_PRODUCT_AREA
	THEN
		l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',2); -- Invalid product area
	END IF;
	
	IF l2.dla_question1 IS NOT NULL
	AND p1.LDL_QUESTION_ANSWER1 IS NULL 
	THEN
		l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',3); -- Missing Question 1
	END IF;

	IF l2.dla_question2 IS NOT NULL
	AND p1.LDL_QUESTION_ANSWER2 IS NULL 
	THEN
		l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',4); -- Missing Question 2
	END IF;
END IF; 


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
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
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
      RAISE;
--
END dataload_validate;
--
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) 
IS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ldl_dlb_batch_id
,ldl_dl_seqno
,LDL_BATCH_SEQNO
,LDL_BATCH_ID
,LDL_PRODUCT_AREA
,LDL_DATALOAD_AREA
,LDL_QUESTION_ANSWER1
,LDL_QUESTION_ANSWER2
FROM  DL_CUS_DATALOAD_BATCHES
WHERE  ldl_dlb_batch_id   = p_batch_id
AND   ldl_dl_load_status = 'C';
--
--

i INTEGER := 0;
--

--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_CUS_DATALOAD_BATCHES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_DL_CUS_DATALOAD_BATCHES.dataload_DELETE');
fsc_utils.debug_message( 's_DL_CUS_DATALOAD_BATCHES.dataload_DELETE',3 );
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--

--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
-- Called by trigger dps_after_u on dl_process_summary
-- Intention is to automate load, validate, and create of the dataload 
/*
CREATE OR REPLACE TRIGGER DPS_AFTER_U
AFTER UPDATE ON DL_PROCESS_SUMMARY
FOR EACH ROW
DECLARE
 l_job number;
BEGIN
   IF :NEW.dps_status LIKE 'COM%'
   THEN
     dbms_job.submit(job=>l_job,
                     what=>'begin s_dl_cus_dataload_batches.run_next( '''|| :NEW.dps_status||''','''||:NEW.dps_process||''','''|| :NEW.DPS_DLB_BATCH_ID||''' ); end;');     
  END IF;   
END DPS_BR_U;
*/ 
PROCEDURE run_next ( p_status VARCHAR2
                   , p_process VARCHAR2
                   , p_run_id VARCHAR2) AS

  l_still_running VARCHAR2(1) := 'N';
  l_job NUMBER;
  l_errors NUMBER;
  
  CURSOR c_get_batch_run (cp_batch_id VARCHAR2) IS
  SELECT ldl_refno
  ,      ldl_dlb_batch_id
  ,      ldl_batch_id
  ,      ldl_batch_seqno
  ,      dlb_question_answer1 ldl_question_answer1
  ,      dlb_question_answer2 ldl_question_answer2
  FROM DL_CUS_DATALOAD_BATCHES
  JOIN DL_BATCHES ON DLB_BATCH_ID = LDL_DLB_BATCH_ID 
  WHERE LDL_BATCH_ID = cp_batch_id
  AND LDL_DL_LOAD_STATUS = 'C';
  
  l_batch_run c_get_batch_run%ROWTYPE;
  
  CURSOR c1 (cp_batch_id VARCHAR2, cp_batch_run VARCHAR2) IS
  SELECT 'Y'
  FROM DL_CUS_DATALOAD_BATCHES  
  JOIN DL_PROCESS_SUMMARY ON dps_dlb_batch_id = ldl_batch_id
  WHERE dps_status NOT LIKE 'COM%'
  AND   dps_status NOT LIKE 'FAIL%' 
  AND ldl_dlb_batch_id = cp_batch_id
  AND ldl_batch_id != cp_batch_run
  AND LDL_DL_LOAD_STATUS = 'C';
  
  CURSOR c_get_err_count (cp_batch_id VARCHAR2, cp_batch_run VARCHAR2) IS
  SELECT SUM(NVL(DPS_FAILED_RECORDS,0))
  FROM DL_CUS_DATALOAD_BATCHES v
  JOIN DL_PROCESS_SUMMARY s ON dps_dlb_batch_id = ldl_batch_id
  WHERE ldl_dlb_batch_id = cp_batch_id
  AND ldl_batch_id = cp_batch_run
  AND LDL_DL_LOAD_STATUS = 'C'
  -- only consider processes at the current seqno
  AND NOT EXISTS ( SELECT NULL FROM DL_CUS_DATALOAD_BATCHES v1 
                   JOIN  DL_PROCESS_SUMMARY s1 ON s1.dps_dlb_batch_id = v1.ldl_batch_id
                   WHERE v1.ldl_batch_seqno > v.ldl_batch_seqno
                   AND v1.ldl_dlb_batch_id = v.ldl_dlb_batch_id 
                   AND s1.dps_process = s.dps_process
                   AND v1.LDL_DL_LOAD_STATUS = 'C' )
  -- only consider processes that are at the current stage
  AND NOT EXISTS ( SELECT NULL FROM DL_CUS_DATALOAD_BATCHES v1 
                   JOIN DL_PROCESS_SUMMARY s1 ON s1.dps_dlb_batch_id = v1.ldl_batch_id
                   WHERE v1.ldl_batch_seqno = v.ldl_batch_seqno
                   AND v1.ldl_dlb_batch_id = v.ldl_dlb_batch_id
                   AND v1.LDL_DL_LOAD_STATUS = 'C'
                   AND s1.dps_process = CASE s.dps_process WHEN 'LOAD' THEN 'VALIDATE'
                                                           WHEN 'VALIDATE' THEN 'CREATE' END)
                   ;
  -- Get all records to create
  -- all create_start must be null
  -- and no of the validate ends must be null
  CURSOR c2 ( cp_batch_id VARCHAR2, cp_batch_seqno NUMBER) IS 
  SELECT d1.ldl_refno
  ,      d1.ldl_batch_id
  ,      d1.ldl_product_area
  ,      d1.ldl_dataload_area      
  FROM DL_CUS_DATALOAD_BATCHES d1
  WHERE d1.LDL_DLB_BATCH_ID = cp_batch_id
  AND d1.ldl_batch_seqno = cp_batch_seqno
  AND d1.ldl_create_start IS NULL 
  AND (d1.LDL_DL_LOAD_STATUS = 'C' OR dl.ldl_dataload_area = 'PROPERTY_STATUES')
  AND NOT EXISTS (SELECT NULL FROM DL_CUS_DATALOAD_BATCHES d2 
                  WHERE d2.LDL_DLB_BATCH_ID = cp_batch_id
                  AND d2.ldl_batch_seqno = cp_batch_seqno 
                  AND d2.ldl_validate_end IS NULL)
  ORDER BY ldl_total_records DESC;
  
  -- Get all records to validate
  -- all the loads must be done
  -- and all the validates have not started
  CURSOR c3 ( cp_batch_id VARCHAR2, cp_batch_seqno NUMBER)  IS 
  SELECT d1.ldl_refno
  ,      d1.ldl_batch_id
  ,      d1.ldl_product_area
  ,      d1.ldl_dataload_area      
  FROM DL_CUS_DATALOAD_BATCHES d1
  WHERE d1.LDL_DLB_BATCH_ID = cp_batch_id
  AND d1.ldl_validate_start IS NULL 
  AND (d1.LDL_DL_LOAD_STATUS = 'C' OR dl.ldl_dataload_area = 'PROPERTY_STATUES')
  AND d1.ldl_batch_seqno = (SELECT MIN(d3.ldl_batch_seqno) 
                            FROM DL_CUS_DATALOAD_BATCHES d3
                            WHERE d3.LDL_DLB_BATCH_ID = d1.LDL_DLB_BATCH_ID
                            AND d3.ldl_validate_start IS NULL                       
                            )
  AND NOT EXISTS (SELECT NULL FROM DL_CUS_DATALOAD_BATCHES d2 
                  WHERE d2.LDL_DLB_BATCH_ID = cp_batch_id
                  AND d2.ldl_batch_seqno = cp_batch_seqno 
                  AND d2.ldl_load_end IS NULL)
  ORDER BY ldl_total_records DESC;
                          
  
BEGIN
   fsc_utils.proc_start('s_DL_CUS_DATALOAD_BATCHES.run_next');
   fsc_utils.debug_message( 's_DL_CUS_DATALOAD_BATCHES.run_next',3 );
   -- if the status is complete then we'll see if we can run the next stage in the batch
   
   fsc_utils.display_error(1,'FSC','Calling Run Next with p_status:'||p_status ||' p_process:'|| p_process ||' p_run_id:'|| p_run_id );
   
   IF p_status LIKE 'COM%'
   THEN
     -- make sure that the thing we're running is a batch run
     OPEN c_get_batch_run(p_run_id);
     FETCH c_get_batch_run INTO l_batch_run;
     CLOSE c_get_batch_run;
     
     IF p_process = 'LOAD'
     THEN 
        UPDATE DL_CUS_DATALOAD_BATCHES
        SET ldl_load_end      = SYSDATE
        ,   ldl_total_records = (SELECT d1.dps_total_records 
                                 FROM dl_process_summary d1
                                 WHERE d1.dps_dlb_batch_id = ldl_batch_id
                                 AND d1.dps_process = 'LOAD'
                                 AND d1.dps_created_date = (SELECT max(d2.dps_created_date) FROM dl_process_summary d2 
                                                            WHERE d1.ldl_batch_id = d2.ldl_batch_id
                                                            AND d1.dps_process = 'LOAD'))                                  
        WHERE ldl_refno = l_batch_run.ldl_refno;
     ELSE
        UPDATE DL_CUS_DATALOAD_BATCHES
        SET ldl_validate_end = CASE p_process WHEN 'VALIDATE' THEN SYSDATE ELSE ldl_validate_end END
        ,   ldl_create_end   = CASE p_process WHEN 'CREATE'   THEN SYSDATE ELSE ldl_create_end END        
        WHERE ldl_refno = l_batch_run.ldl_refno;
     END IF;
     COMMIT;

     IF l_batch_run.ldl_dlb_batch_id IS NOT NULL
     AND NVL(l_batch_run.ldl_question_answer1,'N') = 'Y' -- Continue after Load is Y
     THEN
       -- if any part of the batch is still running we're not going to do anything.
       -- when that last part finishes running it will update dl_cus_dataload_batches
       -- and then we'll kick off the next stage
       OPEN c1(l_batch_run.ldl_dlb_batch_id, p_run_id);
       FETCH c1 INTO l_still_running;
       IF c1%NOTFOUND
       THEN
         l_still_running := 'N';
       END IF;
       CLOSE c1;
       
       OPEN c_get_err_count(l_batch_run.ldl_dlb_batch_id, p_run_id);
       FETCH c_get_err_count INTO l_errors;
       IF c_get_err_count%NOTFOUND
       THEN
         l_errors := '0';
       END IF;
       CLOSE c_get_err_count;
       
       fsc_utils.display_error(1,'FSC','=> ldl_question_answer1:'||l_batch_run.ldl_question_answer1 ||' ldl_question_answer2:'|| l_batch_run.ldl_question_answer2||' l_still_running:' ||l_still_running ||' l_errors:'||l_errors);
       
       -- every thing has stopped running for the batch,
       -- now we need to see what has finished and kick off the next stage
       IF l_still_running = 'N' 
       AND (NVL(l_errors,0) = 0 OR NVL(l_batch_run.ldl_question_answer2,'N') = 'Y') -- no errors or question 2 was y
       THEN
         fsc_utils.display_error(1,'FSC','Everything has finished running '||p_process||' for '||l_batch_run.ldl_dlb_batch_id);
         -- if we just finished loading
         -- we need to validate at the lowest sequence number in the batch
         IF p_process = 'LOAD'
         THEN
            FOR idx IN c3(l_batch_run.ldl_dlb_batch_id)
            LOOP 
                fsc_utils.display_error(1,'FSC','Create Validate (after load) for '||idx.ldl_batch_id);
                BEGIN
                   a_dl_batches.validate_dataloads_api
                   ( p_dlb_batch_id                  => idx.ldl_batch_id
                   , p_batch_mode                    => 'N'
                   , p_session_id                    => 1
                   , p_commit                        => TRUE);

                   UPDATE DL_CUS_DATALOAD_BATCHES
                   SET ldl_validate_start = SYSDATE
                   WHERE ldl_refno = idx.ldl_refno;
                   COMMIT;

                EXCEPTION
                WHEN OTHERS THEN
                  fsc_utils.display_error(1,'FSC','Failed to Create Validate (after load) for '||idx.ldl_batch_id);
                END;
            END LOOP;
         END IF;
         
         -- if we just finished validating 
         -- we need to kick of the create at the same sequence number
         -- 
         IF p_process = 'VALIDATE' 
         THEN
            FOR idx IN c2(l_batch_run.ldl_dlb_batch_id, l_batch_run.ldl_batch_seqno)
            LOOP
              fsc_utils.display_error(1,'FSC','Create create for '||idx.ldl_batch_id);
              BEGIN
                  a_dl_batches.create_dataloads_api
                  ( p_dlb_batch_id                   => idx.ldl_batch_id
                  , p_dlb_dla_product_area           => idx.ldl_product_area
                  , p_dlb_dla_dataload_area          => idx.ldl_dataload_area
                  , p_batch_mode                     => 'N'
                  , p_session_id                     => 1
                  , p_commit                         => TRUE);

                  UPDATE DL_CUS_DATALOAD_BATCHES
                  SET ldl_create_start = SYSDATE
                  WHERE ldl_refno = idx.ldl_refno;
                  COMMIT;
              EXCEPTION
                WHEN OTHERS THEN
                  fsc_utils.display_error(1,'FSC','Failed to Create create for '||idx.ldl_batch_id);
              END;
            END LOOP;
         END IF;
         
         -- if we just finished creating then we need to find
         -- the next lowest validate to start
         IF  p_process = 'CREATE' 
         THEN
           FOR idx IN c3(l_batch_run.ldl_dlb_batch_id, l_batch_run.ldl_batch_seqno)
           LOOP 
              fsc_utils.display_error(1,'FSC','Create validate (after create) for '||idx.ldl_batch_id);
              BEGIN
               a_dl_batches.validate_dataloads_api
               ( p_dlb_batch_id                 => idx.ldl_batch_id
               , p_batch_mode                    => 'N'
               , p_session_id                    => 1
               , p_commit                        => TRUE);

               UPDATE DL_CUS_DATALOAD_BATCHES
               SET ldl_validate_start = SYSDATE
               WHERE ldl_refno = idx.ldl_refno;
               COMMIT;

              EXCEPTION
                WHEN OTHERS THEN
                  fsc_utils.display_error(1,'FSC','Failed to Create validate (after create) for '||idx.ldl_batch_id);
              END;                           
            END LOOP;
         END IF;
       END IF;     
    END IF;
  END IF;
  fsc_utils.proc_end;  
EXCEPTION
  WHEN OTHERS THEN
    fsc_utils.display_error(1,'FSC','Something went wrong!' );
    fsc_utils.proc_end;
END RUN_NEXT;
----
----
END S_DL_CUS_DATALOAD_BATCHES;
/

