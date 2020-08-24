CREATE OR REPLACE PACKAGE BODY s_dl_hrm_sor_effort
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Vers  WHO            WHEN       WHY
--      1.0          P. Bouchier  31/01/05   Dataload
--
--      2.0          V Shah       06/07/06   General Tidy up
--      2.1 5.10.0   PH           26/07/06   Added DB Vers to this section.
--
--      3.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hrm_sor_effort
  SET lseff_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_sor_effort');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
-- 
--
--  declare package variables and constants
--
-- ***********************************************************************************
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT rowid rec_rowid,
       LSEFF_DLB_BATCH_ID,             
       LSEFF_DL_SEQNO,
       LSEFF_DL_LOAD_STATUS,
       LSEFF_REFNO,
       LSEFF_SOR_CODE,
       LSEFF_START_DATE,
       LSEFF_ESTIMATED_EFFORT,
       NVL(LSEFF_ESTIMATED_EFFORT_UNIT,'M')	LSEFF_ESTIMATED_EFFORT_UNIT,
       NVL(LSEFF_EFFORT_DRIVEN_IND,'N')		LSEFF_EFFORT_DRIVEN_IND,
       NVL(LSEFF_MIN_OPERATIVES,1)		LSEFF_MIN_OPERATIVES,
       LSEFF_MAX_OPERATIVES,
       LSEFF_END_DATE,
       LSEFF_NEXT_JOB_DELAY_TIME,
       LSEFF_NXT_JOB_DEL_TIME_UNIT
  FROM dl_hrm_sor_effort
 WHERE lseff_dlb_batch_id   = p_batch_id
   AND lseff_dl_load_status = 'V';
--
-- ***************************************************************************
--
-- Constants for process_summary
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'CREATE';
ct       	VARCHAR2(30) := 'DL_HRM_SOR_EFFORT';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     ROWID;
l_an_tab 	VARCHAR2(1);
--
-- Other variables
--
i            INTEGER := 0;
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_sor_effort.dataload_create');
    fsc_utils.debug_message( 's_dl_hrm_sor_effort.dataload_create',3);
--
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1(p_batch_id) loop
--
      BEGIN
-- 
          cs := p1.lseff_dl_seqno;
          l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--    
-- Create sor_effort record
--
          INSERT INTO sor_effort (SEFF_REFNO,                     
           			  SEFF_SOR_CODE,
           			  SEFF_START_DATE,
           			  SEFF_ESTIMATED_EFFORT,
           			  SEFF_ESTIMATED_EFFORT_UNIT,
           			  SEFF_EFFORT_DRIVEN_IND,
				  SEFF_MIN_OPERATIVES,
               	                  SEFF_MAX_OPERATIVES,
                                  SEFF_END_DATE,
                                  SEFF_NEXT_JOB_DELAY_TIME,
                                  SEFF_NXT_JOB_DEL_TIME_UNIT
                                 )
--
                           VALUES(p1.lseff_refno,
                                  p1.LSEFF_SOR_CODE ,                
                                  p1.LSEFF_START_DATE,               
           			  p1.LSEFF_ESTIMATED_EFFORT,         
           			  p1.LSEFF_ESTIMATED_EFFORT_UNIT,    
           			  p1.LSEFF_EFFORT_DRIVEN_IND,        
           			  p1.LSEFF_MIN_OPERATIVES,
           			  p1.LSEFF_MAX_OPERATIVES,           
           			  p1.LSEFF_END_DATE,                 
                                  p1.LSEFF_NEXT_JOB_DELAY_TIME,      
           			  p1.LSEFF_NXT_JOB_DEL_TIME_UNIT
                                 );                                    
--
--
-- keep a count of the rows processed and commit after every 1000
--
      i := i+1; 
--
      IF MOD(i,1000)=0 THEN 
       COMMIT; 
      END IF;
--
--
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'C');
--
      EXCEPTION
           WHEN OTHERS THEN
           ROLLBACK TO SP1;
           ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
           set_record_status_flag(l_id,'O');
           s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab := s_dl_hou_utils.dl_comp_stats('SOR_EFFORT');
--
    fsc_utils.proc_end;
--
    commit;
--
    EXCEPTION
         WHEN OTHERS THEN
         set_record_status_flag(l_id,'O');
         s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
END dataload_create;
--
--
-- ***************************************************************************
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
      			    p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       LSEFF_DLB_BATCH_ID,             
       LSEFF_DL_SEQNO,
       LSEFF_DL_LOAD_STATUS,
       LSEFF_REFNO,
       LSEFF_SOR_CODE,
       LSEFF_START_DATE,
       LSEFF_ESTIMATED_EFFORT,
       NVL(LSEFF_ESTIMATED_EFFORT_UNIT,'M')	LSEFF_ESTIMATED_EFFORT_UNIT,
       NVL(LSEFF_EFFORT_DRIVEN_IND,'N')		LSEFF_EFFORT_DRIVEN_IND,
       NVL(LSEFF_MIN_OPERATIVES,1)		LSEFF_MIN_OPERATIVES,
       LSEFF_MAX_OPERATIVES,
       LSEFF_END_DATE,
       LSEFF_NEXT_JOB_DELAY_TIME,
       LSEFF_NXT_JOB_DEL_TIME_UNIT
  FROM dl_hrm_sor_effort
 WHERE lseff_dlb_batch_id    = p_batch_id
   AND lseff_dl_load_status  IN ('L','F','O');
--
-- ***************************************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'VALIDATE';
ct       	VARCHAR2(30) := 'DL_HRM_SOR_EFFORT';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     ROWID;
--
l_errors        VARCHAR2(10);
l_error_ind     VARCHAR2(10);
i               INTEGER :=0;
--
-- Other variables
--
l_dummy             VARCHAR2(10);
l_is_inactive       BOOLEAN DEFAULT FALSE; 
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_sor_effort.dataload_validate');
    fsc_utils.debug_message( 's_dl_hrm_sor_effort.dataload_validate',3);
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
          cs := p1.lseff_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- *********************************
--                                 *
-- VALIDATE FIELDS       	   *
--                                 *
-- *********************************
--
--
-- *******************************************************************************
--
-- Validate the SOR CODE. Check to see that it is supplied and Valid.
-- 
          IF (p1.lseff_sor_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',143);
--
	  ELSIF (NOT s_schedule_of_rates.check_sor_exists(p1.lseff_sor_code)) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',140);
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Start date. check to see that it has been supplied.
--
          IF (p1.lseff_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',144);
          END IF;
--
-- *******************************************************************************
--
-- Validate the Estimated Effort. Check to see that it has been supplied.

          IF (p1.lseff_estimated_effort IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',145);
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Estimated Effort Unit. Check to see that it has been supplied and is 
-- M/H/D. Note that this will default to M if not supplied so it should never be null.
--
          IF (p1.lseff_estimated_effort_unit IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',146);
--
          ELSIF (p1.lseff_estimated_effort_unit NOT IN ( 'M','H','D')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',149);
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Effort Driven Indicator. Check to see that it has been supplied and is
-- Y/N. Note that this will default to N if not supplied so it should never be null.
--
          IF (p1.lseff_effort_driven_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',147);
--
          ELSIF (p1.lseff_effort_driven_ind NOT IN ( 'Y', 'N' )) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',150);
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Minimum Operative. Check to see that it has been supplied.
-- Note that this will default to 1 if not supplied so it should never be null.
--
          IF (p1.lseff_min_operatives IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',148);
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Next Job Delay Time is not < 0
--
          IF (    p1.lseff_next_job_delay_time IS NOT NULL 
              AND p1.lseff_next_job_delay_time < 0 ) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',175);
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Max Operative. Check to see that it is not < 0
--
          IF (    p1.lseff_max_operatives IS NOT NULL 
              AND p1.lseff_max_operatives < 1) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',173);
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Min Operative. Check to see that it is not < 0
--
          IF (    p1.lseff_min_operatives IS NOT NULL 
              AND p1.lseff_min_operatives < 1 ) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',174);
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Next Del Time Unit. If Supplied it is M/H/D
--
          IF (p1.lseff_nxt_job_del_time_unit IS NOT NULL) THEN
--
           IF (p1.lseff_nxt_job_del_time_unit NOT IN ( 'M','H','D')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',172);
           END IF;
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate that an record for SOR and Start Date does not already exist
--
          IF (    p1.lseff_sor_code       IS NOT NULL
              AND p1.lseff_start_date     IS NOT NULL) THEN
--
           IF (s_sor_effort.check_sor_effort_exists(p1.lseff_sor_code,p1.lseff_start_date)) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',141);
           END IF;
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate for Checks performed by Trigger(seff_ar_iu) on table SOR_EFFORT
--
          IF (p1.lseff_start_date > NVL(p1.lseff_end_date,p1.lseff_start_date)) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',136);
          END IF;
--
--
          IF (    p1.lseff_effort_driven_ind      = 'Y'
              AND NVL(p1.lseff_max_operatives,-1) < 1) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',137);
          END IF;
--
-- 
          IF (    p1.lseff_effort_driven_ind = 'N'
              AND p1.lseff_max_operatives    IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',138);
--
          END IF;
--
-- 
          IF (    p1.lseff_next_job_delay_time   IS NOT NULL
              AND p1.lseff_nxt_job_del_time_unit IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',139);
--
          END IF;
--
--
          IF (    p1.lseff_next_job_delay_time   IS NULL
              AND p1.lseff_nxt_job_del_time_unit IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',139);
--
          END IF;
--
-- *******************************************************************************
--
-- Now UPDATE the record count and error code
--
          IF (l_errors = 'F') THEN
           l_error_ind := 'Y';
          ELSE
             l_error_ind := 'N';
          END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
          set_record_status_flag(l_id,l_errors);
-- 
          EXCEPTION
               WHEN OTHERS THEN
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
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
-- ***************************************************************************
--
PROCEDURE dataload_delete(p_batch_id        IN VARCHAR2,
                          p_date            IN DATE) IS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       lseff_dlb_batch_id,
       lseff_dl_seqno,
       lseff_dl_load_status,
       lseff_refno
  FROM dl_hrm_sor_effort
 WHERE lseff_dlb_batch_id   = p_batch_id
   AND lseff_dl_load_status = 'C';
--
-- ***************************************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HRM_SOR_EFFORT';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     ROWID;
--
i        	INTEGER := 0;
l_an_tab 	VARCHAR2(1);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_sor_effort.dataload_delete');
    fsc_utils.debug_message( 's_dl_hrm_sor_effort.dataload_delete',3 );
--
    cp := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lseff_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
--
-- Delete from sor efforts table, using the lseff_refno. This is the unique primary key
--
          DELETE 
            FROM sor_effort
           WHERE seff_refno = p1.lseff_refno;
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          EXCEPTION
               WHEN OTHERS THEN
               ROLLBACK TO SP1;
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
               set_record_status_flag(l_id,'C');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab := s_dl_hou_utils.dl_comp_stats('SOR_EFFORT');
--
    COMMIT;
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
         RAISE;
--
END dataload_delete;
--
--
END s_dl_hrm_sor_effort;
/

