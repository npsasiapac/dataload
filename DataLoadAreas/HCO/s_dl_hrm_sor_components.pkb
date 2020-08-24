CREATE OR REPLACE PACKAGE BODY s_dl_hrm_sor_components
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION WHO            WHEN       	WHY
--      1.0 Paul Bouchier  02/02/05   	Dataload
--
--      1.1 Vishad Shah    23-FEB-2007 	General ISG Tidy up and get it working
--                                    	Removing reference to the cross reference table/function.
--					To link it the right sor cmpt specification all you need
--					is the sor_code and start date which is unique to get the
--                                      sor cmpt specs refno.
--
--      2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      2.1 5.13.0   PH   04-MAr-2008 Included rec_rowid in all cursors
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
  UPDATE dl_hrm_sor_components
  SET lscmp_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_sor_components');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--  declare package variables and constants
--
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) 
IS
SELECT rowid   rec_rowid,
       lscmp_dlb_batch_id,
       lscmp_dl_seqno,
       lscmp_dl_load_status,
       lscmp_sor_code,
       lscmp_start_date,
       lscmp_scmt_code,
       lscmp_cost,
       lscmp_percentage_ind
  FROM dl_hrm_sor_components
 WHERE lscmp_dlb_batch_id   = p_batch_id
   AND lscmp_dl_load_status = 'V';
--
-- **********************************************************
--
CURSOR get_sor_cmpt_specs_refno(p_sor_code	VARCHAR2,
                                p_start_date	DATE)
IS
SELECT scsp_refno
  FROM sor_cmpt_specifications
 WHERE scsp_sor_code = p_sor_code
   AND scsp_start_date = p_start_date;
--
-- **********************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'CREATE';
ct       		VARCHAR2(40) := 'DL_HRM_SOR_COMPONENTS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id     ROWID;
l_an_tab 		VARCHAR2(1);
--
-- **********************************************************
--
-- Other variables
--
i            		INTEGER := 0;
l_scmp_scsp_refno	NUMBER(10);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_sor_components.dataload_create');
    fsc_utils.debug_message('s_dl_hrm_sor_components.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1(p_batch_id) LOOP
--
      BEGIN
--       
          cs := p1.lscmp_dl_seqno;
          l_id := p1.rec_rowid;
--                    
-- Get SOR Component Specification Refno
--              
          l_scmp_scsp_refno := NULL;
--
           OPEN get_sor_cmpt_specs_refno(p1.lscmp_sor_code, p1.lscmp_start_date);
          FETCH get_sor_cmpt_specs_refno INTO l_scmp_scsp_refno;
          CLOSE get_sor_cmpt_specs_refno;
--
--                    
-- Create sor_component record
--              
          INSERT INTO sor_components (SCMP_SCSP_REFNO,               
                                      SCMP_SCMT_CODE,             
                                      SCMP_COST,                      
                                      SCMP_PERCENTAGE_IND
                                     )
--
                              VALUES (l_scmp_scsp_refno,                          
                                      p1.lscmp_scmt_code,                
                                      p1.lscmp_cost, 
                                      p1.lscmp_percentage_ind
                                     ); 
--
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000) = 0 THEN 
           COMMIT; 
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'C');
--
          EXCEPTION 
               WHEN OTHERS THEN
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
               set_record_status_flag(l_id,'O');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
--
-- Section to analyze the table(s) populated by this dataload
--
    fsc_utils.proc_end;
    commit;
--
    EXCEPTION 
         WHEN OTHERS THEN
         set_record_status_flag(l_id,'O');
         s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
END dataload_create;
--
-- *******************************************************************************************************
--
PROCEDURE dataload_validate(p_batch_id	IN VARCHAR2,
                            p_date	IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid   rec_rowid,
       lscmp_dlb_batch_id,             
       lscmp_dl_seqno,                 
       lscmp_dl_load_status,                              
       lscmp_sor_code,
       lscmp_start_date,                
       lscmp_scmt_code,
       lscmp_cost,
       lscmp_percentage_ind
  FROM dl_hrm_sor_components
 WHERE lscmp_dlb_batch_id     = p_batch_id
   AND lscmp_dl_load_status  IN ('L','F','O');
--
-- **********************************************************
--
CURSOR chk_component_type(p_scmt_code VARCHAR2)
IS
SELECT 'X'
  FROM sor_component_types
 WHERE scmt_code = p_scmt_code;
--
-- **********************************************************
--
CURSOR get_sor_cmpt_specs_refno(p_sor_code	VARCHAR2,
                                p_start_date	DATE)
IS
SELECT scsp_refno
  FROM sor_cmpt_specifications
 WHERE scsp_sor_code   = p_sor_code
   AND scsp_start_date = p_start_date;
--
-- **********************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'VALIDATE';
ct       	VARCHAR2(40) := 'DL_HRM_SOR_COMPONENTS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     ROWID;
--
i               INTEGER :=0;
l_errors        VARCHAR2(10);
l_error_ind     VARCHAR2(10);
--
--
-- Other variables
--
l_dummy             		VARCHAR2(10);
l_component_type_exists		VARCHAR2(1);
l_scmp_scsp_refno		NUMBER(10); 
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_sor_components.dataload_validate');
    fsc_utils.debug_message('s_dl_hrm_sor_components.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lscmp_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- **********************************************************
--
-- CHECK For MANDATORY FIELDS
--
--
-- CHECK Component Spec SOR Code is Supplied
--
          IF (p1.lscmp_sor_code IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',235);
--
          END IF;
--
--
-- CHECK Component Spec Start Date is Supplied
--
          IF (p1.lscmp_start_date IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',236);
--
          END IF;
--
-- 
-- CHECK SOR Component Type is Supplied
--
          IF (p1.lscmp_scmt_code IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',227);
--
          END IF;
--
-- 
-- CHECK SOR Component Cost is Supplied
--
          IF (p1.lscmp_cost IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',228);
--
          END IF;
--
-- 
-- CHECK SOR Component Indicator is Supplied
--
          IF (p1.lscmp_percentage_ind IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',229);
--
          END IF;
--
-- **********************************************************
--
--
-- Check SOR Component Specification exists for SOR/Start Date combination
--
-- 
          IF (    p1.lscmp_sor_code IS NOT NULL
              AND p1.lscmp_start_date IS NOT NULL) THEN
--
           l_scmp_scsp_refno := NULL;
--
            OPEN get_sor_cmpt_specs_refno(p1.lscmp_sor_code,p1.lscmp_start_date);
           FETCH get_sor_cmpt_specs_refno INTO l_scmp_scsp_refno;
           CLOSE get_sor_cmpt_specs_refno;
--
           IF (l_scmp_scsp_refno IS NULL) THEN
--
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',230);
--
           END IF;
--
          END IF;   
--
-- **********************************************************
--
--
-- Check SOR Component Type Code supplied is valid.
--
--
          IF (p1.lscmp_scmt_code IS NOT NULL) THEN
--
            l_component_type_exists := NULL;
--
            OPEN chk_component_type(p1.lscmp_scmt_code);
           FETCH chk_component_type INTO l_component_type_exists;
           CLOSE chk_component_type;
--
           IF (l_component_type_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',231);
           END IF;
--
          END IF;
--
-- **********************************************************
--
-- Check Percentage Indicator supplied is valid.
--
--
          IF (p1.lscmp_percentage_ind IS NOT NULL) THEN
--
           IF (p1.lscmp_percentage_ind NOT IN ('Y','N')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',232);
           END IF;
--
          END IF;
--
-- **********************************************************
--
-- Check Cost supplied is valid.
--
--
          IF (p1.lscmp_cost IS NOT NULL) THEN
--
           IF (p1.lscmp_cost < 0) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',233);
           END IF;
--
          END IF;
--
-- **********************************************************
--
-- Check SOR Component already exists
--
--
          IF (    l_scmp_scsp_refno IS NOT NULL 
              AND l_component_type_exists IS NOT NULL) THEN

           IF s_sor_components.check_scmp_exists(l_scmp_scsp_refno,
                                                 p1.lscmp_scmt_code) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',234);
--
           END IF;
--
          END IF;
--
-- **********************************************************
--
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
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
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
    COMMIT;
--
    fsc_utils.proc_END;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
-- *******************************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 
IS
SELECT rowid   rec_rowid,
       lscmp_dlb_batch_id,             
       lscmp_dl_seqno,                 
       lscmp_dl_load_status,                              
       lscmp_sor_code,
       lscmp_start_date,                
       lscmp_scmt_code,
       lscmp_cost,
       lscmp_percentage_ind
  FROM dl_hrm_sor_components
 WHERE lscmp_dlb_batch_id   = p_batch_id
   AND lscmp_dl_load_status = 'C';
--
-- **********************************************************
--
CURSOR get_sor_cmpt_specs_refno(p_sor_code	VARCHAR2,
                                p_start_date	DATE)
IS
SELECT scsp_refno
  FROM sor_cmpt_specifications
 WHERE scsp_sor_code   = p_sor_code
   AND scsp_start_date = p_start_date;
--
-- **********************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'DELETE';
ct       		VARCHAR2(40) := 'DL_HRM_SOR_COMPONENTS';
cs       		INTEGER;
ce			VARCHAR(200);
l_id     ROWID;
--
i        		INTEGER := 0;
l_scmp_scsp_refno	NUMBER(10);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_sor_components.dataload_delete');
    fsc_utils.debug_message('s_dl_hrm_sor_components.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lscmp_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Get the sor component specs refno
--
          l_scmp_scsp_refno := NULL;
--
           OPEN get_sor_cmpt_specs_refno(p1.lscmp_sor_code,p1.lscmp_start_date);
          FETCH get_sor_cmpt_specs_refno INTO l_scmp_scsp_refno;
          CLOSE get_sor_cmpt_specs_refno;
--
--
-- Delete from sor components table, using the sor component specification refno 
-- and the sor component type. This is the unique primary key
--
--
         DELETE 
           FROM sor_components
          WHERE scmp_scsp_refno = l_scmp_scsp_refno
            AND scmp_scmt_code  = p1.lscmp_scmt_code;
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
    fsc_utils.proc_end;
    commit;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
         RAISE;
--
END dataload_delete;
--
--
END s_dl_hrm_sor_components;
/