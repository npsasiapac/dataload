CREATE OR REPLACE PACKAGE BODY s_dl_hco_operative_skills
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Vers  WHO  WHEN       WHY
--      1.0           MJK  31/01/05   Dataload
--
--      2.0          VRS  14/07/06   Tidy Up
--      2.1 5.10.0   PH   27/07/06   Added Savepoints
--      3.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      3.1 5.13.0   PH   04-MAR-2008 Added Preference field
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
  UPDATE dl_hco_operative_skills
  SET loskl_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_operative_skills');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--
--
--
--  declare package variables AND constants
--
--
-- ****************************************************************************************
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       loskl_dlb_batch_id,
       loskl_dl_seqno,
       loskl_dl_load_status,
       loskl_ipp_shortname,
       loskl_ipt_code,
       loskl_sor_code,
       loskl_proficiency_pct,
       NVL(loskl_current_ind,'Y')	loskl_current_ind,
       loskl_preference
  FROM dl_hco_operative_skills
 WHERE loskl_dlb_batch_id   = p_batch_id
   AND loskl_dl_load_status = 'V';
--
-- ***************************************************************************
--
CURSOR get_ipp_refno(cp_ipp_shortname VARCHAR2,
                     cp_ipt_code      VARCHAR2) 
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = cp_ipp_shortname
   AND ipp_ipt_code  = cp_ipt_code;
--
-- ***************************************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'CREATE';
ct       	VARCHAR2(30) := 'DL_HCO_OPERATIVE_SKILLS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_an_tab 	VARCHAR2(1);
l_id     ROWID;
--
-- Other variables
--
i            	INTEGER := 0;
l_ipp_refno	NUMBER(10);
--
--
BEGIN
    fsc_utils.proc_start('s_dl_hco_operative_skills.dataload_create');
    fsc_utils.debug_message('s_dl_hco_operative_skills.dataload_create',3);
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
          cs := p1.loskl_dl_seqno;
          l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get ipp_refno
--
          l_ipp_refno := NULL;
--
           OPEN get_ipp_refno(p1.loskl_ipp_shortname,p1.loskl_ipt_code);
          FETCH get_ipp_refno INTO l_ipp_refno;
          CLOSE get_ipp_refno;
--
-- Create operative_skills record
--
          INSERT INTO operative_skills(oskl_ipp_refno,
                                       oskl_sor_code,
                                       oskl_proficiency_pct,
                                       oskl_current_ind,
                                       oskl_preference
                                      )
--
                               VALUES (l_ipp_refno,
                                       p1.loskl_sor_code,
                                       p1.loskl_proficiency_pct,
                                       p1.loskl_current_ind,
                                       p1.loskl_preference
                                      );
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
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
               set_record_status_flag(l_id,'O');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab := s_dl_hou_utils.dl_comp_stats('OPERATIVE_SKILLS');
--
    fsc_utils.proc_end;
--
    COMMIT;
--

    EXCEPTION 
         WHEN OTHERS THEN
         set_record_status_flag(l_id,'O');
         s_dl_process_summary.update_summary(cb,cp,cd,'FAILED'); 
--
END dataload_create;
--
-- ****************************************************************************************
--
--
-- As defined in FUNCTION H400.60.10.40.10.20
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
      			    p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       loskl_dlb_batch_id,
       loskl_dl_seqno,
       loskl_dl_load_status,
       loskl_ipp_shortname,
       loskl_ipt_code,
       loskl_sor_code,
       loskl_proficiency_pct,
       NVL(loskl_current_ind,'Y')	loskl_current_ind,
       loskl_preference
  FROM dl_hco_operative_skills
 WHERE loskl_dlb_batch_id    = p_batch_id
   AND loskl_dl_load_status in ('L','F','O');
--
-- ***************************************************************************
--
CURSOR get_ipp_refno(p_ipp_shortname VARCHAR2,
                     p_ipt_code      VARCHAR2) 
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipt_code;
--
-- ***************************************************************************
--
CURSOR chk_ipt_code(p_ipt_code VARCHAR2) 
IS
SELECT 'X'
  FROM interested_party_types
 WHERE ipt_code = p_ipt_code;
--
-- ***************************************************************************
--
CURSOR chk_oskl_exists(p_ipp_refno NUMBER,
                       p_sor_code  VARCHAR2) 
IS
SELECT 'X'
  FROM operative_skills
 WHERE oskl_ipp_refno = p_ipp_refno
   AND oskl_sor_code  = p_sor_code;
--
-- ***************************************************************************
--
CURSOR chk_sor_exists(p_sor_code  VARCHAR2) 
IS
SELECT 'X'
  FROM schedule_of_rates
 WHERE sor_code = p_sor_code;
--
-- ***************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'VALIDATE';
ct       		VARCHAR2(30) := 'DL_HCO_OPERATIVE_SKILLS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id     ROWID;
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
-- Other variables
--
l_dummy             	VARCHAR2(2000);
l_ipt_exists		VARCHAR2(1);
l_sor_exists		VARCHAR2(1);
l_oskl_exists		VARCHAR2(1);
l_date              	DATE;
l_ipp_refno		NUMBER(10);
--
--
BEGIN
    fsc_utils.proc_start('s_dl_hco_operative_skills.dataload_validate');
    fsc_utils.debug_message('s_dl_hco_operative_skills.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 IN c1  LOOP
--
      BEGIN
--
          cs := p1.loskl_dl_seqno;
--
          l_errors := 'V';
          l_error_ind := 'N';
          l_id := p1.rec_rowid;
--
-- *********************************
--                                 *
-- VALIDATE FIELDS       	   *
--                                 *
-- *********************************
--
-- ***************************************************************************
--
-- Validate interested party type. Check to see that it has been supplied and
-- valid
--
          IF (p1.loskl_ipt_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',021);
          ELSE
--
              OPEN chk_ipt_code(p1.loskl_ipt_code);
             FETCH chk_ipt_code INTO l_ipt_exists;
             CLOSE chk_ipt_code;
--
             IF (l_ipt_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',153);
             END IF;
--
          END IF;
--
-- ***************************************************************************
--      
-- Validate interested party. Check to see that it has been supplied
--
          IF (p1.loskl_ipp_shortname IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',022);
          END IF;
--
-- ***************************************************************************
--
-- Validate the interested party and Party Type. Check to see that it exists in 
-- Interested parties.
--      
          IF (    p1.loskl_ipp_shortname IS NOT NULL
              AND p1.loskl_ipt_code      IS NOT NULL) THEN
--
           l_ipp_refno := NULL;
--
            OPEN get_ipp_refno(p1.loskl_ipp_shortname,p1.loskl_ipt_code);
           FETCH get_ipp_refno INTO l_ipp_refno;
           CLOSE get_ipp_refno;
--
           IF (l_ipp_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',190);
           END IF;
--
          END IF;
--
-- ***************************************************************************
--
-- Validate sor code. Check to see that it has been supplied and valid
--
          IF (p1.loskl_sor_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',023); 
          ELSE
--
             l_sor_exists := NULL;
--
              OPEN chk_sor_exists(p1.loskl_sor_code);
             FETCH chk_sor_exists INTO l_sor_exists;
             CLOSE chk_sor_exists;
--
             IF (l_sor_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',140);
             END IF;
--
          END IF;
--
-- ***************************************************************************
--      
-- Validate proficiency pct. Check to see that it has been supplied and valid
--
          IF (p1.loskl_proficiency_pct IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',024);
--
          ELSIF (   p1.loskl_proficiency_pct < 0
                 OR p1.loskl_proficiency_pct > 100) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',192);
--
          END IF;
--
-- ***************************************************************************
--  
-- Validate current ind is Y or N. This field will be defaulted 'Y' if left blank.
--
          IF (p1.loskl_current_ind NOT IN ('Y','N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);
          END IF;
--
-- ***************************************************************************
--  
-- Validate for record exists on operative_skills
--
          IF (    l_ipp_refno  IS NOT NULL
              AND l_sor_exists IS NOT NULL) THEN
--
           l_oskl_exists := NULL;
--
            OPEN chk_oskl_exists(l_ipp_refno, p1.loskl_sor_code);
           FETCH chk_oskl_exists INTO l_oskl_exists;
           CLOSE chk_oskl_exists;
--
           IF (l_oskl_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',025);
           END IF;
--
          END IF;
--
--
-- ***************************************************************************
--
-- Validate Preference
--
         IF (nvl(p1.loskl_preference, 'X') NOT IN 
                 ('1', '6', '7', '8', '9', 'M', '2', '3', '4', '5')
            )
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',522);
         END IF;
--
-- ***************************************************************************
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
-- ****************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
CURSOR c1 
IS
SELECT rowid rec_rowid,
       loskl_dlb_batch_id,
       loskl_dl_seqno,
       loskl_dl_load_status,
       loskl_ipp_shortname,
       loskl_ipt_code,
       loskl_sor_code,
       loskl_proficiency_pct,
       NVL(loskl_current_ind,'Y')	loskl_current_ind        
  FROM dl_hco_operative_skills
 WHERE loskl_dlb_batch_id   = p_batch_id
   AND loskl_dl_load_status = 'C';
--
-- ***************************************************************************
--
CURSOR get_ipp_refno(p_ipp_shortname VARCHAR2,
                     p_ipt_code      VARCHAR2) 
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipt_code;
--
-- ***************************************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HCO_OPERATIVE_SKILLS';
cs       	INTEGER;
ce              VARCHAR2(200);
l_an_tab        VARCHAR2(1);
l_id     ROWID;
--
i        	INTEGER := 0;
l_ipp_refno     NUMBER(10);
--
--
BEGIN
    fsc_utils.proc_start('s_dl_hco_operative_skills.dataload_delete');
    fsc_utils.debug_message('s_dl_hco_operative_skills.dataload_delete',3 );
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
          cs := p1.loskl_dl_seqno;
          l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get the ipp_refno
--
          l_ipp_refno := NULL;
--
           OPEN get_ipp_refno(p1.loskl_ipp_shortname,p1.loskl_ipt_code);
          FETCH get_ipp_refno INTO l_ipp_refno;
          CLOSE get_ipp_refno;
--
-- Delete from operative_skills
--          
          DELETE 
            FROM operative_skills
           WHERE oskl_ipp_refno = l_ipp_refno
             AND oskl_sor_code  = p1.loskl_sor_code;
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
EXCEPTION
WHEN OTHERS 
THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
    COMMIT;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('OPERATIVE_SKILLS');
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
         set_record_status_flag(l_id,'O');
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
END s_dl_hco_operative_skills;
/