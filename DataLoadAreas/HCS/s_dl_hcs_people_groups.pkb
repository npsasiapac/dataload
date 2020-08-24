CREATE OR REPLACE PACKAGE BODY s_dl_hcs_people_groups
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  VERSION  DB Ver   WHO            WHEN         WHY
--      1.0  6.18     Rob Heath      25-JAN-2019  Initial Creation for SAHT
--                                                New Data Load
--                                    
-- 
-- ***********************************************************************
--
--  declare package variables AND constants
--
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
   UPDATE dl_hcs_people_groups
   SET    lpeg_dl_load_status = p_status
   WHERE  rowid = p_rowid;
--
EXCEPTION
   WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hcs_people_groups');
      RAISE;
--
END set_record_status_flag;
--
--
PROCEDURE dataload_create
   (p_batch_id          IN VARCHAR2
   ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       lpeg_dlb_batch_id,
       lpeg_dl_seqno,
       lpeg_dl_load_status,
       lpeg_code,
       lpeg_description,
       lpeg_start_date,
       lpeg_pgt_code,
       lpeg_sco_code,
       lpeg_comments,
       lpeg_aun_code,
       lpeg_end_date,
       lpeg_created_date,
       lpeg_created_by
FROM  dl_hcs_people_groups
WHERE lpeg_dlb_batch_id   = p_batch_id
AND   lpeg_dl_load_status = 'V';
--
-- *****************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'dl_hcs_people_groups';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- Other variables
--
l_id     ROWID;
l_an_tab VARCHAR2(1);
i        INTEGER := 0;
--
-- *****************************
--
BEGIN
--
   fsc_utils.proc_start('s_'||ct||'.dataload_create');
   fsc_utils.debug_message('s_'||ct||'.dataload_create',3);
--
   cb := p_batch_id;
   cd := p_date;
--
   s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
   FOR p1 in c1 LOOP
      BEGIN
--
         cs   := p1.lpeg_dl_seqno;
         l_id := p1.rec_rowid;
--
         SAVEPOINT SP1;
--
         INSERT INTO people_groups
            (peg_code,
             peg_description,
             peg_start_date,
             peg_pgt_code,
             peg_sco_code,
             peg_reusable_refno,
             peg_comments,
             peg_aun_code,
             peg_end_date)
         VALUES
            (p1.lpeg_code,
             p1.lpeg_description,
             p1.lpeg_start_date,
             p1.lpeg_pgt_code,
             p1.lpeg_sco_code,
             reusable_refno_seq.nextval,
             p1.lpeg_comments,
             p1.lpeg_aun_code,
             p1.lpeg_end_date);
--
-- Now update record with created date as trigger sets it to sysdate
-- only needed if creation date supplied otherwise sysdate is fine   
--
         IF p1.lpeg_created_date IS NOT NULL AND p1.lpeg_created_by IS NOT NULL
         THEN
            UPDATE people_groups
            SET    peg_created_date = p1.lpeg_created_date,
                   peg_created_by = p1.lpeg_created_by
            WHERE  peg_code = p1.lpeg_code;
         END IF;
--
--       Now UPDATE the record status and process count
--
         i := i + 1;
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
      END;
--
   END LOOP;
--
   COMMIT;
--
-- ***********************************************************************
-- Section to analyze the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('PEOPLE_GROUPS');
--
   fsc_utils.proc_END;
--
EXCEPTION
   WHEN OTHERS THEN
     s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
     RAISE;
END dataload_create;
--
-- ***********************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       lpeg_dlb_batch_id,
       lpeg_dl_seqno,
       lpeg_dl_load_status,
       lpeg_code,
       lpeg_description,
       lpeg_start_date,
       lpeg_pgt_code,
       lpeg_sco_code,
       lpeg_comments,
       lpeg_aun_code,
       lpeg_end_date,
       lpeg_created_date
FROM   dl_hcs_people_groups
WHERE  lpeg_dlb_batch_id   = p_batch_id
AND    lpeg_dl_load_status IN ('L','F','O');
--
-- *****************************
-- Check for duplicate People Group Code
--
CURSOR chk_peg_code(p_peg_code VARCHAR2)
IS
SELECT 'X'
  FROM people_groups
 WHERE peg_code = p_peg_code;
--
-- *****************************
-- Check for duplicate People Group Code in data load batch
CURSOR chk_dup_dl (p_dlb_batch_id  VARCHAR2
                  ,p_dl_seqno      INTEGER
                  ,p_peg_code      VARCHAR2)
IS
SELECT count(*)
FROM   dl_hcs_people_groups
WHERE  lpeg_dlb_batch_id = p_dlb_batch_id
AND    lpeg_code = p_peg_code
AND    lpeg_dl_seqno < p_dl_seqno
AND    lpeg_dl_load_status != 'F';
  
-- *****************************
-- Check People Group Type is Valid
--
CURSOR chk_pgt_code(p_pgt_code VARCHAR2)
IS
SELECT 'X'
  FROM people_group_types
 WHERE pgt_code = p_pgt_code
   AND pgt_current_ind = 'Y';
--
-- ***********************************************************************
-- Check Status Code is Valid
--
CURSOR chk_sco_code(p_sco_code VARCHAR2)
IS
SELECT 'X'
  FROM status_codes
 WHERE sco_code = p_sco_code;
--
-- ***********************************************************************
-- Check Admin Unit Code is Valid
--
CURSOR chk_aun_code(p_aun_code VARCHAR2)
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
-- ***********************************************************************
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HCS_PEOPLE_GROUPS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER := 0;
l_chk            VARCHAR2(1);
l_chk_dup_dl     INTEGER;
--
-- *****************************
--
BEGIN
--
   fsc_utils.proc_start('s_'||ct||'.dataload_validate');
   fsc_utils.debug_message('s_'||ct||'.dataload_validate',3 );
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
         cs := p1.lpeg_dl_seqno;
         l_id := p1.rec_rowid;
--
         l_errors := 'V';
         l_error_ind := 'N';
--
--       Mandatory field check and content
--
--       People Group Code
         IF p1.lpeg_code IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',677);
--          'People Group Code must be supplied'
         ELSE
            l_chk := NULL;
--
            OPEN chk_peg_code(p1.lpeg_code);
            FETCH chk_peg_code INTO l_chk;
            CLOSE chk_peg_code;
--
            IF l_chk IS NOT NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',678);
--             'People Group Code supplied already exists'   
            ELSE
               l_chk_dup_dl := 0;
--                                 
--             Check data load batch for duplicates
               OPEN  chk_dup_dl(p1.lpeg_dlb_batch_id
                               ,p1.lpeg_dl_seqno
                               ,p1.lpeg_code);
--
               FETCH chk_dup_dl INTO l_chk_dup_dl;
               CLOSE chk_dup_dl;
--
               IF l_chk_dup_dl > 0
               THEN
                  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',679);
                  -- 'Record is a duplicate in the data load batch'
               END IF;            
--            
            END IF;
         END IF;      
--       Peg_Description
         IF p1.lpeg_description IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',680);
--          'Peg_Description must be supplied'         
         END IF;            
--       Start Date
         IF p1.lpeg_start_date IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',681);
--          'Start Date must be supplied'
         END IF;
--       People Group Type
         IF p1.lpeg_pgt_code IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',682);
--          'People Group Type must be supplied'
         ELSE
            l_chk := NULL;
--
            OPEN chk_pgt_code(p1.lpeg_pgt_code);
            FETCH chk_pgt_code INTO l_chk;
            CLOSE chk_pgt_code;
--
            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',683);
--             'People Group Type supplied is invalid'               
            END IF;
--         
         END IF;
--       Status code
         IF p1.lpeg_sco_code IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',684);
--          'Status code must be supplied'
         ELSE
            l_chk := NULL;
--
            OPEN chk_sco_code(p1.lpeg_sco_code);
            FETCH chk_sco_code INTO l_chk;
            CLOSE chk_sco_code;
--
            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',685);
--             'Status code supplied is invalid'               
            END IF;
--            
         END IF;  
--       Admin Unit Code
         IF p1.lpeg_aun_code IS NOT NULL
         THEN

            l_chk := NULL;
--
            OPEN chk_aun_code(p1.lpeg_aun_code);
            FETCH chk_aun_code INTO l_chk;
            CLOSE chk_aun_code;
--
            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',700);
--             'Admin Unit Code supplied is invalid'               
            END IF;
--                     
         END IF;                 
--       Check the Created date if supplied
         IF NVL(p1.lpeg_created_date,TRUNC(SYSDATE)) > TRUNC(SYSDATE)
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',701);
--          'The Created date cannot be later than today (truncated sysdate)'
         END IF;
--
--       Check the End Date if supplied
         IF p1.lpeg_end_date IS NOT NULL
         THEN
            IF p1.lpeg_end_date <= p1.lpeg_start_date
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',709);
--            'The End Date cannot be on or before Start Date'
            END IF;
            IF p1.lpeg_end_date > TRUNC(SYSDATE)
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',710);
--            'The End Date cannot be in the future'
            END IF;
         END IF;         
--         
--       *****************************************
--       Now UPDATE the record count and error code
         IF l_errors = 'F' THEN
            l_error_ind := 'Y';
         ELSE
            l_error_ind := 'N';
         END IF;
--
--       keep a count of the rows processed and commit after every 1000
         i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
--
   fsc_utils.proc_end;
   COMMIT;
--
EXCEPTION
   WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
END dataload_validate;
--
--
-- ***********************************************************************
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       lpeg_dl_seqno,
       lpeg_code
FROM   dl_hcs_people_groups
WHERE  lpeg_dlb_batch_id   = p_batch_id
AND    lpeg_dl_load_status = 'C';
--
--
-- *******************************
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'dl_hcs_people_groups';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- ***********************************************************************
--
-- Other variables
--
--
i        INTEGER := 0;
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
BEGIN
--
    fsc_utils.proc_start('s_'||ct||'.dataload_delete');
    fsc_utils.debug_message('s_'||ct||'.dataload_delete',3 );
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
          cs   := p1.lpeg_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from PEOPLE_GROUPS table
--
          DELETE
            FROM people_groups
           WHERE peg_code = p1.lpeg_code;
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
           COMMIT;
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
                  set_record_status_flag(l_id,'C');
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
--
    END LOOP;
--
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PEOPLE_GROUPS');
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

END s_dl_hcs_people_groups;
/

show errors
