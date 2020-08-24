CREATE OR REPLACE PACKAGE BODY s_dl_hcs_people_group_roles
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
   UPDATE dl_hcs_people_group_roles
   SET    lpgo_dl_load_status = p_status
   WHERE  rowid = p_rowid;
--
EXCEPTION
   WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hcs_people_group_roles');
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
SELECT pgo.rowid rec_rowid,
       lpgo_dlb_batch_id,
       lpgo_dl_seqno,
       lpgo_dl_load_status,
       lpgo_pge_peg_code,
       lpgo_pge_par_refno,
       pge.pge_refno,
       lpgo_gro_code,
       lpgo_start_date,
       lpgo_end_date,
       lpgo_comments,       
       lpgo_created_date,
       lpgo_created_by
FROM  dl_hcs_people_group_roles pgo, people_group_members pge
WHERE lpgo_dlb_batch_id   = p_batch_id
AND   lpgo_dl_load_status = 'V'
AND   lpgo_pge_peg_code = pge_peg_code(+)
AND   lpgo_pge_par_refno = pge_par_refno(+)
AND   pge_start_date <= lpgo_start_date
AND   NVL(pge_end_date,TO_DATE('31129999','DDMMYYYY')) >= NVL(lpgo_end_date,TO_DATE('31129999','DDMMYYYY'));
--
-- *****************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'dl_hcs_people_group_roles';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- Other variables
--
l_id        ROWID;
l_an_tab    VARCHAR2(1);
i           INTEGER := 0;
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
         cs   := p1.lpgo_dl_seqno;
         l_id := p1.rec_rowid;
--
         SAVEPOINT SP1;
         
         INSERT INTO people_group_roles
            (pgo_pge_refno,
             pgo_gro_code,
             pgo_start_date,
             pgo_end_date,
             pgo_comments)
         VALUES
            (p1.pge_refno,
             p1.lpgo_gro_code,
             p1.lpgo_start_date,
             p1.lpgo_end_date,
             p1.lpgo_comments);
--
-- Now update record with created date as trigger sets it to sysdate
-- only needed if creation date supplied otherwise sysdate is fine   
--
         IF p1.lpgo_created_date IS NOT NULL AND p1.lpgo_created_by IS NOT NULL
         THEN
            UPDATE people_group_roles
            SET    pgo_created_date = p1.lpgo_created_date,
                   pgo_created_by = p1.lpgo_created_by
            WHERE  pgo_pge_refno = p1.pge_refno;
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('PEOPLE_GROUP_ROLES');
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
       lpgo_dlb_batch_id,
       lpgo_dl_seqno,
       lpgo_dl_load_status,
       lpgo_pge_peg_code,
       lpgo_pge_par_refno,
       lpgo_gro_code,
       lpgo_start_date,       
       lpgo_end_date,
       lpgo_comments,
       lpgo_created_date
FROM   dl_hcs_people_group_roles
WHERE  lpgo_dlb_batch_id   = p_batch_id
AND    lpgo_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
-- Check People Group Code is Valid
--
CURSOR chk_peg_code(p_peg_code VARCHAR2)
IS
SELECT 'X'
  FROM people_groups
 WHERE peg_code = p_peg_code;
--
-- ***********************************************************************
-- Check Party is Valid
--
CURSOR chk_par_refno(p_par_refno VARCHAR2)
IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_par_refno
   AND par_type = 'HOUP'
   AND SYSDATE <= NVL(par_per_hou_end_date,SYSDATE);
--
-- ***********************************************************************
-- Check Group Role Code is Valid
--
CURSOR chk_gro_code(p_gro_code VARCHAR2)
IS
SELECT 'X'
  FROM group_roles
 WHERE gro_code = p_gro_code
   AND gro_current_ind = 'Y';
--
-- ***********************************************************************
-- Get_pge_refno
--
CURSOR get_pge_refno(p_peg_code   VARCHAR2,
                     p_par_refno  INTEGER,
                     p_start_date DATE,
                     p_end_date   DATE)
IS
SELECT pge_refno
  FROM people_group_members
 WHERE pge_peg_code = p_peg_code
   AND pge_par_refno = p_par_refno
   AND pge_start_date <= p_start_date
   AND NVL(pge_end_date,TO_DATE('31129999','DDMMYYYY')) >= NVL(p_end_date,TO_DATE('31129999','DDMMYYYY'));
--
-- ***********************************************************************
-- Check dates between start and end of people group members record  
--
CURSOR chk_people_group_dates(p_pge_refno  INTEGER,
                              p_start_date DATE,
                              p_end_date   DATE )
IS
SELECT 'X'
FROM   people_group_members
WHERE  pge_refno = p_pge_refno
AND    pge_start_date <= p_start_date 
AND    NVL(p_end_date,TO_DATE('31129999','DDMMYYYY')) <= NVL(pge_end_date,TO_DATE('31129999','DDMMYYYY'));

--
-- ***********************************************************************
-- Check for overlapping records
--
CURSOR chk_overlap(p_pge_refno  INTEGER,
                   p_gro_code   VARCHAR2,
                   p_start_date DATE,
                   p_end_date   DATE )
IS
SELECT 'X'
FROM   people_group_roles
WHERE  pgo_pge_refno = p_pge_refno
AND    pgo_gro_code = p_gro_code
AND    p_start_date <= NVL(pgo_end_date,TO_DATE('31129999','DDMMYYYY'))
AND    NVL(p_end_date,TO_DATE('31129999','DDMMYYYY')) >= pgo_start_date;
--
--
-- ***********************************************************************
-- Check for duplicate peg_code, par_refno, gro_code, /overlapping dates in data load batch
CURSOR chk_dup_dl (p_dlb_batch_id  VARCHAR2
                  ,p_dl_seqno      INTEGER
                  ,p_peg_code      VARCHAR2
                  ,p_par_refno     VARCHAR2
                  ,p_gro_code      VARCHAR2
                  ,p_start_date    DATE
                  ,p_end_date      DATE  )
IS
SELECT count(*)
FROM   dl_hcs_people_group_roles
WHERE  lpgo_dlb_batch_id = p_dlb_batch_id
AND    lpgo_pge_peg_code = p_peg_code
AND    lpgo_pge_par_refno = p_par_refno
AND    lpgo_gro_code = p_gro_code 
AND    NVL(p_end_date,TO_DATE('31129999','DDMMYYYY')) >= lpgo_start_date
AND    p_start_date <= NVL(lpgo_end_date,TO_DATE('31129999','DDMMYYYY'))
AND    lpgo_dl_seqno < p_dl_seqno
AND    lpgo_dl_load_status != 'F';
--  
-- ***********************************************************************
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HCS_PEOPLE_GROUP_ROLES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER := 0;
l_chk        	  VARCHAR2(1);
l_chk_dup_dl     INTEGER;
l_pge_refno      INTEGER;
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
         cs := p1.lpgo_dl_seqno;
         l_id := p1.rec_rowid;
--
         l_errors := 'V';
         l_error_ind := 'N';
--
--       Mandatory field check and content
--
--       People Group Code
         IF p1.lpgo_pge_peg_code IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',677);
--          'People Group Code must be supplied'
         ELSE
            l_chk := NULL;
--
            OPEN chk_peg_code(p1.lpgo_pge_peg_code);
            FETCH chk_peg_code INTO l_chk;
            CLOSE chk_peg_code;
--
            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',702);
--             'People Group Code supplied is invalid'               
            END IF;
--         
         END IF;
--       Party Reference Number
         IF p1.lpgo_pge_par_refno IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',703);
--          'Party Reference Number must be supplied'
         ELSE
            l_chk := NULL;
--
            OPEN chk_par_refno(p1.lpgo_pge_par_refno);
            FETCH chk_par_refno INTO l_chk;
            CLOSE chk_par_refno;
--
            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',704);
--             'Party Reference Number supplied is invalid'               
            END IF;
--         
         END IF;
--       Group Role Code
         IF p1.lpgo_gro_code IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',707);
--          'Group Role Code must be supplied'
         ELSE
            l_chk := NULL;
--
            OPEN chk_gro_code(p1.lpgo_gro_code);
            FETCH chk_gro_code INTO l_chk;
            CLOSE chk_gro_code;
--
            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',708);
--             'Group Role Code supplied is invalid'               
            END IF;
--         
         END IF;         
--       Start Date
         IF p1.lpgo_start_date IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',681);
--          'Start Date must be supplied'
         END IF;     
         
--       Get pge_refno
         IF p1.lpgo_pge_peg_code IS NOT NULL AND
            p1.lpgo_pge_par_refno IS NOT NULL AND
            p1.lpgo_start_date IS NOT NULL
         THEN 
            l_pge_refno := NULL;
--
            OPEN get_pge_refno(p1.lpgo_pge_peg_code,
                               p1.lpgo_pge_par_refno,
                               p1.lpgo_start_date,
                               NVL(p1.lpgo_end_date,TO_DATE('31129999','DDMMYYYY')));
            FETCH get_pge_refno INTO l_pge_refno;
            CLOSE get_pge_refno;
--
            IF l_pge_refno IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',686);
--             'No matching People Group Members record found for the dates specified'               
            END IF;
         END IF;
--             
--       Check the End Date if supplied
         IF p1.lpgo_end_date IS NOT NULL
         THEN
            IF p1.lpgo_end_date <= p1.lpgo_start_date
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',709);
--            'The End Date cannot be on or before Start Date'
            END IF;
            IF p1.lpgo_end_date > TRUNC(SYSDATE)
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',710);
--            'The End Date cannot be in the future'
            END IF;
         END IF;
--         
--       chk_dates between start and end of people group members record  
         IF l_pge_refno IS NOT NULL AND
            p1.lpgo_start_date IS NOT NULL
         THEN   
            l_chk := NULL;
--                                 
            OPEN chk_people_group_dates (l_pge_refno
                                        ,p1.lpgo_start_date
                                        ,NVL(p1.lpgo_end_date,TO_DATE('31129999','DDMMYYYY')));
--
            FETCH chk_people_group_dates INTO l_chk;
            CLOSE chk_people_group_dates;
--
            IF l_chk IS NULL
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',711);
               -- 'Start and End Dates must be between the Start and End Dates of a corresponding People Group'
            END IF;            
-- 
         END IF; 
--         
--       chk_overlapping dates 
         IF l_pge_refno IS NOT NULL AND
            p1.lpgo_gro_code IS NOT NULL AND
            p1.lpgo_start_date IS NOT NULL
         THEN   
            l_chk := NULL;
--                                 
            OPEN chk_overlap (l_pge_refno
                             ,p1.lpgo_gro_code
                             ,p1.lpgo_start_date
                             ,NVL(p1.lpgo_end_date,TO_DATE('31129999','DDMMYYYY')));
            FETCH chk_overlap INTO l_chk;
            CLOSE chk_overlap ;
--
            IF l_chk IS NOT NULL
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',706);
--             'Dates overlap with an existing record'
            END IF;
         END IF;             
--                          
--       Check data load batch for duplicates    
         IF p1.lpgo_pge_peg_code IS NOT NULL AND
            p1.lpgo_pge_par_refno IS NOT NULL AND
            p1.lpgo_gro_code IS NOT NULL AND
            p1.lpgo_start_date IS NOT NULL
         THEN        
            l_chk_dup_dl := 0;
--                                 
            OPEN  chk_dup_dl(p1.lpgo_dlb_batch_id
                            ,p1.lpgo_dl_seqno
                            ,p1.lpgo_pge_peg_code
                            ,p1.lpgo_pge_par_refno
                            ,p1.lpgo_gro_code
                            ,p1.lpgo_start_date
                            ,p1.lpgo_end_date);
            FETCH chk_dup_dl INTO l_chk_dup_dl;
            CLOSE chk_dup_dl;
--
            IF l_chk_dup_dl > 0
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',679);
--             'Record is a duplicate in the data load batch'
            END IF;
         END IF;            
--                 
--       Check the Created date if supplied
         IF (NVl(p1.lpgo_created_date,TRUNC(SYSDATE)) > TRUNC(SYSDATE))
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',701);
--          'The Created date cannot be later than today (truncated sysdate)'
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
       lpgo_dl_seqno,
       lpgo_pge_peg_code,
       lpgo_pge_par_refno,
       lpgo_gro_code,
       lpgo_start_date,
       lpgo_end_date
FROM   dl_hcs_people_group_roles
WHERE  lpgo_dlb_batch_id   = p_batch_id
AND    lpgo_dl_load_status = 'C';
--
-- ***********************************************************************
-- Get_pge_refno
--
CURSOR get_pge_refno(p_peg_code   VARCHAR2,
                     p_par_refno  INTEGER,
                     p_start_date DATE,
                     p_end_date   DATE)
IS
SELECT pge_refno
  FROM people_group_members
 WHERE pge_peg_code = p_peg_code
   AND pge_par_refno = p_par_refno
   AND p_start_date >= pge_start_date
   AND p_end_date <= pge_end_date;
--   
-- ***********************************************************************
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'dl_hcs_people_group_roles';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- ***********************************************************************
--
-- Other variables
--
--
i           INTEGER := 0;
l_id        ROWID;
l_an_tab    VARCHAR2(1);
l_pge_refno INTEGER;
--
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
      BEGIN
--
          cs   := p1.lpgo_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
          
--       get_pge_refno
         l_pge_refno := NULL;
--
         OPEN get_pge_refno(p1.lpgo_pge_peg_code,
                            p1.lpgo_pge_par_refno,
                            p1.lpgo_start_date,
                            NVL(p1.lpgo_end_date,TO_DATE('31129999','DDMMYYYY')));
         FETCH get_pge_refno INTO l_pge_refno;
         CLOSE get_pge_refno;
                   
--
-- Delete from PEOPLE_GROUP_ROLES table
--
          DELETE
            FROM people_group_roles
           WHERE pgo_pge_refno = l_pge_refno
             AND pgo_gro_code = p1.lpgo_gro_code
             AND pgo_start_date = p1.lpgo_start_date;
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
    END LOOP;
--
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PEOPLE_GROUP_ROLES');
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

END s_dl_hcs_people_group_roles;
/

show errors
