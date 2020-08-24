CREATE OR REPLACE PACKAGE BODY s_dl_hem_org_admin_units
AS
-- ***********************************************************************
--
--  CHANGE CONTROL
--  VER  DB Ver WHO  WHEN         WHY
--  1.0  6.14   AJ   22-DEC-2016  Initial Creation of data loader for
--                                Organisation Admin Units functionality
--  1.1  6.14   AJ   07-MAR-2017  Checked from start before continuing with
--                                development
--  1.2  6.14   AJ   05-APR-2017  Further updates done script completed and tested 
--  1.3  6.14   AJ   06-APR-2017  Further updates done during testing 
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hem_org_admin_units
  SET lorau_dl_load_status = p_status
  WHERE rowid = p_rowid;
--
 EXCEPTION
  WHEN OTHERS THEN
   dbms_output.put_line('Error updating status of dl_hem_org_admin_units');
  RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1
   (p_batch_id VARCHAR2)
IS
SELECT
rowid rec_rowid
,lorau_dlb_batch_id
,lorau_dl_seqno
,lorau_dl_load_status
,lorau_par_org_name
,lorau_par_org_short_name
,lorau_par_org_frv_oty_code
,lorau_par_refno
,lorau_aun_code
,lorau_start_date
,lorau_frv_oar_code
,lorau_created_date
,lorau_created_by
,lorau_end_date
,lorau_comments
,lorau_new_refno
,lorau_c_par_refno
,lorau_old_refno
,lorau_old_start_date
,lorau_old_end_date
,lorau_old_frv_oar_code
,lorau_old_comments
FROM dl_hem_org_admin_units
WHERE lorau_dlb_batch_id   = p_batch_id
AND   lorau_dl_load_status = 'V';
--
--*************************
CURSOR chk_orau_dup(p_aun_code  VARCHAR2
               ,p_par_refno     NUMBER
               ,p_date          DATE)
IS
SELECT 'X'
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND p_date BETWEEN orau_start_date
           AND nvl(orau_end_date,p_date +1);
--
--*************************
CURSOR chk_orau_dup2 (p_aun_code    VARCHAR2
                     ,p_par_refno   NUMBER
                     ,p_start_date  DATE
                     ,p_end_date    DATE)
IS
SELECT 'X'
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date > p_start_date
AND orau_start_date < p_end_date
AND nvl(orau_end_date,p_end_date -1)< p_end_date;
--
--*************************
CURSOR c2 (p_aun_code   VARCHAR2
          ,p_par_refno  NUMBER
          ,p_start_date DATE
          ,p_end_date   DATE
          ,p_refno      NUMBER)
IS
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date > p_start_date
AND p_end_date BETWEEN orau_start_date
           AND nvl(orau_end_date,p_end_date +1)
AND orau_refno != p_refno
UNION
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date < p_start_date
AND p_end_date BETWEEN orau_start_date
           AND nvl(orau_end_date,p_end_date +1)
AND orau_refno != p_refno
UNION
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date > p_start_date
AND orau_start_date < p_end_date
AND nvl(orau_end_date,p_end_date -1)< p_end_date
AND orau_refno != p_refno
UNION
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date < p_start_date
AND orau_start_date < p_end_date
AND nvl(orau_end_date,p_end_date -1)< p_end_date
AND orau_refno != p_refno;
--
--*************************
CURSOR get_orau_refno (p_orau_refno     NUMBER
                      ,p_aun            VARCHAR2
                      ,p_start_date     DATE
                      ,p_frv_oar_code   VARCHAR2)
IS
SELECT orau_refno
FROM organisation_admin_units
WHERE ORAU_PAR_REFNO = p_orau_refno
AND ORAU_AUN_CODE = p_aun
AND ORAU_START_DATE = p_start_date
AND ORAU_FRV_OAR_CODE = p_frv_oar_code;
--
--*************************
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_ORG_ADMIN_UNITS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_an_tab       VARCHAR2(1);
i              INTEGER :=0;
l_answer       VARCHAR2(1);
l_orau_refno   NUMBER(10);
l_dup_start    VARCHAR2(1);
l_dup_end      VARCHAR2(1);
l_dup          VARCHAR2(1);
--
--*************************
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_org_admin_units.dataload_create');
 fsc_utils.debug_message( 's_dl_hem_org_admin_units.dataload_create',3);
--
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
 l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
 FOR p1 IN c1(p_batch_id)
  LOOP
--
  BEGIN
--
   cs := p1.lorau_dl_seqno;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--********************
   l_orau_refno := NULL;
   l_dup_start := NULL;
   l_dup_end := NULL;
   l_dup := NULL;
--
   OPEN chk_orau_dup(p1.lorau_aun_code
                    ,p1.lorau_c_par_refno
                    ,p1.lorau_start_date);   
   FETCH  chk_orau_dup INTO l_dup_start;
   CLOSE  chk_orau_dup;
--
   OPEN chk_orau_dup(p1.lorau_aun_code
                    ,p1.lorau_c_par_refno
                    ,nvl(p1.lorau_end_date,trunc(sysdate)+5000));   
   FETCH  chk_orau_dup INTO l_dup_end;
   CLOSE  chk_orau_dup;
--
   OPEN chk_orau_dup2(p1.lorau_aun_code
                     ,p1.lorau_c_par_refno
                     ,p1.lorau_start_date
                     ,nvl(p1.lorau_end_date,trunc(sysdate)+5000));   
   FETCH  chk_orau_dup2 INTO l_dup;
   CLOSE  chk_orau_dup2;
--
-- check and insert only if no duplicates
--
   IF(nvl(l_answer,'N')='N'  AND
      l_dup_start IS NULL    AND
      l_dup_end   IS NULL    AND
      l_dup       IS NULL       )
    THEN
--   
     INSERT INTO organisation_admin_units
        (ORAU_PAR_REFNO
        ,ORAU_AUN_CODE
        ,ORAU_START_DATE
        ,ORAU_FRV_OAR_CODE
        ,ORAU_END_DATE
        ,ORAU_COMMENTS
        )
        VALUES
        (p1.lorau_c_par_refno
        ,p1.lorau_aun_code
        ,p1.lorau_start_date
        ,p1.lorau_frv_oar_code
        ,p1.lorau_end_date
        ,p1.lorau_comments
        );
--
-- *****************************************
-- *****************************************
-- *****************************************
--
-- need at add update to change created_by and
-- created_date to what is supplied
--
     IF (p1.lorau_created_date IS NULL)
      THEN
       p1.lorau_created_date:= trunc(sysdate);
     END IF;
--
     UPDATE organisation_admin_units
        SET ORAU_CREATED_DATE = p1.lorau_created_date
      WHERE ORAU_PAR_REFNO = p1.lorau_c_par_refno
        AND ORAU_AUN_CODE = p1.lorau_aun_code
        AND ORAU_START_DATE = p1.lorau_start_date
        AND ORAU_FRV_OAR_CODE = p1.lorau_frv_oar_code;
--
     IF (p1.lorau_created_by IS NULL)
      THEN
       p1.lorau_created_by:= 'DATALOAD';
     END IF;
--
     UPDATE organisation_admin_units
        SET ORAU_CREATED_BY = p1.lorau_created_by
      WHERE ORAU_PAR_REFNO = p1.lorau_c_par_refno
        AND ORAU_AUN_CODE = p1.lorau_aun_code
        AND ORAU_START_DATE = p1.lorau_start_date
        AND ORAU_FRV_OAR_CODE = p1.lorau_frv_oar_code;
--
-- need to get orau_refno after create to put in data load table as cannot supply as trigger
-- assumes that it is NOT supplied and does not cater for it if it is...
--
     OPEN get_orau_refno(p1.lorau_c_par_refno
                        ,p1.lorau_aun_code
                        ,p1.lorau_start_date
                        ,p1.lorau_frv_oar_code);   
     FETCH get_orau_refno INTO l_orau_refno;
     CLOSE get_orau_refno;
--   
     UPDATE dl_hem_org_admin_units
        SET lorau_new_refno = l_orau_refno
      WHERE lorau_dlb_batch_id = p_batch_id
        AND rowid = p1.rec_rowid;
--
-- *****************************************
-- *****************************************
-- *****************************************
--
   END IF; -- end if no duplicates or overlaps and l_answer is N
--
-- Need to update old record if one identified during validate (lorau_old_refno)
-- First can insert record if current record does not exist
-- 
   IF nvl(l_answer,'N')='Y'
    THEN
--
     IF(l_dup_start IS NULL    AND
        l_dup_end   IS NULL    AND
        l_dup       IS NULL    AND
        p1.lorau_old_refno IS NULL)
      THEN
--   
       INSERT INTO organisation_admin_units
        (ORAU_PAR_REFNO
        ,ORAU_AUN_CODE
        ,ORAU_START_DATE
        ,ORAU_FRV_OAR_CODE
        ,ORAU_END_DATE
        ,ORAU_COMMENTS
        )
        VALUES
        (p1.lorau_c_par_refno
        ,p1.lorau_aun_code
        ,p1.lorau_start_date
        ,p1.lorau_frv_oar_code
        ,p1.lorau_end_date
        ,p1.lorau_comments
        );
--
-- *****************************************
-- *****************************************
-- *****************************************
--
-- need at add update to change created_by and
-- created_date to what is supplied
--
     IF (p1.lorau_created_date IS NULL)
      THEN
       p1.lorau_created_date:= trunc(sysdate);
     END IF;
--
     UPDATE organisation_admin_units
        SET ORAU_CREATED_DATE = p1.lorau_created_date
      WHERE ORAU_PAR_REFNO = p1.lorau_c_par_refno
        AND ORAU_AUN_CODE = p1.lorau_aun_code
        AND ORAU_START_DATE = p1.lorau_start_date
        AND ORAU_FRV_OAR_CODE = p1.lorau_frv_oar_code;
--
     IF (p1.lorau_created_by IS NULL)
      THEN
       p1.lorau_created_by:= 'DATALOAD';
     END IF;
--
     UPDATE organisation_admin_units
        SET ORAU_CREATED_BY = p1.lorau_created_by
      WHERE ORAU_PAR_REFNO = p1.lorau_c_par_refno
        AND ORAU_AUN_CODE = p1.lorau_aun_code
        AND ORAU_START_DATE = p1.lorau_start_date
        AND ORAU_FRV_OAR_CODE = p1.lorau_frv_oar_code;
--
-- need to get orau_refno after create to put in data load table as cannot supply as trigger
-- assumes that it is NOT supplied and does not cater for it if it is...
--
     OPEN get_orau_refno(p1.lorau_c_par_refno
                        ,p1.lorau_aun_code
                        ,p1.lorau_start_date
                        ,p1.lorau_frv_oar_code);   
     FETCH get_orau_refno INTO l_orau_refno;
     CLOSE get_orau_refno;
--   
     UPDATE dl_hem_org_admin_units
        SET lorau_new_refno = l_orau_refno
      WHERE lorau_dlb_batch_id = p_batch_id
        AND rowid = p1.rec_rowid;
--
-- *****************************************
-- *****************************************
-- *****************************************
--
     END IF; -- end of l_answer is Y no overlaps no update but create needed
--
-- IF Update required for current record but old record does not overlap
-- then can just update old record with new details
-- 
     IF(l_dup_start IS NULL    AND
        l_dup_end   IS NULL    AND
        l_dup       IS NULL    AND
        p1.lorau_old_refno IS NOT NULL)
      THEN
--   
     UPDATE organisation_admin_units
        SET ORAU_START_DATE = p1.lorau_start_date
           ,ORAU_FRV_OAR_CODE = p1.lorau_frv_oar_code
           ,ORAU_END_DATE = p1.lorau_end_date
           ,ORAU_COMMENTS = p1.lorau_comments
      WHERE ORAU_REFNO = p1.lorau_old_refno
        AND ORAU_PAR_REFNO = p1.lorau_c_par_refno
        AND ORAU_AUN_CODE = p1.lorau_aun_code;
--
     END IF; -- end of l_answer is Y no overlaps but update needed
--
-- if an Update required for current record but old records do overlap
-- 
     IF(l_dup_start IS NOT NULL OR
        l_dup_end   IS NOT NULL OR
        l_dup       IS NOT NULL   )
      THEN
       IF (p1.lorau_old_refno IS NOT NULL)
        THEN
--
-- Firstly delete any records that overlap and its not the one to be updated
--
        FOR p2 IN c2(p1.lorau_aun_code
                    ,p1.lorau_c_par_refno
                    ,p1.lorau_start_date
                    ,nvl(p1.lorau_end_date,trunc(sysdate)+5000)
                    ,p1.lorau_old_refno)
         LOOP
--
         BEGIN
--   
          DELETE FROM organisation_admin_units
          WHERE orau_refno = p2.orau_refno;
--
         END;
        END LOOP; --p2
--
-- now update with new details the one to be updated
--  
        UPDATE organisation_admin_units
           SET ORAU_START_DATE = p1.lorau_start_date
              ,ORAU_FRV_OAR_CODE = p1.lorau_frv_oar_code
              ,ORAU_END_DATE = p1.lorau_end_date
              ,ORAU_COMMENTS = p1.lorau_comments
         WHERE ORAU_REFNO = p1.lorau_old_refno
           AND ORAU_PAR_REFNO = p1.lorau_c_par_refno
           AND ORAU_AUN_CODE = p1.lorau_aun_code;
--
       END IF;
     END IF; -- overlaps but update needed
--
   END IF; -- end of l_answer is Y
--
--*************************
--
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
--
  END;	
--	
 END LOOP;  --p1
--
COMMIT;
--
--*************************
--
-- Section to analyse the table(s) populated by this Dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ORGANISATION_ADMIN_UNITS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HEM_ORG_ADMIN_UNITS');
--
fsc_utils.proc_end;
COMMIT;
--
EXCEPTION
 WHEN OTHERS THEN
  s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
   RAISE;
--
END dataload_create;
--
--************************************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1
   (p_batch_id VARCHAR2)
IS
SELECT rowid rec_rowid
,lorau_dlb_batch_id
,lorau_dl_seqno
,lorau_dl_load_status
,lorau_par_org_name           -- validate DONE
,lorau_par_org_short_name     -- validate DONE
,lorau_par_org_frv_oty_code   -- validate DONE
,lorau_par_refno              -- validate DONE
,lorau_aun_code               -- validate DONE
,lorau_start_date             -- validate DONE
,lorau_frv_oar_code           -- validate DONE
,lorau_created_date           -- default if not supplied DONE
,lorau_created_by             -- default if not supplied DONE
,lorau_end_date               -- validate DONE
,lorau_comments               -- validate DONE
,lorau_new_refno              -- delete/update DONE BY CREATE
,lorau_c_par_refno            -- use on insert get during validation DONE
,lorau_old_refno              -- delete/update
,lorau_old_start_date         -- delete/update
,lorau_old_end_date           -- delete/update
,lorau_old_frv_oar_code       -- delete/update
,lorau_old_comments           -- delete/update
FROM dl_hem_org_admin_units
WHERE lorau_dlb_batch_id = p_batch_id
AND   lorau_dl_load_status in ('L','F','O');
--
--*************************
--
CURSOR chk_org_a (p_org_name  VARCHAR2
                 ,p_par_refno NUMBER)
IS
SELECT par_refno
FROM parties
WHERE par_org_name = p_org_name
AND par_refno      = p_par_refno
AND par_type       = 'ORG';
--
--*************************
CURSOR chk_org_type(p_org_type  VARCHAR2)
IS
SELECT 'X'
FROM first_ref_values
WHERE frv_code = p_org_type
AND frv_frd_domain = 'ORG_TYPE';
--
--*************************
-- Check b
CURSOR count_org_b(p_org_name       VARCHAR2
                  ,p_org_short_name VARCHAR2)
IS
SELECT count(par_refno)
FROM parties
WHERE par_org_name     = p_org_name
AND par_org_short_name = p_org_short_name
AND par_type           = 'ORG';
--
-- Check b
CURSOR get_org_b(p_org_name       VARCHAR2
                ,p_org_short_name VARCHAR2)
IS
SELECT par_refno
FROM parties
WHERE par_org_name     = p_org_name
AND par_org_short_name = p_org_short_name
AND par_type           = 'ORG';
--
--*************************
-- Check c
CURSOR count_org_c(p_org_name         VARCHAR2
                  ,p_org_short_name   VARCHAR2
                  ,p_org_frv_oty_code VARCHAR2)
IS
SELECT count(par_refno)
FROM parties
WHERE par_org_name       = p_org_name
AND par_org_short_name   = p_org_short_name
AND par_org_frv_oty_code = p_org_frv_oty_code
AND par_type             = 'ORG';
--
-- Check c
CURSOR get_org_c(p_org_name         VARCHAR2
                ,p_org_short_name   VARCHAR2
                ,p_org_frv_oty_code VARCHAR2)
IS
SELECT par_refno
FROM parties
WHERE par_org_name       = p_org_name
AND par_org_short_name   = p_org_short_name
AND par_org_frv_oty_code = p_org_frv_oty_code
AND par_type             = 'ORG';
--
--*************************
--CURSOR get_par (p_org_name       VARCHAR2
--               ,p_org_short_name VARCHAR2
--               ,p_org_sort_code  VARCHAR2
--               ,p_frv_oty_code   VARCHAR2)
--IS
--SELECT par_refno
--FROM parties
--WHERE nvl(par_org_name,'~')       = nvl(p_org_name,'~')
--AND nvl(par_org_short_name,'~')   = nvl(p_org_short_name,'~') 
--AND nvl(par_org_sort_code,'~')    = nvl(p_org_sort_code,'~')
--AND nvl(par_org_frv_oty_code,'~') = nvl(p_frv_oty_code,'~');
--
--*************************
CURSOR chk_lnk(p_lnk_reason  VARCHAR2)
IS
SELECT 'X'
FROM first_ref_values
WHERE frv_code = p_lnk_reason
AND frv_frd_domain = 'ORG_AUN_REASON';
--
--*************************
CURSOR chk_orau_dup (p_aun_code   VARCHAR2
                    ,p_par_refno  NUMBER
                    ,p_start_date DATE
                    ,p_end_date   DATE)
IS
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date > p_start_date
AND p_end_date BETWEEN orau_start_date
           AND nvl(orau_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND p_start_date BETWEEN orau_start_date
           AND nvl(orau_end_date,p_start_date +1)
UNION
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND p_end_date BETWEEN orau_start_date
           AND nvl(orau_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date < p_start_date
AND p_end_date BETWEEN orau_start_date
           AND nvl(orau_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date > p_start_date
AND orau_start_date < p_end_date
AND nvl(orau_end_date,p_end_date -1)< p_end_date
UNION
SELECT DISTINCT(orau_refno)
FROM organisation_admin_units
WHERE orau_aun_code = p_aun_code
AND orau_par_refno  = p_par_refno
AND orau_start_date < p_start_date
AND orau_start_date < p_end_date
AND nvl(orau_end_date,p_end_date -1)< p_end_date;
--
--*************************
CURSOR get_orau_detail(p_orau_refno NUMBER)
IS
SELECT orau_start_date,
       orau_end_date,
       orau_frv_oar_code,
       orau_comments	   
FROM organisation_admin_units
WHERE orau_refno = p_orau_refno;
--
--*************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HEM_ORG_ADMIN_UNITS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_answer         VARCHAR2(1);
l_c_par_refno    NUMBER(8);
l_count_org      INTEGER :=0;
l_org_type_exist VARCHAR2(1);
l_link           VARCHAR2(1);
l_orau_refno     NUMBER(10);
l_dup            NUMBER(10);
l_old_start      DATE;
l_old_end        DATE;
l_old_reason     VARCHAR2(10);
l_old_comments   VARCHAR2(2000);
--
-- Other variables
--
--*************************
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_org_admin_units.dataload_validate');
 fsc_utils.debug_message( 's_dl_hem_org_admin_units.dataload_validate',3);
--
 cb := p_batch_id;
 cd := p_date;
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
 l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
 FOR p1 IN c1 (p_batch_id)
  LOOP
--
  BEGIN
--
  cs := p1.lorau_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors        := 'V';
  l_error_ind     := 'N';
  l_c_par_refno   := NULL;
  l_count_org     :=0;
  l_org_type_exist:= NULL;
  l_link          := NULL;
  l_orau_refno    := NULL;
  l_dup           := NULL;
  l_old_start     := NULL;
  l_old_end       := NULL;
  l_old_reason    := NULL;
  l_old_comments  := NULL;
--
-- Check that Mandatory fields have been supplied
--
  IF p1.lorau_par_org_name IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',084);
  END IF;
--
  IF p1.lorau_par_refno IS NULL
   THEN
    IF p1.lorau_par_org_short_name IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',085);
    END IF;
  END IF;
--
  IF p1.lorau_aun_code IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',086);
  END IF;
--
  IF p1.lorau_start_date IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',087);
  END IF;
--
  IF p1.lorau_frv_oar_code IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',088);
  END IF;
--
-- Check Organisation Type if supplied
--
  IF   (p1.lorau_par_org_frv_oty_code IS NOT NULL)
   THEN
    OPEN  chk_org_type(p1.lorau_par_org_frv_oty_code);
    FETCH chk_org_type INTO l_org_type_exist;
    CLOSE chk_org_type;
    IF l_org_type_exist IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',094);
    END IF;
  END IF;
--
-- Organisation exists in the Parties table
-- Firstly Name and Reference Combination
--
  IF   (p1.lorau_par_refno    IS NOT NULL AND
        p1.lorau_par_org_name IS NOT NULL     )
   THEN
    OPEN  chk_org_a(p1.lorau_par_org_name, p1.lorau_par_refno);
    FETCH chk_org_a INTO l_c_par_refno;
    CLOSE chk_org_a;
    IF l_c_par_refno IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',089);
    END IF;
  END IF;
--
-- Organisation exists in the Parties table
-- Now Name and Short Name Combination Supplied
--
  IF(p1.lorau_par_refno          IS NULL     AND
     p1.lorau_par_org_name       IS NOT NULL AND
     p1.lorau_par_org_short_name IS NOT NULL     )
   THEN
-- Check b
    IF (p1.lorau_par_org_frv_oty_code IS NULL)
     THEN
      OPEN  count_org_b(p1.lorau_par_org_name, p1.lorau_par_org_short_name);
      FETCH count_org_b INTO l_count_org;
      CLOSE count_org_b;
      IF l_count_org < 1
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',090);
      END IF;
      IF l_count_org > 1
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',091);
      END IF;
      IF l_count_org = 1
       THEN
        OPEN  get_org_b(p1.lorau_par_org_name, p1.lorau_par_org_short_name);
        FETCH get_org_b INTO l_c_par_refno;
        CLOSE get_org_b;
      END IF;
    END IF; -- End of Check b
-- Check c
    IF (p1.lorau_par_org_frv_oty_code IS NOT NULL AND
        l_org_type_exist              IS NOT NULL     )
     THEN
      OPEN  count_org_c(p1.lorau_par_org_name
                       ,p1.lorau_par_org_short_name
                       ,p1.lorau_par_org_frv_oty_code);
      FETCH count_org_c INTO l_count_org;
      CLOSE count_org_c;
      IF l_count_org < 1
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',092);
      END IF;
      IF l_count_org > 1
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',093);
      END IF;
      IF l_count_org = 1
       THEN
        OPEN  get_org_c(p1.lorau_par_org_name
                       ,p1.lorau_par_org_short_name
                       ,p1.lorau_par_org_frv_oty_code);
        FETCH get_org_c INTO l_c_par_refno;
        CLOSE get_org_c;
      END IF;
    END IF; -- End of Check c    
  END IF; -- End of second Organisation check
--
-- Now store organisation par_refno for create
--
  UPDATE dl_hem_org_admin_units
     SET lorau_c_par_refno = l_c_par_refno
   WHERE lorau_dlb_batch_id = p_batch_id
     AND rowid = p1.rec_rowid;
--
-- Check Parent Admin Unit exists when supplied
--
  IF (p1.lorau_aun_code IS NOT NULL)
   THEN
    IF NOT(s_admin_units.is_current_admin_unit(p1.lorau_aun_code))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',095);
    END IF;
  END IF;
--
-- Check Org Admin Unit Link Reason if supplied
--
  IF   (p1.lorau_frv_oar_code IS NOT NULL)
   THEN
    OPEN  chk_lnk(p1.lorau_frv_oar_code);
    FETCH chk_lnk INTO l_link;
    CLOSE chk_lnk;
    IF (l_link IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',096);
    END IF;
  END IF;
--
-- Do checks for Current Organisation Admin Unit link and store old detail if Updating
--
  IF  (p1.lorau_aun_code   IS NOT NULL
   AND p1.lorau_start_date IS NOT NULL
   AND l_c_par_refno       IS NOT NULL ) THEN
   OPEN chk_orau_dup(p1.lorau_aun_code
                    ,l_c_par_refno
                    ,p1.lorau_start_date
                    ,nvl(p1.lorau_end_date,trunc(sysdate)+5000));   
   FETCH  chk_orau_dup INTO l_dup;
   CLOSE  chk_orau_dup;
  END IF;
-- 
  IF nvl(l_answer,'N')='N' THEN
    IF (l_dup IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',097);
    END IF;
  END IF;
--
-- If updating get detail to use in delete if needed
--
  IF nvl(l_answer,'N')='Y' THEN
--
   IF (l_dup IS NOT NULL)  THEN
--
    OPEN get_orau_detail(l_dup);   
    FETCH  get_orau_detail INTO l_old_start, l_old_end, l_old_reason, l_old_comments;
    CLOSE  get_orau_detail;
--
    UPDATE dl_hem_org_admin_units
       SET lorau_old_refno = l_dup
          ,lorau_old_start_date = l_old_start
          ,lorau_old_end_date = l_old_end
          ,lorau_old_frv_oar_code = l_old_reason
          ,lorau_old_comments = l_old_comments
     WHERE lorau_dlb_batch_id = p_batch_id
       AND rowid = p1.rec_rowid;
--
   END IF;
  END IF;
-- 
--*************************
-- Now UPDATE the record count and error code
  IF l_errors = 'F' THEN
    l_error_ind := 'Y';
  ELSE
    l_error_ind := 'N';
  END IF;
--*************************
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
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END dataload_validate;
--
--************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 
    (p_batch_id VARCHAR2)
IS
SELECT
rowid rec_rowid
,lorau_dlb_batch_id
,lorau_dl_seqno
,lorau_dl_load_status
,lorau_par_org_name
,lorau_par_org_short_name
,lorau_par_org_frv_oty_code
,lorau_par_refno
,lorau_aun_code
,lorau_start_date
,lorau_frv_oar_code
,lorau_created_date
,lorau_created_by
,lorau_end_date
,lorau_comments
,lorau_new_refno              -- update/delete
,lorau_c_par_refno            -- update/delete
,lorau_old_refno              -- update/delete
,lorau_old_start_date         -- update/delete
,lorau_old_end_date           -- update/delete
,lorau_old_frv_oar_code       -- update/delete
,lorau_old_comments           -- update/delete
FROM dl_hem_org_admin_units
WHERE lorau_dlb_batch_id   = p_batch_id
AND   lorau_dl_load_status = 'C';
--
--*************************
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_ORG_ADMIN_UNITS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
i        INTEGER := 0;
l_an_tab VARCHAR2(1);
l_answer VARCHAR2(1);
--
--*************************
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_org_admin_units.dataload_delete');
 fsc_utils.debug_message( 's_dl_hem_org_admin_units.dataload_delete',3 );
--
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
 l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
 FOR p1 IN c1(p_batch_id) 
  LOOP
--
   BEGIN
--
   cs := p1.lorau_dl_seqno;
   i := i +1;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
-- remove new record first if created
--
   DELETE FROM organisation_admin_units
   WHERE orau_refno = p1.lorau_new_refno;
--
   UPDATE dl_hem_org_admin_units
      SET lorau_new_refno = null
    WHERE lorau_dlb_batch_id = p1.lorau_dlb_batch_id
      AND lorau_dl_seqno = p1.lorau_dl_seqno
      AND rowid = p1.rec_rowid;
--
-- now update old record if updated rather than created
--
   IF (p1.lorau_new_refno IS NULL      AND
       p1.lorau_old_refno IS NOT NULL      )
    THEN
--
     UPDATE organisation_admin_units
        SET ORAU_START_DATE = p1.lorau_old_start_date
           ,ORAU_FRV_OAR_CODE = p1.lorau_old_frv_oar_code
           ,ORAU_END_DATE = p1.lorau_old_end_date
           ,ORAU_COMMENTS = p1.lorau_old_comments
     WHERE ORAU_REFNO = p1.lorau_old_refno
       AND ORAU_PAR_REFNO = p1.lorau_c_par_refno
       AND ORAU_AUN_CODE = p1.lorau_aun_code;
--
   END IF;
--*************************
-- keep a count of the rows processed and commit after every 1000
--
   i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
   s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
   set_record_status_flag(l_id,'V');
--
   EXCEPTION
    WHEN OTHERS THEN
     ROLLBACK TO SP1;
     ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
     set_record_status_flag(l_id,'C');
     s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
END LOOP;
--
--*************************
-- Section to analyse the table(s) populated by this Dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ORGANISATION_ADMIN_UNITS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HEM_ORG_ADMIN_UNITS');
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
--
END s_dl_hem_org_admin_units;
/

show errors

