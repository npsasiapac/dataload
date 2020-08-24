CREATE OR REPLACE PACKAGE BODY s_dl_hem_org_hierarchy
AS
-- ***********************************************************************
--
--  CHANGE CONTROL
--  VER  DB Ver WHO  WHEN         WHY
--  1.0  6.13   AJ   02-MAR-2016  Initial Creation of data laoder for
--                                New Brunswick Organisations functionality
--  1.1  6.14   AJ   05-APR-2017  Data Load completed under CR502 for Queensland
--  1.2  6.14   AJ   19-MAY-2017  duplicate error numbers 2nd one of the following
--                                amended but error message remains unchanged so
--                                106 and 204, 107 and 205, 109 and 206 different
--                                checks but same messages 
--  1.3  6.15   DLB  22-MAY-2017  Renamed errors 204-206 to 218-220
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
  UPDATE dl_hem_org_hierarchy
  SET lorhi_dl_load_status = p_status
  WHERE rowid = p_rowid;
--
 EXCEPTION
  WHEN OTHERS THEN
   dbms_output.put_line('Error updating status of dl_hem_org_hierarchy');
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
CURSOR c1(p_batch_id VARCHAR2)
IS
SELECT
rowid rec_rowid,
lorhi_dlb_batch_id,
lorhi_dl_seqno,
lorhi_dl_load_status,
lorhi_par_org_name,
lorhi_par_org_short_name,
lorhi_par_org_frv_oty_code,
lorhi_par_refno,
lorhi_par_org_name_c,
lorhi_par_org_short_name_c,
lorhi_par_org_frv_oty_code_c,
lorhi_par_refno_c,
lorhi_start_date,
lorhi_frv_ort_code,
lorhi_created_date,
lorhi_created_by,
lorhi_end_date,
lorhi_comments,
lorhi_refno
FROM dl_hem_org_hierarchy
WHERE lorhi_dlb_batch_id    = p_batch_id
AND   lorhi_dl_load_status = 'V';
--
--*************************
CURSOR get_par (p_org_name       VARCHAR2
               ,p_org_short_name VARCHAR2
               ,p_frv_oty_code   VARCHAR2)
IS
SELECT par_refno
FROM parties
WHERE nvl(par_org_name,'A') = nvl(p_org_name, nvl(par_org_name,'A'))
AND nvl(par_org_short_name, 'A') = nvl(p_org_short_name, nvl(par_org_short_name, 'A'))
AND nvl(par_org_frv_oty_code,'A') = nvl(p_frv_oty_code, nvl(par_org_frv_oty_code,'A'));
--
--*************************
--
CURSOR get_orhi_refno (p_parent         NUMBER
                      ,p_child          NUMBER
                      ,p_start_date     DATE
                      ,p_frv_oty_code   VARCHAR2)
IS
SELECT orhi_refno
FROM organisation_hierarchy
WHERE ORHI_PARENT_PAR_REFNO = p_parent
AND ORHI_CHILD_PAR_REFNO = p_child
AND ORHI_START_DATE = p_start_date
AND ORHI_FRV_ORT_CODE = p_frv_oty_code;
--
--*************************
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_ORG_HIERARCHY';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_an_tab       VARCHAR2(1);
i              INTEGER :=0;
l_orhi_refno   NUMBER(10);
l_parent_refno NUMBER(8);
l_child_refno  NUMBER(8);
--
--*************************
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_org_hierarchy.dataload_create');
 fsc_utils.debug_message( 's_dl_hem_org_hierarchy.dataload_create',3);
--
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1(p_batch_id) LOOP
--
  BEGIN
--
   cs := p1.lorhi_dl_seqno;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
   l_parent_refno:= NULL;
   l_child_refno := NULL;
   l_orhi_refno  := NULL;

   IF (p1.lorhi_par_refno IS NOT NULL)
    THEN
     l_parent_refno:= p1.lorhi_par_refno;
   ELSE
--
    OPEN get_par(p1.lorhi_par_org_name
                ,p1.lorhi_par_org_short_name
                ,p1.lorhi_par_org_frv_oty_code);   
    FETCH  get_par INTO l_parent_refno;
    CLOSE  get_par;
    --    
   END IF;
--
   IF (p1.lorhi_par_refno_c IS NOT NULL)
    THEN
     l_child_refno:= p1.lorhi_par_refno_c;
   ELSE
--
    OPEN get_par(p1.lorhi_par_org_name_c
                ,p1.lorhi_par_org_short_name_c
                ,p1.lorhi_par_org_frv_oty_code_c);   
    FETCH get_par INTO l_child_refno;
    CLOSE get_par;
--    
   END IF;
--
   INSERT INTO
    organisation_hierarchy (
                            ORHI_PARENT_PAR_REFNO
                           ,ORHI_CHILD_PAR_REFNO
                           ,ORHI_START_DATE
                           ,ORHI_FRV_ORT_CODE
                           ,ORHI_END_DATE
                           ,ORHI_COMMENTS
                           )
   VALUES                 (
                           l_parent_refno
                          ,l_child_refno
                          ,p1.lorhi_start_date
                          ,p1.lorhi_frv_ort_code
                          ,p1.lorhi_end_date
                          ,p1.lorhi_comments
                           );
--
-- need at add update to change created_by and
-- created_date to what is supplied
--
   IF (p1.lorhi_created_date IS NULL)
    THEN
     p1.lorhi_created_date:= trunc(sysdate);
   END IF;
--
   UPDATE organisation_hierarchy
   SET ORHI_CREATED_DATE = p1.lorhi_created_date
   WHERE ORHI_PARENT_PAR_REFNO = l_parent_refno
     AND ORHI_CHILD_PAR_REFNO = l_child_refno
     AND ORHI_START_DATE = p1.lorhi_start_date
     AND ORHI_FRV_ORT_CODE = p1.lorhi_frv_ort_code;
--
   IF (p1.lorhi_created_by IS NULL)
    THEN
     p1.lorhi_created_by:= 'DATALOAD';
   END IF;
--
   UPDATE organisation_hierarchy
   SET ORHI_CREATED_BY = p1.lorhi_created_by
   WHERE ORHI_PARENT_PAR_REFNO = l_parent_refno
     AND ORHI_CHILD_PAR_REFNO = l_child_refno
     AND ORHI_START_DATE = p1.lorhi_start_date
     AND ORHI_FRV_ORT_CODE = p1.lorhi_frv_ort_code;
--
-- need to get ohri_refno after create to put in data load table as cannot supply as trigger
-- assumes that it is NOT supplied and does not cater for it if it is...
--
   OPEN get_orhi_refno(l_parent_refno
                      ,l_child_refno
                      ,p1.lorhi_start_date
                      ,p1.lorhi_frv_ort_code);   
   FETCH get_orhi_refno INTO l_orhi_refno;
   CLOSE get_orhi_refno;
--   
   UPDATE dl_hem_org_hierarchy
   SET lorhi_refno = l_orhi_refno
   WHERE lorhi_dlb_batch_id = p1.lorhi_dlb_batch_id
   AND rowid = p1.rec_rowid;
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
 END LOOP;
--
COMMIT;
--
--*************************
--
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ORGANISATION_HIERARCHY');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HEM_ORG_HIERARCHY');
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
CURSOR c1 (p_batch_id VARCHAR2)
IS
SELECT
rowid rec_rowid,
lorhi_dlb_batch_id,
lorhi_dl_seqno,
lorhi_dl_load_status,
lorhi_par_org_name,
lorhi_par_org_short_name,
lorhi_par_org_frv_oty_code,
lorhi_par_refno,
lorhi_par_org_name_c,
lorhi_par_org_short_name_c,
lorhi_par_org_frv_oty_code_c,
lorhi_par_refno_c,
lorhi_start_date,
lorhi_frv_ort_code,
lorhi_created_date,
lorhi_created_by,
lorhi_end_date,
lorhi_comments,
lorhi_refno
FROM dl_hem_org_hierarchy
WHERE lorhi_dlb_batch_id = p_batch_id
AND   lorhi_dl_load_status in ('L','F','O');
--
--*************************
CURSOR chk_get_par (p_org_name       VARCHAR2
                   ,p_org_short_name VARCHAR2
                   ,p_frv_oty_code   VARCHAR2)
IS
SELECT par_refno
FROM parties
WHERE nvl(par_org_name,'A') = nvl(p_org_name, nvl(par_org_name,'A'))
AND nvl(par_org_short_name, 'A') = nvl(p_org_short_name, nvl(par_org_short_name, 'A'))
AND nvl(par_org_frv_oty_code,'A') = nvl(p_frv_oty_code, nvl(par_org_frv_oty_code,'A'));
--
--*************************
CURSOR chk_no_par (p_org_name       VARCHAR2
                  ,p_org_short_name VARCHAR2
                  ,p_frv_oty_code   VARCHAR2)
IS
SELECT count(par_refno)
FROM parties
WHERE nvl(par_org_name,'A') = nvl(p_org_name, nvl(par_org_name,'A'))
AND nvl(par_org_short_name, 'A') = nvl(p_org_short_name, nvl(par_org_short_name, 'A'))
AND nvl(par_org_frv_oty_code,'A') = nvl(p_frv_oty_code, nvl(par_org_frv_oty_code,'A'));
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
CURSOR chk_rel_type(p_rel_type  VARCHAR2)
IS
SELECT 'X'
FROM first_ref_values
WHERE frv_code = p_rel_type
AND frv_frd_domain = 'ORG_REL_TYPE';
--
--*************************
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
CURSOR chk_orhi_dup (p_parent_refno  NUMBER
                    ,p_child_refno   NUMBER
                    ,p_start_date    DATE
                    ,p_end_date      DATE)
IS
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_child_par_refno  = p_child_refno
AND orhi_start_date > p_start_date
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_child_par_refno  = p_child_refno
AND p_start_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_start_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_child_par_refno  = p_child_refno
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_child_par_refno  = p_child_refno
AND orhi_start_date < p_start_date
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_child_par_refno  = p_child_refno
AND orhi_start_date > p_start_date
AND orhi_start_date < p_end_date
AND nvl(orhi_end_date,p_end_date -1)< p_end_date
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_child_par_refno  = p_child_refno
AND orhi_start_date < p_start_date
AND orhi_start_date < p_end_date
AND nvl(orhi_end_date,p_end_date -1)< p_end_date;
--
--*************************
CURSOR chk_orhi_dup_c (p_child_refno   NUMBER
                      ,p_start_date    DATE
                      ,p_end_date      DATE)
IS
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_child_par_refno  = p_child_refno
AND orhi_start_date > p_start_date
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_child_par_refno  = p_child_refno
AND p_start_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_start_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_child_par_refno  = p_child_refno
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_child_par_refno  = p_child_refno
AND orhi_start_date < p_start_date
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_child_par_refno  = p_child_refno
AND orhi_start_date > p_start_date
AND orhi_start_date < p_end_date
AND nvl(orhi_end_date,p_end_date -1)< p_end_date
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_child_par_refno  = p_child_refno
AND orhi_start_date < p_start_date
AND orhi_start_date < p_end_date
AND nvl(orhi_end_date,p_end_date -1)< p_end_date;
--
--*************************
CURSOR chk_orhi_dup_p (p_parent_refno  NUMBER
                      ,p_start_date    DATE
                      ,p_end_date      DATE)
IS
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_start_date > p_start_date
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND p_start_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_start_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_start_date < p_start_date
AND p_end_date BETWEEN orhi_start_date
           AND nvl(orhi_end_date,p_end_date +1)
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_start_date > p_start_date
AND orhi_start_date < p_end_date
AND nvl(orhi_end_date,p_end_date -1)< p_end_date
UNION
SELECT DISTINCT(orhi_refno)
FROM organisation_hierarchy
WHERE orhi_parent_par_refno = p_parent_refno
AND orhi_start_date < p_start_date
AND orhi_start_date < p_end_date
AND nvl(orhi_end_date,p_end_date -1)< p_end_date;
--
--*************************
CURSOR chk_batch_p ( p_org_name       VARCHAR2
                    ,p_org_short_name VARCHAR2
                    ,p_frv_oty_code   VARCHAR2
                    ,p_par_refno      NUMBER
                    ,p_batch_id       VARCHAR2 )
IS
SELECT 'X'
FROM dl_hem_org_hierarchy
WHERE lorhi_par_org_name = p_org_name
AND nvl(lorhi_par_org_short_name, 'A') = nvl(p_org_short_name, nvl(lorhi_par_org_short_name, 'A'))
AND nvl(lorhi_par_org_frv_oty_code,'A') = nvl(p_frv_oty_code, nvl(lorhi_par_org_frv_oty_code,'A'))
AND nvl(lorhi_par_refno,'1') = nvl(p_par_refno, nvl(lorhi_par_refno,'1'))
AND lorhi_dlb_batch_id = p_batch_id;
--*************************
CURSOR chk_batch_c ( p_org_name       VARCHAR2
                    ,p_org_short_name VARCHAR2
                    ,p_frv_oty_code   VARCHAR2
                    ,p_par_refno      NUMBER
                    ,p_batch_id       VARCHAR2 )
IS
SELECT 'X'
FROM dl_hem_org_hierarchy
WHERE lorhi_par_org_name_c = p_org_name
AND nvl(lorhi_par_org_short_name_c, 'A') = nvl(p_org_short_name, nvl(lorhi_par_org_short_name_c, 'A'))
AND nvl(lorhi_par_org_frv_oty_code_c,'A') = nvl(p_frv_oty_code, nvl(lorhi_par_org_frv_oty_code_c,'A'))
AND nvl(lorhi_par_refno_c,'1') = nvl(p_par_refno, nvl(lorhi_par_refno_c,'1'))
AND lorhi_dlb_batch_id = p_batch_id;
--
--*************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HEM_ORG_HIERARCHY';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
-- Other variables
--
l_orhi_refno     INTEGER :=0;
l_parent_refno   NUMBER(8);
l_child_refno    NUMBER(8);
l_porg_type      VARCHAR2(1);
l_chorg_type     VARCHAR2(1);
l_rel_type       VARCHAR2(1);
l_count_porg     INTEGER :=0;
l_count_chorg    INTEGER :=0;
l_dup_pc         NUMBER(8);
l_dup_cp         NUMBER(8);
l_dup_a          NUMBER(8);
l_dup_b          NUMBER(8);
l_dup_c          NUMBER(8);
l_batch_p        VARCHAR2(1);
l_batch_c        VARCHAR2(1);
--
--*************************
--
BEGIN
  --
 fsc_utils.proc_start('s_dl_hem_org_hierarchy.dataload_validate');
 fsc_utils.debug_message( 's_dl_hem_org_hierarchy.dataload_validate',3);
--
 cb := p_batch_id;
 cd := p_date;
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 (p_batch_id)
  LOOP
--
  BEGIN
--
   cs := p1.lorhi_dl_seqno;
   l_id := p1.rec_rowid;
--
   l_errors       := 'V';
   l_error_ind    := 'N';
   l_orhi_refno   :=0;
   l_parent_refno := NULL;
   l_child_refno  := NULL;
   l_porg_type    := NULL;
   l_chorg_type   := NULL;
   l_rel_type     := NULL;
   l_count_porg   :=0;
   l_count_chorg  :=0;
   l_dup_pc       := NULL;
   l_dup_cp       := NULL;
   l_dup_a        := NULL;
   l_dup_b        := NULL;
   l_dup_c        := NULL;
   l_batch_p      := NULL;
   l_batch_c      := NULL;
--
-- Check that Mandatory fields have been supplied
--
-- For Parent Organisation Supplied
--
  IF (p1.lorhi_par_org_name IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',098);
  END IF;
--
  IF (p1.lorhi_par_refno IS NULL)
   THEN
    IF p1.lorhi_par_org_short_name IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',085);
    END IF;
  END IF;
--
-- Check Parent Organisation Type if supplied
--
  IF (p1.lorhi_par_org_frv_oty_code IS NOT NULL)
   THEN
    OPEN  chk_org_type(p1.lorhi_par_org_frv_oty_code);
    FETCH chk_org_type INTO l_porg_type;
    CLOSE chk_org_type;
    IF l_porg_type IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',103);
    END IF;
  END IF;
--
-- For Child Organisation Supplied
--
  IF (p1.lorhi_par_org_name_c IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',099);
  END IF;
--
  IF (p1.lorhi_par_refno_c IS NULL)
   THEN
    IF p1.lorhi_par_org_short_name_c IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',100);
    END IF;
  END IF;
--
-- Check Child Organisation Type if supplied
--
  IF (p1.lorhi_par_org_frv_oty_code_c IS NOT NULL)
   THEN
    OPEN  chk_org_type(p1.lorhi_par_org_frv_oty_code_c);
    FETCH chk_org_type INTO l_chorg_type;
    CLOSE chk_org_type;
    IF l_chorg_type IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',104);
    END IF;
  END IF;
--
-- Main Record
--
  IF (p1.lorhi_start_date IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',101);
  END IF;
--
  IF (p1.lorhi_frv_ort_code IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',102);
  END IF;
--
  IF (p1.lorhi_frv_ort_code IS NOT NULL)
   THEN
    OPEN  chk_rel_type(p1.lorhi_frv_ort_code);
    FETCH chk_rel_type INTO l_rel_type;
    CLOSE chk_rel_type;
    IF l_rel_type IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',105);
    END IF;
  END IF;
--
  IF (p1.lorhi_start_date IS NOT NULL)
   THEN
    IF (p1.lorhi_start_date > nvl(p1.lorhi_end_date, p1.lorhi_start_date +5000))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',106);
    END IF;
  END IF;
--
  IF (p1.lorhi_start_date IS NOT NULL)
   THEN
    IF (p1.lorhi_start_date = nvl(p1.lorhi_end_date, p1.lorhi_start_date +5000))
     THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',218);
   END IF;
  END IF;
--
-- Parent Organisation exists in the Parties table
-- Firstly Name and Reference Combination
--
  IF   (p1.lorhi_par_refno    IS NOT NULL AND
        p1.lorhi_par_org_name IS NOT NULL     )
   THEN
    OPEN  chk_org_a(p1.lorhi_par_org_name, p1.lorhi_par_refno);
    FETCH chk_org_a INTO l_parent_refno;
    CLOSE chk_org_a;
    IF l_parent_refno IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',107);
    END IF;
  END IF;
--
-- Organisation exists in the Parties table
-- Now Name and Short Name Combination Supplied
--
  IF(p1.lorhi_par_refno          IS NULL     AND
     p1.lorhi_par_org_name       IS NOT NULL AND
     p1.lorhi_par_org_short_name IS NOT NULL     )
   THEN
--
    OPEN chk_no_par(p1.lorhi_par_org_name
                   ,p1.lorhi_par_org_short_name
                   ,p1.lorhi_par_org_frv_oty_code);
    FETCH chk_no_par INTO l_count_porg;
    CLOSE chk_no_par;
--
    IF l_count_porg < 1
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',219);
    END IF;
    IF l_count_porg > 1
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',108);
    END IF;
    IF l_count_porg = 1
     THEN
      OPEN chk_get_par(p1.lorhi_par_org_name
                      ,p1.lorhi_par_org_short_name
                      ,p1.lorhi_par_org_frv_oty_code);
      FETCH chk_get_par INTO l_parent_refno;
      CLOSE chk_get_par;
    END IF;
  END IF; -- End of Check Parent Org
--
-- Child Organisation exists in the Parties table
-- Firstly Name and Reference Combination
--
  IF   (p1.lorhi_par_refno_c    IS NOT NULL AND
        p1.lorhi_par_org_name_c IS NOT NULL     )
   THEN
    OPEN  chk_org_a(p1.lorhi_par_org_name_c, p1.lorhi_par_refno_c);
    FETCH chk_org_a INTO l_child_refno;
    CLOSE chk_org_a;
    IF l_child_refno IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',109);
    END IF;
  END IF;
--
-- Organisation exists in the Parties table
-- Now Name and Short Name Combination Supplied
--
  IF(p1.lorhi_par_refno_c          IS NULL     AND
     p1.lorhi_par_org_name_c       IS NOT NULL AND
     p1.lorhi_par_org_short_name_c IS NOT NULL     )
   THEN
--
    OPEN chk_no_par(p1.lorhi_par_org_name_c
                   ,p1.lorhi_par_org_short_name_c
                   ,p1.lorhi_par_org_frv_oty_code_c);
    FETCH chk_no_par INTO l_count_chorg;
    CLOSE chk_no_par;
--
    IF l_count_chorg < 1
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',220);
    END IF;
    IF l_count_chorg > 1
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',110);
    END IF;
    IF l_count_chorg = 1
     THEN
      OPEN chk_get_par(p1.lorhi_par_org_name_c
                      ,p1.lorhi_par_org_short_name_c
                      ,p1.lorhi_par_org_frv_oty_code_c);
      FETCH chk_get_par INTO l_child_refno;
      CLOSE chk_get_par;
    END IF;
  END IF; -- End of Check Child Org
--
-- Parent Child Checks once Organisations are validated
--
  IF  (l_child_refno       IS NOT NULL
   AND l_parent_refno      IS NOT NULL
   AND p1.lorhi_start_date IS NOT NULL ) THEN
--
-- Parent Child must be different
--
    IF (l_child_refno = l_parent_refno)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',195);
    END IF;
--
-- Parent Child Combination Checks
--
    OPEN chk_orhi_dup(l_parent_refno
                     ,l_child_refno
                     ,p1.lorhi_start_date
                     ,nvl(p1.lorhi_end_date,trunc(sysdate)+5000));   
    FETCH  chk_orhi_dup INTO l_dup_pc;
    CLOSE  chk_orhi_dup;
--
    IF (l_dup_pc IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',196);
    END IF;
--
    OPEN chk_orhi_dup(l_child_refno
                     ,l_parent_refno
                     ,p1.lorhi_start_date
                     ,nvl(p1.lorhi_end_date,trunc(sysdate)+5000));   
    FETCH  chk_orhi_dup INTO l_dup_cp;
    CLOSE  chk_orhi_dup;
--
    IF (l_dup_cp IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',197);
    END IF;
--
-- Checks Required are
-- a) Current Parent cannot also be a Child
-- b) Current Child cannot be Child on more than 1 Parent
-- c) Current Child cannot be Parent
--
-- a)
    OPEN chk_orhi_dup_c(l_parent_refno
                       ,p1.lorhi_start_date
                       ,nvl(p1.lorhi_end_date,trunc(sysdate)+5000));   
    FETCH  chk_orhi_dup_c INTO l_dup_a;
    CLOSE  chk_orhi_dup_c;
--
    IF (l_dup_a IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',198);
    END IF;
-- b)
    OPEN chk_orhi_dup_c(l_child_refno
                       ,p1.lorhi_start_date
                       ,nvl(p1.lorhi_end_date,trunc(sysdate)+5000));   
    FETCH  chk_orhi_dup_c INTO l_dup_b;
    CLOSE  chk_orhi_dup_c;
--
    IF (l_dup_b IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',199);
    END IF;
-- c)
    OPEN chk_orhi_dup_p(l_child_refno
                       ,p1.lorhi_start_date
                       ,nvl(p1.lorhi_end_date,trunc(sysdate)+5000));   
    FETCH  chk_orhi_dup_p INTO l_dup_c;
    CLOSE  chk_orhi_dup_p;
--
    IF (l_dup_c IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',200);
    END IF;
--
  END IF; -- end of parent and child checks 
--
-- Checks Required are
-- a) Current batch does not contain Parents that are also Children
-- b) Current batch does not contain Children that are also Parents
--
-- a)
    OPEN chk_batch_c(p1.lorhi_par_org_name
                    ,p1.lorhi_par_org_short_name
                    ,p1.lorhi_par_org_frv_oty_code
                    ,p1.lorhi_par_refno
                    ,p1.lorhi_dlb_batch_id);   
    FETCH  chk_batch_c INTO l_batch_c;
    CLOSE  chk_batch_c;
--
    IF (l_batch_c IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',201);
    END IF;
-- b)
    OPEN chk_batch_p(p1.lorhi_par_org_name_c
                    ,p1.lorhi_par_org_short_name_c
                    ,p1.lorhi_par_org_frv_oty_code_c
                    ,p1.lorhi_par_refno_c
                    ,p1.lorhi_dlb_batch_id);   
    FETCH  chk_batch_p INTO l_batch_p;
    CLOSE  chk_batch_p;
--
    IF (l_batch_p IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',202);
    END IF;
--
--*************************************
-- Now UPDATE the record count and error code
--
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
CURSOR c1 (p_batch_id VARCHAR2)
IS
SELECT
rowid rec_rowid,
lorhi_dlb_batch_id,
lorhi_dl_seqno,
lorhi_dl_load_status,
lorhi_par_org_name,
lorhi_par_org_short_name,
lorhi_par_org_frv_oty_code,
lorhi_par_refno,
lorhi_par_org_name_c,
lorhi_par_org_short_name_c,
lorhi_par_org_frv_oty_code_c,
lorhi_par_refno_c,
lorhi_start_date,
lorhi_frv_ort_code,
lorhi_created_date,
lorhi_created_by,
lorhi_end_date,
lorhi_comments,
lorhi_refno
FROM dl_hem_org_hierarchy
WHERE lorhi_dlb_batch_id   = p_batch_id
AND   lorhi_dl_load_status = 'C';
--
--*************************
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_ORG_HIERARCHY';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
i        INTEGER := 0;
l_an_tab VARCHAR2(1);
--
--*************************
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_org_hierarchy.dataload_delete');
 fsc_utils.debug_message( 's_dl_hem_org_hierarchy.dataload_delete',3 );
--
 cb := p_batch_id;
 cd := p_date;
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 (p_batch_id)
  LOOP
--
   BEGIN
--
   cs := p1.lorhi_dl_seqno;
   i := i +1;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
   DELETE FROM ORGANISATION_HIERARCHY
   WHERE orhi_refno = p1.lorhi_refno;
--
   UPDATE dl_hem_org_hierarchy
   SET lorhi_refno = null
   WHERE lorhi_dlb_batch_id = p1.lorhi_dlb_batch_id
   AND lorhi_dl_seqno = p1.lorhi_dl_seqno
   AND rowid = p1.rec_rowid;
--
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
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ORGANISATION_HIERARCHY');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_HEM_ORG_HIERARCHY');
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
END s_dl_hem_org_hierarchy;
/

show errors

