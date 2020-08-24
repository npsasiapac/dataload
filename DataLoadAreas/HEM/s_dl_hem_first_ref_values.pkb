CREATE OR REPLACE PACKAGE BODY s_dl_hem_first_ref_values
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0  6.16     AJ   28/03/2018 Initial Creation for OHMS Migration Project
--      1.1  6.16     AJ   18/04/2018 complete
--      1.2  6.16     AJ   18/04/2018 Amended to initial insert sysdate then
--                                    amend by p2 if date supplied
-- 
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hem_first_ref_values
  SET lfrv_dl_load_status = p_status
  WHERE rowid = p_rowid;
--
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_first_ref_values');
     RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
--  declare package variables AND constants
--
PROCEDURE dataload_create
   (p_batch_id          IN VARCHAR2
   ,p_date              IN DATE    )
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid,
lfrv_dlb_batch_id,
lfrv_dl_seqno,
lfrv_dl_load_status,
lfrv_frd_domain,
lfrv_code,
lfrv_name,
lfrv_current_ind, 
lfrv_usage, 
lfrv_creation_date,
lfrv_created_by,
lfrv_default_ind,
lfrv_sequence,
lfrv_text,
lfrv_code_mlang,
lfrv_name_mlang
FROM  dl_hem_first_ref_values
WHERE lfrv_dlb_batch_id   = p_batch_id
AND   lfrv_dl_load_status = 'V';
--
-- *****************************
--
CURSOR c_chk_for_dup (p_domain VARCHAR2
                     ,p_code   VARCHAR2 ) IS
SELECT 'X'
FROM first_ref_values
WHERE frv_frd_domain = p_domain
AND frv_code = p_code;
-- *******************
--
CURSOR c_chk_for_dup_mlang (p_domain VARCHAR2
                           ,p_code   VARCHAR2 ) IS
SELECT 'X'
FROM first_ref_values
WHERE frv_frd_domain = p_domain
AND frv_code_mlang = p_code;
-- *******************
--
CURSOR c2 IS
SELECT NULL
FROM   first_ref_values
ORDER  BY frv_creation_date DESC
FOR    UPDATE OF frv_creation_date;
-- *****************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_FIRST_REF_VALUES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_default_ind    first_ref_values.frv_default_ind%TYPE;
l_usage          first_ref_values.frv_usage%TYPE;
l_current_ind    first_ref_values.frv_current_ind%TYPE;
l_creation_date  first_ref_values.frv_creation_date%TYPE;
l_created_by     first_ref_values.frv_created_by%TYPE;
p2               c2%ROWTYPE;
l_chk_for_dup    VARCHAR2(1);
--
-- *****************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_first_ref_values.dataload_create');
fsc_utils.debug_message( 's_dl_hem_first_ref_values.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lfrv_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'C';
  l_error_ind := 'N';
  l_default_ind   := NULL;
  l_usage         := NULL;
  l_current_ind   := NULL;
  l_creation_date := NULL;
  l_created_by    := NULL;
  p2 := NULL;
  l_chk_for_dup   := NULL;
--
  SAVEPOINT SP1;
--
-- Check make sure record will not violate PK
-- (PK) FRV_PK => FRV_FRD_DOMAIN + FRV_CODE
-- and also       FRV_FRD_DOMAIN + FRV_CODE_MLANG
--
  OPEN  c_chk_for_dup(p1.lfrv_frd_domain,p1.lfrv_code);
  FETCH c_chk_for_dup INTO l_chk_for_dup;
  CLOSE c_chk_for_dup;
--
  IF( p1.lfrv_code_mlang IS NOT NULL  AND
      l_chk_for_dup      IS NULL         )
   THEN  
    OPEN  c_chk_for_dup_mlang(p1.lfrv_frd_domain,p1.lfrv_code_mlang);
    FETCH c_chk_for_dup_mlang INTO l_chk_for_dup;
    CLOSE c_chk_for_dup_mlang;
  END IF;
--
-- Raise an error if duplicate found after previously passing validation
--
  IF (l_chk_for_dup IS NOT NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',576);
--  'The Domain and frv_code or frv_code_mlang combination already exist'
  ELSE
--
-- Check and set defaults fields
--
   IF (p1.lfrv_default_ind IS NULL)
    THEN
     l_default_ind := 'N';
   ELSE
     l_default_ind := p1.lfrv_default_ind;
   END IF;
--
   IF (p1.lfrv_usage IS NULL)
    THEN
     l_usage := 'USR';
   ELSE
     l_usage := p1.lfrv_usage;
   END IF;
--
   IF (p1.lfrv_current_ind IS NULL)
    THEN
     l_current_ind := 'Y';
   ELSE
     l_current_ind := p1.lfrv_current_ind;
   END IF;
--
   IF (p1.lfrv_creation_date IS NULL)
    THEN
     l_creation_date := SYSDATE;
   ELSE
     l_creation_date := p1.lfrv_creation_date;
   END IF;
--
   IF (p1.lfrv_created_by IS NULL)
    THEN
     l_created_by := 'DATALOAD';
   ELSE
     l_created_by := p1.lfrv_created_by;
   END IF;
--
--
   INSERT INTO first_ref_values
          (FRV_FRD_DOMAIN,
           FRV_CODE,
           FRV_NAME,
           FRV_CURRENT_IND,
           FRV_USAGE,
           FRV_CREATION_DATE,
           FRV_CREATED_BY,
           FRV_DEFAULT_IND,
           FRV_SEQUENCE,
           FRV_TEXT,
           FRV_CODE_MLANG,
           FRV_NAME_MLANG)
    VALUES
          (p1.lfrv_frd_domain,
           p1.lfrv_code,
           p1.lfrv_name,
           l_current_ind, 
           l_usage, 
           SYSDATE,
           l_created_by,
           l_default_ind,
           p1.lfrv_sequence,
           p1.lfrv_text,
           p1.lfrv_code_mlang,
           p1.lfrv_name_mlang);

--
-- Now update record with created date as trigger frv_br_i sets it to sysdate
-- only needed if creation date supplied otherwise sysdate is fine   
-- INSERT INTO first_ref_values ..... do NOT use APPEND for this
--
   IF (p1.lfrv_creation_date IS NOT NULL)
    THEN
     OPEN  c2;
     FETCH c2 INTO p2;
       IF    c2%FOUND
        THEN
         UPDATE first_ref_values
         SET    frv_creation_date = l_creation_date
--               ,frd_created_by = l_created_by
         WHERE  CURRENT OF c2;
       END IF;
     CLOSE c2;
   END IF;
--
  END IF;
--
-- *****************************
-- keep a count of the rows processed and commit after every 1000
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
  s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
  set_record_status_flag(l_id,l_errors);
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
-- *****************************
-- Section to analyse the tables populated with this data load
--
  l_an_tab := s_dl_hem_utils.dl_comp_stats('FIRST_REF_VALUES');
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
-- ***********************************************************************
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid,
lfrv_dlb_batch_id,
lfrv_dl_seqno,
lfrv_dl_load_status,
lfrv_frd_domain,
lfrv_code,
lfrv_name,
nvl(lfrv_current_ind,'Y') lfrv_current_ind, 
nvl(lfrv_usage,'USR') lfrv_usage, 
lfrv_creation_date,
nvl(lfrv_created_by,'DATALOAD') lfrv_created_by,
nvl(lfrv_default_ind,'N') lfrv_default_ind,
lfrv_sequence,
lfrv_text,
lfrv_code_mlang,
lfrv_name_mlang
FROM  dl_hem_first_ref_values
WHERE lfrv_dlb_batch_id   = p_batch_id
AND   lfrv_dl_load_status IN ('L','F','O');
--
-- *****************************
--
CURSOR c_chk_frv_dup (p_domain VARCHAR2
                     ,p_code   VARCHAR2 ) IS
SELECT 'X'
FROM first_ref_values
WHERE frv_frd_domain = p_domain
AND frv_code = p_code;
-- *******************
--
CURSOR c_chk_frv_dup_dl (p_batch  VARCHAR2
                        ,p_domain VARCHAR2
                        ,p_code   VARCHAR2 ) IS
SELECT count(*)
FROM dl_hem_first_ref_values
WHERE lfrv_dlb_batch_id = p_batch
AND lfrv_frd_domain = p_domain
AND lfrv_code = p_code;
-- *******************
--
CURSOR c_chk_mlang_dup (p_domain VARCHAR2
                       ,p_code   VARCHAR2 ) IS
SELECT 'X'
FROM first_ref_values
WHERE frv_frd_domain = p_domain
AND frv_code_mlang = p_code;
-- *******************
--
CURSOR c_chk_mlang_dup_dl (p_batch  VARCHAR2
                          ,p_domain VARCHAR2
                          ,p_code   VARCHAR2 ) IS
SELECT count(*)
FROM dl_hem_first_ref_values
WHERE lfrv_dlb_batch_id = p_batch
AND lfrv_frd_domain = p_domain
AND lfrv_code_mlang = p_code;
-- *******************
--
CURSOR c_chk_for_frd (p_domain VARCHAR2) IS
SELECT 'X'
FROM first_ref_domains
WHERE frd_domain = p_domain;
-- *****************************
--
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HEM_FIRST_REF_VALUES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER := 0;
l_chk_frd        VARCHAR2(1);
l_chk_frv_dup    VARCHAR2(1);
l_chk_mlang_dup  VARCHAR2(1);
l_chk_dl_frv     INTEGER;
l_chk_dl_mlang   INTEGER;
--
-- *****************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_first_ref_values.dataload_validate');
fsc_utils.debug_message( 's_dl_hem_first_ref_values.dataload_validate',3 );
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
  cs := p1.lfrv_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
  l_chk_frd := NULL;
  l_chk_frv_dup := NULL;
  l_chk_mlang_dup := NULL;
  l_chk_dl_frv := 0;
  l_chk_dl_mlang := 0;
--
-- Mandatory field check and content
--
-- Domain Code
--
  IF (p1.lfrv_frd_domain  IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',577);
-- 'The Domain Code must be supplied'
--
  ELSE
--
    OPEN  c_chk_for_frd(p1.lfrv_frd_domain);
    FETCH c_chk_for_frd INTO l_chk_frd;
    CLOSE c_chk_for_frd;
     IF (l_chk_frd IS NULL)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',578);
-- 'The Domain Code supplied does not exist'
     END IF;
--
  END IF;
--
-- frv_code and description
--
  IF (p1.lfrv_code  IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',579);
-- 'The First Ref Value Code must be supplied'
  END IF;
--
  IF (p1.lfrv_name  IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',580);
-- 'The First Ref Value Description must be supplied'
  END IF;
--
-- Other Fields and combinations
--
-- Current Ind must be Y or N (default is Y)
-- 
  IF (p1.lfrv_current_ind NOT IN ('Y', 'N'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',581);
-- 'The Current Indicator if supplied must be Y or N'
  END IF;
--
-- Default Ind must be Y or N (default is N)
-- 
  IF (p1.lfrv_default_ind NOT IN ('Y', 'N'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',582);
-- 'The Default Indicator if supplied must be Y or N'
  END IF;
--
-- Usage Ind must be USR (could be SYS but data load only loads USR types)
-- 
  IF (p1.lfrv_usage != 'USR')
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',583);
-- 'The Usage Type if supplied must be USR'
  END IF;
--
-- Sequence number
-- 
  IF (p1.lfrv_sequence IS NOT NULL)
   THEN
    IF (p1.lfrv_sequence < 1 OR p1.lfrv_sequence > 999998)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',584);
-- 'The Sequence number must be between 0 and 999999'
    END IF;
  END IF;
--
-- Check Domain Code and frv code combinations
-- in main table and data load batch
--
  IF (  p1.lfrv_frd_domain  IS NOT NULL
    AND p1.lfrv_code        IS NOT NULL )
   THEN
--
    OPEN  c_chk_frv_dup( p1.lfrv_frd_domain
                        ,p1.lfrv_code      );
    FETCH c_chk_frv_dup INTO l_chk_frv_dup;
    CLOSE c_chk_frv_dup;
--
    IF (l_chk_frv_dup IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',585);
-- 'The First Ref Value Code already exists against the domain supplied'
    END IF;
--
    OPEN  c_chk_frv_dup_dl( p1.lfrv_dlb_batch_id
                           ,p1.lfrv_frd_domain
                           ,p1.lfrv_code      );
    FETCH c_chk_frv_dup_dl INTO l_chk_dl_frv;
    CLOSE c_chk_frv_dup_dl;
--
    IF (l_chk_dl_frv > 1)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',586);
-- 'Duplicate records exist in the data load batch for the First Ref Value Code and Domain'
    END IF;
--
  END IF;
--
--
-- Check Domain Code and frv_code_MLANG combinations
-- in main table and data load batch
--
  IF (  p1.lfrv_frd_domain  IS NOT NULL
    AND p1.lfrv_code_mlang  IS NOT NULL )
   THEN
--
    OPEN  c_chk_mlang_dup( p1.lfrv_frd_domain
                          ,p1.lfrv_code_mlang );
    FETCH c_chk_mlang_dup INTO l_chk_mlang_dup;
    CLOSE c_chk_mlang_dup;
--
    IF (l_chk_mlang_dup IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',587);
-- 'The First Ref Value MLANG Code already exists against the domain supplied'
    END IF;
--
    OPEN  c_chk_mlang_dup_dl( p1.lfrv_dlb_batch_id
                             ,p1.lfrv_frd_domain
                             ,p1.lfrv_code_mlang );
    FETCH c_chk_mlang_dup_dl INTO l_chk_dl_mlang;
    CLOSE c_chk_mlang_dup_dl;
--
    IF (l_chk_dl_mlang > 1)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',588);
-- 'Duplicate records exist in the data load batch for the First Ref Value MLANG Code and Domain'
    END IF;
--
  END IF;
--
-- The frv_code_MLANG and frv_name_MLANG are mandatory
-- if either has been supplied
--
  IF (  p1.lfrv_name_mlang  IS NULL
    AND p1.lfrv_code_mlang  IS NOT NULL )
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',589);
-- 'Bilingual(MLANG) Name must be supplied as the (MLANG) Code has been supplied'
  END IF;
--
  IF (  p1.lfrv_name_mlang  IS NOT NULL
    AND p1.lfrv_code_mlang  IS NULL )
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',590);
-- 'Bilingual(MLANG) Code must be supplied as the (MLANG) Name has been supplied'
  END IF;
--
-- Check the Created date if supplied
--
  IF (nvl(p1.lfrv_creation_date,TRUNC(SYSDATE)) > TRUNC(SYSDATE))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',531);
-- 'The Created date cannot be later than today (truncated sysdate)'
  END IF;
--
-- *****************************************
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
--
  END;
--
 END LOOP;
--
--
fsc_utils.proc_end;
commit;
--
  EXCEPTION
   WHEN OTHERS THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ***********************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid,
lfrv_dlb_batch_id,
lfrv_dl_seqno,
lfrv_dl_load_status,
lfrv_frd_domain,
lfrv_code,
lfrv_name,
lfrv_current_ind, 
lfrv_usage, 
lfrv_creation_date,
lfrv_created_by,
lfrv_default_ind,
lfrv_sequence,
lfrv_text,
lfrv_code_mlang,
lfrv_name_mlang
FROM  dl_hem_first_ref_values
WHERE lfrv_dlb_batch_id   = p_batch_id
AND   lfrv_dl_load_status = 'C';
--
-- ********************************
i INTEGER := 0;
l_an_tab  VARCHAR2(1);
--
-- *******************************
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_FIRST_REF_VALUES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_first_ref_values.dataload_delete');
fsc_utils.debug_message( 's_dl_hem_first_ref_values.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lfrv_dl_seqno;
  i  := i +1;
  l_id := p1.rec_rowid;
--
-- delete record created just needs to match PK
-- (PK) FRV_PK => FRV_FRD_DOMAIN + FRV_CODE
--  and also      FRV_FRD_DOMAIN + FRV_CODE_MLANG
--
  DELETE FROM first_ref_values
   WHERE frv_frd_domain = p1.lfrv_frd_domain
     AND frv_code = p1.lfrv_code;
--
--
-- ***********************************
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'V');
--
  EXCEPTION
   WHEN OTHERS THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
 END LOOP;
--
-- ***********************************
-- Section to analyse the tables populated with this dataload
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('FIRST_REF_VALUES');
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
END s_dl_hem_first_ref_values;
/

show errors

