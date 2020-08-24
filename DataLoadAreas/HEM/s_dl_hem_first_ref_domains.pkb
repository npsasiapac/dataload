CREATE OR REPLACE PACKAGE BODY s_dl_hem_first_ref_domains
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0  6.16     AJ   18/04/2018 Initial Creation addition to the OHMS
--                                    Migration Project request for a
--                                    first_ref_values data loader
--      1.1  6.16     AJ   24/04/2018 Amended to initial insert sysdate then
--                                    amend by p2 if date supplied
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
  UPDATE dl_hem_first_ref_domains
  SET lfrd_dl_load_status = p_status
  WHERE rowid = p_rowid;
--
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_first_ref_domains');
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
lfrd_dlb_batch_id,
lfrd_dl_seqno,
lfrd_dl_load_status,
lfrd_domain,
lfrd_name,
lfrd_current_ind,
lfrd_default_opt_ind,
lfrd_length,
lfrd_usage,
lfrd_creation_date,
lfrd_created_by,
lfrd_product_ind,
lfrd_domain_mlang,
lfrd_name_mlang
FROM  dl_hem_first_ref_domains
WHERE lfrd_dlb_batch_id   = p_batch_id
AND   lfrd_dl_load_status = 'V';
--
-- *****************************
--
CURSOR c_chk_for_dup (p_domain VARCHAR2) IS
SELECT 'X'
FROM first_ref_domains
WHERE frd_domain = p_domain;
-- *******************
--
CURSOR c_chk_for_dup_mlang (p_frd_mlang VARCHAR2) IS
SELECT 'X'
FROM first_ref_domains
WHERE frd_domain_mlang = p_frd_mlang;
-- *******************
--
CURSOR c2 IS
SELECT NULL
FROM   first_ref_domains
ORDER  BY frd_creation_date DESC
FOR    UPDATE OF frd_creation_date;
-- *****************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_FIRST_REF_DOMAINS';
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
l_default_ind    first_ref_domains.frd_default_opt_ind%TYPE;
l_usage          first_ref_domains.frd_usage%TYPE;
l_current_ind    first_ref_domains.frd_current_ind%TYPE;
l_creation_date  first_ref_domains.frd_creation_date%TYPE;
l_created_by     first_ref_domains.frd_created_by%TYPE;
l_length         INTEGER := 0;
p2               c2%ROWTYPE;
l_chk_for_dup    VARCHAR2(1);
--
-- *****************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_first_ref_domains.dataload_create');
fsc_utils.debug_message( 's_dl_hem_first_ref_domains.dataload_create',3);
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
  cs := p1.lfrd_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'C';
  l_error_ind := 'N';
  l_default_ind   := NULL;
  l_usage         := NULL;
  l_current_ind   := NULL;
  l_creation_date := NULL;
  l_created_by    := NULL;
  l_length        := 0;
  p2 := NULL;
  l_chk_for_dup   := NULL;
--
  SAVEPOINT SP1;
--
-- Check make sure record will not violate PK
-- (PK) FRD_PK => FRD_DOMAIN (UK)
-- and also (UK) FRD_DOMAIN_MLANG
--
  OPEN  c_chk_for_dup(p1.lfrd_domain);
  FETCH c_chk_for_dup INTO l_chk_for_dup;
  CLOSE c_chk_for_dup;
--
  IF( p1.lfrd_domain_mlang IS NOT NULL  AND
      l_chk_for_dup      IS NULL         )
   THEN  
    OPEN  c_chk_for_dup_mlang(p1.lfrd_domain_mlang);
    FETCH c_chk_for_dup_mlang INTO l_chk_for_dup;
    CLOSE c_chk_for_dup_mlang;
  END IF;
--
-- Raise an error if duplicate found after previously passing validation
--
  IF (l_chk_for_dup IS NOT NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',591);
--  'The Domain Code supplied already exists'
  ELSE
--
-- Check and set defaults fields
--
   IF (p1.lfrd_default_opt_ind IS NULL)
    THEN
     l_default_ind := 'N';
   ELSE
     l_default_ind := p1.lfrd_default_opt_ind;
   END IF;
--
   IF (p1.lfrd_usage IS NULL)
    THEN
     l_usage := 'USR';
   ELSE
     l_usage := p1.lfrd_usage;
   END IF;
--
   IF (p1.lfrd_current_ind IS NULL)
    THEN
     l_current_ind := 'Y';
   ELSE
     l_current_ind := p1.lfrd_current_ind;
   END IF;
--
   IF (p1.lfrd_creation_date IS NULL)
    THEN
     l_creation_date := SYSDATE;
   ELSE
     l_creation_date := p1.lfrd_creation_date;
   END IF;
--
   IF (p1.lfrd_created_by IS NULL)
    THEN
     l_created_by := 'DATALOAD';
   ELSE
     l_created_by := p1.lfrd_created_by;
   END IF;
--
   IF (p1.lfrd_length IS NULL)
    THEN
     l_length :=10;
   ELSE
     l_length := p1.lfrd_length;
   END IF;
--
--
   INSERT INTO first_ref_domains
          (FRD_DOMAIN,
           FRD_NAME,
           FRD_CURRENT_IND,
           FRD_DEFAULT_OPT_IND,
           FRD_LENGTH,
           FRD_USAGE,
           FRD_CREATION_DATE,
           FRD_CREATED_BY,
           FRD_PRODUCT_IND,
           FRD_DOMAIN_MLANG,
           FRD_NAME_MLANG)
    VALUES
          (p1.lfrd_domain,
           p1.lfrd_name,
           l_current_ind,
           l_default_ind,
           l_length,
           l_usage,
           SYSDATE,
           l_created_by,
           p1.lfrd_product_ind,
           p1.lfrd_domain_mlang,
           p1.lfrd_name_mlang);

--
-- Now update record with created date as trigger FRD_CREATED_BY_DEFAULT sets it to sysdate
-- only needed if creation date supplied otherwise sysdate is fine   
-- INSERT INTO first_ref_values ..... do NOT use APPEND for this
--
   IF (p1.lfrd_creation_date IS NOT NULL)
    THEN
     OPEN  c2;
     FETCH c2 INTO p2;
       IF    c2%FOUND
        THEN
         UPDATE first_ref_domains
         SET    frd_creation_date = l_creation_date
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
  l_an_tab := s_dl_hem_utils.dl_comp_stats('FIRST_REF_DOMAINS');
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
lfrd_dlb_batch_id,
lfrd_dl_seqno,
lfrd_dl_load_status,
lfrd_domain,
lfrd_name,
nvl(lfrd_current_ind,'Y') lfrd_current_ind, 
nvl(lfrd_default_opt_ind,'N') lfrd_default_opt_ind,
nvl(lfrd_length,10) lfrd_length,
nvl(lfrd_usage,'USR') lfrd_usage, 
lfrd_creation_date,
nvl(lfrd_created_by,'DATALOAD') lfrd_created_by,
lfrd_product_ind,
lfrd_domain_mlang,
lfrd_name_mlang
FROM  dl_hem_first_ref_domains
WHERE lfrd_dlb_batch_id   = p_batch_id
AND   lfrd_dl_load_status IN ('L','F','O');
--
-- *****************************
--
CURSOR c_chk_pro_ind (p_pro_ind VARCHAR2) IS
SELECT 'X'
FROM first_ref_values
WHERE frv_frd_domain = 'DOMAIN_PRODUCT'
AND frv_current_ind = 'Y'
AND frv_code = p_pro_ind;
-- *******************
--
CURSOR c_chk_frd_dup_dl (p_batch  VARCHAR2
                        ,p_domain VARCHAR2) IS
SELECT count(*)
FROM dl_hem_first_ref_domains
WHERE lfrd_dlb_batch_id = p_batch
AND lfrd_domain = p_domain;
-- *******************
--
CURSOR c_chk_mlang_dup (p_domain VARCHAR2) IS
SELECT 'X'
FROM first_ref_domains
WHERE frd_domain_mlang = p_domain;
-- *******************
--
CURSOR c_chk_mlang_dup_dl (p_batch  VARCHAR2
                          ,p_domain VARCHAR2) IS
SELECT count(*)
FROM dl_hem_first_ref_domains
WHERE lfrd_dlb_batch_id = p_batch
AND lfrd_domain_mlang = p_domain;
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
ct VARCHAR2(30) := 'DL_HEM_FIRST_REF_DOMAINS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER := 0;
l_chk_frd        VARCHAR2(1);
l_chk_pro_ind    VARCHAR2(1);
l_chk_mlang_dup  VARCHAR2(1);
l_chk_dl_frd     INTEGER;
l_chk_dl_mlang   INTEGER;
--
-- *****************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_first_ref_domains.dataload_validate');
fsc_utils.debug_message( 's_dl_hem_first_ref_domains.dataload_validate',3 );
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
  cs := p1.lfrd_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
  l_chk_frd := NULL;
  l_chk_pro_ind := NULL;
  l_chk_mlang_dup := NULL;
  l_chk_dl_frd := 0;
  l_chk_dl_mlang := 0;
--
-- Mandatory field check and content
--
-- Domain Code
--
  IF (p1.lfrd_domain  IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',577);
-- 'The Domain Code must be supplied'
--
  ELSE
--
    OPEN  c_chk_for_frd(p1.lfrd_domain);
    FETCH c_chk_for_frd INTO l_chk_frd;
    CLOSE c_chk_for_frd;
     IF (l_chk_frd IS NOT NULL)
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',591);
-- 'The Domain Code supplied already exists'
     END IF;
--
  END IF;
--
-- frd_domain description
--
  IF (p1.lfrd_name  IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',593);
-- 'The Domain Code Description must be supplied'
  END IF;
--
-- Other Fields and combinations
--
-- product indicator
--
  IF (p1.lfrd_product_ind  IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',594);
-- 'The Application Product Indicator must be supplied'
  END IF;
--
  IF (p1.lfrd_product_ind  IS NOT NULL)
   THEN
--
    OPEN  c_chk_pro_ind( p1.lfrd_product_ind);
    FETCH c_chk_pro_ind INTO l_chk_pro_ind;
    CLOSE c_chk_pro_ind;
--
    IF (l_chk_pro_ind IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',595);
-- 'The Application Product Indicator must exist in the REFERENCE domain DOMAIN_PRODUCT'
    END IF;
--
  END IF;
--
-- Current Ind must be Y or N (default is Y)
-- 
  IF (p1.lfrd_current_ind NOT IN ('Y', 'N'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',581);
-- 'The Current Indicator if supplied must be Y or N'
  END IF;
--
-- Default Ind must be Y or N (default is N)
-- 
  IF (p1.lfrd_default_opt_ind NOT IN ('Y', 'N'))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',582);
-- 'The Default Indicator if supplied must be Y or N'
  END IF;
--
-- Usage Ind must be USR (could be SYS but data load only loads USR types)
-- 
  IF (p1.lfrd_usage != 'USR')
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',583);
-- 'The Usage Type if supplied must be USR'
  END IF;
--
-- check length (maximum of 10)
-- 
  IF (p1.lfrd_length < 1 OR p1.lfrd_length > 10)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',592);
-- 'The Domain Code values length has a maximum of 10'
  END IF;
--
-- Check Domain Code in data load batch for duplicates
--
  IF (  p1.lfrd_domain  IS NOT NULL)
   THEN
--
    OPEN  c_chk_frd_dup_dl( p1.lfrd_dlb_batch_id
                           ,p1.lfrd_domain);
    FETCH c_chk_frd_dup_dl INTO l_chk_dl_frd;
    CLOSE c_chk_frd_dup_dl;
--
    IF (l_chk_dl_frd > 1)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',586);
-- 'Duplicate records exist in the data load batch for the First Ref Value Code and Domain'
    END IF;
--
  END IF;
--
-- Check Domain Code MLANG in main table and data load batch
--
  IF ( p1.lfrd_domain_mlang  IS NOT NULL)
   THEN
--
    OPEN  c_chk_mlang_dup(p1.lfrd_domain_mlang);
    FETCH c_chk_mlang_dup INTO l_chk_mlang_dup;
    CLOSE c_chk_mlang_dup;
--
    IF (l_chk_mlang_dup IS NOT NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',596);
-- 'The Mlang Domain Code supplied already exists'
    END IF;
--
    OPEN  c_chk_mlang_dup_dl( p1.lfrd_dlb_batch_id
                             ,p1.lfrd_domain_mlang);
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
-- The frd_domain_MLANG and frd_name_MLANG are mandatory
-- if either has been supplied
--
  IF (  p1.lfrd_name_mlang  IS NULL
    AND p1.lfrd_domain_mlang  IS NOT NULL )
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',589);
-- 'Bilingual(MLANG) Name must be supplied as the (MLANG) Code has been supplied'
  END IF;
--
  IF (  p1.lfrd_name_mlang  IS NOT NULL
    AND p1.lfrd_domain_mlang  IS NULL )
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',590);
-- 'Bilingual(MLANG) Code must be supplied as the (MLANG) Name has been supplied'
  END IF;
--
-- Check the Created date if supplied
--
  IF (nvl(p1.lfrd_creation_date,TRUNC(SYSDATE)) > TRUNC(SYSDATE))
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
--
-- ***********************************************************************
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid,
lfrd_dlb_batch_id,
lfrd_dl_seqno,
lfrd_dl_load_status,
lfrd_domain,
lfrd_name,
lfrd_current_ind,
lfrd_default_opt_ind,
lfrd_length,
lfrd_usage,
lfrd_creation_date,
lfrd_created_by,
lfrd_product_ind,
lfrd_domain_mlang,
lfrd_name_mlang
FROM  dl_hem_first_ref_domains
WHERE lfrd_dlb_batch_id   = p_batch_id
AND   lfrd_dl_load_status = 'C';
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
ct       VARCHAR2(30) := 'DL_HEM_FIRST_REF_DOMAINS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_first_ref_domains.dataload_delete');
fsc_utils.debug_message( 's_dl_hem_first_ref_domains.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lfrd_dl_seqno;
  i  := i +1;
  l_id := p1.rec_rowid;
--
-- delete record created just needs to match PK
-- (PK) FRD_PK => FRD_DOMAIN
--
  DELETE FROM first_ref_domains
   WHERE frd_domain = p1.lfrd_domain;
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
l_an_tab := s_dl_hem_utils.dl_comp_stats('FIRST_REF_DOMAINS');
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
END s_dl_hem_first_ref_domains;
/

show errors

