--
-- CHECK VALIDATE AROUND AGENT AND PARTY REF FOR LANDLORD AS ISSUE WITH
-- CREATING PROPERTY LANDLORDS AGENT ONLY REQUIRED IF NOT PAYING DIRECT TO LANDLORD
--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_prop_landlord_banks
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VER  DB Ver   WHO  WHEN       	WHY
--  1.0  6.13.0   MB   18-JUL-2016  Initial Dataload
--  1.1  6.13     AJ   26-JUL-2016  Further changes to create, validate
--                                   and delete done along with format changes
--  1.2  6.13     AJ   27-JUL-2016  1) Bank and Branch Codes and Mlang fields
--                                  added to create
--                                  2) Updates to DL table in create section
--                                  to aid delete of created records
--  1.3  6.13     AJ   02_AUG-2016  more validation added around bank details
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
  UPDATE dl_hem_prop_landlord_banks
  SET lplb_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_prop_landlord_banks');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************     
--
PROCEDURE dataload_create
(p_batch_id	IN VARCHAR2,
 p_date     IN DATE)
AS
--
CURSOR c1 is
  SELECT rowid rec_rowid,
         LPLB_DL_SEQNO,
         LPLB_PRO_PROPREF,
         LPLB_PLD_START_DATE,
         LPLB_PAR_REFNO,
         LPLB_ALT_PAR_REF,
         LPLB_START_DATE,
         LPLB_BAD_ACCOUNT_NO,
         LPLB_BAD_ACCOUNT_NAME,
         LPLB_BAD_SORT_CODE,
         nvl(LPLB_BAD_START_DATE,LPLB_START_DATE) LPLB_BAD_START_DATE,
         LPLB_BDE_BANK_NAME,
         LPLB_BDE_BRANCH_NAME,
         LPLB_BDE_BTY_CODE,
         LPLB_BDE_BANK_CODE,
         LPLB_BDE_BRANCH_CODE,
         LPLB_BDE_BANK_NAME_MLANG,
         LPLB_BDE_BRANCH_NAME_MLANG
    FROM dl_hem_prop_landlord_banks
   WHERE lplb_dlb_batch_id   = p_batch_id
     AND lplb_dl_load_status = 'V'
ORDER BY lplb_dl_seqno asc;
--
-- **********************************
--
-- Get the par_refno for lplb_alt_par_ref and lplb_agent_alt_par_ref
--
CURSOR get_par_refno(p_alt_par_ref VARCHAR2) IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_alt_par_ref;
--
-- Get the pld_refno for the Property Landlord record
--
CURSOR get_pld_refno(p_par_refno NUMBER, p_pro_refno NUMBER, p_start_date DATE)
IS
SELECT pld_refno
FROM property_landlords
WHERE pld_pro_refno = p_pro_refno
AND   pld_par_refno = p_par_refno
AND   pld_start_date = p_start_date;
--
CURSOR c_bde_exists(p_bank_name VARCHAR2, p_branch_name VARCHAR2)
IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name   = p_bank_name
   AND nvl(bde_branch_name,'~') = nvl(p_branch_name,'~');
--
CURSOR c_bde_refno
IS
SELECT bde_refno_seq.nextval
  FROM dual;
--
CURSOR c_bad_exists(p_bde_refno NUMBER, p_account_no VARCHAR2,
                    p_account_name VARCHAR2, p_sort_code VARCHAR2,
					p_start_date DATE)
IS
SELECT  bad_refno
  FROM  bank_account_details
  WHERE bad_bde_refno = p_bde_refno
  AND   nvl(bad_sort_code,'ABCDEFG') = nvl(p_sort_code,'ABCDEFG')
  AND   bad_account_no   = p_account_no
  AND   bad_account_name = p_account_name
  AND   bad_start_date   = p_start_date;
--
CURSOR c_bad_refno
IS
SELECT bad_refno_seq.nextval
  FROM dual;
--
CURSOR c_pba_exists (p_par_refno NUMBER, p_bad_refno NUMBER, p_start_date DATE)
IS
  SELECT 'X'
  FROM   party_bank_accounts
  WHERE  pba_par_refno  = p_par_refno
  AND    pba_bad_refno  = p_bad_refno
  AND    pba_start_date = p_start_date;
--
CURSOR c_plb_end_date (p_pld_refno NUMBER, p_start_date DATE)
IS
  SELECT plb_end_date
  FROM   property_landlord_banks
  WHERE  plb_pld_refno = p_pld_refno
  AND    nvl(plb_end_date,p_start_date) > p_start_date - 1;
--
-- **********************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_PROP_LANDLORD_BANKS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
i			    INTEGER := 0;
l_an_tab		VARCHAR2(1);
l_answer        VARCHAR2(1);
l_pro_refno		NUMBER(10);
l_par_refno		NUMBER(10);
l_pld_refno	    NUMBER(10);
l_bde_refno     NUMBER(10);
l_bad_refno     NUMBER(10);
l_exists        VARCHAR2(1);
l_bank_code     VARCHAR2(10);
l_branch_code   VARCHAR2(10);
l_bank_name_mlang    VARCHAR2(35);
l_branch_name_mlang  VARCHAR2(30);
l_plb_end_date  DATE;

--
-- **********************************
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_prop_landlord_banks.dataload_create');
 fsc_utils.debug_message('s_dl_hem_prop_landlord_banks.dataload_create',3);
--
 cb := p_batch_id;
 cd := p_date;
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 l_answer  := s_dl_batches.get_answer(p_batch_id, 1);	
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lplb_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_pro_refno := NULL;
  l_par_refno := NULL;
  l_pld_refno := NULL;
  l_bde_refno := NULL;
  l_bad_refno := NULL;
  l_plb_end_date := NULL;
--
  IF (p1.lplb_bde_bank_code IS NOT NULL)
   THEN
    l_bank_code := p1.lplb_bde_bank_code;
   ELSE
    l_bank_code := NULL;
  END IF;
--
  IF (p1.lplb_bde_branch_code IS NOT NULL)
   THEN
    l_branch_code := p1.lplb_bde_branch_code;
   ELSE
    l_branch_code := NULL;
  END IF;
--
  IF (p1.lplb_bde_bank_name_mlang IS NOT NULL)
   THEN
    l_bank_name_mlang := p1.lplb_bde_bank_name_mlang;
   ELSE
    l_bank_name_mlang := NULL;
  END IF;
--
  IF (p1.lplb_bde_branch_name_mlang IS NOT NULL)
   THEN
    l_branch_name_mlang := p1.lplb_bde_branch_name_mlang;
   ELSE
    l_branch_name_mlang := NULL;
  END IF;
--
-- get the pro_refno (PROPERTIES) and capture for delete if needed
--
  l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lplb_pro_propref);
--
-- set LPLB_PLD_PRO_REFNO as l_pro_refno
--
  UPDATE dl_hem_prop_landlord_banks
  SET lplb_pld_pro_refno = l_pro_refno
  WHERE rowid = p1.rec_rowid
  AND lplb_dl_seqno = p1.lplb_dl_seqno;
--
-- get the par_refno (PARTIES) and capture for delete if needed
--
  IF (p1.lplb_alt_par_ref IS NOT NULL) THEN
--
   OPEN get_par_refno(p1.lplb_alt_par_ref);
   FETCH get_par_refno INTO l_par_refno;
   CLOSE get_par_refno;
--
  ELSE
--
   l_par_refno := p1.lplb_par_refno;
--
  END IF;
--
-- LPLB_DEL_PAR_REFNO as l_par_refno
--
  UPDATE dl_hem_prop_landlord_banks
  SET lplb_del_par_refno = l_par_refno
  WHERE rowid = p1.rec_rowid
  AND lplb_dl_seqno = p1.lplb_dl_seqno;
--
-- get the pld_refno (PROPERTY_LANDLORDS) and capture for delete if needed
--
  OPEN get_pld_refno(l_par_refno, l_pro_refno, p1.lplb_pld_start_date);
  FETCH get_pld_refno INTO l_pld_refno;
  CLOSE get_pld_refno;
--
-- LPLB_DEL_PLD_REFNO as l_pld_refno
--
  UPDATE dl_hem_prop_landlord_banks
  SET lplb_del_pld_refno = l_pld_refno
  WHERE rowid = p1.rec_rowid
  AND lplb_dl_seqno = p1.lplb_dl_seqno;
--
-- If previous record needs ending...check and record plb_end_date
-- if one already exists so it can be put back by delete process if
-- required
--
  IF l_answer = 'Y'
   THEN
    l_plb_end_date := NULL;
--
    OPEN c_plb_end_date(l_pld_refno, p1.lplb_start_date);    
    FETCH c_plb_end_date into l_plb_end_date;           
    CLOSE c_plb_end_date;  
--
    IF l_plb_end_date IS NOT NULL
     THEN
      UPDATE dl_hem_prop_landlord_banks
      SET lplb_end_date = l_plb_end_date
      WHERE rowid = p1.rec_rowid
      AND lplb_dl_seqno = p1.lplb_dl_seqno;
    END IF;
--
    UPDATE property_landlord_banks
    SET    plb_end_date = p1.lplb_start_date-1
    WHERE  plb_pld_refno = l_pld_refno
    AND    nvl(plb_end_date,p1.lplb_start_date) > p1.lplb_start_date - 1;
  END IF;
--
-- Now we need to get refno's insert into bank_details,        
-- bank account details, party_bank_accounts and       
-- property_landlord_banks   
--         
-- So does the bank detail already exist       
--
  OPEN c_bde_exists(p1.lplb_bde_bank_name,p1.lplb_bde_branch_name);    
  FETCH c_bde_exists into l_bde_refno;           
  CLOSE c_bde_exists;        
--
-- create new bank detail record if not found
-- 1 record only is required for each Bank Branch combination
-- set lplb_del_bde_ind to Y if record is being created and
-- capture the bde_refno
--      
  IF l_bde_refno IS NULL         
   THEN       
    OPEN c_bde_refno;        
    FETCH c_bde_refno into l_bde_refno;          
    CLOSE c_bde_refno;       
--
    INSERT INTO BANK_DETAILS             
    (bde_refno       
    ,bde_bank_name   
    ,bde_created_by  
    ,bde_created_date        
    ,bde_branch_name         
    ,bde_bty_code
    ,bde_bank_code
    ,bde_branch_code
    ,bde_bank_name_mlang
    ,bde_branch_name_mlang)   
    VALUES
    (l_bde_refno     
    ,p1.lplb_bde_bank_name       
    ,'DATALOAD'   
    ,sysdate         
    ,p1.lplb_bde_branch_name             
    ,p1.lplb_bde_bty_code
    ,l_bank_code
    ,l_branch_code
    ,l_bank_name_mlang
    ,l_branch_name_mlang);
--
-- set delete indicator as created by DL
--
    UPDATE dl_hem_prop_landlord_banks
    SET lplb_del_bde_ind = 'Y'
    WHERE rowid = p1.rec_rowid
    AND lplb_dl_seqno = p1.lplb_dl_seqno;
--
  END IF;    
--
-- set LPLB_DEL_BDE_REFNO as l_bde_refno
--
  UPDATE dl_hem_prop_landlord_banks
  SET lplb_del_bde_refno = l_bde_refno
  WHERE rowid = p1.rec_rowid
  AND lplb_dl_seqno = p1.lplb_dl_seqno;
--
--
-- Now insert into bank_account_details   
-- many bank account details link to 1 bank detail
-- set lplb_del_bad_ind to Y if record is being created and
-- capture the bad_refno
--
  l_bad_refno := NULL; 
--
  OPEN c_bad_exists(l_bde_refno,
                   p1.lplb_bad_account_no,
                   p1.lplb_bad_account_name,
                   p1.lplb_bad_sort_code,
                   p1.lplb_bad_start_date);
  FETCH c_bad_exists INTO l_bad_refno;
  CLOSE c_bad_exists;
--
  IF l_bad_refno IS NULL
   THEN 
--
    OPEN c_bad_refno;  
    FETCH c_bad_refno into l_bad_refno;    
    CLOSE c_bad_refno;         
--         
    INSERT INTO BANK_ACCOUNT_DETAILS       
    (bad_refno         
    ,bad_type          
    ,bad_sort_code     
    ,bad_bde_refno     
    ,bad_account_no    
    ,bad_account_name  
    ,bad_start_date    
    ,bad_created_by    
    ,bad_created_date  
    ,bad_end_date      
    ,bad_user_alt_ref)         
    VALUES     
    (l_bad_refno       
    ,'CUS'     
    ,p1.lplb_bad_sort_code         
    ,l_bde_refno       
    ,p1.lplb_bad_account_no        
    ,p1.lplb_bad_account_name      
    ,p1.lplb_bad_start_date        
    ,'DATALOAD'
    ,sysdate           
    ,NULL      
    ,NULL);    
--
-- set delete indicator as created by DL
--
    UPDATE dl_hem_prop_landlord_banks
    SET lplb_del_bad_ind = 'Y'
    WHERE rowid = p1.rec_rowid
    AND lplb_dl_seqno = p1.lplb_dl_seqno;
--
  END IF;
--
-- set LPLB_DEL_BAD_REFNO as l_bad_refno
--
  UPDATE dl_hem_prop_landlord_banks
  SET lplb_del_bad_refno = l_bad_refno
  WHERE rowid = p1.rec_rowid
  AND lplb_dl_seqno = p1.lplb_dl_seqno;
--
--         
-- and now into party_bank_accounts   
-- many bank account details can link to many party bank accounts
-- set lplb_del_pba_ind to Y if record is being created
--
  l_exists := NULL;
--
  OPEN c_pba_exists (l_par_refno, l_bad_refno, p1.lplb_start_date);	   
  FETCH c_pba_exists INTO l_exists;
  CLOSE c_pba_exists;
--
  IF l_exists IS NULL
   THEN 
--
    INSERT INTO PARTY_BANK_ACCOUNTS        
    (pba_par_refno     
    ,pba_bad_refno     
    ,pba_start_date    
    ,pba_created_date  
    ,pba_created_by    
    ,pba_end_date)     
    VALUES     
    (l_par_refno       
    ,l_bad_refno       
    ,p1.lplb_start_date        
    ,sysdate           
    ,'DATALOAD'
    ,NULL);    
--
-- set delete indicator as created by DL
--
    UPDATE dl_hem_prop_landlord_banks
    SET lplb_del_pba_ind = 'Y'
    WHERE rowid = p1.rec_rowid
    AND lplb_dl_seqno = p1.lplb_dl_seqno;
--
  END IF;
--         
-- and now into property_landlord_banks
-- many party bank accounts can link to many property landlord banks
-- BUT can ONLY have 1 current property landlord bank record for each
-- separate property landlord(plb_pld_refno)at any 1 time
--
  INSERT INTO PROPERTY_LANDLORD_BANKS
  (plb_pba_par_refno
  ,plb_pba_bad_refno 
  ,plb_pba_start_date
  ,plb_pld_refno      --can only be 1 current record   
  ,plb_start_date
  ,plb_created_by
  ,plb_created_date)
  VALUES
  (l_par_refno
  ,l_bad_refno
  ,p1.lplb_start_date 
  ,l_pld_refno
  ,p1.lplb_start_date
  ,'DATALOAD'
  ,sysdate);
--
-- **********************************
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
  set_record_status_flag(l_id,'C');
--
  EXCEPTION
  WHEN OTHERS THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
 END LOOP;
--
-- Section to analyse the table populated by this dataload
--
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PROPERTY_LANDLORD_BANKS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('BANK_ACCOUNT_DETAILS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCOUNTS');
    l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HEM_PROP_LANDLORD_BANKS');
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
PROCEDURE dataload_validate (p_batch_id  IN VARCHAR2,
                             p_date      IN DATE)
AS
--
CURSOR c1 is
SELECT rowid rec_rowid,
       LPLB_DL_SEQNO,
       LPLB_PRO_PROPREF,
       LPLB_PLD_START_DATE,
       LPLB_PAR_REFNO,
       LPLB_ALT_PAR_REF,
       LPLB_START_DATE,
       LPLB_BAD_ACCOUNT_NO,
       LPLB_BAD_ACCOUNT_NAME,
       LPLB_BAD_SORT_CODE,
       nvl(LPLB_BAD_START_DATE,LPLB_START_DATE) LPLB_BAD_START_DATE,
       LPLB_BDE_BANK_NAME,
       LPLB_BDE_BRANCH_NAME,
       LPLB_BDE_BTY_CODE,
       LPLB_BDE_BANK_CODE,
       LPLB_BDE_BRANCH_CODE,
       LPLB_BDE_BANK_NAME_MLANG,
       LPLB_BDE_BRANCH_NAME_MLANG
  FROM dl_hem_prop_landlord_banks
 WHERE lplb_dlb_batch_id = p_batch_id
   AND lplb_dl_load_status IN ('L','F','O');
--
-- **********************************
--
-- Check property_landlord reference doesn't already exist
--
CURSOR chk_pld_ref_exists(p_pld_refno NUMBER) IS
SELECT 'X'
  FROM property_landlords
 WHERE pld_refno = p_pld_refno;
--
-- Check property_landlord record  exists and needs to be current
--
CURSOR chk_pld_exists(p_pro_refno  NUMBER,
                      p_start_date DATE,
                      p_par_refno  NUMBER) IS
SELECT 'X'
  FROM property_landlords
 WHERE pld_pro_refno  = p_pro_refno
   AND pld_start_date = p_start_date
   AND pld_par_refno  = p_par_refno;
--
-- Check property_landlord_banks record doesn't already exist
--
CURSOR chk_plb(p_pro_refno  NUMBER,
               p_pld_start_date DATE,
               p_par_refno  NUMBER,
               p_start_date DATE) IS
SELECT 'X'
  FROM property_landlords, property_landlord_banks
 WHERE pld_pro_refno  = p_pro_refno
   AND pld_start_date = p_pld_start_date
   AND pld_par_refno  = p_par_refno
   AND pld_refno = plb_pld_refno
   AND p_start_date <= nvl(plb_end_date,p_start_date);
--
-- Get the par_refno for lplb_alt_par_ref and lplb_agent_alt_par_ref
--
CURSOR get_par_refno(p_alt_par_ref VARCHAR2) IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_alt_par_ref;
--
-- Check pro_refno exists
--
CURSOR chk_par_refno_exists(p_pld_par_refno NUMBER) IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_pld_par_refno;
--
-- Check bank type
--
CURSOR c_bty_code(p_bde_bty_code VARCHAR2) IS       
SELECT bty_code
      ,bty_branch_code_mandatory_flag
      ,bty_bank_code_mandatory_flag
      ,bty_min_len
      ,bty_max_len
  FROM bank_types
 WHERE bty_code = p_bde_bty_code;
--
-- Account length
--
CURSOR c_check_len(p_bad_acc_no NUMBER) IS
select 
length(regexp_substr(replace(p_bad_acc_no,',',''), '[^.]+', 1, 1)) as whole_part_count 
from dual; 
--
-- Bank details mlang check already exists
--
CURSOR c_bde_mlang_exists(p_bank_name    VARCHAR2
                         ,p_bank_mlang   VARCHAR2
                         ,p_branch_mlang VARCHAR2)
IS
SELECT 'X'
  FROM bank_details
 WHERE bde_bank_name   != p_bank_name
   AND nvl(bde_bank_name_mlang,'A') = nvl(p_bank_mlang,'B')
   AND nvl(bde_branch_name_mlang,'A') = nvl(p_branch_mlang,'A');
--
-- **********************************
--
-- constants FOR error process
--
cb 	VARCHAR2(30);
cd 	DATE;
cp 	VARCHAR2(30) := 'VALIDATE';
ct 	VARCHAR2(30) := 'DL_HEM_PROP_LANDLORD_BANKS';
cs 	INTEGER;
ce 	VARCHAR2(200);
l_id     ROWID;
--
-- other variables
--
l_errors      VARCHAR2(10);
l_error_ind   VARCHAR2(10);
i             INTEGER := 0;
--
l_pld_ref_exists         VARCHAR2(1);
l_pld_exists             VARCHAR2(1);
l_par_refno_exists		 VARCHAR2(1);
l_agent_par_refno_exists VARCHAR2(1);
l_pro_refno              NUMBER(10);
l_par_refno              NUMBER(10);
l_answer                 VARCHAR2(1);
l_bty_code               bank_types.bty_code%type;
l_branch_mand            bank_types.bty_branch_code_mandatory_flag%type;
l_bank_mand              bank_types.bty_bank_code_mandatory_flag%type;
l_bty_min_len            bank_types.bty_min_len%type;
l_bty_max_len            bank_types.bty_max_len%type;
l_exists                 VARCHAR2(1);
l_mlang_exists           VARCHAR2(1);
l_whole_part_count       NUMBER(10); 
--
-- **********************************
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_prop_landlord_banks.dataload_validate');
 fsc_utils.debug_message('s_dl_hem_prop_landlord_banks.dataload_validate',3 );
--
 cb := p_batch_id;
 cd := p_date;
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 l_answer  := s_dl_batches.get_answer(p_batch_id, 1);	
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lplb_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
  l_pro_refno := NULL;
  l_par_refno := NULL;
  l_par_refno_exists := NULL;
--
-- Check Property Landlord Property Ref is supplied and valid (lpld_pro_propref)
--
  IF (p1.lplb_pro_propref IS NULL) THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',203);
--
  ELSE
   l_pro_refno := NULL;
   l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lplb_pro_propref);
--
   IF (l_pro_refno is NULL) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',55);
   END IF;
  END IF;
--
-- Check Property Landlord Start Date is supplied (lpld_start_date)
--
  IF (p1.lplb_pld_start_date IS NULL) THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',204);
  END IF;
--
-- Check Property Landlord Party Refno or alt_par_ref, one or the other, not both is 
-- supplied and valid (lpld_par_refno, lplb_alt_par_ref)
--
  IF (p1.lplb_par_refno IS NULL AND p1.lplb_alt_par_ref IS NULL) THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',205);
--
  ELSIF (p1.lplb_par_refno IS NOT NULL AND p1.lplb_alt_par_ref IS NOT NULL) THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',206);
--
  ELSIF (p1.lplb_alt_par_ref IS NOT NULL AND p1.lplb_par_refno IS NULL)
   THEN
    l_par_refno := NULL;
--
    OPEN get_par_refno(p1.lplb_alt_par_ref);
    FETCH get_par_refno INTO l_par_refno;
    CLOSE get_par_refno;
--
    IF (l_par_refno IS NULL) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',207);
    END IF;
--
  ELSIF (p1.lplb_par_refno IS NOT NULL AND p1.lplb_alt_par_ref IS NULL)
   THEN
    l_par_refno_exists := NULL;
--
    OPEN chk_par_refno_exists(p1.lplb_par_refno);
    FETCH chk_par_refno_exists INTO l_par_refno_exists;
    CLOSE chk_par_refno_exists;
--
    IF (l_par_refno_exists IS NULL) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',208);
    END IF;
  END IF;
--
-- Check Property Landlord reference exists (lpld_refno)
--
  l_pld_exists := NULL;
--
  IF  ( l_pro_refno  IS NOT NULL
   AND  p1.lplb_pld_start_date IS NOT NULL
   AND (l_par_refno IS NOT NULL OR l_par_refno_exists IS NOT NULL) )
   THEN
--
    OPEN chk_pld_exists(l_pro_refno,p1.lplb_pld_start_date, NVL(l_par_refno,p1.lplb_par_refno));
    FETCH chk_pld_exists INTO l_pld_exists;
    CLOSE chk_pld_exists;
--
    IF l_pld_exists IS NULL THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',3);
    END IF;
  END IF;
--
-- Check Reg pay rules
-- Check the bank type and bank fields valid and supplied if mandatory
--
  l_bty_code     := NULL;
  l_branch_mand  := NULL;
  l_bank_mand    := NULL;
  l_bty_min_len  := NULL;
  l_bty_max_len  := NULL;
  l_whole_part_count := NULL;
--
-- first check for mandatory bank details not supplied
--
  IF p1.lplb_bad_account_name IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',6);
  END IF;
--
  IF p1.lplb_bad_account_no IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',7);
  END IF;
--
  IF p1.lplb_bad_sort_code IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',8);
  END IF;
--
  IF p1.lplb_bde_bank_name IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',9);
  END IF;
--
  IF p1.lplb_bde_bty_code IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',866);
  END IF;
--
  IF p1.lplb_bde_bty_code IS NOT NULL
   THEN
--
    OPEN c_bty_code(p1.lplb_bde_bty_code);
    FETCH c_bty_code into l_bty_code
                         ,l_branch_mand
                         ,l_bank_mand
                         ,l_bty_min_len
                         ,l_bty_max_len;
    CLOSE c_bty_code;
--
    IF l_bty_code IS NULL
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',866);
    END IF;
--
    IF (l_branch_mand = 'MAN')
     THEN
      IF ((   p1.lplb_bde_branch_name  IS NULL 
           OR p1.lplb_bde_branch_code  IS NULL ))
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',10);
      END IF;
--
    END IF;
--
    IF (l_bank_mand = 'MAN')
     THEN
      IF ( p1.lplb_bde_bank_code IS NULL )
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',11);
      END IF;
    END IF;
--
-- Check length of bank account number against bank type MIN and MAX
-- in any case cannot exceed a length of 30
--
    IF (p1.lplb_bad_account_no IS NOT NULL) 
     THEN  
      OPEN  c_check_len(p1.lplb_bad_account_no);
      FETCH c_check_len INTO l_whole_part_count;
      CLOSE c_check_len;
--
      IF (nvl(l_whole_part_count,30) > l_bty_max_len)
       THEN     
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',1);
      END IF;
--
      IF (nvl(l_whole_part_count,1) < l_bty_min_len)
       THEN     
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',4);
      END IF;
--
    END IF;
--
  END IF;
--
-- Bank details mlang check already exists
--
  l_mlang_exists  :=NULL;
--
  IF  (p1.lplb_bde_bank_name IS NOT NULL
   AND(p1.lplb_bde_bank_name_mlang IS NOT NULL OR p1.lplb_bde_branch_name_mlang IS NOT NULL)) 
   THEN  
    OPEN  c_bde_mlang_exists(p1.lplb_bde_bank_name
                            ,p1.lplb_bde_bank_name_mlang
                            ,p1.lplb_bde_branch_name_mlang);
    FETCH c_bde_mlang_exists INTO l_mlang_exists;
    CLOSE c_bde_mlang_exists;
--
    IF l_mlang_exists IS NOT NULL
     THEN     
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',2);
    END IF;
--
  END IF;
--
-- Check, if not updating, that no current Bank details present
--
  IF nvl(l_answer,'N') != 'Y'
   THEN
    l_exists := NULL;
--
    OPEN chk_plb(l_pro_refno
                ,p1.lplb_pld_start_date
                ,NVL(l_par_refno,p1.lplb_par_refno)
                ,p1.lplb_start_date);
    FETCH chk_plb INTO l_exists;
    CLOSE chk_plb;
   
   IF l_exists IS NOT NULL
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',695); 
   END IF; 
  END IF;
--
-- **********************************
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
  s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
  set_record_status_flag(l_id,l_errors);
--
  EXCEPTION
   WHEN OTHERS THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
   set_record_status_flag(l_id,ce);
  END;
--
 END LOOP;
--
commit;
--
fsc_utils.proc_END;
--
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
PROCEDURE dataload_delete (p_batch_id  IN VARCHAR2,
                           p_date      IN DATE)
IS
CURSOR c1 IS
SELECT rowid rec_rowid,
       LPLB_DL_SEQNO,
       LPLB_PRO_PROPREF,
       LPLB_PLD_START_DATE,
       LPLB_PAR_REFNO,
       LPLB_ALT_PAR_REF,
       LPLB_START_DATE,
       LPLB_BAD_ACCOUNT_NO,
       LPLB_BAD_ACCOUNT_NAME,
       LPLB_BAD_SORT_CODE,
       nvl(LPLB_BAD_START_DATE,LPLB_START_DATE) LPLB_BAD_START_DATE,
       LPLB_BDE_BANK_NAME,
       LPLB_BDE_BRANCH_NAME,
       LPLB_BDE_BTY_CODE,
       LPLB_BDE_BANK_CODE,
       LPLB_BDE_BRANCH_CODE,
       LPLB_BDE_BANK_NAME_MLANG,
       LPLB_BDE_BRANCH_NAME_MLANG,
       LPLB_DEL_BDE_IND,
       LPLB_DEL_BAD_IND,
       LPLB_DEL_PBA_IND, -- allow for old
       LPLB_DEL_BDE_REFNO,
       LPLB_DEL_BAD_REFNO,
       LPLB_DEL_PAR_REFNO,  -- allow for old
       LPLB_DEL_PLD_REFNO,  -- allow for old
       LPLB_PLD_PRO_REFNO,  -- allow for old
       LPLB_END_DATE        -- allow for old
  FROM dl_hem_prop_landlord_banks
 WHERE lplb_dlb_batch_id   = p_batch_id
   AND lplb_dl_load_status = 'C'
 ORDER BY lplb_dl_seqno desc;
--
-- **********************************
--
-- Get the par_refno for lplb_alt_par_ref 
--
CURSOR get_par_refno(p_alt_par_ref VARCHAR2) IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_alt_par_ref;
--
-- Get the pld_refno for propref/par_refno/start_date 
--
CURSOR get_pld_refno(p_pro_refno NUMBER
                    ,p_par_refno NUMBER
                    ,p_start_date DATE) IS
SELECT pld_refno
  FROM property_landlords
 WHERE pld_par_refno = p_par_refno
 AND   pld_pro_Refno = p_pro_refno
 AND   pld_start_date = p_start_date;
--
-- **********************************
--
-- Constants FOR process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HEM_PROP_LANDLORD_BANKS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     	ROWID;
l_answer    VARCHAR2(1);
--
i 			INTEGER := 0;
l_an_tab 	VARCHAR2(1);
l_pro_refno	NUMBER(10);
l_par_refno	NUMBER(10);
l_pld_refno NUMBER(10);
--
-- **********************************
--
BEGIN
--
 fsc_utils.proc_start('s_dl_hem_prop_landlord_banks.dataload_delete');
 fsc_utils.debug_message('s_dl_hem_prop_landlord_banks.dataload_delete',3);
--
 cb := p_batch_id;
 cd := p_date;
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 l_answer  := s_dl_batches.get_answer(p_batch_id, 1);
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lplb_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_pro_refno  := NULL;
  l_par_refno  := NULL;
--
-- get the pro_refno BUT pro_refno captured on create
-- also allow if LPLB_PLD_PRO_REFNO is null for old inserts
--
  IF (p1.lplb_pld_pro_refno IS NULL)
   THEN 
      l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lplb_pro_propref);
  ELSE
      l_pro_refno := p1.lplb_pld_pro_refno;
  END IF;
--
-- get the par_refno BUT par_refno captured on create
-- also allow if LPLB_DEL_PAR_REFNO is null for old inserts
--
  IF (p1.lplb_del_par_refno IS NULL)
   THEN 
    IF (p1.lplb_alt_par_ref IS NOT NULL)
     THEN
      OPEN get_par_refno(p1.lplb_alt_par_ref);
      FETCH get_par_refno INTO l_par_refno;
      CLOSE get_par_refno;
    ELSE
      l_par_refno := p1.lplb_par_refno;
    END IF;
  ELSE
      l_par_refno := p1.lplb_del_par_refno;
  END IF;
--
-- get pld_refno BUT pld_refno captured on create
-- also allow if LPLB_DEL_PLD_REFNO is null for old inserts
--
  l_pld_refno := NULL;
--
  IF (p1.lplb_del_pld_refno IS NULL)
   THEN 
    OPEN get_pld_refno(l_pro_refno, l_par_refno, p1.lplb_pld_start_date);
    FETCH get_pld_refno INTO l_pld_refno;
    CLOSE get_pld_refno;
  ELSE
      l_pld_refno := p1.lplb_del_pld_refno;
  END IF;
--
-- Delete the Property Landlords Record from the Property_Landlords table
-- 1 or many party bank accounts can link to 1 or many property landlord banks
-- BUT can ONLY have 1 current property landlord bank record for each
-- separate property landlord(plb_pld_refno)at any 1 time
--
  DELETE FROM PROPERTY_LANDLORD_BANKS
   WHERE plb_pld_refno  = l_pld_refno
     AND plb_start_date = p1.lplb_start_date;
--
-- If previous record updates were allowed remove end date from previous plb record
-- or put previous end date back if lplb_end_date is populated captured from create
--
-- previous record need was ended...
--
  IF l_answer = 'Y'
   THEN
    IF (p1.lplb_end_date IS NULL)
     THEN
      UPDATE property_landlord_banks
      SET    plb_end_date = NULL
      WHERE  plb_pld_refno = l_pld_refno
      AND    plb_end_date = p1.lplb_start_date-1;
	ELSE
      UPDATE property_landlord_banks
      SET    plb_end_date = p1.lplb_end_date
      WHERE  plb_pld_refno = l_pld_refno
      AND    plb_end_date = p1.lplb_start_date-1;
    END IF;
  END IF;
--
-- delete party bank account record if created lplb_del_pba_ind ='Y'
-- using PBA_HOU_PK combination to find record
--
  IF (p1.lplb_del_pba_ind = 'Y')
   THEN
    DELETE FROM PARTY_BANK_ACCOUNTS
     WHERE pba_par_refno = p1.lplb_del_par_refno
       AND pba_bad_refno = p1.lplb_del_bad_refno
       AND pba_start_date = p1.lplb_start_date;
  END IF;
--    
-- delete bank account details record if created lplb_del_bad_ind ='Y'
-- using BAD_PK (bad_refno) to find record
--
  IF (p1.lplb_del_bad_ind = 'Y')
   THEN
    DELETE FROM BANK_ACCOUNT_DETAILS
     WHERE bad_refno = p1.lplb_del_bad_refno;
  END IF;
-- 
-- delete bank details record if created lplb_del_bde_ind ='Y'
-- using BDE_PK (bde_refno) to find record
--
  IF (p1.lplb_del_bde_ind = 'Y')
   THEN
    DELETE FROM BANK_DETAILS
     WHERE bde_refno = p1.lplb_del_bde_refno;
  END IF;
--
-- **********************************
--
-- keep a count of the rows processed and commit after every 1000
--        
  i  := i +1;
--
  IF MOD(i,1000)=0 THEN 
   COMMIT; 
  END IF;
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
-- Section to analyse the table populated by this dataload
--
 l_an_tab := s_dl_hem_utils.dl_comp_stats('PROPERTY_LANDLORD_BANKS');
 l_an_tab := s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');
 l_an_tab := s_dl_hem_utils.dl_comp_stats('BANK_ACCOUNT_DETAILS');
 l_an_tab := s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCOUNTS');
 l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HEM_PROP_LANDLORD_BANKS');
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
END s_dl_hem_prop_landlord_banks;
/
show errors
