CREATE OR REPLACE PACKAGE BODY s_dl_hpp_pp_tenancy_histories
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.1.6     PH   07-JUN-2002  Initial Creation
--      1.1  5.2.0     SB   10-SEP-2002  Only validate Landlord Shortname
--                                       if supplied.
--      1.2  5.5.0     PH   15-APR-2004  Allow for the supply par_refno
--                                       rather than alt_ref. 
--      1.3  5.5.0     PJD  12-MAY-2004  Changed the validation on ipt_hrv_fiy_code
--                                       to check for value of 'PPLL'
--      2.0  5.13.0    PH   06-FEB-2008  Now includes its own 
--                                       set_record_status_flag procedure.
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
  UPDATE dl_hpp_pp_tenancy_histories
  SET lthi_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpp_pp_tenancy_histories');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************     
--
--  declare package variables and constants
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lthi_dlb_batch_id 
,lthi_dl_seqno
,lthi_dl_load_status
,lthi_papp_displayed_reference
,lthi_par_per_alt_ref
,lthi_tht_code
,lthi_period_start
,lthi_period_end
,lthi_tenant_name
,lthi_tenancy_address
,lthi_landlord_ipp_shortname 
,lthi_landlord_free_text
,lthi_comments
,lthi_verified_ind
,lthi_current_landlord_ind
FROM dl_hpp_pp_tenancy_histories
WHERE lthi_dlb_batch_id    = p_batch_id
AND   lthi_dl_load_status = 'V';
--
-- Cursor to get papp_refno etc
--
CURSOR c_papp_refno(p_papp_ref varchar2) IS
SELECT papp_refno, papp_application_date, papp_tho_tcy_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_ref;
--
-- Cursor to get par_refno
--
CURSOR c_par_refno(p_par_refno varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_refno;
--
--
-- Cursor to get the hou_refno for so we
-- will then be able to get the right hop_refno for
-- a insert;
--
CURSOR c_hou_refno(p_tcy_refno number
                  ,p_app_date  date) IS
SELECT hop_hou_refno
FROM   household_persons,
       tenancy_instances
WHERE  tin_hop_refno = hop_refno
AND    tin_tcy_refno = p_tcy_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
-- Cursor to get hop_refno
--
CURSOR c_hop_refno(p_hou_refno number
                  ,p_par_refno number
                  ,p_app_date  date) IS
SELECT hop_refno
FROM   household_persons
WHERE  hop_par_refno = p_par_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_TENANCY_HISTORIES';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i            integer := 0;
l_papp_refno NUMBER(10);
l_app_date   date;
l_tcy_refno  number(10);
l_par_refno  number(10);
l_hou_refno  number(10);
l_hop_refno  number(10);
l_answer     varchar(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_tenancy_histories.dataload_create');
fsc_utils.debug_message( 's_dl_hpp_pp_tenancy_histories.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Use Par Refno Question'
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lthi_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
--
l_papp_refno := null;
l_app_date   := null;
l_tcy_refno  := null;
l_par_refno  := null;
l_hou_refno  := null;
l_hop_refno  := null;
--
  OPEN c_papp_refno(p1.lthi_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno, l_app_date, l_tcy_refno;
  CLOSE c_papp_refno;
--
IF nvl(l_answer, 'N') != 'Y'
 THEN
  OPEN c_par_refno(p1.lthi_par_per_alt_ref);
   FETCH c_par_refno INTO l_par_refno;
  CLOSE c_par_refno;
END IF;
--
IF nvl(l_answer, 'N') = 'Y'
 THEN
  l_par_refno := to_number(p1.lthi_par_per_alt_ref);
END IF;
--
  OPEN c_hou_refno(l_tcy_refno, l_app_date);
   FETCH c_hou_refno INTO l_hou_refno;
  CLOSE c_hou_refno;
--
  OPEN c_hop_refno(l_hou_refno, l_par_refno, l_app_date);
   FETCH c_hop_refno INTO l_hop_refno;
  CLOSE c_hop_refno;
--
      INSERT INTO tenancy_histories
         (thi_period_start               
         ,thi_period_end                 
         ,thi_comments                   
         ,thi_verified_ind              
         ,thi_landlord_free_text         
         ,thi_current_landlord_ind
         ,thi_tenant_name                
         ,thi_tenant_address             
         ,thi_papa_papp_refno  
         ,thi_papa_hop_refno  
         ,thi_tht_code                   
         ,thi_type      
         )
      VALUES
         (p1.lthi_period_start
         ,p1.lthi_period_end
         ,p1.lthi_comments
         ,p1.lthi_verified_ind
         ,p1.lthi_landlord_free_text
         ,p1.lthi_current_landlord_ind
         ,p1.lthi_tenant_name
         ,p1.lthi_tenancy_address
         ,l_papp_refno
         ,l_hop_refno
         ,p1.lthi_tht_code
         ,'GEN'
         );
--
-- keep a count of the rows processed and commit after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END If;
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
COMMIT;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_HISTORIES');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lthi_dlb_batch_id 
,lthi_dl_seqno
,lthi_dl_load_status
,lthi_papp_displayed_reference
,lthi_par_per_alt_ref
,lthi_tht_code
,lthi_period_start
,lthi_period_end
,lthi_tenant_name
,lthi_tenancy_address
,lthi_landlord_ipp_shortname 
,lthi_landlord_free_text
,lthi_comments
,lthi_verified_ind
,lthi_current_landlord_ind
FROM  dl_hpp_pp_tenancy_histories
WHERE lthi_dlb_batch_id      = p_batch_id
AND   lthi_dl_load_status   in ('L','F','O');
--
-- Cursor to see if application exists
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
-- Cursor for Landlord Shortname
--
CURSOR c_land(p_land_short varchar2) IS
SELECT 'X'
FROM   interested_parties, interested_party_types
WHERE  ipt_code         = ipp_ipt_code
AND    ipt_current_ind  = 'Y'
AND    ipp_shortname    = p_land_short
AND    ipt_hrv_fiy_code = 'PPLL';
--
-- Cursor to check Party exists
--
CURSOR c_par_refno(p_par_ref varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_ref;
--
CURSOR c_par_refno2(p_par_ref varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_refno = to_number(p_par_ref);
--
-- Cursor to check tenancy history record does not already exist
--
CURSOR c_thi_check(p_papp_refno number
                  ,p_start_date date
                  ,p_end_date   date)  IS
SELECT 'X'
FROM   tenancy_histories
WHERE  thi_papa_papp_refno = p_papp_refno
AND    thi_period_start    = p_start_date
AND    thi_period_end      = p_end_date;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_TENANCY_HISTORIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_par_refno      NUMBER(10);
l_papp_refno     NUMBER(10);
l_answer         VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_tenancy_histories.dataload_validate');
fsc_utils.debug_message( 's_dl_hpp_pp_tenancy_histories.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Use Par Refno Question'
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lthi_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check application exists
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lthi_papp_displayed_reference);
   FETCH c_papp_refno into l_papp_refno;
    IF c_papp_refno%NOTFOUND
     THEN 
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',804);
    END IF;
  CLOSE c_papp_refno;
--
-- Check the tenancy hisory grouping code is valid
--
   IF (NOT s_dl_hem_utils.exists_frv('TCY_HIS_TYP', p1.lthi_tht_code, 'N'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',834);
   END IF;
--
-- Check Landlord Reference Shortname
--
   IF (p1.lthi_landlord_ipp_shortname is not null)
   THEN
     OPEN c_land(p1.lthi_landlord_ipp_shortname);
      FETCH c_land into l_exists;
       IF c_land%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',812);
       END IF;
     CLOSE c_land;
   END IF;
--
-- Check the person reference is on parties if supplied
--
l_par_refno := null;
--
IF nvl(l_answer, 'N') != 'Y'
 THEN
--
     OPEN c_par_refno(p1.lthi_par_per_alt_ref);
      FETCH c_par_refno INTO l_par_refno;
       IF c_par_refno%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',823);
       END IF;
     CLOSE c_par_refno;
END IF;
--
IF nvl(l_answer, 'N') = 'Y'
 THEN
--
     OPEN c_par_refno2(p1.lthi_par_per_alt_ref);
      FETCH c_par_refno2 INTO l_par_refno;
       IF c_par_refno2%notfound
        THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',823);
       END IF;
     CLOSE c_par_refno2;
END IF;
--
-- Check the period end date is after the period start date
--
  IF(p1.lthi_period_start >= p1.lthi_period_end)
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',835);
  END IF;
--
-- Check Yes No fields
--
-- Verified Flag
--
    IF (NOT s_dl_hem_utils.yorn(p1.lthi_verified_ind))
     THEN 
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',829);
    END IF;
--
-- Check a matching tenancy history doesn't already exist
--
-- get papp_refno
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lthi_papp_displayed_reference);
   FETCH c_papp_refno into l_papp_refno;
  CLOSE c_papp_refno;
--
-- now do the check
--
  OPEN c_thi_check(l_papp_refno, p1.lthi_period_start, p1.lthi_period_end);
   FETCH c_thi_check INTO l_exists;
    IF c_thi_check%FOUND
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',831);
    END IF;
  CLOSE c_thi_check;
--
-- Check Mandatory Columns
--
-- Period start
--
  IF p1.lthi_period_start IS NULL
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',518);
    END IF;    
--
-- Period End
--
  IF p1.lthi_period_end IS NULL
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',519);
    END IF;  
--
-- Tenant Name
--
  IF p1.lthi_tenant_name IS NULL
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',520);
    END IF;  
--
-- Tenant address
--
  IF p1.lthi_tenancy_address IS NULL
   THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',521);
    END IF;
--
-- Current landlord ind
--
   IF (NOT s_dl_hem_utils.yorn(p1.lthi_current_landlord_ind))
     THEN 
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',522);
   END IF;
--
--
-- Now UPDATE the record count AND error code
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
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
END;
--
END LOOP;
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
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lthi_dlb_batch_id 
,lthi_dl_seqno
,lthi_dl_load_status
,lthi_papp_displayed_reference
,lthi_par_per_alt_ref
,lthi_tht_code
,lthi_period_start
,lthi_period_end
,lthi_tenant_name
,lthi_tenancy_address
,lthi_landlord_ipp_shortname 
,lthi_landlord_free_text
,lthi_comments
,lthi_verified_ind
,lthi_current_landlord_ind
FROM  dl_hpp_pp_tenancy_histories
WHERE lthi_dlb_batch_id      = p_batch_id
  AND lthi_dl_load_status   = 'C';
--
-- Cursor to get papp_refno etc
--
CURSOR c_papp_refno(p_papp_ref varchar2) IS
SELECT papp_refno, papp_application_date, papp_tho_tcy_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_ref;
--
-- Cursor to get par_refno
--
CURSOR c_par_refno(p_par_refno varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_refno;
--
-- Cursor to get the hou_refno for so we
-- will then be able to get the right hop_refno for
-- a insert;
--
CURSOR c_hou_refno(p_tcy_refno number
                  ,p_app_date  date) IS
SELECT hop_hou_refno
FROM   household_persons,
       tenancy_instances
WHERE  tin_hop_refno = hop_refno
AND    tin_tcy_refno = p_tcy_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
-- Cursor to get hop_refno
--
CURSOR c_hop_refno(p_hou_refno number
                  ,p_par_refno number
                  ,p_app_date  date) IS
SELECT hop_refno
FROM   household_persons
WHERE  hop_par_refno = p_par_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HPP_PP_TENANCY_HISTORIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
l_papp_refno NUMBER(10);
l_par_refno  NUMBER(10);
l_app_date   date;
l_tcy_refno  number(10);
l_hou_refno  number(10);
l_hop_refno  number(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_tenancy_histories.dataload_delete');
fsc_utils.debug_message( 's_dl_hpp_pp_tenancy_histories.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
--
-- s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lthi_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_par_refno  := null;
l_papp_refno := null;
l_app_date   := null;
l_tcy_refno  := null;
l_hou_refno  := null;
l_hop_refno  := null;
--
--  get par_refno
--
  OPEN c_par_refno(p1.lthi_par_per_alt_ref);
   FETCH c_par_refno INTO l_par_refno;
  CLOSE c_par_refno;
--
-- get papp_refno
--
  OPEN c_papp_refno(p1.lthi_papp_displayed_reference);
   FETCH c_papp_refno into l_papp_refno, l_app_date, l_tcy_refno;
  CLOSE c_papp_refno;
--
-- get the hop_refno
--
--
  OPEN c_hou_refno(l_tcy_refno, l_app_date);
   FETCH c_hou_refno INTO l_hou_refno;
  CLOSE c_hou_refno;
--
  OPEN c_hop_refno(l_hou_refno, l_par_refno, l_app_date);
   FETCH c_hop_refno INTO l_hop_refno;
  CLOSE c_hop_refno;
--
        DELETE FROM tenancy_histories
        WHERE  thi_papa_papp_refno = l_papp_refno
        AND    thi_period_start    = p1.lthi_period_start
        AND    thi_papa_hop_refno  = l_hop_refno;
--
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
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
COMMIT;
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_HISTORIES');
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
END s_dl_hpp_pp_tenancy_histories;
/

