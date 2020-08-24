CREATE OR REPLACE PACKAGE BODY s_dl_hpp_pp_tenant_improvs
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.1.6     PH   10-JUN-2002  Initial Creation
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
  UPDATE dl_hpp_pp_tenant_improvs
  SET ltim_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpp_pp_tenant_improvs');
     RAISE;
  --
END set_record_status_flag;
--
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
,ltim_dlb_batch_id 
,ltim_dl_seqno 
,ltim_dl_load_status 
,ltim_papp_displayed_reference 
,ltim_seqno 
,ltim_description 
,ltim_verified_ind 
,ltim_pvr_amount 
,ltim_comments 
,ltim_pval_seqno
FROM dl_hpp_pp_tenant_improvs
WHERE ltim_dlb_batch_id    = p_batch_id
AND   ltim_dl_load_status = 'V';
--
-- Cursor for papp_refno
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno, papp_tho_tcy_refno, papp_application_date
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
-- Cursor for par_refno
--
CURSOR c_par_refno(p_tcy_refno number
                  ,p_app_date  date) IS
SELECT min(hop_par_refno)
FROM   household_persons,
       tenancy_instances
WHERE  tin_hop_refno = hop_refno
AND    tin_tcy_refno = p_tcy_refno
AND    p_app_date between hop_start_date and 
                      nvl(hop_end_date, sysdate);
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_TENANT_IMPROVS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_papp_refno number;
l_tcy_refno  number;
l_app_date   date;
l_par_refno  number;
i           integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_tenant_improvs.dataload_create');
fsc_utils.debug_message( 's_dl_hpp_pp_tenant_improvs.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.ltim_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_papp_refno := null;
l_tcy_refno  := null;
l_app_date   := null;
l_par_refno  := null;
--
  OPEN c_papp_refno(p1.ltim_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno, l_tcy_refno, l_app_date;
  CLOSE c_papp_refno;
--
  OPEN c_par_refno(l_tcy_refno, l_app_date);
   FETCH c_par_refno into l_par_refno;
  CLOSE c_par_refno;
--
      INSERT INTO tenant_improvements
         (tim_seqno              
         ,tim_description        
         ,tim_comments           
         ,tim_verified_ind     
         ,tim_papp_refno         
         ,tim_par_refno 
         )
      VALUES
         (p1.ltim_seqno
         ,p1.ltim_description
         ,p1.ltim_comments
         ,p1.ltim_verified_ind
         ,l_papp_refno
         ,l_par_refno
         );
--
-- If value amount is not null insert into pp_valuation_reductions
--
  IF p1.ltim_pvr_amount IS NOT NULL
   THEN
    INSERT INTO pp_valuation_reductions
         (pvr_amount             
         ,pvr_pval_seqno         
         ,pvr_pval_papp_refno
         ,pvr_tim_seqno          
         ,pvr_tim_papp_refno     
         )
      VALUES
         (p1.ltim_pvr_amount
         ,p1.ltim_pval_seqno
         ,l_papp_refno
         ,p1.ltim_seqno
         ,l_papp_refno
         );
  END IF;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANT_IMPROVEMENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_VALUATION_REDUCTIONS');
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
,ltim_dlb_batch_id 
,ltim_dl_seqno 
,ltim_dl_load_status 
,ltim_papp_displayed_reference 
,ltim_seqno 
,ltim_description 
,ltim_verified_ind 
,ltim_pvr_amount 
,ltim_comments 
,ltim_pval_seqno
FROM dl_hpp_pp_tenant_improvs
WHERE ltim_dlb_batch_id      = p_batch_id
AND   ltim_dl_load_status   in ('L','F','O');
--
--
-- Cursor for papp_refno
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
-- Cursor for tim sequence check
--
CURSOR c_seq_check(p_papp_refno number
                  ,p_tim_seqno  number) IS
SELECT 'X'
FROM   tenant_improvements
WHERE  tim_papp_refno = p_papp_refno
AND    tim_seqno      = p_tim_seqno;
--
--Cursor for pval_seqno check
--
CURSOR c_pval_seq(p_papp_refno number
                 ,p_pval_seq   number) IS
SELECT 'X'
FROM   pp_valuations
WHERE  pval_papp_refno = p_papp_refno
AND    pval_seqno      = p_pval_seq;
-- 
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_TENANT_IMPROVS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_papp_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_tenant_improvs.dataload_validate');
fsc_utils.debug_message( 's_dl_hpp_pp_tenant_improvs.dataload_validate',3);
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
cs := p1.ltim_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
--
-- Check the application exists
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.ltim_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
    IF c_papp_refno%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',804);
    END IF;
  CLOSE c_papp_refno;
--
-- Check that the tim sequence is unique
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.ltim_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
--
  OPEN c_seq_check(l_papp_refno, p1.ltim_seqno);
   IF c_seq_check%FOUND
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',842);
   END IF;
  CLOSE c_seq_check;
--
-- Verified Indicator
--
    IF (NOT s_dl_hem_utils.yorn(p1.ltim_verified_ind))
     THEN 
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',844);
    END IF;
--
-- Check Mandatory Fields
--
-- Check sequence
-- 
  IF p1.ltim_seqno IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',511);
  END IF;
--
-- Description
--
  IF p1.ltim_description IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',512);
  END IF;
--
-- Valuation Sequence if value amount supplied
--
  IF p1.ltim_pvr_amount IS NOT NULL
   THEN
    IF p1.ltim_pval_seqno IS NULL
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',513);
    END IF;
  END IF;
--
-- Check valuation sequence exists if supplied
--
  IF p1.ltim_pval_seqno IS NOT NULL
   THEN
--
-- get the papp_refno
--
l_papp_refno := null;
--
    OPEN c_papp_refno(p1.ltim_papp_displayed_reference);
     FETCH c_papp_refno INTO l_papp_refno;
    CLOSE c_papp_refno;
--
-- now check pp_valuations
--
    OPEN c_pval_seq(l_papp_refno, p1.ltim_pval_seqno);
     FETCH c_pval_seq INTO l_exists;
      IF c_pval_seq%NOTFOUND
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',514);
      END IF;
    CLOSE c_pval_seq;
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
,ltim_dlb_batch_id 
,ltim_dl_seqno 
,ltim_dl_load_status 
,ltim_papp_displayed_reference 
,ltim_seqno 
,ltim_description 
,ltim_verified_ind 
,ltim_pvr_amount 
,ltim_comments 
,ltim_pval_seqno
FROM dl_hpp_pp_tenant_improvs
WHERE ltim_dlb_batch_id     = p_batch_id
  AND ltim_dl_load_status   = 'C';
--
--
-- Cursor for pp_papp_refno
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HPP_PP_TENANT_IMPROVS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
l_papp_refno NUMBER(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_tenant_improvs.dataload_delete');
fsc_utils.debug_message( 's_dl_hpp_pp_tenant_improvs.dataload_delete',3 );
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
cs := p1.ltim_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_papp_refno := null;
--
-- get the papp_refno
--
  OPEN c_papp_refno(p1.ltim_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
--
      DELETE FROM tenant_improvements
      WHERE  tim_papp_refno = l_papp_refno
      AND    tim_seqno      = p1.ltim_seqno;
--
      DELETE FROM pp_valuation_reductions
      WHERE pvr_pval_papp_refno = l_papp_refno
      AND   pvr_tim_seqno       = p1.ltim_seqno
      AND   pvr_pval_seqno      = p1.ltim_pval_seqno
      AND   pvr_amount          = p1.ltim_pvr_amount;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANT_IMPROVEMENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_VALUATION_REDUCTIONS');
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
END s_dl_hpp_pp_tenant_improvs;
/

