CREATE OR REPLACE PACKAGE BODY s_dl_hpp_pp_valuation_defects
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
--      
-- ***********************************************************************     
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hpp_pp_valuation_defects
  SET lpvd_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpp_pp_valuation_defects');
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
,lpvd_dlb_batch_id                     
,lpvd_dl_seqno                         
,lpvd_dl_load_status                   
,lpvd_papp_displayed_reference              
,lpvd_pval_seqno                       
,lpvd_vdt_code                         
,lpvd_amount                           
,lpvd_comments
FROM dl_hpp_pp_valuation_defects
WHERE lpvd_dlb_batch_id    = p_batch_id
AND   lpvd_dl_load_status = 'V';
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_VALUATION_DEFECTS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_papp_refno number;
i           integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_valuation_defects.dataload_create');
fsc_utils.debug_message( 's_dl_hpp_pp_valuation_defects.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lpvd_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the papp_refno
--
l_papp_refno := null;
 --
  OPEN  c_papp_refno(p1.lpvd_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
--
      INSERT INTO pp_valuation_defects
         (pvd_comments           
         ,pvd_created_by         
         ,pvd_created_date     
         ,pvd_amount             
         ,pvd_pval_seqno         
         ,pvd_pval_papp_refno
         ,pvd_vdt_code           

         )
      VALUES
         (p1.lpvd_comments
         ,'DATALOAD'
         ,trunc(sysdate)
         ,p1.lpvd_amount
         ,p1.lpvd_pval_seqno
         ,l_papp_refno
         ,p1.lpvd_vdt_code
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_VALUATION_DEFECTS');
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
,lpvd_dlb_batch_id                     
,lpvd_dl_seqno                         
,lpvd_dl_load_status                   
,lpvd_papp_displayed_reference              
,lpvd_pval_seqno                       
,lpvd_vdt_code                         
,lpvd_amount                           
,lpvd_comments
FROM dl_hpp_pp_valuation_defects
WHERE lpvd_dlb_batch_id      = p_batch_id
AND   lpvd_dl_load_status   in ('L','F','O');
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
--
CURSOR c_val_chk(p_papp_refno number
                ,p_pval_seqno number) IS
SELECT 'X'
FROM   pp_valuations
WHERE  pval_papp_refno = p_papp_refno
AND    pval_seqno      = p_pval_seqno;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_VALUATION_DEFECTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_papp_refno     NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_valuation_defects.dataload_validate');
fsc_utils.debug_message( 's_dl_hpp_pp_valuation_defects.dataload_validate',3);
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
cs := p1.lpvd_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the application exists
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lpvd_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
    IF c_papp_refno%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',804);
    END IF;
  CLOSE c_papp_refno;
--
-- Check the defect code is valid
--
   IF (NOT s_dl_hem_utils.exists_frv('VAL_DEF_TYP', p1.lpvd_vdt_code, 'N'))
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',832);
   END IF;
--
-- Check Parent valuation exists
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lpvd_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
-- 
   OPEN c_val_chk(l_papp_refno, p1.lpvd_pval_seqno);
    FETCH c_val_chk INTO l_exists;
     IF c_val_chk%NOTFOUND
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',843);
     END IF;
   CLOSE c_val_chk;
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
,lpvd_dlb_batch_id                     
,lpvd_dl_seqno                         
,lpvd_dl_load_status                   
,lpvd_papp_displayed_reference              
,lpvd_pval_seqno                       
,lpvd_vdt_code                         
,lpvd_amount                           
,lpvd_comments
FROM dl_hpp_pp_valuation_defects
WHERE lpvd_dlb_batch_id    = p_batch_id
  AND lpvd_dl_load_status   = 'C';
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HPP_PP_VALUATION_DEFECTS';
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
fsc_utils.proc_start('s_dl_hpp_pp_valuation_defects.dataload_delete');
fsc_utils.debug_message( 's_dl_hpp_pp_valuation_defects.dataload_delete',3 );
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
cs := p1.lpvd_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get papp_refno
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lpvd_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
--
       DELETE FROM pp_valuation_defects
       WHERE  pvd_pval_papp_refno = l_papp_refno
       AND    pvd_pval_seqno      = p1.lpvd_pval_seqno;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_VALUATION_DEFECTS');
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
END s_dl_hpp_pp_valuation_defects;
/

