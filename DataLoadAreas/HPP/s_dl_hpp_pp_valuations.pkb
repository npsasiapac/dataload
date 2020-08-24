CREATE OR REPLACE PACKAGE BODY s_dl_hpp_pp_valuations
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN         WHY
--      1.0  5.1.6   PH   10-JUN-2002  Initial Creation
--      2.0  5.2.0   PJD  04-OCT-2002  Changed cursors that select from 
--                                     interested party types to use 
--                                     the ipt_hrv_fiy_code     = 'VALU'
--      2.1  5.7.0   PH   14-JAN-2005  Interested Party no longer in
--                                     pp_valuations, now interested_party_usages
--      2.2  5.7.0   PJD  16-JAN-2005  If Error in Delete Proc, status
--                                     now remains at 'C'.
--      2.3  5.7.0   PH   25-APR-2005  IPUS_BR_IU trigger only allows one of
--                                     ipus_papp_refno or ipus_pval_papp_refno
--                                     to be populated. Amended insert.
--      2.4  5.8.0   VST  18-NOV-2005  Change delete process for IP's to
--                                     look use ipus_pval_papp_refno
--      2.0 5.13.0   PH   06-FEB-2008  Now includes its own 
--                                     set_record_status_flag procedure.
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
  UPDATE dl_hpp_pp_valuations
  SET lpval_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpp_pp_valuations');
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
,lpval_dlb_batch_id
,lpval_dl_seqno
,lpval_dl_load_status
,lpval_papp_displayed_reference
,lpval_seqno                            
,lpval_pro_propref                      
,lpval_valuer_ipp_shortname             
,lpval_surveyor_ipp_shortname           
,lpval_sco_code                         
,lpval_sco_changed_date                 
,lpval_valuation_requested              
,lpval_valuation_date                   
,lpval_valuation_amount                 
,lpval_comments 
FROM dl_hpp_pp_valuations
WHERE lpval_dlb_batch_id    = p_batch_id
AND   lpval_dl_load_status = 'V';
--
--
-- Cursor for papp_refno
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
--
-- Cursor for Valuer ipp_refno
--
CURSOR c_val_ipp_refno(p_val_ipp_refno varchar2) IS
SELECT ipp_refno
FROM   interested_party_types,interested_parties
WHERE  ipp_shortname        = p_val_ipp_refno
AND    ipp_ipt_code         = ipt_code
AND    ipt_hrv_fiy_code     = 'VALU'
AND    ipp_current_ind      = 'Y';
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_VALUATIONS';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_papp_refno    NUMBER(10);
l_val_ipp_refno NUMBER(10);
i               integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_valuations.dataload_create');
fsc_utils.debug_message( 's_dl_hpp_pp_valuations.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lpval_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the papp_refno
--
l_papp_refno := null;
 --
OPEN  c_papp_refno(p1.lpval_papp_displayed_reference);
FETCH c_papp_refno INTO l_papp_refno;
CLOSE c_papp_refno;
--
-- get the valuer ipp_refno
--
l_val_ipp_refno := null;
--
OPEN c_val_ipp_refno(p1.lpval_valuer_ipp_shortname);
FETCH c_val_ipp_refno INTO l_val_ipp_refno;
CLOSE c_val_ipp_refno;
--
      INSERT INTO pp_valuations
         (pval_seqno                     
         ,pval_sco_changed_date          
         ,pval_valuation_requested
         ,pval_valuation_date            
         ,pval_valuation_amount          
         ,pval_comments                  
         ,pval_created_by                
         ,pval_created_date            
         ,pval_papp_refno                
         ,pval_sco_code
         )
      VALUES
         (p1.lpval_seqno
         ,p1.lpval_sco_changed_date
         ,p1.lpval_valuation_requested
         ,p1.lpval_valuation_date
         ,p1.lpval_valuation_amount
         ,p1.lpval_comments
         ,'DATALOAD'
         ,trunc(sysdate)
         ,l_papp_refno
         ,p1.lpval_sco_code
         );
--
-- Insert into Interested_party_usages
--
  IF l_val_ipp_refno is not null
  THEN
      INSERT INTO interested_party_usages
         (ipus_refno
         ,ipus_ipp_refno
         ,ipus_start_date
         ,ipus_app_refno
         ,ipus_vin_refno
         ,ipus_tcy_refno
         ,ipus_end_date
         ,ipus_comments
         ,ipus_apl_refno
         ,ipus_papp_refno
         ,ipus_scs_refno
         ,ipus_cnt_reference
         ,ipus_aun_code
         ,ipus_pro_refno
         ,ipus_psl_refno
         ,ipus_pval_papp_refno
         ,ipus_pval_seqno
         ,ipus_schd_refno
         )
      VALUES
         (ipus_refno_seq.nextval
         ,l_val_ipp_refno
         ,p1.lpval_valuation_date
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null
         ,null  -- was l_papp_refno amended 25/04/05
         ,null
         ,null
         ,null
         ,null
         ,null
         ,l_papp_refno
         ,p1.lpval_seqno
         ,null);
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_VALUATIONS');
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
,lpval_dlb_batch_id                     
,lpval_dl_seqno                         
,lpval_dl_load_status                   
,lpval_papp_displayed_reference              
,lpval_seqno                            
,lpval_pro_propref                      
,lpval_valuer_ipp_shortname             
,lpval_surveyor_ipp_shortname           
,lpval_sco_code                         
,lpval_sco_changed_date                 
,lpval_valuation_requested              
,lpval_valuation_date                   
,lpval_valuation_amount                 
,lpval_comments
FROM  dl_hpp_pp_valuations
WHERE lpval_dlb_batch_id      = p_batch_id
AND   lpval_dl_load_status   in ('L','F','O');
--
--
-- Cursor for papp_refno
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
--
-- Cursor to see if valuation already exists
--
CURSOR c_seq_check(p_papp_refno number
                  ,p_pval_seqno number) IS

SELECT 'X'
FROM   pp_valuations
WHERE  pval_papp_refno = p_papp_refno
AND    pval_seqno      = p_pval_seqno;
--
-- Cursor for property check
--
CURSOR c_pro_refno(p_propref varchar2) IS
SELECT pro_refno
FROM   properties
WHERE  pro_propref = p_propref;
--
-- Check sco_code
--
CURSOR c_sco(p_sco_code varchar2) IS
SELECT 'X'
FROM   status_codes
WHERE  sco_code = p_sco_code;
--
-- Cursor for valuer
--
CURSOR c_val_ipp(p_val_ipp varchar2) IS
SELECT 'X'
FROM   interested_party_types,interested_parties
WHERE  ipp_shortname        = p_val_ipp
AND    ipp_ipt_code         = ipt_code
AND    ipt_hrv_fiy_code     = 'VALU'
AND    ipp_current_ind      = 'Y';

-- CURSOR c_val_ipp(p_val_ipp varchar2) IS
-- SELECT 'X'
-- FROM   interested_parties
-- WHERE  ipp_shortname   = p_val_ipp
-- AND    ipp_ipt_code    = 'VALU'
-- AND    ipp_current_ind = 'Y';
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_VALUATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_papp_refno     NUMBER(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_valuations.dataload_validate');
fsc_utils.debug_message( 's_dl_hpp_pp_valuations.dataload_validate',3);
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
cs := p1.lpval_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the application exists
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lpval_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
    IF c_papp_refno%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',804);
    END IF;
  CLOSE c_papp_refno;
--
-- Check that the sequence doesn't already exist
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lpval_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
--
  OPEN c_seq_check(l_papp_refno, p1.lpval_seqno);
   FETCH c_seq_check INTO l_exists;
    IF c_seq_check%FOUND
     THEN 
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',831);
    END IF;
  CLOSE c_seq_check;
--
-- Check that the property exists
--
l_pro_refno := null;
--
  OPEN c_pro_refno(p1.lpval_pro_propref);
   FETCH c_pro_refno INTO l_pro_refno;
    IF c_pro_refno%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',805);
    END IF;
  CLOSE c_pro_refno;
--
-- Check the status code
--
  OPEN c_sco(p1.lpval_sco_code);
   FETCH c_sco INTO l_exists;
    IF c_sco%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',836);
    END IF;
  CLOSE c_sco;
--
-- Check Valuer reference
--
  IF p1.lpval_valuer_ipp_shortname IS NOT NULL
   THEN
    OPEN c_val_ipp(p1.lpval_valuer_ipp_shortname);
     FETCH c_val_ipp INTO l_exists;
      IF c_val_ipp%NOTFOUND
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',837);
      END IF;
    CLOSE c_val_ipp;
  END IF;
--
--  Check that the request date is before valuation date
--
  IF p1.lpval_valuation_date IS NOT NULL
   THEN
    IF (p1.lpval_valuation_date < p1.lpval_valuation_requested)
     THEN 
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',839);
    END IF;
  END IF;
--
-- Check all other Mandatory Columns
--
-- Sequence number
--
  IF p1.lpval_seqno IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',515);
  END IF;

-- Status changed date
--
  IF p1.lpval_sco_changed_date IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',516);
  END IF;
--
-- Valuation Requested
--
  IF p1.lpval_valuation_requested IS NULL
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',517);
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
,lpval_dlb_batch_id                     
,lpval_dl_seqno                         
,lpval_dl_load_status                   
,lpval_papp_displayed_reference              
,lpval_seqno                            
,lpval_pro_propref                      
,lpval_valuer_ipp_shortname             
,lpval_surveyor_ipp_shortname           
,lpval_sco_code                         
,lpval_sco_changed_date                 
,lpval_valuation_requested              
,lpval_valuation_date                   
,lpval_valuation_amount                 
,lpval_comments
FROM  dl_hpp_pp_valuations
WHERE lpval_dlb_batch_id     = p_batch_id
  AND lpval_dl_load_status   = 'C';
--
--
-- Cursor for papp_refno
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
CURSOR c_val_ipp_refno(p_val_ipp_refno varchar2) IS
SELECT ipp_refno
FROM   interested_party_types,interested_parties
WHERE  ipp_shortname        = p_val_ipp_refno
AND    ipp_ipt_code         = ipt_code
AND    ipt_hrv_fiy_code     = 'VALU';
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HPP_PP_VALUATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
l_papp_refno NUMBER(10);
--
l_ipp_refno  NUMBER(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_valuations.dataload_delete');
fsc_utils.debug_message( 's_dl_hpp_pp_valuations.dataload_delete',3 );
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
cs := p1.lpval_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_papp_refno := null;
l_ipp_refno  := null;
--
--
-- get papp_refno
--
  OPEN c_papp_refno(p1.lpval_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
--
--
  IF p1.lpval_valuer_ipp_shortname IS NOT NULL
   THEN
    OPEN c_val_ipp_refno(p1.lpval_valuer_ipp_shortname);
     FETCH c_val_ipp_refno INTO l_ipp_refno;
    CLOSE c_val_ipp_refno;
--
       DELETE FROM interested_party_usages
       WHERE  ipus_pval_papp_refno  = l_papp_refno
       AND    ipus_pval_seqno       = p1.lpval_seqno
       AND    ipus_ipp_refno        = l_ipp_refno;
--
  END IF;
--
       DELETE FROM pp_valuations
       WHERE pval_papp_refno = l_papp_refno
       AND   pval_seqno      = p1.lpval_seqno;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_VALUATIONS');
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
END s_dl_hpp_pp_valuations;
/

