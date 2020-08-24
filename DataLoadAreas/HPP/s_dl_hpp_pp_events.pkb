CREATE OR REPLACE PACKAGE BODY s_dl_hpp_pp_events
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.1.6     PH   14-APR-2002  Initial Creation
--      1.1  5.2.0     SB   10-SEP-2002  Added Close c_pet cursor in
--                                       Else Clause
--      1.2  5.2.0     SB   12-SEP_2002  Switched Created date and by in
--                                       insert statment
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
  UPDATE dl_hpp_pp_events
  SET lpev_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpp_pp_events');
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
,lpev_dlb_batch_id        
,lpev_dl_seqno           
,lpev_dl_load_status     
,lpev_papp_displayed_reference
,lpev_pet_code           
,lpev_seqno              
,lpev_actual_date        
,lpev_target_date        
,lpev_statutory_date     
,lpev_comments
FROM DL_HPP_PP_EVENTS
WHERE lpev_dlb_batch_id    = p_batch_id
AND   lpev_dl_load_status = 'V';
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
ct       VARCHAR2(30) := 'DL_HPP_PP_EVENTS';
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
fsc_utils.proc_start('s_dl_hpp_pp_events.dataload_create');
fsc_utils.debug_message( 's_dl_hpp_pp_events.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lpev_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the papp_refno
--
l_papp_refno := null;
 --
  OPEN  c_papp_refno(p1.lpev_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
--
      INSERT INTO pp_events
         (pev_seqno              
         ,pev_actual_date        
         ,pev_target_date        
         ,pev_statutory_date     
         ,pev_comments           
         ,pev_created_by       
         ,pev_created_date         
         ,pev_papp_refno         
         ,pev_pet_code           
         ,pev_type
         )
      VALUES
         (to_number(p1.lpev_seqno)
         ,p1.lpev_actual_date
         ,p1.lpev_target_date
         ,p1.lpev_statutory_date
         ,p1.lpev_comments
         ,'DATALOAD'
         ,sysdate
         ,l_papp_refno
         ,p1.lpev_pet_code
         ,'APP'            
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_EVENTS');
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
,lpev_dlb_batch_id        
,lpev_dl_seqno           
,lpev_dl_load_status     
,lpev_papp_displayed_reference
,lpev_pet_code           
,lpev_seqno              
,lpev_actual_date        
,lpev_target_date        
,lpev_statutory_date     
,lpev_comments
FROM  DL_HPP_PP_EVENTS
WHERE lpev_dlb_batch_id      = p_batch_id
AND   lpev_dl_load_status   in ('L','F','O');
--
-- Cursor to see if application exists
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
-- Cursor to check Event Type
--
CURSOR c_pet(p_pet_code varchar2) IS
SELECT 'X'
FROm   pp_event_types
WHERE  pet_code        = p_pet_code
AND    pet_current_ind = 'Y';
--
-- Cursor to get Unique flag
--
CURSOR c_pet_unique(p_pet_code varchar2) IS
SELECT pet_unique_ind
FROM   pp_event_types
WHERE  pet_code        = p_pet_code
AND    pet_current_ind = 'Y';
--
-- Cursor for matching application/event/sequence
--
CURSOR c_papp_pet(p_papp_refno number
                 ,p_pet_code   varchar2
                 ,p_seqno      number) IS
SELECT 'X'
FROM   pp_events
WHERE  pev_papp_refno = p_papp_refno
AND    pev_pet_code   = p_pet_code
AND    pev_seqno      = p_seqno;

--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_EVENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_pet_unique     VARCHAR2(1);
l_papp_refno     NUMBER(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_events.dataload_validate');
fsc_utils.debug_message( 's_dl_hpp_pp_events.dataload_validate',3);
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
cs := p1.lpev_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check application exists
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lpev_papp_displayed_reference);
   FETCH c_papp_refno into l_papp_refno;
    IF c_papp_refno%NOTFOUND
     THEN 
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',804);
    END IF;
  CLOSE c_papp_refno;
--
-- Check the event code is valid
--
  OPEN c_pet(p1.lpev_pet_code);
   FETCH c_pet INTO l_exists;
    IF c_pet%NOTFOUND
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',840);
       CLOSE c_pet;
    ELSE
--
-- Check that where the sequence <> 1 that the event code is not unique
--
    l_pet_unique := null;
--
    CLOSE c_pet;
--
    OPEN c_pet_unique(p1.lpev_pet_code);
     FETCH c_pet_unique INTO l_pet_unique;
    CLOSE c_pet_unique;
--
      IF p1.lpev_seqno > 1
       AND l_pet_unique = 'Y'
        THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',840);
      END IF;
   END IF;
--
-- Check that a matching application event does not already exist
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lpev_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno;
  CLOSE c_papp_refno;
--
  OPEN c_papp_pet(l_papp_refno, p1.lpev_pet_code, p1.lpev_seqno);
   FETCH c_papp_pet INTO l_exists;
    IF c_papp_pet%FOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',831);
    END IF;
  CLOSE c_papp_pet;
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
,lpev_dlb_batch_id        
,lpev_dl_seqno           
,lpev_dl_load_status     
,lpev_papp_displayed_reference
,lpev_pet_code           
,lpev_seqno              
,lpev_actual_date        
,lpev_target_date        
,lpev_statutory_date     
,lpev_comments
FROM DL_HPP_PP_EVENTS
WHERE lpev_dlb_batch_id     = p_batch_id
  AND lpev_dl_load_status   = 'C';
--
--
-- Cursor to get papp_refno
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
ct       VARCHAR2(30) := 'DL_HPP_PP_EVENTS';
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
fsc_utils.proc_start('s_dl_hpp_pp_events.dataload_delete');
fsc_utils.debug_message( 's_dl_hpp_pp_events.dataload_delete',3 );
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
cs := p1.lpev_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_papp_refno := null;
--
  OPEN c_papp_refno(p1.lpev_papp_displayed_reference);
   FETCH c_papp_refno into l_papp_refno;
  CLOSE c_papp_refno;
--
   DELETE FROM pp_events
   WHERE  pev_papp_refno = l_papp_refno
   AND    pev_pet_code   = p1.lpev_pet_code
   AND    pev_seqno      = p1.lpev_seqno;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PP_EVENTS');
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
END s_dl_hpp_pp_events;
/

