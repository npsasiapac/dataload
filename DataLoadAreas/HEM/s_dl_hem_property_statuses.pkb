CREATE OR REPLACE PACKAGE BODY s_dl_hem_property_statuses
AS
-- ***********************************************************************
-- DESCRIPTION:
--
-- CHANGE CONTROL
-- VER  DB VER  WHO  WHEN        WHY
-- 1.0          PJD  05/09/00  Dataload
-- 1.1          PJD  21/03/02  Added cursor for int_seqno
-- 1.2          MH   18/04/02  Removed superfluous dl_status update in delete
--                             procedure
-- 1.3          PJD  15/07/02  Changed insert for VOID event to use l_void_evt
-- 2.0  5.2.0   PJD  21/08/02  Moved 'Gaps' part of code to insert before 
--                             current void part
--                             Allowed use of answer to determine if separate 
--                             void events dataload is to be used - void events
--                             for current voids will not be created if the 
--                             answer to the question is Y.
-- 2.1  5.2.0   PH   04/10/02  Added cursor to get the default void class from 
--                             first_ref_values rather than using DATALOAD.
-- 3.0  5.3.0   PH   20/01/03  Amended so all inserts into void_instances use 
--                             above value.
-- 3.1  5.3.0   SB   20/01/02  Amended last insert statement to void_events to 
--                             use l_void_evt rather than l_start_evt.
-- 3.2  5.3.0   PJD  05/02/03  Changed c_gaps cursor - so that it no longer 
--                             excludes 'V' type records
-- 3.3  5.3.0   SB   04/03/03  Amended void_event inserts (where no no status) 
--                             to use p1.lhps_start_date rather than l_max_end
-- 3.4  5.4.0   PJD  20/11/03  Moved update of Record Status and Process Count
-- 3.5  5.6.0   PJD  20/11/04  Added a Validate Procedure
-- 3.6  5.10.0  PH   26/09/06  Amended Create, added update to void_instances
--                             (status_date) where end event is found.
-- 4.0  5.13.0  PH   06-FEB-08 Now includes its own 
--                             set_record_status_flag procedure.
-- 4.1  5.13.0  PH   04-MAR-08 Added rec_rowid to delete cursor
-- 4.2  5.15.0  PH   11-MAR-09 Amended previous update to void_instances and 
--                             set the status date to be end date +1 as this is
--                             what happens within the application
-- 4.3  5.15.0  PJD  22-JUN-12 Maintain pro_sco_Code in line with 
--                             hou_prop_statuses
-- 4.4  6.9.0   PJD  15-NOV-13 Question used in Create procedure is now going 
--                             to be 'Create defualt events for current Voids'.
--                             
--                             So added new variable 'l_create_events' to Create
--                             and validate processes to make it easier to 
--                             follow logic.
--                             In order to aid linking to relevant void instance
--                             (for the Void Events DL etc) This load will now
--                             put the Vin Refno into the hps_comments field. 
-- 4.5  6.13    MOK  19-MAY-16 Removed Batch Question
-- 4.6  6.13    AJ   01-FEB-17 Further updates to fully remove batch question and to get
--                             vin_refno into hps_comments for use with the void events
--                             and void instances bespoke data loads
-- 4.7  6.13    AJ   03-FEB-17 l_answer removed and l_create_events set to Y and left so
--                             creation of void events can be turned off if required
-- 4.8  6.14/15 AJ   27-MAR-17 Updated IF l_create_events = 'Y' wrong syntax line 1134
-- 4.9  6.14/15 AJ   12-OCT-17 Checked setting of vin_sco_codes currently hard codes to
--                             NEW (vin_sco_code) and FIN (vin_sco_code) they need to existing
--                             in status_codes table and are sys ones so these are fine.
--                             The actual void status (vin_vst_code) is being found looking for the 
--                             vst_new = 'Y' and the vst_completed = 'Y' (like LET) again
--                             these are SYS ones and should exist.
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hem_property_statuses
  SET lhps_DL_load_status= p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_property_statuses');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--  declare package variables AND constants
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id varchar2) is
SELECT
rowid rec_rowid
,lhps_dlb_batch_id
,lhps_dl_seqno
,lhps_dl_load_status
,lhps_pro_propref
,nvl(lhps_start_date,sysdate) lhps_start_date
FROM dl_hem_property_statuses
WHERE lhps_dlb_batch_id    = p_batch_id
AND   lhps_dl_load_status = 'V';
--
CURSOR c_pro_refno(p_propref varchar2) is
SELECT pro_refno FROM properties
WHERE pro_propref = p_propref;
--
CURSOR c_min_date(p_refno number) is
SELECT min(hps_start_date),max(hps_start_date),
       min(hps_end_date)  ,max(nvl(hps_end_date,sysdate+100))
FROM hou_prop_statuses
WHERE hps_pro_refno = p_refno;
--
CURSOR c_vgr(p_refno number) is
SELECT vgr_code,vgr_auto_generate
FROM void_groups g, prop_types t, prop_type_values v,properties p
WHERE p.pro_refno         = p_refno
  AND p.pro_hou_ptv_refno = v.ptv_refno
  AND v.ptv_pty_code      = t.pty_code
  AND t.pty_vgr_code      = g.vgr_code;
--
CURSOR c_start_evt is
SELECT evt_code
FROM event_types
WHERE evt_starting_event = 'Y';
--
CURSOR c_end_evt is
SELECT evt_code
FROM event_types
WHERE evt_ending_event    = 'Y';
--
CURSOR c_void_evt is
SELECT evt_code
FROM event_types
WHERE evt_void_event     = 'Y';
--
CURSOR c_gaps(p_refno NUMBER) IS
SELECT h1.hps_end_date+1 void_start, h2.hps_start_date-1 void_end
FROM hou_prop_statuses h1, hou_prop_statuses h2
WHERE h1.hps_pro_refno  = p_refno
  AND h2.hps_pro_refno  = p_refno
  AND h2.hps_start_date > h1.hps_end_date +1
  AND not exists         (SELECT null FROM hou_prop_statuses h3
                          WHERE h3.hps_pro_refno  = p_refno
                            AND h3.hps_start_date > h1.hps_end_date
                            AND h3.hps_start_date < h2.hps_start_date);
--
CURSOR c_vin_refno IS
SELECT vin_refno_seq.nextval FROM dual;
--
CURSOR c_vst IS
SELECT vst_code
FROM void_statuses
WHERE vst_new = 'Y';
--
CURSOR c_vst_comp IS
SELECT vst_code
FROM void_statuses
WHERE vst_completed = 'Y'
ORDER BY decode(vst_code,'LET',1,2),vst_code;
--
CURSOR c_int_seqno(p_vin_refno number) Is
select max(vev_int_seqno +1)
from void_events
where vev_vin_refno = p_vin_refno;
--
CURSOR c_vcl_code IS
SELECT frv_code
FROM   first_ref_values
WHERE  frv_frd_domain = 'VOID_CLASS'
AND    frv_default_ind = 'Y'
AND    frv_current_ind = 'Y';
--
cursor c_curr_stat(p_pro_refno NUMBER) IS
SELECT DECODE(hps_hpc_type,'O','OCC','C','CLO','VOI')
FROM   hou_prop_statuses
WHERE  hps_pro_refno = p_pro_Refno
AND  hps_end_Date IS NULL;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_PROPERTY_STATUSES';
cs       INTEGER;
ce	     VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_pro_refno     NUMBER;
l_min_start     DATE;
l_min_end       DATE;
l_max_start     DATE;
l_max_end       DATE;
l_start_evt     VARCHAR2(4);
l_end_evt       VARCHAR2(4);
l_void_evt      VARCHAR2(4);
l_vgr_code      VARCHAR2(4);
l_vgr_ag        VARCHAR2(4);
l_vin_refno     INTEGER;
i1              INTEGER;
l_an_tab        VARCHAR2(1);
l_vst_code      VARCHAR2(10);
l_vst_comp      VARCHAR2(10);
l_int_seq       INTEGER;
l_create_events VARCHAR2(1);
l_vcl_code      VARCHAR2(10);
l_curr_stat     VARCHAR2(3);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_property_statuses.dataload_create');
fsc_utils.debug_message( 's_dl_hem_property_statuses.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--**********************************************************************
-- The Start Events are now set to be created if the void instance is also
-- created this being when the Auto Generate Flag is set to Y in the void
-- group to which the property type is associated
-- IF YOU DO NOT REQUIRE THE VOID EVENTS TO BE CREATED FOR CURRENT VOIDS
-- ONLY THEN SET THE l_create_events BELOW TO N (AJ 03Feb2017)
--
-- THIS MUST MATCH THE SETTING IN THE VALIDATE and DELETE SECTIONS
--
l_create_events := 'Y';
--**********************************************************************
--
OPEN  c_start_evt;
FETCH c_start_evt into l_start_evt;
CLOSE c_start_evt;
--
OPEN  c_end_evt;
FETCH c_end_evt into l_end_evt;
CLOSE c_end_evt;
--
OPEN  c_void_evt;
FETCH c_void_evt into l_void_evt;
CLOSE c_void_evt;
--
OPEN  c_vst;
FETCH c_vst into l_vst_code;
CLOSE c_vst;
--
OPEN  c_vst_comp;
FETCH c_vst_comp into l_vst_comp;
CLOSE c_vst_comp;
--
OPEN  c_vcl_code;
FETCH c_vcl_code into l_vcl_code;
CLOSE c_vcl_code;

--
FOR p1 IN c1(p_batch_id) LOOP
--
BEGIN
--
cs := p1.lhps_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the pro_refno
--
l_pro_refno := null;
 --
OPEN c_pro_refno(p1.lhps_pro_propref);
FETCH c_pro_refno into l_pro_refno;
CLOSE c_pro_refno;
--
l_min_start := null;
l_min_end   := null;
l_max_start := null;
l_max_end   := null;
l_vgr_code  := null;
l_vgr_ag    := null;
--
-- get the void group
--
OPEN  c_vgr(l_pro_refno);
FETCH c_vgr into l_vgr_code,l_vgr_ag;
CLOSE c_vgr;
--
-- now look to see if any insert are needed
--
OPEN c_min_date(l_pro_refno);
FETCH c_min_date INTO l_min_start,l_max_start,l_min_end,l_max_end;
CLOSE c_min_date;
--
-- if there is no existing status
--
IF l_min_start IS NULL
THEN
   INSERT INTO hou_prop_statuses
   (hps_pro_refno
   ,hps_hpc_code
   ,hps_hpc_type
   ,hps_start_date
   ,hps_end_date)
   VALUES
   (l_pro_refno
   ,'VOID'
   ,'V'
   ,p1.lhps_start_date
   ,null);
   --
   IF l_vgr_ag = 'Y'
   THEN
     OPEN  c_vin_refno;
     FETCH c_vin_refno into l_vin_refno;
     CLOSE c_vin_refno;
   --
     INSERT INTO void_instances
     (vin_status_start       ,vin_created_date       ,vin_dec_allowance
     ,vin_text               ,vin_man_created        ,vin_vst_code
     ,vin_tgt_date           ,vin_apt_code           ,vin_vgr_code
     ,vin_vpa_curr_code      ,vin_refno              ,vin_sco_code
     ,vin_hrv_rfv_code       ,vin_hrv_vcl_code       ,vin_pro_refno
     ,vin_effective_date     ,vin_reusable_refno)
     VALUES
     (p1.lhps_start_date     ,sysdate                ,null
     ,'DATALOADED'           ,'N'                    ,l_vst_code
     ,null                   ,'PROV'                 ,l_vgr_code
     ,null                   ,l_vin_refno            ,'NEW'
     ,'DATALOAD'             ,l_vcl_code             ,l_pro_refno
     ,p1.lhps_start_date     ,reusable_refno_seq.nextval);
   --
     IF l_create_events = 'Y'
   --
     THEN
       IF l_start_evt IS NOT NULL 
       THEN 
       --    
         OPEN  c_int_seqno(l_vin_refno);
         FETCH c_int_seqno  into l_int_seq;
         CLOSE c_int_seqno;
		 --
         l_int_seq := nvl(l_int_seq,1);
         --
         INSERT INTO void_events
         (vev_vin_refno          ,vev_int_seqno          ,vev_order_seqno
         ,vev_evt_code           ,vev_off_refno          ,vev_event_date
         ,vev_target_date        ,vev_calc_ext           ,vev_text
         ,vev_dev_vpa_code       ,vev_dev_seqno          ,vev_sys_updated
         ,vev_username)
         VALUES
        (l_vin_refno            ,l_int_seq              ,1
         ,l_start_evt            ,null                   ,p1.lhps_start_date
         ,null                   ,null                   ,'DATALOAD'
         ,null                   ,null                   ,sysdate
         ,'DATALOAD');
       END IF;
       --
       IF l_void_evt IS NOT NULL 
       THEN 
         --     Insert the VOID event    
         OPEN  c_int_seqno(l_vin_refno);
         FETCH c_int_seqno  into l_int_seq;
         CLOSE c_int_seqno;
		 
         l_int_seq := nvl(l_int_seq,1);
     
         INSERT INTO void_events
         (vev_vin_refno          ,vev_int_seqno          ,vev_order_seqno
         ,vev_evt_code           ,vev_off_refno          ,vev_event_date
         ,vev_target_date        ,vev_calc_ext           ,vev_text
         ,vev_dev_vpa_code       ,vev_dev_seqno          ,vev_sys_updated
         ,vev_username)
         VALUES
         (l_vin_refno            ,l_int_seq              ,1
         ,l_void_evt              ,null                   ,p1.lhps_start_date
         ,null                   ,null                   ,'DATALOAD'
         ,null                   ,null                   ,sysdate
         ,'DATALOAD');
       END IF;
     END IF; -- l_create_events = 'Y'
     --
     UPDATE hou_prop_statuses
     SET    hps_comments   = '(VIN REFNO = '||l_vin_refno||')'
     WHERE  hps_pro_refno  = l_pro_refno
       AND  hps_hpc_code   = 'VOID'
       AND  hps_start_date = p1.lhps_start_date; 
     --
   END IF;  -- Auto Generate Flag
--
END IF;
--
-- now do any gaps
--
FOR p_gaps IN c_gaps(l_pro_refno) LOOP
--
-- dbms_output.put_line('Filling in gaps');
   INSERT INTO hou_prop_statuses
   (hps_pro_refno
   ,hps_hpc_code
   ,hps_hpc_type
   ,hps_start_date
   ,hps_end_date
   )
   VALUES
   (l_pro_refno
   ,'VOID'
   ,'V'
   ,p_gaps.void_start
   ,p_gaps.void_end);
--
--  IT IS EXPECTED THAT SITES WILL WANT HISTORIC VOID INSTANCES
--  BUT IF THEY DO NOT THEN THE FOLLOWING CODE CAN BE REMOVED
--
   IF l_vgr_ag = 'Y'
   THEN
     OPEN  c_vin_refno;
     FETCH c_vin_refno into l_vin_refno;
     CLOSE c_vin_refno;
--
     INSERT INTO void_instances
     (vin_status_start       ,vin_created_date       ,vin_dec_allowance
     ,vin_text               ,vin_man_created        ,vin_vst_code
     ,vin_tgt_date           ,vin_apt_code           ,vin_vgr_code
     ,vin_vpa_curr_code      ,vin_refno              ,vin_sco_code
     ,vin_hrv_rfv_code       ,vin_hrv_vcl_code       ,vin_pro_refno
     ,vin_effective_date     ,vin_reusable_refno)
     VALUES
     (p_gaps.void_start      ,sysdate                ,null
     ,'DATALOADED'           ,'N'                    ,l_vst_comp
     ,null                   ,'PROV'                 ,l_vgr_code
     ,null                   ,l_vin_refno            ,'FIN'
     ,'DATALOAD'             ,l_vcl_code             ,l_pro_refno
     ,p_gaps.void_start      ,reusable_refno_seq.nextval);
--
     IF l_start_evt IS NOT NULL 
     THEN
    
     OPEN  c_int_seqno(l_vin_refno);
     FETCH c_int_seqno  into l_int_seq;
     CLOSE c_int_seqno;
     l_int_seq := nvl(l_int_seq,1);
     
       INSERT INTO void_events
       (vev_vin_refno          ,vev_int_seqno          ,vev_order_seqno
       ,vev_evt_code           ,vev_off_refno          ,vev_event_date
       ,vev_target_date        ,vev_calc_ext           ,vev_text
       ,vev_dev_vpa_code       ,vev_dev_seqno          ,vev_sys_updated
       ,vev_username)
       VALUES
       (l_vin_refno            ,l_int_seq              ,1
       ,l_start_evt            ,null                   ,p_gaps.void_start
       ,null                   ,null                   ,'DATALOAD'
       ,null                   ,null                   ,sysdate
       ,'DATALOAD');
     END IF;
--
     IF l_void_evt IS NOT NULL 
     THEN
    
     OPEN  c_int_seqno(l_vin_refno);
     FETCH c_int_seqno  into l_int_seq;
     CLOSE c_int_seqno;
     l_int_seq := nvl(l_int_seq,1);
     
       INSERT INTO void_events
       (vev_vin_refno          ,vev_int_seqno          ,vev_order_seqno
       ,vev_evt_code           ,vev_off_refno          ,vev_event_date
       ,vev_target_date        ,vev_calc_ext           ,vev_text
       ,vev_dev_vpa_code       ,vev_dev_seqno          ,vev_sys_updated
       ,vev_username)
       VALUES
       (l_vin_refno            ,2                      ,2
       ,l_void_evt             ,null                   ,p_gaps.void_start
       ,null                   ,null                   ,'DATALOAD'
       ,null                   ,null                   ,sysdate
       ,'DATALOAD');
     END IF;
--
     IF l_end_evt IS NOT NULL 
     THEN
    
       OPEN  c_int_seqno(l_vin_refno);
       FETCH c_int_seqno  into l_int_seq;
       CLOSE c_int_seqno;
       l_int_seq := nvl(l_int_seq,1);
     
       INSERT INTO void_events
       (vev_vin_refno          ,vev_int_seqno          ,vev_order_seqno
       ,vev_evt_code           ,vev_off_refno          ,vev_event_date
       ,vev_target_date        ,vev_calc_ext           ,vev_text
       ,vev_dev_vpa_code       ,vev_dev_seqno          ,vev_sys_updated
       ,vev_username)
       VALUES
       (l_vin_refno            ,l_int_seq              ,3
       ,l_end_evt              ,null                   ,p_gaps.void_end
       ,null                   ,null                   ,'DATALOAD'
       ,null                   ,null                   ,sysdate
       ,'DATALOAD');
       --
       -- New code added to update the status start of the instance
       -- as it's an ended event.
       --
       -- Added a day to this so its the start of tcy as happens in application
       --
       UPDATE void_instances
          SET vin_status_start = p_gaps.void_end+1
        WHERE vin_refno        = l_vin_refno;
       --
     END IF;
     --
     UPDATE hou_prop_statuses
     SET    hps_comments   = '(VIN REFNO = '||l_vin_refno||')'
     WHERE  hps_pro_refno  = l_pro_refno
       AND  hps_hpc_code   = 'VOID'
       AND  hps_start_date = p_gaps.void_start
     ; 
     --
   END IF;  -- Auto Generate Flag
--
-- END OF SECTION FOR VOID INSTANCES FOR HISTORIC VOID PERIODS
--
-- end of c_gaps loop 
END LOOP;
--
-- START OF SECTION FOR CURRENT VOID PERIOD FOLLOWING A TENANCY
--
IF l_max_end < sysdate +99 -- i.e. currently void or about to become void
 THEN
    INSERT INTO hou_prop_statuses
   (hps_pro_refno
   ,hps_hpc_code
   ,hps_hpc_type
   ,hps_start_date
   ,hps_end_date
   )
   VALUES
   (l_pro_refno
   ,'VOID'
   ,'V'
   ,l_max_end +1
   ,null);
   --
   IF l_vgr_ag = 'Y'
   THEN
     OPEN  c_vin_refno;
     FETCH c_vin_refno into l_vin_refno;
     CLOSE c_vin_refno;
     --
     INSERT INTO void_instances
     (vin_status_start       ,vin_created_date       ,vin_dec_allowance
     ,vin_text               ,vin_man_created        ,vin_vst_code
     ,vin_tgt_date           ,vin_apt_code           ,vin_vgr_code
     ,vin_vpa_curr_code      ,vin_refno              ,vin_sco_code
     ,vin_hrv_rfv_code       ,vin_hrv_vcl_code       ,vin_pro_refno
     ,vin_effective_date     ,vin_reusable_refno)
     VALUES
     (l_max_end +1           ,sysdate                ,null
     ,'DATALOADED'           ,'N'                    ,l_vst_code
     ,null                   ,'PROV'                 ,l_vgr_code
     ,null                   ,l_vin_refno            ,'NEW'
     ,'DATALOAD'             ,l_vcl_code             ,l_pro_refno
     ,l_max_end +1           ,reusable_refno_seq.nextval);
     --
     IF l_create_events = 'Y'
     THEN
       IF l_start_evt IS NOT NULL 
       THEN 
         OPEN  c_int_seqno(l_vin_refno);
         FETCH c_int_seqno  into l_int_seq;
         CLOSE c_int_seqno;
         l_int_seq := nvl(l_int_seq,1);
         --
         INSERT INTO void_events
         (vev_vin_refno          ,vev_int_seqno          ,vev_order_seqno
         ,vev_evt_code           ,vev_off_refno          ,vev_event_date
         ,vev_target_date        ,vev_calc_ext           ,vev_text
         ,vev_dev_vpa_code       ,vev_dev_seqno          ,vev_sys_updated
         ,VEV_USERNAME)
         VALUES
         (l_vin_refno            ,l_int_seq              ,1
         ,l_start_evt            ,null                   ,l_max_end +1
         ,null                   ,null                   ,'DATALOAD'
         ,null                   ,null                   ,sysdate
         ,'DATALOAD');
       END IF;
       --
       IF l_void_evt IS NOT NULL 
       THEN
         OPEN  c_int_seqno(l_vin_refno);
         FETCH c_int_seqno  into l_int_seq;
         CLOSE c_int_seqno;
         l_int_seq := nvl(l_int_seq,1);
         --
         INSERT INTO void_events
         (vev_vin_refno          ,vev_int_seqno          ,vev_order_seqno
         ,vev_evt_code           ,vev_off_refno          ,vev_event_date
         ,vev_target_date        ,vev_calc_ext           ,vev_text
         ,vev_dev_vpa_code       ,vev_dev_seqno          ,vev_sys_updated
         ,vev_username)
         VALUES
         (l_vin_refno            ,l_int_seq              ,1
         ,l_void_evt            ,null                   ,l_max_end +1
         ,null                   ,null                   ,'DATALOAD'
         ,null                   ,null                   ,sysdate
         ,'DATALOAD');
       END IF;
     END IF; -- l_create_events = Y
       --
     UPDATE hou_prop_statuses
     SET    hps_comments   = '(VIN REFNO = '||l_vin_refno||')'
     WHERE  hps_pro_refno  = l_pro_refno
       AND  hps_hpc_code   = 'VOID'
       AND  hps_start_date = l_max_end +1; 
     --
   END IF;  -- Auto Generate Flag
--
END IF;
--
-- Finally.......set the pro_sco_Code equal to the latest prop status
--
l_curr_stat := NULL;
OPEN c_curr_stat(l_pro_refno);
FETCH c_curr_stat INTO l_curr_stat;
CLOSE c_curr_stat;
--
UPDATE properties
SET    pro_sco_code = NVL(l_curr_stat,'VOI')
WHERE  pro_refno    = l_pro_refno;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed AND commit after every 1000
--
i1 := i1+1; IF MOD(i1,1000)=0 THEN COMMIT; END If;
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
COMMIT;
--
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOU_PROP_STATUSES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_INSTANCES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_EVENTS');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
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
CURSOR c1(p_batch_id varchar2) is
SELECT
rowid rec_rowid
,lhps_dlb_batch_id
,lhps_dl_seqno
,lhps_DL_load_status
,lhps_pro_propref
,nvl(lhps_start_date,sysdate) lhps_start_date
FROM dl_hem_property_statuses
WHERE lhps_dlb_batch_id    = p_batch_id
AND   lhps_dl_load_status IN ('L','F','O');
--
CURSOR c_pro_refno(p_propref varchar2) is
SELECT pro_refno FROM properties
WHERE pro_propref = p_propref;
--
CURSOR c_min_date(p_refno number) is
SELECT min(hps_start_date),max(hps_start_date),
       min(hps_end_date)  ,max(nvl(hps_end_date,sysdate+100))
FROM hou_prop_statuses
WHERE hps_pro_refno = p_refno;
--
CURSOR c_vgr(p_refno number) is
SELECT vgr_code,vgr_auto_generate
FROM void_groups g, prop_types t, prop_type_values v,properties p
WHERE p.pro_refno         = p_refno
  AND p.pro_hou_ptv_refno = v.ptv_refno
  AND v.ptv_pty_code      = t.pty_code
  AND t.pty_vgr_code      = g.vgr_code;
--
CURSOR c_start_evt is
SELECT evt_code
FROM event_types
WHERE evt_starting_event = 'Y';
--
CURSOR c_end_evt is
SELECT evt_code
FROM event_types
WHERE evt_ending_event    = 'Y';
--
CURSOR c_void_evt is
SELECT evt_code
FROM event_types
WHERE evt_void_event     = 'Y';
--
CURSOR c_vst IS
SELECT vst_code
FROM void_statuses
WHERE vst_new = 'Y';
--
CURSOR c_vst_comp IS
SELECT vst_code
FROM void_statuses
WHERE vst_completed = 'Y'
ORDER BY decode(vst_code,'LET',1,2),vst_code;
--
CURSOR c_vcl_code IS
SELECT frv_code
FROM   first_ref_values
WHERE  frv_frd_domain = 'VOID_CLASS'
AND    frv_default_ind = 'Y'
AND    frv_current_ind = 'Y';
--
CURSOR c_gaps(p_refno NUMBER) IS
SELECT 'Y'
FROM hou_prop_statuses h1, hou_prop_statuses h2
WHERE h1.hps_pro_refno  = p_refno
  AND h2.hps_pro_refno  = p_refno
  AND h2.hps_start_date > h1.hps_end_date +1
  AND not exists         (SELECT null FROM hou_prop_statuses h3
                          WHERE h3.hps_pro_refno  = p_refno
                            AND h3.hps_start_date > h1.hps_end_date
                            AND h3.hps_start_date < h2.hps_start_date);
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HEM_PROPERTY_STATUSES';
cs       INTEGER;
ce	     VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_errors          VARCHAR2(1);
l_error_ind       VARCHAR2(1);
l_gaps_exist      VARCHAR2(1);
l_pro_refno       NUMBER;
l_min_start       DATE;
l_min_end         DATE;
l_max_start       DATE;
l_max_end         DATE;
l_start_evt       VARCHAR2(4);
l_end_evt         VARCHAR2(4);
l_void_evt        VARCHAR2(4);
l_vgr_code        VARCHAR2(4);
l_vgr_ag          VARCHAR2(4);
l_vin_refno       INTEGER;
i1                INTEGER;
l_an_tab          VARCHAR2(1);
l_vst_code        VARCHAR2(10);
l_vst_comp        VARCHAR2(10);
l_int_seq         INTEGER;
l_create_events   VARCHAR2(1);
l_vcl_code        VARCHAR2(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_property_statuses.dataload_validate');
fsc_utils.debug_message( 's_dl_hem_property_statuses.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
--**********************************************************************
-- The Start Events are now set to be created if the void instance is also
-- created this being when the Auto Generate Flag is set to Y in the void
-- group to which the property type is associated
-- IF YOU DO NOT REQUIRE THE VOID EVENTS TO BE CREATED FOR CURRENT VOIDS
-- ONLY THEN SET THE l_create_events BELOW TO N (AJ 03Feb2017)
--
-- THIS MUST MATCH THE SETTING IN THE CREATE and DELETE SECTIONS
--
l_create_events := 'Y';
--**********************************************************************
--
OPEN  c_start_evt;
FETCH c_start_evt into l_start_evt;
CLOSE c_start_evt;
--
OPEN  c_end_evt;
FETCH c_end_evt into l_end_evt;
CLOSE c_end_evt;
--
OPEN  c_void_evt;
FETCH c_void_evt into l_void_evt;
CLOSE c_void_evt;
--
OPEN  c_vst;
FETCH c_vst into l_vst_code;
CLOSE c_vst;
--
OPEN  c_vst_comp;
FETCH c_vst_comp into l_vst_comp;
CLOSE c_vst_comp;
--
OPEN  c_vcl_code;
FETCH c_vcl_code into l_vcl_code;
CLOSE c_vcl_code;
--
FOR p1 IN c1(p_batch_id) LOOP
--
BEGIN
--
l_errors := 'V';
l_error_ind := 'N';
--
cs := p1.lhps_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the pro_refno
--
l_pro_refno  := NULL;
l_gaps_exist := NULL;
 --
OPEN c_pro_refno(p1.lhps_pro_propref);
FETCH c_pro_refno into l_pro_refno;
CLOSE c_pro_refno;
--
IF l_pro_refno IS NULL
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
ELSE
  OPEN  c_gaps(l_pro_refno);
  FETCH c_gaps INTO l_gaps_exist;
  CLOSE c_gaps;
END IF;
--
l_min_start := null;
l_min_end   := null;
l_max_start := null;
l_max_end   := null;
l_vgr_code  := null;
l_vgr_ag    := null;
--
-- get the void group
--
OPEN  c_vgr(l_pro_refno);
FETCH c_vgr into l_vgr_code,l_vgr_ag;
CLOSE c_vgr;
--
-- IF l_vgr_code IS NULL
-- THEN
--  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',358);
-- END IF;
--
-- now look to see if any insertS are needed
--
OPEN c_min_date(l_pro_refno);
FETCH c_min_date INTO l_min_start,l_max_start,l_min_end,l_max_end;
CLOSE c_min_date;
--
-- if there is no existing status
--
IF l_min_start IS NULL
THEN
  --
  IF l_vgr_ag = 'Y'
  THEN
    -- Validate l_vst_code, l_vgr_code, l_vcl_code
    IF l_vst_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',359);
    END IF;
    --
    IF l_vgr_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',358);
    END IF;
    --
    IF l_vcl_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',360);
    END IF;
    --
    --
    IF l_create_events = 'Y'
    THEN
      -- validate l_start_evt, l_void_event
      IF l_start_evt IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',361);
      END IF;
      --
      IF l_void_evt IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',362);
      END IF;
      --
    END IF; -- l_create_events = 'Y'
  END IF;  -- Auto Generate Flag
--
-- now do any gaps
--
-- elseif? if p_gaps_exists
-- validate l_start_evt, l_void_event, l_end_event
--
ELSIF NVL(l_gaps_exist,'N') = 'Y'
THEN
  --
  IF l_vgr_ag = 'Y'
  THEN
    -- Validate l_vst_code, l_vgr_code, l_vcl_code
    IF l_vst_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',359);
    END IF;
    --
    IF l_vgr_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',358);
    END IF;
    --
    IF l_vst_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',360);
    END IF;
    --
    IF l_create_events = 'Y'
    THEN
      -- validate l_start_evt, l_void_event, l_end_event
      IF l_start_evt IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',361);
      END IF;
      --
      IF l_void_evt IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',362);
      END IF;
      --
      IF l_end_evt IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',363);
      END IF;
      --
    END IF; -- l_create_events = 'Y'
  END IF;  -- Auto Generate Flag
--
-- elseif l_max_end < sysdate +99 -- i.e. currently void or about to become void
--
ELSIF l_max_end < sysdate +99 THEN
  --
    IF l_vgr_ag = 'Y'
  THEN
    -- Validate l_vst_code, l_vgr_code, l_vcl_code
    IF l_vst_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',359);
    END IF;
    --
    IF l_vgr_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',358);
    END IF;
    --
    IF l_vst_code IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',360);
    END IF;
    --
    --
    IF l_create_events = 'Y'
    THEN
      -- validate l_start_evt, l_void_event
      IF l_start_evt IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',361);
      END IF;
      --
      IF l_void_evt IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',362);
      END IF;
      --
    END IF; -- l_create_events = 'Y'
  END IF;  -- Auto Generate Flag
--
END IF;
--
-- Now UPDATE the record count AND error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
--
-- keep a count of the rows processed and commit after every 1000
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
i1 := i1+1; IF MOD(i1,1000)=0 THEN COMMIT; END IF;
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
COMMIT;
--
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_validate;
--
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT lhps.rowid lhps_rowid, hps.rowid hps_rowid
      ,hps.hps_start_date   , nvl(hps.hps_end_date,sysdate) hps_end_date
      ,hps.hps_pro_refno    , lhps.lhps_dl_seqno
      ,lhps.rowid   rec_rowid
FROM   hou_prop_statuses         hps ,
       properties                pro ,
       dl_hem_property_statuses lhps
WHERE  hps.hps_pro_refno             = pro.pro_refno
  AND  lhps.lhps_pro_propref         = pro.pro_propref
  AND  hps.hps_hpc_code              = 'VOID'
  AND  lhps.lhps_dlb_batch_id        = p_batch_id
  AND  lhps.lhps_dl_load_status      = 'C';
--
CURSOR c_vev (p_pro_refno number, p_void_evt varchar2,
              p_pst_start date,   p_pst_end date) is
SELECT vev.vev_vin_refno
FROM   void_events    vev,
       void_instances vin
WHERE  vev.vev_vin_refno   = vin.vin_refno
AND    vin.vin_pro_refno   = p_pro_refno
AND    vev.vev_event_date  BETWEEN p_pst_start AND p_pst_end
AND    vev.vev_evt_code    = p_void_evt;
--
CURSOR c_void_evt is
SELECT evt_code
FROM   event_types
WHERE  evt_void_event     = 'Y';
--
CURSOR c_get_hps_vin (p_pro_refno  NUMBER
                     ,p_start_date DATE
                     ,p_hpc_code   VARCHAR2) IS
SELECT TO_NUMBER(SUBSTR(hps_comments,14,INSTR(hps_comments,')',1,1) -14)) hps_vin_refno
FROM   hou_prop_statuses
WHERE  hps_pro_refno = p_pro_refno
AND    hps_start_date = p_start_date
AND    hps_hpc_code = p_hpc_code
AND    hps_comments LIKE '(VIN REFNO = %';
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_PROPERTY_STATUSES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- other variables
i1          INTEGER := 0;
l_pro_refno INTEGER;
l_vin_refno INTEGER;
l_an_tab    VARCHAR2(1);
l_void_evt  VARCHAR2(10);
l_create_events   VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_property_statuses.dataload_DELETE');
fsc_utils.debug_message( 's_dl_hem_property_statuses.dataload_DELETE',3 );
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
OPEN  c_void_evt;
FETCH c_void_evt into l_void_evt;
CLOSE c_void_evt;
--
--**********************************************************************
-- The Start Events are now set to be created if the void instance is also
-- created this being when the Auto Generate Flag is set to Y in the void
-- group to which the property type is associated
-- IF YOU DO NOT REQUIRE THE VOID EVENTS TO BE CREATED FOR CURRENT VOIDS
-- ONLY THEN SET THE l_create_events BELOW TO N (AJ 03Feb2017)
--
-- THIS MUST MATCH THE SETTING IN THE VALIDATE and CREATE SECTIONS
--
l_create_events := 'Y';
--**********************************************************************
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lhps_dl_seqno;
  i1 := i1 +1;
  l_id := p1.rec_rowid;
--
  SAVEPOINT SP1;
--
  l_vin_refno := null;
  IF l_create_events = 'Y'
  THEN
   OPEN c_vev(p1.hps_pro_refno,l_void_evt
             ,p1.hps_start_date,p1.hps_end_date);
   FETCH c_vev into l_vin_refno;
   CLOSE c_vev;
  ELSE
   OPEN c_get_hps_vin(p1.hps_pro_refno
                     ,p1.hps_start_date
                     ,'VOID');
   FETCH c_get_hps_vin into l_vin_refno;
   CLOSE c_get_hps_vin; 
  END IF;  
--
  IF l_vin_refno IS NOT NULL THEN
--
-- delete from actual_stages
--
   DELETE FROM actual_stages WHERE acs_vev_vin_refno = l_vin_refno;
--
-- delete from void events
--
   DELETE FROM void_events WHERE vev_vin_refno = l_vin_refno;
--
-- delete from void_path_hist
--
   DELETE FROM void_path_hist WHERE vph_vin_refno = l_vin_refno;
--
-- delete from void_status_hist
--
   DELETE FROM void_status_hist WHERE vsh_vin_refno = l_vin_refno;
--
-- delete from void_instances
--
   DELETE FROM void_instances WHERE vin_refno = l_vin_refno;
--
  END IF;
--
 DELETE FROM hou_prop_statuses
 WHERE rowid = p1.hps_rowid;
--
-- Finally.......set the pro_sco_code equal to VOI
--
 UPDATE properties
 SET    pro_sco_code = 'VOI'
 WHERE  pro_refno    = p1.hps_pro_refno
   AND  pro_sco_code != 'VOI'
 ;

-- Update Record Status and Process Count
--
 s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
 set_record_status_flag(l_id,'V');
--
--
-- keep a count of the rows processed and commit after every 1000
--
 i1 := i1+1; IF MOD(i1,1000)=0 THEN COMMIT; END IF;
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
--  Some records will not have been processed because they had no voids
-- therefore need to reset these
--

 UPDATE dl_hem_property_statuses
 SET    lhps_dl_load_status = 'V'
 WHERE  lhps_dl_load_status = 'C'
 AND    lhps_dlb_batch_id   = p_batch_id;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOU_PROP_STATUSES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_INSTANCES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('VOID_EVENTS');
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
END s_dl_hem_property_statuses;
/

show errors
