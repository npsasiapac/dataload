CREATE OR REPLACE PACKAGE BODY s_dl_hem_tenancies
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver    WHO    WHEN       WHY
--      1.0           PJD  05/09/2000   Product Dataload
--      1.1           PJD  18/12/2000   Changes to error handling etc.
--      1.2           PJD  20/12/2000   Minor changes to exception handling
--      1.3           MH   07/09/2001   Changes to address format
--      1.4	      SPAU 09/11/2001   Addition of CONTACT_DETAILS	
--      1.5           PJD  07/12/2001   Correction of Tenancy Holding Error Codes
--      1.6           SB   21/05/2002   Check on Address combinations
--                                      for postal add and Fwd Add
--      1.7           PJD  22/05/2002   Added periodic analyze table within create 
--                                      process
--      2.0  5.2.0    PH   08/07/2002   Amendments for 5.2.0 Release. Added new field 
--                                      for termination reason for tenancy/property. 
--                                      Also removed create on addresses as this will 
--                                      be done from parties.
--                                      Still validate on address as they can be 
--                                      supplied as part of this load.
--      2.1  5.2.0    PJD  03/09/2002   Re-introduce the validation on 
--                                      flat/building/street_no
--      2.2  5.2.0    PH   07/10/2002   Added validation, Tenancy Holding end date must 
--                                      be supplied if tenancy end date supplied.
--      2.3  5.3.0    PJD  13/11/2003   Added new validation on overlapping status 
--                                      Also New Validation in Create Proc
--      2.4  5.4.0    PJD  20/11/2003   Move update to record status and process count
--      2.5  5.5.0    PJD  20/05/2004   Removed superfluous RAISE commands from ins_prop
--                                      Changed the order of processing in ins_prop
--      2.6  5.7.0    PH   12/01/2005   Added new field for 570 release 
--                                      ltcy_perm_temp_ind
--      2.7  5.7.0    PJD  29/04/2005   Minor change to validation on ltcy_perm_temp_ind
--                                      (P or T) rather than (Y or N).
--
--      2.8  5.9.0    VRS  16/01/2006   Commented out the INSERT into CONTACT_DETAILS for
--                                      TELEPHONE (CREATE)
--
--      2.9  5.9.0    VRS  16/01/2006   Commented out the DELETE from CONTACT_DETAILS for
--                                      tenancy (DELETE)
--      3.0  5.10.0   PH   08/05/2006   Removed all references to Addresses as these should
--                                      be loaded using Addresses Dataload
--      3.1  5.13.0   PH   06/02/2008   Amended validate on Termination reason
--                                      (HDL079) included ltcy_expected_end_date.
--      3.2  5.13.0   PH   06-FEB-2008  Now includes its own 
--                                      set_record_status_flag procedure.
--      3.3  5.13.0   PH   05-SEP-2008  Added validate on correspond name
--      3.4  6.13     AJ   26-APR-2016  ORDER BY ltcy_act_start_date added to main cursor
--                                      C1 in create process as per PD
--      4.0  6.18     PL   31-MAY-2019  Use CUX rather than CUR.
-- ***********************************************************************
--
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hem_tenancies
  SET ltcy_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_tenancies');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
procedure ins_prop (p_propref   IN VARCHAR2,
                    p_start     IN DATE,
                    p_end       IN DATE,
                    p_tcy_refno IN INTEGER,
                    p_rec_status IN OUT VARCHAR2,
                    p_ttr_code  IN VARCHAR2) IS
--
CURSOR c_pro_refno (p_propref VARCHAR2) IS
SELECT pro_refno
FROM properties
WHERE pro_propref = p_propref;
--
CURSOR c_check_hps (p_pro_refno INTEGER
                   ,p_start     DATE
                   ,p_end       DATE) IS
SELECT 'x'
FROM hou_prop_statuses
WHERE hps_pro_refno = p_pro_Refno
AND  ( p_start BETWEEN hps_start_date and NVL(hps_end_date,p_start)
      OR
       p_end -1   BETWEEN hps_start_date and NVL(hps_end_date,sysdate));
--
l_pro_refno             INTEGER;
l_debug                 VARCHAR2(300);
l_exists                VARCHAR2(1);
l_overlapping_status    VARCHAR2(4);
--
e_dup_hps  EXCEPTION;
--
BEGIN
--
  OPEN c_pro_refno(p_propref);
  FETCH c_pro_refno INTO l_pro_refno;
  CLOSE c_pro_refno;
  --
  SAVEPOINT SP2;
  --
   --
   l_overlapping_status := NULL; 
   --
   l_overlapping_status :=
   s_dl_hem_utils.hps_hpc_code_for_date(p_propref,
                                        p_start);
   IF l_overlapping_status is not null
   THEN
     ROLLBACK TO SP2;
     RAISE e_dup_hps;
   ELSE
   --
   --
   -- INSERT INTO TENANCY_HOLDINGS
   --
   INSERT INTO tenancy_holdings
   (tho_tcy_refno,
    tho_pro_refno,
    tho_start_date,
    tho_end_date,
    tho_rac_accno,
    tho_created_by,
    tho_created_date,
    tho_hrv_ttr_code)
    VALUES
   (p_tcy_refno,
    l_pro_refno,
    p_start,
    p_end,
    NULL,
    'DATALOAD',
    sysdate,
    p_ttr_code);
    --
    -- INSERT INTO PROP_STATUSES
    --
    BEGIN
    --
      INSERT INTO hou_prop_statuses
      (hps_pro_refno,
       hps_hpc_code,
       hps_hpc_type,
       hps_start_date,
       hps_END_date)
       VALUES
      (l_pro_refno,
      'OCCP',
      'O',
      p_start,
      p_end);
      --
    EXCEPTION
    WHEN OTHERS
    THEN
    p_rec_status := 'P';
    --
    END;
    -- 
  END IF;
  --
  --
  EXCEPTION
    WHEN e_dup_hps THEN
    p_rec_status := 'D';
   --
   WHEN OTHERS THEN
   ROLLBACK TO SP2;
   p_rec_status := 'O';
 END ins_prop;
--
--
--
PROCEDURE dataload_create
      (p_batch_id          IN VARCHAR2,
       p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid        ,
ltcy_dlb_batch_id      ,
ltcy_dl_seqno          ,
ltcy_dl_load_status    ,
LTCY_ALT_REF           ,
LTCY_TTY_CODE          ,
LTCY_ACT_START_DATE    ,
LTCY_CORRESPOND_NAME   ,
LTCY_HRV_TTYP_CODE     ,
LTCY_HRV_TSO_CODE      ,
LTCY_ACT_END_DATE      ,
LTCY_NOTICE_GIVEN_DATE ,
LTCY_NOTICE_REC_DATE   ,
LTCY_REVIEW_DATE       ,
LTCY_EXPECTED_END_DATE ,
LTCY_RTB_RECEIVED_DATE ,
LTCY_RTB_ADMITTED_DATE ,
LTCY_RTB_HELD_DATE     ,
LTCY_RTB_WITHDRAWN_DATE,
LTCY_RTB_APP_EXPECTED_END_DATE,
LTCY_HRV_TST_CODE      ,
LTCY_HRV_TPT_CODE      ,
LTCY_HRV_TTR_CODE      ,
LTCY_HRV_TSC_CODE      ,
LTCY_HRV_TNR_CODE      ,
LTCY_HRV_TSE_CODE      ,
LTCY_HRV_RHR_CODE      ,
LTCY_HRV_RWR_CODE      ,
LTCY_RTB_APP_REFERENCE ,
LTCY_THO_PROPREF1      ,
LTCY_THO_START_DATE1   ,
LTCY_THO_END_DATE1     ,
LTCY_THO_HRV_TTR_CODE1 ,
LTCY_THO_PROPREF2      ,
LTCY_THO_START_DATE2   ,
LTCY_THO_END_DATE2     ,
LTCY_THO_HRV_TTR_CODE2 ,
LTCY_THO_PROPREF3      ,
LTCY_THO_START_DATE3   ,
LTCY_THO_END_DATE3     ,
LTCY_THO_HRV_TTR_CODE3 ,
LTCY_THO_PROPREF4      ,
LTCY_THO_START_DATE4   ,
LTCY_THO_END_DATE4     ,
LTCY_THO_HRV_TTR_CODE4 ,
LTCY_THO_PROPREF5      ,
LTCY_THO_START_DATE5   ,
LTCY_THO_END_DATE5     ,
LTCY_THO_HRV_TTR_CODE5 ,
LTCY_THO_PROPREF6      ,
LTCY_THO_START_DATE6   ,
LTCY_THO_END_DATE6     ,
LTCY_THO_HRV_TTR_CODE6 ,
LTCY_PHONE             ,
LTCY_HRV_FTC_CODE      ,
LTCY_PROPOSED_END_DATE ,
nvl(LTCY_PERM_TEMP_IND, 'P') ltcy_perm_temp_ind
FROM dl_hem_tenancies
WHERE ltcy_dlb_batch_id     = p_batch_id
AND   ltcy_dl_load_status   = 'V'
ORDER BY ltcy_act_start_date;
--
--
CURSOR c_tcy_refno IS
SELECT tcy_refno_seq.nextval FROM dual;
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_TENANCIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_sco_code          VARCHAR2(3);
l_pro_refno         NUMBER;
l_tcy_refno         INTEGER;
i                   INTEGER:=0;
ai                  INTEGER:=100;
l_rec_status        VARCHAR2(1);
l_an_tab            VARCHAR2(1);
l_debug             VARCHAR2(300);
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_tenancies.dataload_create');
fsc_utils.debug_message('s_dl_hem_tenancies.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
SAVEPOINT SP1;
--
cs := p1.ltcy_dl_seqno;
l_rec_status := 'V';
l_id := p1.rec_rowid;
--
-- Get the tcy_refno
--
l_tcy_refno := null;
OPEN c_tcy_refno;
FETCH c_tcy_refno INTO l_tcy_refno;
CLOSE c_tcy_refno;
--
-- SET THE SCO CODE AND INSERT INTO TENANCIES
--
IF p1.ltcy_act_end_date IS NULL THEN
   l_sco_code := 'CUX';
ELSE
   l_sco_code := 'FOR';
END IF;
--
INSERT INTO tenancies
(TCY_REFNO
,TCY_ALT_REF
,TCY_TTY_CODE
,TCY_ACT_START_DATE
,TCY_CORRESPOND_NAME
,TCY_HRV_TTYP_CODE
,TCY_HRV_TSO_CODE
,TCY_CREATED_BY
,TCY_CREATED_DATE
,TCY_SCO_CODE
,TCY_REUSABLE_REFNO
,TCY_ACT_END_DATE
,TCY_PROXY_NAME
,TCY_NOTICE_GIVEN_DATE
,TCY_NOTICE_REC_DATE
,TCY_REVIEW_DATE
,TCY_EXPECTED_END_DATE
,TCY_RTB_RECEIVED_DATE
,TCY_RTB_ADMITTED_DATE
,TCY_RTB_HELD_DATE
,TCY_RTB_WITHDRAWN_DATE
,TCY_ENDED_DATE
,TCY_ENDED_BY
,TCY_HRV_TST_CODE
,TCY_HRV_TPT_CODE
,TCY_HRV_TTR_CODE
,TCY_HRV_TSC_CODE
,TCY_HRV_TNR_CODE
,TCY_HRV_TSE_CODE
,TCY_HRV_RHR_CODE
,TCY_HRV_RWR_CODE
,TCY_APP_REFNO
,TCY_RTB_APP_REFERENCE
,TCY_RTB_APP_EXPECTED_END_DATE
,TCY_HRV_FTC_CODE
,TCY_PROPOSED_END_DATE
,TCY_PERM_TEMP_IND
)
VALUES
(
l_tcy_refno                        ,
p1.LTCY_ALT_REF                    ,
p1.LTCY_TTY_CODE                   ,
p1.LTCY_ACT_START_DATE             ,
p1.LTCY_CORRESPOND_NAME            ,
p1.LTCY_HRV_TTYP_CODE              ,
p1.LTCY_HRV_TSO_CODE               ,
'DATALOAD'                         ,
sysdate                            ,
l_sco_code                         ,
reusable_refno_seq.nextval         ,
p1.LTCY_ACT_END_DATE               ,
null                               ,
p1.LTCY_NOTICE_GIVEN_DATE          ,
p1.LTCY_NOTICE_REC_DATE            ,
p1.LTCY_REVIEW_DATE                ,
p1.LTCY_EXPECTED_END_DATE          ,
p1.LTCY_RTB_RECEIVED_DATE          ,
p1.LTCY_RTB_ADMITTED_DATE          ,
p1.LTCY_RTB_HELD_DATE              ,
p1.LTCY_RTB_WITHDRAWN_DATE         ,
null,
null,
p1.LTCY_HRV_TST_CODE               ,
p1.LTCY_HRV_TPT_CODE               ,
p1.LTCY_HRV_TTR_CODE               ,
p1.LTCY_HRV_TSC_CODE               ,
p1.LTCY_HRV_TNR_CODE               ,
p1.LTCY_HRV_TSE_CODE               ,
p1.LTCY_HRV_RHR_CODE               ,
p1.LTCY_HRV_RWR_CODE               ,
null                               ,
p1.LTCY_RTB_APP_REFERENCE          ,
p1.LTCY_RTB_APP_EXPECTED_END_DATE  ,
p1.LTCY_HRV_FTC_CODE               ,
p1.LTCY_PROPOSED_END_DATE          ,
p1.LTCY_PERM_TEMP_IND              );
--
--
-- INSERT INTO TCY_HOLDINGS AND PROP STATUSES
--
ins_prop (p1.ltcy_tho_propref1,
          nvl(p1.ltcy_tho_start_date1,p1.ltcy_act_start_date),
          nvl(p1.ltcy_tho_end_date1,p1.ltcy_act_end_date),
          l_tcy_refno,
          l_rec_status,
          p1.ltcy_tho_hrv_ttr_code1);
--
IF (l_rec_status = 'V' and p1.ltcy_tho_propref2 IS NOT NULL) THEN
  ins_prop (p1.ltcy_tho_propref2,
          nvl(p1.ltcy_tho_start_date2,p1.ltcy_act_start_date),
          nvl(p1.ltcy_tho_end_date2,p1.ltcy_act_end_date),
            l_tcy_refno,
            l_rec_status,
            p1.ltcy_tho_hrv_ttr_code2);
END IF;
--
IF (l_rec_status = 'V' and p1.ltcy_tho_propref3 IS NOT NULL) THEN
  ins_prop (p1.ltcy_tho_propref3,
          nvl(p1.ltcy_tho_start_date3,p1.ltcy_act_start_date),
          nvl(p1.ltcy_tho_end_date3,p1.ltcy_act_end_date),
            l_tcy_refno,
            l_rec_status,
            p1.ltcy_tho_hrv_ttr_code3);
END IF;
--
IF (l_rec_status = 'V' and p1.ltcy_tho_propref4 IS NOT NULL) THEN
ins_prop (p1.ltcy_tho_propref4,
          nvl(p1.ltcy_tho_start_date4,p1.ltcy_act_start_date),
          nvl(p1.ltcy_tho_end_date4,p1.ltcy_act_end_date),
          l_tcy_refno,
          l_rec_status,
          p1.ltcy_tho_hrv_ttr_code4);
END IF;
--
IF (l_rec_status = 'V' and p1.ltcy_tho_propref5 IS NOT NULL) THEN
ins_prop (p1.ltcy_tho_propref5,
          nvl(p1.ltcy_tho_start_date5,p1.ltcy_act_start_date),
          nvl(p1.ltcy_tho_end_date5,p1.ltcy_act_end_date),
          l_tcy_refno,
          l_rec_status,
          p1.ltcy_tho_hrv_ttr_code5);
END IF;
--
IF (l_rec_status = 'V' and p1.ltcy_tho_propref6 IS NOT NULL) THEN
ins_prop (p1.ltcy_tho_propref6,
          nvl(p1.ltcy_tho_start_date6,p1.ltcy_act_start_date),
          nvl(p1.ltcy_tho_end_date6,p1.ltcy_act_end_date),
          l_tcy_refno,
          l_rec_status,
          p1.ltcy_tho_hrv_ttr_code6);
END IF;
--
--
-- Do the insert into contact_details
--
/*
IF p1.ltcy_phone IS NOT NULL
  THEN
  INSERT INTO contact_details
 (CDE_REFNO 
 ,CDE_START_DATE
 ,CDE_CREATED_DATE
 ,CDE_CREATED_BY
 ,CDE_CONTACT_VALUE
 ,CDE_FRV_CME_CODE
 ,CDE_CONTACT_NAME
 ,CDE_END_DATE
 ,CDE_PRO_REFNO
 ,CDE_AUN_CODE
 ,CDE_PAR_REFNO
 ,CDE_TCY_REFNO
 ,CDE_BDE_REFNO
 ,CDE_COS_CODE
 ,CDE_CSE_CONTACT
 ,CDE_SRQ_NO )
 values
 (
  cde_refno.nextval
 ,p1.ltcy_act_start_date
 ,trunc(sysdate)
 ,'DATALOAD'
 ,p1.ltcy_phone
 ,'TELEPHONE'
 ,null
 ,null
 ,null
 ,null
 ,null
 ,l_tcy_refno
 ,null
 ,null
 ,null
 ,null
 );
END IF;
*/
--
IF l_rec_status = 'V' THEN
   s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
   set_record_status_flag(l_id,'C');
ELSIF l_rec_status = 'D' THEN  -- duplicate hps status found
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',423);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
ELSIF l_rec_status = 'O' THEN  -- error in prop_ins proc    
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',059);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
ELSIF l_rec_status = 'P' THEN  -- error in prop_ins proc    
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',028);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
ELSE
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
END IF;
--
-- keep a count of the rows processed and commit after every 1000
-- also analyze address tables from time to time
--
i := i+1; 
IF MOD(i,1000)=0 
THEN 
  COMMIT;
END IF;
--
 EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
END;
 END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCIES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_HOLDINGS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOU_PROP_STATUSES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_USAGES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
--
fsc_utils.proc_END;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_create;
--
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid               ,
ltcy_dlb_batch_id             ,
ltcy_dl_seqno                 ,
LTCY_ALT_REF                  ,
LTCY_TTY_CODE                 ,
LTCY_ACT_START_DATE           ,
LTCY_CORRESPOND_NAME          ,
LTCY_HRV_TTYP_CODE            ,
LTCY_HRV_TSO_CODE             ,
LTCY_ACT_END_DATE             ,
LTCY_NOTICE_GIVEN_DATE        ,
LTCY_NOTICE_REC_DATE          ,
LTCY_REVIEW_DATE              ,
LTCY_EXPECTED_END_DATE        ,
LTCY_RTB_RECEIVED_DATE        ,
LTCY_RTB_ADMITTED_DATE        ,
LTCY_RTB_HELD_DATE            ,
LTCY_RTB_WITHDRAWN_DATE       ,
LTCY_RTB_APP_EXPECTED_END_DATE,
LTCY_HRV_TST_CODE             ,
LTCY_HRV_TPT_CODE             ,
LTCY_HRV_TTR_CODE             ,
LTCY_HRV_TSC_CODE             ,
LTCY_HRV_TNR_CODE             ,
LTCY_HRV_TSE_CODE             ,
LTCY_HRV_RHR_CODE             ,
LTCY_HRV_RWR_CODE             ,
LTCY_RTB_APP_REFERENCE        ,
LTCY_THO_PROPREF1             ,
LTCY_THO_START_DATE1          ,
LTCY_THO_END_DATE1            ,
LTCY_THO_HRV_TTR_CODE1        ,
LTCY_THO_PROPREF2             ,
LTCY_THO_START_DATE2          ,
LTCY_THO_END_DATE2            ,
LTCY_THO_HRV_TTR_CODE2        ,
LTCY_THO_PROPREF3             ,
LTCY_THO_START_DATE3          ,
LTCY_THO_END_DATE3            ,
LTCY_THO_HRV_TTR_CODE3        ,
LTCY_THO_PROPREF4             ,
LTCY_THO_START_DATE4          ,
LTCY_THO_END_DATE4            ,
LTCY_THO_HRV_TTR_CODE4        ,
LTCY_THO_PROPREF5             ,
LTCY_THO_START_DATE5          ,
LTCY_THO_END_DATE5            ,
LTCY_THO_HRV_TTR_CODE5        ,
LTCY_THO_PROPREF6             ,
LTCY_THO_START_DATE6          ,
LTCY_THO_END_DATE6            ,
LTCY_THO_HRV_TTR_CODE6        ,
LTCY_PHONE                    ,
LTCY_HRV_FTC_CODE             ,
LTCY_PROPOSED_END_DATE        ,
LTCY_PERM_TEMP_IND
FROM  dl_hem_tenancies
WHERE ltcy_dlb_batch_id    = p_batch_id
AND   ltcy_dl_load_status IN ('L','F','O');
--
-- CURSOR to check the property exists on PROPERTIES
--
CURSOR c_chk_tcy_alt_ref(p_tcy_alt_ref VARCHAR2) IS
  SELECT 'x'
  FROM tenancies
  WHERE tcy_alt_ref = p_tcy_alt_ref;
--
CURSOR c_chk_diff_tcy_prop(p_ltcy_alt_ref VARCHAR2, p_ltcy_tho_propref VARCHAR2,
                           p_ltcy_tho_start_date date) IS
  SELECT 'x'
  FROM dl_hem_tenancies
  WHERE ltcy_alt_ref      != p_ltcy_alt_ref
    AND ltcy_tho_propref1  = p_ltcy_tho_propref
    AND trunc(ltcy_tho_start_date1) = trunc(p_ltcy_tho_start_date);
--
-- CURSOR to check for tenancy type
--
CURSOR c_tty(p_tcy_type VARCHAR2) IS
  SELECT 'x'
  FROM   tenancy_types
  WHERE  tty_code         = p_tcy_type;
--
-- constants FOR summary reporting
--
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HEM_TENANCIES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
l_overlapping_tcy_refno INTEGER;
l_overlapping_status    VARCHAR2(4);
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER  (10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
l_dummy          VARCHAR2(10);
i                INTEGER :=0;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_tenancies.dataload_validate');
fsc_utils.debug_message( 's_dl_hem_tenancies.dataload_validate',3 );
--
cb := p_batch_id;
cd := p_DATE;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.ltcy_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
-- Check the tenancy has not already been loaded onto TENANCIES
--
OPEN c_chk_tcy_alt_ref(p1.ltcy_alt_ref);
FETCH c_chk_tcy_alt_ref INTO l_exists;
IF c_chk_tcy_alt_ref%found THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',059);
END IF;
CLOSE c_chk_tcy_alt_ref;
--
-- Check the tenancy type code
--
OPEN c_tty(p1.ltcy_tty_code);
FETCH c_tty INTO l_exists;
IF c_tty%notfound THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',062);
END IF;
CLOSE c_tty;
--
-- Check reference VALUES
--
-- Tenure type
--
IF (NOT s_dl_hem_utils.exists_frv('TENURE',p1.ltcy_hrv_ttyp_code,'N'))
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',060);
END IF;
--
-- Tenancy source
--
IF (NOT s_dl_hem_utils.exists_frv('TEN_SRCE',p1.ltcy_hrv_tso_code,'N'))
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',061);
END IF;
--
--
-- Tenancy termination code
--
IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM',p1.ltcy_hrv_ttr_code,'Y'))
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',063);
END IF;
--
-- Either none or both fields must be completed
--
IF ((p1.ltcy_hrv_ttr_code IS NULL AND p1.ltcy_act_end_date IS NOT NULL)
    or
    (p1.ltcy_hrv_ttr_code IS NOT NULL AND p1.ltcy_act_end_date IS NULL
                                      AND p1.ltcy_expected_end_date IS NULL))
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',079);
END IF;
--
-- Notice to quit reason
--
IF (NOT s_dl_hem_utils.exists_frv('NOTICE',p1.ltcy_hrv_tnr_code,'Y'))
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',064);
END IF;
--
--  IF (p1.ltcy_hrv_ntq_code IS NULL)
--  THEN
--    error := hdl_utils.f_val_err(tab,row,'HDL052');
--  END IF;
--
-- Right to buy deferred reason
--
IF (NOT s_dl_hem_utils.exists_frv('RTB_DEF',p1.ltcy_hrv_rhr_code,'Y'))
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',065);
END IF;
--
IF ((p1.ltcy_rtb_held_date IS NOT NULL AND p1.ltcy_hrv_rhr_code IS NULL)
     or
    (p1.ltcy_rtb_held_date IS NULL AND p1.ltcy_hrv_rhr_code IS NOT NULL))
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',053);
END IF;
--
--
-- Right to buy cancelled reason
--
IF (not s_dl_hem_utils.exists_frv('RTB_CANC',p1.ltcy_hrv_rwr_code,'Y'))
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',066);
END IF;
--
IF ((p1.ltcy_rtb_withdrawn_date IS NOT NULL AND p1.ltcy_hrv_rwr_code IS NULL)
   or
   (p1.ltcy_rtb_withdrawn_date IS NULL AND p1.ltcy_hrv_rwr_code IS NOT NULL))
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',054);
END IF;
--
-- Tenancy status
--
IF (not s_dl_hem_utils.exists_frv('TCY_STAT',p1.ltcy_hrv_tst_code,'Y'))
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',067);
END IF;
--
-- Tenancy Termination Condition code
--
IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM_COND',p1.ltcy_hrv_ftc_code,'Y'))
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',405);
END IF;
--
--
-- Check the property ref and tcy_holding info
--
IF (not s_dl_hem_utils.exists_propref(p1.ltcy_tho_propref1))
THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
END IF;
--
l_overlapping_tcy_refno :=
s_dl_hem_utils.tho_tcy_refno_for_date(p1.ltcy_tho_propref1,
                                   p1.ltcy_tho_start_date1,
                                   p1.ltcy_tho_END_date1);
IF l_overlapping_tcy_refno is not null
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',097);
   l_overlapping_tcy_refno := null;
END IF;
IF p1.ltcy_tho_hrv_ttr_code1 is not null
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM',p1.ltcy_tho_hrv_ttr_code1,'Y'))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',406);
    END IF;
END IF;
--
l_overlapping_status :=
s_dl_hem_utils.hps_hpc_code_for_date(p1.ltcy_tho_propref1,
                                     p1.ltcy_tho_start_date1);
IF l_overlapping_status is not null
   THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',417);
   l_overlapping_status := null;
END IF;
--
--
IF p1.ltcy_tho_propref2 IS NOT NULL
  THEN
  IF (not s_dl_hem_utils.exists_propref(p1.ltcy_tho_propref2))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',869);
  END IF;
--
  l_overlapping_tcy_refno :=
  s_dl_hem_utils.tho_tcy_refno_for_date(p1.ltcy_tho_propref2,
                                   p1.ltcy_tho_start_date2,
                                   p1.ltcy_tho_END_date2);
  IF l_overlapping_tcy_refno is not null
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',874);
    l_overlapping_tcy_refno := null;
  END IF;
  --
  l_overlapping_status :=
  s_dl_hem_utils.hps_hpc_code_for_date(p1.ltcy_tho_propref2,
                                       p1.ltcy_tho_start_date2);
  IF l_overlapping_status is not null
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',418);
    l_overlapping_status := null;
  END IF;
  --
END IF;
IF p1.ltcy_tho_hrv_ttr_code2 is not null
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM',p1.ltcy_tho_hrv_ttr_code2,'Y'))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',407);
    END IF;
END IF;
--
IF p1.ltcy_tho_propref3 IS NOT NULL
  THEN
  IF (not s_dl_hem_utils.exists_propref(p1.ltcy_tho_propref3))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',870);
  END IF;
--
  l_overlapping_tcy_refno :=
  s_dl_hem_utils.tho_tcy_refno_for_date(p1.ltcy_tho_propref3,
                                   p1.ltcy_tho_start_date3,
                                   p1.ltcy_tho_END_date3);
  IF l_overlapping_tcy_refno is not null
    THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',875);
    l_overlapping_tcy_refno := null;
  END IF;
  --
  l_overlapping_status :=
  s_dl_hem_utils.hps_hpc_code_for_date(p1.ltcy_tho_propref3,
                                       p1.ltcy_tho_start_date3);
  IF l_overlapping_status is not null
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',419);
    l_overlapping_status := null;
  END IF;
  --
END IF;
IF p1.ltcy_tho_hrv_ttr_code3 is not null
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM',p1.ltcy_tho_hrv_ttr_code3,'Y'))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',408);
    END IF;
END IF;
--
IF p1.ltcy_tho_propref4 IS NOT NULL
  THEN
  IF (not s_dl_hem_utils.exists_propref(p1.ltcy_tho_propref4))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',871);
  END IF;
--
  l_overlapping_tcy_refno :=
  s_dl_hem_utils.tho_tcy_refno_for_date(p1.ltcy_tho_propref4,
                                   p1.ltcy_tho_start_date4,
                                   p1.ltcy_tho_end_date4);
  IF l_overlapping_tcy_refno is not null
    THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',876);
    l_overlapping_tcy_refno := null;
  END IF;
  --
  l_overlapping_status :=
  s_dl_hem_utils.hps_hpc_code_for_date(p1.ltcy_tho_propref4,
                                       p1.ltcy_tho_start_date4);
  IF l_overlapping_status is not null
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',420);
    l_overlapping_status := null;
  END IF;
  --
END IF;
IF p1.ltcy_tho_hrv_ttr_code4 is not null
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM',p1.ltcy_tho_hrv_ttr_code4,'Y'))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',409);
    END IF;
END IF;
--
IF p1.ltcy_tho_propref5 IS NOT NULL
  THEN
  IF (not s_dl_hem_utils.exists_propref(p1.ltcy_tho_propref5))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',872);
  END IF;
--
  l_overlapping_tcy_refno :=
  s_dl_hem_utils.tho_tcy_refno_for_date(p1.ltcy_tho_propref5,
                                   p1.ltcy_tho_start_date5,
                                   p1.ltcy_tho_END_date5);
  IF l_overlapping_tcy_refno is not null
    THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',877);
           l_overlapping_tcy_refno := null;
  END IF;
  --
  l_overlapping_status :=
  s_dl_hem_utils.hps_hpc_code_for_date(p1.ltcy_tho_propref5,
                                       p1.ltcy_tho_start_date5);
  IF l_overlapping_status is not null
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',421);
    l_overlapping_status := null;
  END IF;
  --
END IF;
IF p1.ltcy_tho_hrv_ttr_code5 is not null
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM',p1.ltcy_tho_hrv_ttr_code5,'Y'))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',410);
    END IF;
END IF;
--
IF p1.ltcy_tho_propref6 IS NOT NULL
  THEN
  IF (not s_dl_hem_utils.exists_propref(p1.ltcy_tho_propref6))
  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',873);
  END IF;
--
  l_overlapping_tcy_refno :=
  s_dl_hem_utils.tho_tcy_refno_for_date(p1.ltcy_tho_propref6,
                                   p1.ltcy_tho_start_date6,
                                   p1.ltcy_tho_END_date6);
  IF l_overlapping_tcy_refno is not null
    THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',878);
    l_overlapping_tcy_refno := null;
  END IF;
END IF;
IF p1.ltcy_tho_hrv_ttr_code6 is not null
   THEN
    IF (NOT s_dl_hem_utils.exists_frv('TCY_TERM',p1.ltcy_tho_hrv_ttr_code6,'Y'))
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',411);
    END IF;
  --
  l_overlapping_status :=
  s_dl_hem_utils.hps_hpc_code_for_date(p1.ltcy_tho_propref6,
                                       p1.ltcy_tho_start_date6);
  IF l_overlapping_status is not null
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',422);
    l_overlapping_status := null;
  END IF;
  --
END IF;
--
-- Check that IF the tenancy has been terminated a reason has been
-- supplied
--
IF (p1.ltcy_act_END_date IS NOT NULL AND
    p1.ltcy_hrv_ttr_code IS NULL)
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',073);
END IF;
--
-- Check that IF notice to quit has been given, a reason has been
-- supplied
--
IF (p1.ltcy_notice_given_date IS NOT NULL AND
    p1.ltcy_hrv_tnr_code      IS NULL)
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',074);
END IF;
--
-- Check that for a secure tenancy, the start day matches the
-- rent week start held on HOU_SYS_PARAMS (RWSTART)
--
-- IF (p_ltcy_tcy_hrv_tenure = 'SECURE')
-- THEN
-- IF (rtrim(to_char(p_ltcy_tcy_act_start,'DAY')) !=
--   hsp$.f_v('HSP_VALUE','RWSTART','SYSTEM'))
--   THEN
--     error := hdl_utils.f_val_err(tab,row,'HDL075');
--   END IF;
-- END IF;
--
-- Check that the tenancy start date has been supplied
--
IF  (p1.ltcy_act_start_date IS NULL)
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',854);
END IF;
--
-- Check that the tenancy end date is not earlier than the tenancy
-- start date
--
IF  (nvl(p1.ltcy_act_end_date,p1.ltcy_act_start_date) <
                             p1.ltcy_act_start_date)
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',076);
END IF;
--
-- Check that various other dates are not earlier than the tenancy
-- start date
--
IF  (nvl(p1.ltcy_notice_given_date,p1.ltcy_act_start_date) <
                                  p1.ltcy_act_start_date)
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',055);
END IF;
--
IF  (nvl(p1.ltcy_expected_END_date,p1.ltcy_act_start_date) <
                                  p1.ltcy_act_start_date)
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',056);
END IF;
--
IF  (nvl(p1.ltcy_notice_rec_date,p1.ltcy_act_start_date) <
                                p1.ltcy_act_start_date)
  THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',057);
END IF;
--
-- Check that if a tenancy end date has been supplied the
-- tenancy holding end date has also been supplied
--
-- removed at ECHG on 13/09/04  PJD
/* IF (p1.ltcy_act_END_date IS NOT NULL AND
    p1.ltcy_tho_END_date1 IS NULL)
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',098);
END IF;
*/
--
-- Check the Perm Temp Indicator
--
  IF nvl(p1.ltcy_perm_temp_ind, 'P') not in ('P', 'T')
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',099);
  END IF;
--
-- Check Correspond name has been supplied
--
  IF p1.ltcy_correspond_name is NULL
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',541);
  END IF;
--
--
-- Now update the record count and error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
-- Update Record Status and Process Count
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
END;
--
END LOOP;
--
fsc_utils.proc_END;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ltcy_dlb_batch_id
,ltcy_dl_seqno
,ltcy_alt_ref
FROM  dl_hem_tenancies
WHERE ltcy_dlb_batch_id   = p_batch_id
AND   ltcy_dl_load_status = 'C';
--
cursor c2(p_tcy_refno number) is
select tho_pro_refno,tho_start_date
from tenancy_holdings
where tho_tcy_refno = p_tcy_refno;
--
i INTEGER := 0;
l_tcy_refno INTEGER;
l_an_tab VARCHAR2(1);
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_TENANCIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_tenancies.dataload_delete');
fsc_utils.debug_message( 's_dl_hem_tenancies.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.ltcy_dl_seqno;
l_id := p1.rec_rowid;
i  := i +1;
--
-- Get the tcy_refno
--
l_tcy_refno := null;
l_tcy_refno:=s_dl_hem_utils.tcy_refno_for_alt_ref(p1.ltcy_alt_ref);
--
--
--
FOR p2 IN c2(l_tcy_refno) LOOP
--
  DELETE FROM hou_prop_statuses
  WHERE  hps_pro_refno  = p2.tho_pro_refno
  AND    hps_start_date = p2.tho_start_date
  AND    hps_hpc_type   = 'O';
--
END LOOP;
--
DELETE FROM  tenancy_holdings
WHERE tho_tcy_refno = l_tcy_refno;
--
/*
DELETE FROM contact_details
WHERE cde_tcy_refno = l_tcy_refno;
*/
--
DELETE FROM  tenancies
WHERE tcy_refno     = l_tcy_refno;
--
-- Update record status and process count
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCIES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANCY_HOLDINGS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOU_PROP_STATUSES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
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
END s_dl_hem_tenancies;
/

