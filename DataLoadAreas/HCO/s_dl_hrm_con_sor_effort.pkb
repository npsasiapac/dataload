CREATE OR REPLACE PACKAGE BODY s_dl_hrm_con_sor_effort
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL    
--  VERSION  DB VER   WHO          WHEN         WHY
--      1.0           P Bouchier   04/02/2005   Dataload
--      1.1  5.10.0   PH           26/07/2006   Added in lcsef_refno to the .ctl
--                                              and the table to make delete easier
--                                              Also corrected delete process. 
--      1.2  5.10.0   PH           16/08/06     Removed validate on created by/date
--
--      2.0  5.13.0   PH   	    06-FEB-2008	Now includes its own 
--                                        		set_record_status_flag procedure.
--      2.1  5.15.1   PH   	    07-APR-2009 	Amended validate to use cursor
--                                               c_cspg_for_ppc rather than packaged
--                                               procedure. 
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
  UPDATE dl_hrm_con_sor_effort
  SET lcsef_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_con_sor_effort');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
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
,lcsef_dlb_batch_id
,lcsef_dl_seqno
,lcsef_dl_load_status
,lcsef_refno
,lcsef_ppc_ppp_ppg_code
,lcsef_ppc_ppp_wpr_code
,lcsef_ppc_ppp_start_date
,lcsef_ppc_cos_code
,lcsef_start_date
,lcsef_end_date
,lcsef_csp_sor_code
,lcsef_estimated_effort          
,lcsef_estimated_effort_unit
,lcsef_effort_driven_ind      
,lcsef_max_operatives       
,lcsef_next_job_delay_time       
,lcsef_nxt_job_del_time_unit
,lcsef_min_operatives
,lcsef_cspg_refno
FROM dl_hrm_con_sor_effort
WHERE lcsef_dlb_batch_id    = p_batch_id
AND   lcsef_dl_load_status = 'V';
--
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SOR_EFFORT';
cs       INTEGER;
ce	 VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_an_tab    VARCHAR2(1);
i           integer := 0;
l_sor_refno con_sor_effort.csef_refno%TYPE;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_con_sor_effort.dataload_create');
fsc_utils.debug_message( 's_dl_hrm_con_sor_effort.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
--     DBMS_OUTPUT.PUT_LINE('1');
--     SELECT csef_refno_seq.nextval INTO l_sor_refno FROM DUAL;
--
     cs := p1.lcsef_dl_seqno;
     l_id := p1.rec_rowid;
--
--          DBMS_OUTPUT.PUT_LINE('2');
--
SAVEPOINT SP1;
--
-- Insert into con_site_prices
--
     INSERT into con_sor_effort
                 (csef_refno,
                  csef_csp_cspg_refno,                    
                  csef_csp_sor_code  ,                    
                  csef_start_date    ,                    
                  csef_estimated_effort,                  
                  csef_estimated_effort_unit,             
                  csef_effort_driven_ind,                 
                  csef_created_by       ,                 
                  csef_created_date     ,                 
                  csef_max_operatives   ,                 
                  csef_end_date         ,                
                  csef_next_job_delay_time,               
                  csef_nxt_job_del_time_unit,             
                  csef_min_operatives                    )
     VALUES      ( p1.lcsef_refno
                  ,p1.lcsef_cspg_refno
                  ,p1.lcsef_csp_sor_code
                  ,p1.lcsef_start_date
                  ,p1.lcsef_estimated_effort          
                  ,p1.lcsef_estimated_effort_unit
                  ,p1.lcsef_effort_driven_ind         
                  ,'DATALOAD'
                  ,sysdate
                  ,p1.lcsef_max_operatives
                  ,p1.lcsef_end_date             
                  ,p1.lcsef_next_job_delay_time       
                  ,p1.lcsef_nxt_job_del_time_unit
                  ,p1.lcsef_min_operatives );
                       DBMS_OUTPUT.PUT_LINE('3');
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
--   DBMS_OUTPUT.PUT_LINE('Err1 '||SQLERRM);
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
 END;
--
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SOR_EFFORT');
--
commit;
--
fsc_utils.proc_end;
--
EXCEPTION
  WHEN OTHERS THEN
    -- DBMS_OUTPUT.PUT_LINE('Err2 '||SQLERRM);
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
CURSOR c1 is
SELECT
 rowid rec_rowid
,lcsef_dlb_batch_id
,lcsef_dl_seqno
,lcsef_dl_load_status
,lcsef_refno
,lcsef_ppc_ppp_ppg_code
,lcsef_ppc_ppp_wpr_code
,lcsef_ppc_ppp_start_date
,lcsef_ppc_cos_code
,lcsef_start_date
,lcsef_end_date
,lcsef_csp_sor_code
,lcsef_estimated_effort          
,lcsef_estimated_effort_unit
,lcsef_effort_driven_ind         
,lcsef_max_operatives         
,lcsef_next_job_delay_time       
,lcsef_nxt_job_del_time_unit
,lcsef_min_operatives
,lcsef_cspg_refno
FROM dl_hrm_con_sor_effort
WHERE lcsef_dlb_batch_id      = p_batch_id
AND   lcsef_dl_load_status       in ('L','F','O');
--
CURSOR c_pol (p_pol_code varchar2) IS
SELECT 'x'
FROM   pricing_policy_groups
WHERE  ppg_code = p_pol_code;
--
CURSOR c_cspg_for_ppc (p_ppg_code    VARCHAR2
                      ,p_wpr_code    VARCHAR2
                      ,p_cos_code    VARCHAR2
                      ,p_ppc_start   DATE
                      ,p_date        DATE)    IS
SELECT  cspg_refno,
        cspg_start_date,
        cspg_end_date
FROM    con_site_price_groups
WHERE   cspg_ppc_ppp_ppg_code          = p_ppg_code
AND     cspg_ppc_ppp_wpr_code          = p_wpr_code
AND     cspg_ppc_cos_code              = p_cos_code
AND     trunc(cspg_ppc_ppp_start_date) = trunc(p_ppc_start)
AND     TRUNC(p_date) BETWEEN TRUNC(cspg_start_date) 
                          AND TRUNC(NVL(cspg_end_date, p_date + 1));
--
--
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SOR_EFFORT';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_seqno          NUMBER(10);
l_start_date     DATE;
l_end_date       DATE;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_con_site_prices.dataload_validate');
fsc_utils.debug_message( 's_dl_hrm_con_site_prices.dataload_validate',3);
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
cs   := p1.lcsef_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors    := 'V';
l_error_ind := 'N';
--
-- Find the Contractor Price Group associated
--
   l_seqno       := NULL;
   l_start_date  := NULL;
   l_end_date    := NULL;
--
    OPEN c_cspg_for_ppc ( p1.lcsef_ppc_ppp_ppg_code,
                          p1.lcsef_ppc_ppp_wpr_code,
                          p1.lcsef_ppc_cos_code,
                          p1.lcsef_ppc_ppp_start_date,
                          p1.lcsef_start_date);
     FETCH c_cspg_for_ppc INTO l_seqno, l_start_date, l_end_date;
    CLOSE c_cspg_for_ppc;
--
    --IF l_seqno IS NULL THEN
    --   DBMS_OUTPUT.PUT_LINE(' PPG '||p1.lcsef_ppc_ppp_ppg_code||
    --                        ' WPR '||p1.lcsef_ppc_ppp_wpr_code||
    --                        ' COS '||p1.lcsef_ppc_cos_code||
    --                        ' Date '||TO_CHAR(p1.lcsef_ppc_ppp_start_date,'DD-MON-YY HH24:MI:SS')||
    --                        ' Stdate '||TO_CHAR(p1.lcsef_start_date,'DD-MON-YY HH24:MI:SS'));
    --END IF;
--
-- RI Checks
--

    IF p1.lcsef_csp_sor_code IS NOT NULL THEN
    -- Val related SOR exist 
       IF NOT s_schedule_of_rates.check_sor_exists(p1.lcsef_csp_sor_code)                      
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',140);
       END IF;
    END IF;
--
    IF p1.lcsef_ppc_cos_code IS NOT NULL THEN
    -- Val related COS exist 
       IF NOT s_contractor_sites.check_cos_exists(p1.lcsef_ppc_cos_code)                      
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',178);
       END IF;
    END IF;
--
    IF p1.lcsef_ppc_ppp_wpr_code IS NOT NULL THEN
    -- Val related WPR exist 
       IF NOT s_work_programmes.wpr_code_exists(p1.lcsef_ppc_ppp_wpr_code)                      
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',180);
       END IF;
    END IF;
--
    IF p1.lcsef_ppc_ppp_ppg_code IS NOT NULL THEN
    -- Val related PPG exist 
       IF NOT s_pricing_policy_programmes.does_ppg_code_exist(p1.lcsef_ppc_ppp_ppg_code)                      
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',179);
       END IF;
    END IF;
--
    -- Val does not already exist
    IF p1.lcsef_csp_sor_code IS NOT NULL AND
       l_seqno IS NOT NULL AND 
       p1.lcsef_start_date IS NOT NULL  THEN
--
         IF s_con_sor_effort.check_con_sor_effort_exists(l_seqno,
                                                         p1.lcsef_csp_sor_code,
                                                         p1.lcsef_start_date)                    
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',186);
         END IF;
--       
    END IF;
    --
    -- Val Con Sor Price exists
    --
    IF l_seqno IS NOT NULL AND
       p1.lcsef_csp_sor_code IS NOT NULL THEN
       IF NOT s_con_site_prices.check_sor_exists_for_cspg( l_seqno,
                                                           p1.lcsef_csp_sor_code )
          THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',187);
       END IF;
--
    END IF;
--
-- Check values have been supplied For mandatory columns
--
    IF p1.lcsef_ppc_ppp_ppg_code IS NULL 
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',182);
    END IF;
    IF p1.lcsef_ppc_ppp_wpr_code IS NULL 
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',181);
    END IF;
    IF p1.lcsef_ppc_cos_code IS NULL 
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',183);
    END IF;
    IF p1.lcsef_ppc_ppp_start_date IS NULL 
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',184);
    END IF;
    IF l_seqno IS NULL THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',185);
    ELSE
       UPDATE dl_hrm_con_sor_effort 
       SET lcsef_cspg_refno = l_seqno
       WHERE rowid = p1.rec_rowid;
    END IF;
--
    IF p1.lcsef_start_date IS NULL 
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',144);
    END IF;
    IF p1.lcsef_estimated_effort IS NULL 
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',145);
    END IF;
    IF p1.lcsef_estimated_effort_unit IS NULL
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',146);
    ELSIF p1.lcsef_estimated_effort_unit NOT IN ( 'M','H','D') 
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',149);
    END IF;
    IF p1.lcsef_effort_driven_ind IS NULL 
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',147);
    ELSIF p1.lcsef_effort_driven_ind IS NOT NULL AND
          p1.lcsef_effort_driven_ind NOT IN ( 'Y', 'N' )
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',150);
    END IF;
    IF p1.lcsef_min_operatives IS NULL
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',148);
    END IF;
    --
    IF p1.lcsef_start_date > NVL(p1.lcsef_end_date,p1.lcsef_start_date)
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',136);
    END IF;
    IF  p1.lcsef_effort_driven_ind = 'Y'
    AND NVL(p1.lcsef_max_operatives,-1) < 1
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',137);
    END IF;
    IF  p1.lcsef_effort_driven_ind = 'N'
    AND p1.lcsef_max_operatives IS NOT NULL
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',138);
    END IF;
    IF p1.lcsef_max_operatives IS NOT NULL AND p1.lcsef_max_operatives < 1
        THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',173);
    END IF;
    IF p1.lcsef_min_operatives IS NOT NULL AND p1.lcsef_min_operatives < 1
        THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',174);
    END IF;
    IF p1.lcsef_next_job_delay_time IS NOT NULL AND
       p1.lcsef_next_job_delay_time < 0 THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',175);
    END IF;
    IF p1.lcsef_next_job_delay_time IS NOT NULL
    AND p1.lcsef_nxt_job_del_time_unit IS NULL
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',139);
    END IF;
    IF p1.lcsef_next_job_delay_time IS NULL
    AND p1.lcsef_nxt_job_del_time_unit IS NOT NULL
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',139);
    END IF;
    IF p1.lcsef_nxt_job_del_time_unit IS NOT NULL 
    THEN
       IF p1.lcsef_nxt_job_del_time_unit NOT IN ( 'M','H','D')
       THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',172);
       END IF;
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
      RAISE;
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
 d1.rowid rec_rowid
,d1.lcsef_dlb_batch_id
,d1.lcsef_dl_seqno
,d1.lcsef_dl_load_status
,d1.lcsef_refno
FROM dl_hrm_con_sor_effort d1
WHERE d1.lcsef_dlb_batch_id = p_batch_id
AND d1.lcsef_dl_load_status = 'C';

--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRM_CON_SOR_EFFORT';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_an_tab VARCHAR2(1);
i integer := 0;
l_cspg_refno VARCHAR2(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hrm_con_sor_effort.dataload_delete');
fsc_utils.debug_message( 's_dl_hrm_con_sor_effort.dataload_delete',3 );
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
cs := p1.lcsef_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
     DELETE con_sor_effort
     WHERE  csef_refno = p1.lcsef_refno;
--
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
   s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
   set_record_status_flag(l_id,'O');
--
END;
--
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SOR_EFFORT');
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
END s_dl_hrm_con_sor_effort;
/
