CREATE OR REPLACE PACKAGE BODY s_dl_hrm_con_sor_cmpt_specs
AS
-- ***********************************************************************
--
--  DESCRIPTION:           
--           
--  CHANGE CONTROL           
--  VERSION     WHO            WHEN       WHY           
--      1.0     Paul Bouchier  08/02/05   Dataload           
--
--      2.0     Vishad Shah    29/05/07   General Tidy Up.
--
--      3.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      3.1 5.15.1   PH   07-APR-2009 Added trunc to ppc_ppp_start_dates on
--                                    create and validate.
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
  UPDATE dl_hrm_con_sor_cmpt_specs
  SET lcscs_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_con_sor_cmpt_specs');
     RAISE;
  --
END set_record_status_flag;
--
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT rowid rec_rowid,
       LCSCS_DLB_BATCH_ID,
       LCSCS_DL_SEQNO,
       LCSCS_DL_LOAD_STATUS,
       LCSPG_PPC_COS_CODE,
       LCSPG_PPC_PPP_PPG_CODE,
       LCSPG_PPC_PPP_WPR_CODE,
       LCSPG_PPC_PPP_START_DATE,
       LCSPG_START_DATE,
       LCSCS_CSP_SOR_CODE,
       LCSCS_START_DATE,
       LCSCS_END_DATE,
       LCSCS_REFNO
  FROM dl_hrm_con_sor_cmpt_specs
 WHERE lcscs_dlb_batch_id   = p_batch_id
   AND lcscs_dl_load_status = 'V';
--
--
-- *******************************************************************
--
CURSOR get_cscs_refno 
IS
SELECT cscs_refno_seq.NEXTVAL
  FROM dual;
--
-- *******************************************************************
--
CURSOR get_cspg_refno(P_CSPG_PPC_COS_CODE 		VARCHAR2,
                      P_CSPG_PPC_PPP_PPG_CODE		VARCHAR2,
                      P_CSPG_PPC_PPP_WPR_CODE		VARCHAR2,
                      P_CSPG_PPC_PPP_START_DATE		DATE,
                      P_CSPG_START_DATE			DATE)
IS
SELECT cspg_refno
  FROM con_site_price_groups
 WHERE cspg_ppc_cos_code              = p_cspg_ppc_cos_code
   AND cspg_ppc_ppp_ppg_code          = p_cspg_ppc_ppp_ppg_code
   AND cspg_ppc_ppp_wpr_code          = p_cspg_ppc_ppp_wpr_code
   AND trunc(cspg_ppc_ppp_start_date) = trunc(p_cspg_ppc_ppp_start_date)
   AND cspg_start_date                = p_cspg_start_date;
--
-- *******************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'CREATE';
ct       		VARCHAR2(40) := 'DL_HRM_CON_SOR_CMPT_SPECS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id     ROWID;
--
l_cscs_refno 	        NUMBER(10);
l_cspg_refno		NUMBER(10);
--
-- Other variables
--
i            INTEGER := 0;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_con_sor_cmpt_specs.dataload_create');
    fsc_utils.debug_message('s_dl_hrm_con_sor_cmpt_specs.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1(p_batch_id) LOOP
--
      BEGIN
--
          cs := p1.lcscs_DL_SEQNO;
          l_id := p1.rec_rowid;
-- 
          l_cscs_refno := NULL;
--
           OPEN get_cscs_refno;
          FETCH get_cscs_refno INTO l_cscs_refno;
          CLOSE get_cscs_refno;
-- 
          l_cspg_refno := NULL;
--
           OPEN get_cspg_refno(p1.LCSPG_PPC_COS_CODE,
                               p1.LCSPG_PPC_PPP_PPG_CODE,
                               p1.LCSPG_PPC_PPP_WPR_CODE,
                               p1.LCSPG_PPC_PPP_START_DATE,
                               p1.LCSPG_START_DATE);
--
          FETCH get_cspg_refno INTO l_cspg_refno;
          CLOSE get_cspg_refno;
--    
--                
-- Create sor_cmpt_specifications record
--
          INSERT INTO con_sor_cmpt_specifications
                (cscs_refno,
                 cscs_csp_cspg_refno,
                 cscs_csp_SOR_CODE,                  
                 cscs_start_date,                     
                 cscs_end_date
                )
          VALUES
                (l_cscs_refno, 
                 l_cspg_refno,
                 p1.lcscs_csp_sor_code,                
                 p1.lcscs_start_date,                       
                 p1.lcscs_end_date
                ); 
--
--
-- Record the primary key against the external key so subsequent loads FK 
-- can be mapped to this record
--
--
          UPDATE dl_hrm_con_sor_cmpt_specs
             SET lcscs_refno = l_cscs_refno
           WHERE rowid       = p1.rec_rowid;
--
--      
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
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
               set_record_status_flag(l_id,'O');
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
-- Section to analyze the table(s) populated by this dataload
--
    fsc_utils.proc_end;
    commit;
--
    EXCEPTION
         WHEN OTHERS THEN
         set_record_status_flag(l_id,'O');
         s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
END dataload_create;
--
-- ************************************************************************************************
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LCSCS_DLB_BATCH_ID,
       LCSCS_DL_SEQNO,
       LCSCS_DL_LOAD_STATUS,
       LCSPG_PPC_COS_CODE,
       LCSPG_PPC_PPP_PPG_CODE,
       LCSPG_PPC_PPP_WPR_CODE,
       LCSPG_PPC_PPP_START_DATE,
       LCSPG_START_DATE,
       LCSCS_CSP_SOR_CODE,
       LCSCS_START_DATE,
       LCSCS_END_DATE,
       LCSCS_REFNO
  FROM dl_hrm_con_sor_cmpt_specs
 WHERE lcscs_dlb_batch_id    = p_batch_id
   AND lcscs_dl_load_status in ('L','F','O');
--
-- *******************************************************************
--
CURSOR get_cspg_refno(P_CSPG_PPC_COS_CODE 		VARCHAR2,
                      P_CSPG_PPC_PPP_PPG_CODE		VARCHAR2,
                      P_CSPG_PPC_PPP_WPR_CODE		VARCHAR2,
                      P_CSPG_PPC_PPP_START_DATE		DATE,
                      P_CSPG_START_DATE			DATE)
IS
SELECT cspg_refno
  FROM con_site_price_groups
 WHERE cspg_ppc_cos_code              = p_cspg_ppc_cos_code
   AND cspg_ppc_ppp_ppg_code          = p_cspg_ppc_ppp_ppg_code
   AND cspg_ppc_ppp_wpr_code          = p_cspg_ppc_ppp_wpr_code
   AND trunc(cspg_ppc_ppp_start_date) = trunc(p_cspg_ppc_ppp_start_date)
   AND cspg_start_date                = p_cspg_start_date;
--
-- *******************************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'VALIDATE';
ct       	VARCHAR2(40) := 'DL_HRM_CON_SOR_CMPT_SPECS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     ROWID;
--
l_errors        VARCHAR2(10);
l_error_ind     VARCHAR2(10);
i               INTEGER :=0;
l_cspg_refno    NUMBER(10);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_con_sor_cmpt_specs.dataload_validate');
    fsc_utils.debug_message( 's_dl_hrm_con_sor_cmpt_specs.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--  
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lcscs_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
-- *******************************************************************
--
-- PERFORM VARIOUS VALIDATION CHECKS
-- 
--
-- CONTRACTOR SITE CODE
--
          IF (p1.lcspg_ppc_cos_code IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',250);
--
          ELSIF NOT s_contractor_sites.check_cos_exists(p1.lcspg_ppc_cos_code) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',251);
--
          END IF;
--
-- *******************************************************************
--
--
-- PRICING POLICY GROUP CODE
--
--
          IF (p1.lcspg_ppc_ppp_ppg_code IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',252);
-- 
          ELSIF NOT s_pricing_policy_programmes.does_ppg_code_exist(p1.lcspg_ppc_ppp_ppg_code) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',253);
--
          END IF;
--
-- *******************************************************************
--
--
-- WORK PROGRAMME CODE
--
--
          IF (p1.lcspg_ppc_ppp_wpr_code IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',254);
--
          ELSIF NOT s_work_programmes.wpr_code_exists(p1.lcspg_ppc_ppp_wpr_code) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',255);
--
          END IF;
--
-- *******************************************************************
--
--
-- THE DATE WHEN THIS PRICING POLICY PROGRAMME STARTS TO APPLY
--
--
          IF (p1.lcspg_ppc_ppp_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',256);
          END IF;
--
-- *******************************************************************
--
--
-- THE DATE WHEN THIS CONTRACTOR SITE PRICE GROUP'S PRICES START TO APPLY
--
--
          IF (p1.lcspg_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',257);
          END IF;
--
-- *******************************************************************
--
--
-- THE SOR CODE 
--
--
          IF (p1.lcscs_csp_sor_code IS NULL)  THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',235);
--
          ELSIF NOT s_schedule_of_rates.check_sor_exists(p1.lcscs_csp_sor_code) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',237);
--
          END IF;
--
-- *******************************************************************
--
--
-- THE DATE FROM WHICH THE ASSOCIATED CONTRACTOR SITE PRICE SOR COMPONENT 
-- OVERHEAD COSTS ARE EFFECTIVE
--
--
          IF (p1.lcscs_start_date IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',258);
--
          ELSIF (p1.lcscs_start_date > NVL(p1.lcscs_end_date,p1.lcscs_start_date)) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',238);
--
          END IF;    
--
-- *******************************************************************
--
--
-- Check to see that the Contractor Price Group associated exists
--
          l_cspg_refno := NULL;
--  
           OPEN get_cspg_refno(p1.lcspg_ppc_cos_code,
                               p1.lcspg_ppc_ppp_ppg_code,
                               p1.lcspg_ppc_ppp_wpr_code,
                               p1.lcspg_ppc_ppp_start_date,
                               p1.lcspg_start_date);
--
          FETCH get_cspg_refno INTO l_cspg_refno;
          CLOSE get_cspg_refno;
--
          IF (l_cspg_refno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',259);
          END IF;
--
-- *******************************************************************
--
-- Check that the a con_sor_cmpt_specification record to be loaded doesn't 
-- already exist 
--
-- 
          IF (    l_cspg_refno          IS NOT NULL
              AND p1.lcscs_csp_sor_code IS NOT NULL 
              AND p1.lcscs_start_date   IS NOT NULL) THEN
--
           IF s_con_sor_cmpt_specifications.check_con_sor_cmpt_spec_exists(l_cspg_refno,
                                                                           p1.lcscs_csp_sor_code,
                                                                           p1.lcscs_start_date) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',260);
--
           END IF;
--
          END IF;   
--
-- *******************************************************************
--
-- Check that a con_site_prices record exist 
--
--       
          IF (    l_cspg_refno          IS NOT NULL
              AND p1.lcscs_csp_sor_code IS NOT NULL) THEN
--
           IF NOT s_con_site_prices.csp_exists (p1.lcscs_csp_sor_code,l_cspg_refno) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',261);
           END IF; 
--
          END IF;
--
-- *******************************************************************
--
-- Now UPDATE the record count and error code 
--
          IF l_errors = 'F' THEN
           l_error_ind := 'Y';
          ELSE
             l_error_ind := 'N';
          END IF;
--
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
          set_record_status_flag(l_id,l_errors);
-- 
          EXCEPTION
               WHEN OTHERS THEN
               ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
               s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
               set_record_status_flag(l_id,'O');
--
      END;
--
    END LOOP;
--
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
-- ************************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LCSCS_DLB_BATCH_ID,
       LCSCS_DL_SEQNO,
       LCSCS_DL_LOAD_STATUS,
       LCSPG_PPC_COS_CODE,
       LCSPG_PPC_PPP_PPG_CODE,
       LCSPG_PPC_PPP_WPR_CODE,
       LCSPG_PPC_PPP_START_DATE,
       LCSPG_START_DATE,
       LCSCS_CSP_SOR_CODE,
       LCSCS_START_DATE,
       LCSCS_END_DATE,
       LCSCS_REFNO
  FROM dl_hrm_con_sor_cmpt_specs
 WHERE lcscs_dlb_batch_id   = p_batch_id
   AND lcscs_dl_load_status = 'C';
--
-- *******************************************************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(40) := 'DL_HRM_CON_SOR_CMPT_SPECS';
cs       INTEGER;
l_id     ROWID;
--
i        INTEGER := 0;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_con_sor_cmpt_specs.dataload_delete');
    fsc_utils.debug_message('s_dl_hrm_con_sor_cmpt_specs.dataload_delete',3);
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
          cs   := p1.lcscs_dl_seqno;
          l_id := p1.rec_rowid;
--
          DELETE 
            FROM con_sor_cmpt_specifications
           WHERE cscs_refno = p1.lcscs_refno; 
--
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
          set_record_status_flag(l_id,'V');
--
      END;
--
    END LOOP;
--
    fsc_utils.proc_end;
    commit;
--
    EXCEPTION
         WHEN OTHERS THEN
         set_record_status_flag(l_id,'C');
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hrm_con_sor_cmpt_specs;
/