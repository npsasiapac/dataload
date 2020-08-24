CREATE OR REPLACE PACKAGE BODY s_dl_hrm_con_sor_components
AS
-- ***********************************************************************
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO            WHEN       WHY
--      1.0     Paul Bouchier  08/02/05   Dataload
--
--      2.0     Vishad Shah    31/05/07   General Tidy Up.
--					  Remove reference to external reference
--
--      2.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      2.1 5.15.1   PH   07-APR-2009 Added trunc to ppc_ppp_start_dates on
--                                    create, validate and delete.
--      2.2 5.15.1   PH   23-SEP-2009 Added rowid into select statements
--
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
  UPDATE dl_hrm_con_sor_components
  SET lcsco_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hrm_con_sor_components');
     RAISE;
  --
END set_record_status_flag;
--
--
--  declare package variables and constants
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) 
IS
SELECT rowid rec_rowid,
       LCSCO_DLB_BATCH_ID,
       LCSCO_DL_SEQNO,
       LCSCO_DL_LOAD_STATUS,
       LCSPG_PPC_COS_CODE,
       LCSPG_PPC_PPP_PPG_CODE,
       LCSPG_PPC_PPP_WPR_CODE,
       LCSPG_PPC_PPP_START_DATE,
       LCSPG_START_DATE,
       LCSCS_CSP_SOR_CODE,
       LCSCS_START_DATE,
       LCSCO_SCMT_CODE,
       LCSCO_COST,
       NVL(LCSCO_PERCENTAGE_IND,'N') LCSCO_PERCENTAGE_IND
  FROM dl_hrm_con_sor_components
 WHERE lcsco_dlb_batch_id   = p_batch_id
   AND lcsco_dl_load_status = 'V';
--
-- ************************************************************************
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
CURSOR get_cscs_refno(P_CSCS_CSP_CSPG_REFNO 		NUMBER,
                      P_CSCS_CSP_SOR_CODE		VARCHAR2,
                      P_CSCS_START_DATE			DATE)
IS
SELECT cscs_refno
  FROM con_sor_cmpt_specifications
 WHERE cscs_csp_cspg_refno = p_cscs_csp_cspg_refno
   AND cscs_csp_sor_code   = p_cscs_csp_sor_code
   AND cscs_start_date     = p_cscs_start_date;
--
-- *******************************************************************
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'CREATE';
ct       	VARCHAR2(40) := 'DL_HRM_CON_SOR_COMPONENTS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     ROWID;
l_an_tab 	VARCHAR2(1);
--
l_cscs_refno 	NUMBER(10);
l_cspg_refno	NUMBER(10);
--
-- Other variables
--
i            	INTEGER := 0;

--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_con_sor_components.dataload_create');
    fsc_utils.debug_message( 's_dl_hrm_con_sor_components.dataload_create',3);
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
          cs := p1.lcsco_dl_seqno;
          l_id := p1.rec_rowid;
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
          l_cscs_refno := NULL;
--
           OPEN get_cscs_refno(l_cspg_refno,p1.lcscs_csp_sor_code, p1.lcscs_start_date);
          FETCH get_cscs_refno INTO l_cscs_refno;
          CLOSE get_cscs_refno;
-- 
--    
--
-- Create con_sor_component record
--    
          INSERT INTO con_sor_components
                (CSCO_cscs_REFNO,
                 CSCO_COST,              
                 CSCO_SCMT_CODE,             
                 CSCO_PERCENTAGE_IND
                )
          VALUES
                (l_cscs_refno,
                 p1.lcsco_COST,                   
                 p1.lcsco_SCMT_CODE, 
                 p1.lcsco_PERCENTAGE_IND
                ); 
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
--
-- ****************************************************************************************************
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LCSCO_DLB_BATCH_ID,
       LCSCO_DL_SEQNO,
       LCSCO_DL_LOAD_STATUS,
       LCSPG_PPC_COS_CODE,
       LCSPG_PPC_PPP_PPG_CODE,
       LCSPG_PPC_PPP_WPR_CODE,
       LCSPG_PPC_PPP_START_DATE,
       LCSPG_START_DATE,
       LCSCS_CSP_SOR_CODE,
       LCSCS_START_DATE,
       LCSCO_SCMT_CODE,
       LCSCO_COST,
       NVL(LCSCO_PERCENTAGE_IND,'N') LCSCO_PERCENTAGE_IND
  FROM dl_hrm_con_sor_components
 WHERE lcsco_dlb_batch_id    = p_batch_id
   AND lcsco_dl_load_status in ('L','F','O');
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
CURSOR get_cscs_refno(P_CSCS_CSP_CSPG_REFNO 		NUMBER,
                      P_CSCS_CSP_SOR_CODE		VARCHAR2,
                      P_CSCS_START_DATE			DATE)
IS
SELECT cscs_refno
  FROM con_sor_cmpt_specifications
 WHERE cscs_csp_cspg_refno = p_cscs_csp_cspg_refno
   AND cscs_csp_sor_code   = p_cscs_csp_sor_code
   AND cscs_start_date     = p_cscs_start_date;
--
-- *******************************************************************
--
CURSOR chk_scmt_exists(P_CSCO_SCMT_CODE 		VARCHAR2)
IS
SELECT 'X'
  FROM sor_component_types
 WHERE scmt_code = p_csco_scmt_code;
--
-- *******************************************************************
--
-- Constants for process_summary
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'VALIDATE';
ct       		VARCHAR2(40) := 'DL_HRM_CON_SOR_COMPONENTS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id     ROWID;
--
i                	INTEGER :=0;
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
--
l_cscs_refno     	NUMBER(10);
l_cspg_refno	 	NUMBER(10);
l_scmt_exists		VARCHAR2(1);
--
-- Other variables
--
l_dummy             VARCHAR2(10); 
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_con_sor_components.dataload_validate');
    fsc_utils.debug_message('s_dl_hrm_con_sor_components.dataload_validate',3);
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
          cs := p1.lcsco_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
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
-- THE SCMT CODE is supplied and valid
--
--
          IF (p1.lcsco_scmt_code IS NULL)  THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',262);
--
          ELSE
--
             l_scmt_exists := NULL;
--
              OPEN chk_scmt_exists(p1.lcsco_scmt_code);
             FETCH chk_scmt_exists INTO l_scmt_exists;
             CLOSE chk_scmt_exists;
--
             IF (l_scmt_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',263);
             END IF;
--
          END IF;
--
-- *******************************************************************
--
--
-- THE COST is supplied and not < 0.00
--
--
          IF (p1.lcsco_cost IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',264);
--
          ELSIF (p1.lcsco_cost < 0) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',265);
--
          END IF;
--
-- *******************************************************************
--
--
-- THE PERCENATAGE/MONETARY Indicator. No need to check for null as this
-- is defaulted to 'N' 
--
--
          IF (p1.lcsco_percentage_ind NOT IN ('Y','N')) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',266);
--
          END IF;
--
-- *******************************************************************
--
--
-- Check to see that the Contractor Price Group associated exists
--
          IF (    p1.lcspg_ppc_cos_code       IS NOT NULL
              AND p1.lcspg_ppc_ppp_ppg_code   IS NOT NULL
              AND p1.lcspg_ppc_ppp_wpr_code   IS NOT NULL
              AND p1.lcspg_ppc_ppp_start_date IS NOT NULL
              AND p1.lcspg_start_date         IS NOT NULL) THEN
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
          END IF;
--
-- *******************************************************************
--
-- Get the con_sor_cmpt_specifications refno. Used to link the 
-- con_cor_component record to the right con_sor_cmpt_specifications
--
-- 
          IF (    l_cspg_refno          IS NOT NULL
              AND p1.lcscs_csp_sor_code IS NOT NULL 
              AND p1.lcscs_start_date   IS NOT NULL) THEN
--
            OPEN get_cscs_refno(l_cspg_refno, p1.lcscs_csp_sor_code, p1.lcscs_start_date);
           FETCH get_cscs_refno INTO l_cscs_refno;
           CLOSE get_cscs_refno;
--
           IF (l_cscs_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',267);
           END IF;
--
          END IF;   
--
-- *******************************************************************
--
-- Check con_sor_component doesn't alread exist
--
-- 
          IF (    l_cscs_refno       IS NOT NULL 
              AND p1.lcsco_scmt_code IS NOT NULL) THEN
--
           IF s_con_sor_components.check_csco_exists(l_cscs_refno, p1.lcsco_scmt_code) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',268);
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
-- ****************************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LCSCO_DLB_BATCH_ID,
       LCSCO_DL_SEQNO,
       LCSCO_DL_LOAD_STATUS,
       LCSPG_PPC_COS_CODE,
       LCSPG_PPC_PPP_PPG_CODE,
       LCSPG_PPC_PPP_WPR_CODE,
       LCSPG_PPC_PPP_START_DATE,
       LCSPG_START_DATE,
       LCSCS_CSP_SOR_CODE,
       LCSCS_START_DATE,
       LCSCO_SCMT_CODE,
       LCSCO_COST,
       NVL(LCSCO_PERCENTAGE_IND,'N') LCSCO_PERCENTAGE_IND
  FROM dl_hrm_con_sor_components
 WHERE lcsco_dlb_batch_id   = p_batch_id
   AND lcsco_dl_load_status = 'C';

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
CURSOR get_cscs_refno(P_CSCS_CSP_CSPG_REFNO 		NUMBER,
                      P_CSCS_CSP_SOR_CODE		VARCHAR2,
                      P_CSCS_START_DATE			DATE)
IS
SELECT cscs_refno
  FROM con_sor_cmpt_specifications
 WHERE cscs_csp_cspg_refno = p_cscs_csp_cspg_refno
   AND cscs_csp_sor_code   = p_cscs_csp_sor_code
   AND cscs_start_date     = p_cscs_start_date;
--
-- *******************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'DELETE';
ct       		VARCHAR2(40) := 'DL_HRM_CON_SOR_COMPONENTS';
cs       		INTEGER;
l_id     ROWID;
--
i        		INTEGER := 0;
--
l_cscs_refno     	NUMBER(10);
l_cspg_refno	 	NUMBER(10);
l_scmt_exists		VARCHAR2(1);
--

BEGIN
--
    fsc_utils.proc_start('s_dl_hrm_con_sor_components.dataload_delete');
    fsc_utils.debug_message( 's_dl_hrm_con_sor_components.dataload_delete',3 );
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
          cs := p1.lcsco_dl_seqno;
          l_id := p1.rec_rowid;
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
--
          l_cscs_refno := NULL;
--
           OPEN get_cscs_refno(l_cspg_refno, p1.lcscs_csp_sor_code, p1.lcscs_start_date);
          FETCH get_cscs_refno INTO l_cscs_refno;
          CLOSE get_cscs_refno;
--
--
          DELETE 
            FROM con_sor_components
           WHERE csco_cscs_refno = l_cscs_refno
             AND csco_scmt_code  = p1.lcsco_scmt_code;
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
END s_dl_hrm_con_sor_components;
/