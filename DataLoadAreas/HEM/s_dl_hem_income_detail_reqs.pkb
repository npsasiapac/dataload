--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_income_detail_reqs
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   18-MAR-2013  Initial Creation.
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
    UPDATE dl_hem_income_detail_reqs
       SET lindr_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_income_detail_reqs');
            RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id IN VARCHAR2,
                          p_date     IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LINDR_DLB_BATCH_ID,
       LINDR_DL_SEQNO,
       LINDR_DL_LOAD_STATUS,
       LINDR_REFERENCE,
       LINDR_TYPE,
       NVL(LINDR_CREATED_BY, 'DATALOAD') LINDR_CREATED_BY,
       NVL(LINDR_CREATED_DATE, SYSDATE)  LINDR_CREATED_DATE,
       LINDR_PAR_REFERENCE_TYPE,
       LINDR_PAR_REFERENCE,
       LINDR_SCO_CODE,
       LINDR_STATUS_DATE,
       LINDR_HRV_IDTY_CODE,
       LINDR_PARTNER_IND,
       LINDR_LEGACY_REFERENCE_TYPE,
       LINDR_LEGACY_REFERENCE,
       LINDR_NUM_OF_CHILDREN,
       LINDR_REQUEST_PROCESS_DATE,
       LINDR_REQUEST_DATE,
       LINDR_IFP_CODE
  FROM dl_hem_income_detail_reqs
 WHERE lindr_dlb_batch_id   = p_batch_id
   AND lindr_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- ***********************************************************************
--
CURSOR c_get_par(p_par_refno VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_refno;
--
-- ***********************************************************************
--
CURSOR c_get_prf(p_par_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_app(p_app_refno VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_refno = p_app_refno;
--
-- ***********************************************************************
--
CURSOR c_get_alr(p_app_legacy_ref VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;
--
-- ***********************************************************************
--
CURSOR c_get_tcy(p_tcy_refno VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_refno = p_tcy_refno;
--
-- ***********************************************************************
--
CURSOR c_get_tar(p_tcy_alt_ref VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_surv(p_surv_refno VARCHAR2) 
IS
SELECT surv_refno
  FROM subsidy_reviews  
 WHERE surv_refno = p_surv_refno;
--
-- ***********************************************************************
--
CURSOR c_get_acas(p_acas_alt_ref VARCHAR2) 
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = p_acas_alt_ref;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_INCOME_DETAIL_REQS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                          INTEGER := 0;
l_exists                   VARCHAR2(1);
l_par_refno                parties.par_refno%type;
l_arc_code                 income_detail_requests.indr_arc_code%type;
l_app_refno                applications.app_refno%type;
l_tcy_refno                tenancies.tcy_refno%type;
l_surv_refno               subsidy_reviews.surv_refno%type;
l_acas_reference           advice_cases.acas_reference%type;
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger INDR_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_income_detail_reqs.dataload_create');
    fsc_utils.debug_message('s_dl_hem_income_detail_reqs.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lindr_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          l_arc_code       := NULL;
          l_par_refno      := NULL;
          l_app_refno      := NULL;
          l_tcy_refno      := NULL;
          l_surv_refno     := NULL;
          l_acas_reference := NULL;
--
          IF (p1.LINDR_PAR_REFERENCE_TYPE = 'PAR') THEN
--
            OPEN c_get_par(p1.lindr_par_reference);
           FETCH c_get_par INTO l_par_refno;
           CLOSE c_get_par;
--
          ELSIF (p1.LINDR_PAR_REFERENCE_TYPE = 'PRF') THEN
--
               OPEN c_get_prf(p1.lindr_par_reference);
              FETCH c_get_prf INTO l_par_refno;
              CLOSE c_get_prf;
--
          END IF;
--
-- Work out what the legacy reference should be
--
          IF (p1.lindr_legacy_reference_type = 'APP') THEN
--
            OPEN c_get_app(p1.lindr_legacy_reference);
           FETCH c_get_app INTO l_app_refno;
           CLOSE c_get_app;
--
           l_arc_code := 'HAT';
--
          ELSIF (p1.lindr_legacy_reference_type = 'ALR') THEN
--
               OPEN c_get_alr(p1.lindr_legacy_reference);
              FETCH c_get_alr INTO l_app_refno;
              CLOSE c_get_alr;
--
              l_arc_code := 'HAT';
--
          ELSIF (p1.lindr_legacy_reference_type = 'TCY') THEN
--
               OPEN c_get_tcy(p1.lindr_legacy_reference);
              FETCH c_get_tcy INTO l_tcy_refno;
              CLOSE c_get_tcy;
--
              l_arc_code := 'HEM';
--
          ELSIF (p1.lindr_legacy_reference_type = 'TAR') THEN
--
               OPEN c_get_tar(p1.lindr_legacy_reference);
              FETCH c_get_tar INTO l_tcy_refno;
              CLOSE c_get_tar;
--
              l_arc_code := 'HEM';
--
          ELSIF (p1.lindr_legacy_reference_type = 'SURV') THEN
--
               OPEN c_get_surv(p1.lindr_legacy_reference);
              FETCH c_get_surv INTO l_surv_refno;
              CLOSE c_get_surv;
--
              l_arc_code := 'HRA';
--
          ELSIF (p1.lindr_legacy_reference_type = 'ACAS') THEN
--
               OPEN c_get_acas(p1.lindr_legacy_reference);
              FETCH c_get_acas INTO l_acas_reference;
              CLOSE c_get_acas;
--
              l_arc_code := 'HAD';
--
          END IF;
--
-- Insert into income_detail_requests table
--
          INSERT /* +APPEND */ INTO  income_detail_requests(INDR_REFNO,
                                                            INDR_TYPE,
                                                            INDR_PAR_REFNO,
                                                            INDR_ARC_CODE,
                                                            INDR_ARC_SYS_CODE,
                                                            INDR_SCO_CODE,
                                                            INDR_STATUS_DATE,
                                                            INDR_HRV_IDTY_CODE,
                                                            INDR_PARTNER_IND,
                                                            INDR_APP_REFNO,
                                                            INDR_TCY_REFNO,
                                                            INDR_BAN_REFERENCE,
                                                            INDR_SUAP_REFERENCE,
                                                            INDR_NUM_OF_CHILDREN,
                                                            INDR_REQUEST_PROCESS_DATE,
                                                            INDR_REQUEST_DATE,
                                                            INDR_IFP_CODE,
                                                            INDR_SURV_REFNO,
                                                            INDR_ACAS_REFERENCE
                                                           )
--
                                                     VALUES(p1.lindr_reference,
                                                            p1.lindr_type,
                                                            l_par_refno,
                                                            l_arc_code,
                                                            'HOU',
                                                            p1.lindr_sco_code,
                                                            p1.lindr_status_date,
                                                            p1.lindr_hrv_idty_code,
                                                            p1.lindr_partner_ind,
                                                            l_app_refno,
                                                            l_tcy_refno,
                                                            NULL, -- ban_reference
                                                            NULL, -- suap_reference
                                                            p1.lindr_num_of_children,
                                                            p1.lindr_request_process_date,
                                                            p1.lindr_request_date,
                                                            p1.lindr_ifp_code,
                                                            l_surv_refno,
                                                            l_acas_reference
                                                           );
--
-- Update the CREATED BY/DATE
--
          UPDATE income_detail_requests
             SET indr_created_by   = p1.lindr_created_by,
                 indr_created_date = p1.lindr_created_date
           WHERE indr_refno = p1.lindr_reference;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,500000) = 0 THEN 
           COMMIT; 
          END IF;
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
--
      END;
--
    END LOOP;
--   
    COMMIT;
--
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_DETAIL_REQUESTS');
--
    execute immediate 'alter trigger INDR_BR_I enable';
--
    fsc_utils.proc_END;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_create;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_validate(p_batch_id IN VARCHAR2,
                            p_date     IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LINDR_DLB_BATCH_ID,
       LINDR_DL_SEQNO,
       LINDR_DL_LOAD_STATUS,
       LINDR_REFERENCE,
       LINDR_TYPE,
       LINDR_PAR_REFERENCE_TYPE,
       LINDR_PAR_REFERENCE,
       LINDR_SCO_CODE,
       LINDR_STATUS_DATE,
       LINDR_HRV_IDTY_CODE,
       LINDR_PARTNER_IND,
       LINDR_LEGACY_REFERENCE_TYPE,
       LINDR_LEGACY_REFERENCE,
       LINDR_NUM_OF_CHILDREN,
       LINDR_REQUEST_PROCESS_DATE,
       LINDR_REQUEST_DATE,
       LINDR_IFP_CODE
  FROM dl_hem_income_detail_reqs
 WHERE lindr_dlb_batch_id    = p_batch_id
   AND lindr_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- ***********************************************************************
--
CURSOR c_chk_par(p_par_refno VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_refno;
--
-- ***********************************************************************
--
CURSOR c_chk_prf(p_par_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_chk_app(p_app_refno VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_refno = p_app_refno;
--
-- ***********************************************************************
--
CURSOR c_chk_alr(p_app_legacy_ref VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;
--
-- ***********************************************************************
--
CURSOR c_chk_tcy(p_tcy_refno VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_refno = p_tcy_refno;
--
-- ***********************************************************************
--
CURSOR c_chk_tar(p_tcy_alt_ref VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_chk_surv(p_surv_refno VARCHAR2) 
IS
SELECT surv_refno
  FROM subsidy_reviews  
 WHERE surv_refno = p_surv_refno;
--
-- ***********************************************************************
--
CURSOR c_chk_acas(p_acas_alt_ref VARCHAR2) 
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = p_acas_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_chk_ifp_code( p_ifp_code  VARCHAR2 ) 
IS
SELECT 'X'
  FROM ics_financial_periods
 WHERE ifp_code = p_ifp_code;
--
-- ***********************************************************************
--
CURSOR chk_indr_ref_exists(p_indr_reference NUMBER) 
IS
SELECT 'X'
  FROM income_detail_requests
 WHERE indr_refno = p_indr_reference;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HEM_INCOME_DETAIL_REQS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists           VARCHAR2(1);
l_indr_ref_exists  VARCHAR2(1);
l_ifp_exists       VARCHAR2(1);
l_errors           VARCHAR2(10);
l_error_ind        VARCHAR2(10);
i                  INTEGER :=0;
--
l_par_refno        parties.par_refno%type;
l_app_refno        applications.app_refno%type;
l_tcy_refno        tenancies.tcy_refno%type;
l_surv_refno       subsidy_reviews.surv_refno%type;
l_acas_reference   advice_cases.acas_reference%type;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_income_detail_reqs.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_income_detail_reqs.dataload_validate',3);
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
          cs   := p1.lindr_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
          l_par_refno      := NULL;
          l_app_refno      := NULL;
          l_tcy_refno      := NULL;
          l_surv_refno     := NULL;
          l_acas_reference := NULL;
--
-- ***********************************************************************************
--
--
-- Validation checks required
--
-- ***********************************************************************************
--
-- Income Detail Request Reference has been supplied and doesn't already exist
--
          IF (p1.lindr_reference IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',327);
          ELSE
--
             l_indr_ref_exists := NULL;
--
              OPEN chk_indr_ref_exists(p1.lindr_reference);
             FETCH chk_indr_ref_exists INTO l_indr_ref_exists;
             CLOSE chk_indr_ref_exists;
--
             IF (l_indr_ref_exists IS NOT NULL) THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',328);
             END IF;
--
          END IF;
--
-- ***********************************************************************************
--
-- Income Detail Request Type must supplied and valid
--
          IF (p1.lindr_type IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',592);
--
          ELSIF (p1.lindr_hrv_idty_code IN ('PIT', 'CUR') AND p1.lindr_type != 'DREQ') THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',593);
--
          ELSIF (p1.lindr_hrv_idty_code IN ('QTR', 'FYR') AND p1.lindr_type != 'PREQ') THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',594);
--
          END IF;
--
-- ***********************************************************************
--
-- Check Person Reference Value Type has been supplied and is valid
--
          IF (p1.lindr_par_reference_type IS NULL) THEN
--
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',580);
--
          ELSIF (p1.lindr_par_reference_type NOT IN ('PAR','PRF')) THEN
--
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',581);
--
          END IF;
--
-- ***********************************************************************
--
-- Check the Person Reference Value has been supplied and is valid
--
--
          IF (p1.lindr_par_reference IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',582);
          ELSE
--
             IF (p1.lindr_par_reference_type = 'PAR') THEN
--
               OPEN c_chk_par(p1.lindr_par_reference);
              FETCH c_chk_par INTO l_par_refno;
              CLOSE c_chk_par;
--
              IF (l_par_refno IS NULL) THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',583);
              END IF;
--
             ELSIF (p1.lindr_par_reference_type = 'PRF') THEN
--
                  OPEN c_chk_prf(p1.lindr_par_reference);
                 FETCH c_chk_prf INTO l_par_refno;
                 CLOSE c_chk_prf;
--
                 IF (l_par_refno IS NULL) THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',583);
                 END IF;
--
             END IF; 
--
          END IF; --(p1.LINDR_PAR_REFERENCE IS NULL)
--
-- ***********************************************************************************
--
-- Validate Status Code has been supplied and is valid
--
          IF (p1.lindr_sco_code IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',587);
--
          ELSIF (p1.lindr_sco_code NOT IN ('RAI','SEN','UNA','COM','TBC','CAN','ERR','CRS','RNC','CAF','PER')) THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',36);
--
          END IF;
--
-- ***********************************************************************************
--
-- Status Date has been supplied
--
          IF (p1.lindr_status_date IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',16);
          END IF;
--
-- ***********************************************************************************
--
-- Request Type must be valid as an Income Detail Type
--
          IF (NOT s_dl_hem_utils.exists_frv('INCOMEDETAILTYPE',p1.lindr_hrv_idty_code,'N')) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',27);
          END IF;
--
-- ***********************************************************************************
--
-- Partner Indicator must only be supplied if Request Status is COM
--
          IF (p1.lindr_partner_ind IS NOT NULL) THEN
--
           IF (p1.lindr_partner_ind NOT IN ('Y', 'N')) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',37);
           END IF;
--
           IF (p1.lindr_sco_code != 'COM') THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',588);
           END IF; 
--
          END IF;
--
-- ***********************************************************************************
--
-- Check the Legacy Reference Type is supplied and valid
--
          IF (p1.lindr_legacy_reference_type IS NULL ) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',28);
--
          ELSIF (p1.lindr_legacy_reference_type NOT IN ( 'APP', 'ALR', 'TCY', 'TAR', 'SURV', 'ACAS' )) THEN
              l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',28);
          END IF;
--
-- ***********************************************************************************
--
-- Work out what the legacy reference should be
--
          IF (p1.lindr_legacy_reference_type = 'APP') THEN
--
            OPEN c_chk_app(p1.lindr_legacy_reference);
           FETCH c_chk_app INTO l_app_refno;
--
           IF (c_chk_app%NOTFOUND) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',29);
           END IF;
--
           CLOSE c_chk_app;
--
          ELSIF (p1.lindr_legacy_reference_type = 'ALR') THEN
--
               OPEN c_chk_alr(p1.lindr_legacy_reference);
              FETCH c_chk_alr INTO l_app_refno;
--
              IF (c_chk_alr%NOTFOUND) THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',29);
              END IF;
--
              CLOSE c_chk_alr;
--
          ELSIF (p1.lindr_legacy_reference_type = 'TCY') THEN
--
               OPEN c_chk_tcy(p1.lindr_legacy_reference);
              FETCH c_chk_tcy INTO l_tcy_refno;
--
              IF (c_chk_tcy%NOTFOUND) THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',30);
              END IF;
--
              CLOSE c_chk_tcy;
--
          ELSIF (p1.lindr_legacy_reference_type = 'TAR') THEN
--
               OPEN c_chk_tar(p1.lindr_legacy_reference);
              FETCH c_chk_tar INTO l_tcy_refno;
--
              IF (c_chk_tar%NOTFOUND) THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',30);
              END IF;
--
              CLOSE c_chk_tar;
--
          ELSIF (p1.lindr_legacy_reference_type = 'SURV') THEN
--
               OPEN c_chk_surv(p1.lindr_legacy_reference);
              FETCH c_chk_surv INTO l_surv_refno;
--
              IF (c_chk_surv%NOTFOUND) THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',33);
              END IF;
--
              CLOSE c_chk_surv;
--
          ELSIF (p1.lindr_legacy_reference_type = 'ACAS') THEN
--
               OPEN c_chk_acas(p1.lindr_legacy_reference);
              FETCH c_chk_acas INTO l_acas_reference;
--
              IF (c_chk_acas%NOTFOUND) THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',34);
              END IF;
--
              CLOSE c_chk_acas;
--
          END IF;
--
-- ***********************************************************************************
--
-- Number of Children must only be supplied if Request Status is COM
--
          IF (p1.lindr_num_of_children IS NOT NULL) THEN
--
           IF (p1.lindr_sco_code != 'COM') THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',589);
           END IF;  
--
          END IF;
--
-- ***********************************************************************************
--
-- Other Validation Checks
--
-- Request Date must be supplied if Request Type is PIT
--
          IF (p1.lindr_hrv_idty_code = 'PIT' AND p1.lindr_request_date IS NULL) THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',38);
          END IF;
--
-- ***********************************************************************************
--
-- Request Date must only be supplied if Request Type is PIT
--
          IF (p1.lindr_request_date IS NOT NULL AND p1.lindr_hrv_idty_code != 'PIT') THEN
           l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',038);
          END IF;
--
-- ***********************************************************************************
--
-- If supplied the Financial Period Code must exist in ICS Financial Periods
--
          IF (p1.lindr_ifp_code IS NOT NULL) THEN
--
            OPEN c_chk_ifp_code(p1.lindr_ifp_code);
           FETCH c_chk_ifp_code INTO l_ifp_exists;
--
           IF (c_chk_ifp_code%NOTFOUND) THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',35);
           END IF;
--
           CLOSE c_chk_ifp_code;
--
          END IF;
--
-- ***********************************************************************************
--
-- Financial Period Code must be supplied if Request Type is FYR/QTR
--
          IF (p1.lindr_hrv_idty_code IN ('FYR', 'QTR') AND p1.lindr_ifp_code IS NULL) THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',39);
          END IF;
--
-- ***********************************************************************************
--
-- Financial Period Code must only be supplied if Request Type is FYR/QTR
--
          IF (p1.lindr_ifp_code IS NOT NULL AND p1.lindr_hrv_idty_code NOT IN ('FYR', 'QTR')) THEN
             l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',591);
          END IF;
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
         IF (l_errors = 'F') THEN
          l_error_ind := 'Y';
         ELSE
            l_error_ind := 'N';
         END IF;
--
         s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
         set_record_status_flag(l_id,l_errors);
--
-- keep a count of the rows processed and commit after every 1000
--
         i := i+1; 
--
         IF MOD(i,1000)=0 THEN 
          COMMIT; 
         END IF;
--
         EXCEPTION
              WHEN OTHERS THEN
              ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
              s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
              set_record_status_flag(l_id,'O');
--
      END;
--
    END LOOP;
--
    fsc_utils.proc_END;
--
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
-- 
END dataload_validate;
--
--
-- ***********************************************************************
--
PROCEDURE dataload_delete(p_batch_id IN VARCHAR2,
                          p_date     IN date) 
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       lindr_dlb_batch_id,
       lindr_dl_seqno,
       lindr_dl_load_status,
       lindr_reference
  FROM dl_hem_income_detail_reqs
 WHERE lindr_dlb_batch_id   = p_batch_id
   AND lindr_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HEM_INCOME_DETAIL_REQS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     	ROWID;
l_an_tab	VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_income_detail_reqs.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_income_detail_reqs.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lindr_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from table
--
          DELETE 
            FROM income_detail_requests
           WHERE indr_refno = p1.lindr_reference;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          IF mod(i,5000) = 0 THEN 
           COMMIT; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
                  set_record_status_flag(l_id,'C');
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_DETAIL_REQUESTS');
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
END s_dl_hem_income_detail_reqs;
/