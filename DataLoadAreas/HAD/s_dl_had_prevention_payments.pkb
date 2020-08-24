--
CREATE OR REPLACE PACKAGE BODY s_dl_had_prevention_payments
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    IR   24-FEB-2009  Initial Creation.
--
--  2.0     5.15.0    VS   17-APR-2009  Tidy up CREATE process, add
--                                      if null checks.    
--
--  3.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      PPYT_BR_I in CREATE Process
--
--  4.0     5.15.0    VS   17-FEB-2010  Add TO_CHAR to cursors to make sure
--                                      indexes are used correctly
--
--                                      Changed commit 500000 to 50000
--
--  5.0     5.15.0    VS   08-MAR-2010  Defect 3665 Fix. Party Reference
--                                      not being populated.
--
--  5.1     6.13.0    AJ   23-FEB-2016  added LPPYT_PAR_PER_ALT_IND and LPPYT_LAND_PAR_PER_ALT_IND
--                                      in create and validate so par_refno and par_per_alt_ref
--                                      can be used for Person or Landlord Alternative Reference
--
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
  UPDATE dl_had_prevention_payments
     SET lppyt_dl_load_status = p_status
   WHERE rowid                = p_rowid;
  --
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_had_prevention_payments');
          RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT ROWID REC_ROWID,
       LPPYT_DLB_BATCH_ID,
       LPPYT_DL_SEQNO,
       LPPYT_DL_LOAD_STATUS,
       LPPYT_ACAS_ALTERNATE_REF,
       LPPYT_PAYMENT_AMOUNT,
       LPPYT_PAYMENT_DATE,
       LPPYT_PPTP_CODE,
       LPPYT_PAYEE_TYPE,
       LPPYT_HRV_HHPF_CODE,
       LPPYT_HRV_HPPM_CODE,
       LPPYT_SCO_CODE,
       NVL(LPPYT_CREATED_BY,'DATALOAD') LPPYT_CREATED_BY,
       NVL(LPPYT_CREATED_DATE,SYSDATE)  LPPYT_CREATED_DATE,
       LPPYT_STATUS_DATE,
       LPPYT_PAR_PER_ALT_REF,
       LPPYT_PAR_ORG_ALT_REF,
       LPPYT_ACPE_PAR_PER_ALT_REF,
       LPPYT_LAND_PAR_PER_ALT_REF,
       LPPYT_COMMENTS,
       LPPYT_ALTERNATIVE_REFERENCE,
       LPPYT_PAY_REF,
       LPPYT_TRA_EFFECTIVE_DATE,
       LPPYT_TRA_TRT_CODE,
       LPPYT_TRA_TRS_CODE,
       LPPYT_AUTHORISED_DATE,
       LPPYT_AUTHORISED_BY,
       LPPYT_HRV_HPCR_CODE,
       LPPYT_REVIEW_DATE,
       LPPYT_ACHO_LEGACY_REF,
       LPPYT_AUN_CODE,
       LPPYT_IPP_SHORTNAME,
       LPPYT_REFNO,
       LPPYT_PAR_PER_ALT_IND,
       LPPYT_LAND_PAR_PER_ALT_IND
  FROM dl_had_prevention_payments
 WHERE lppyt_dlb_batch_id   = p_batch_id
   AND lppyt_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR get_acas_ref(p_acas_reference VARCHAR2)
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_acas_reference);
--
-- ***********************************************************************
--
CURSOR get_ipp_ref(p_ipp_shortname VARCHAR2)
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname;
--
-- ***********************************************************************
--
CURSOR get_par_ref(p_par_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR get_acho_ref(P_acho_alt_ref  VARCHAR2)
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = TO_CHAR(p_acho_alt_ref);
--
-- ***********************************************************************
--
CURSOR get_rac_ref(p_rac_pay_ref   VARCHAR2)
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_rac_pay_ref;
--
-- ***********************************************************************
--
CURSOR get_org_ref(p_org_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_org_short_name = p_org_alt_ref;
--
-- ***********************************************************************
--
CURSOR get_acpe_ref(p_acpe_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_acpe_alt_ref;
--
-- ***********************************************************************
--
CURSOR get_land_ref(p_land_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_land_alt_ref;
--
--
-- ***********************************************************************
--
CURSOR get_par_alt_ref(p_par_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR get_land_par_ref(p_land_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_land_alt_ref;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HAD_PREVENTION_PAYMENTS';
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
i                 INTEGER := 0;
l_exists          VARCHAR2(1);
l_acas_ref        NUMBER(10);
l_ipp_ref         NUMBER(10);
l_par_ref         NUMBER(10);
l_acho_ref        NUMBER(10);
l_rac_ref         NUMBER(10);
l_org_ref         NUMBER(10);
l_acpe_ref        NUMBER(10);
l_land_ref        NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger PPYT_BR_I disable';
--
    fsc_utils.proc_start('s_dl_had_prevention_payments.dataload_create');
    fsc_utils.debug_message('s_dl_had_prevention_payments.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lppyt_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
          l_acas_ref := NULL;
          l_ipp_ref  := NULL;
          l_par_ref  := NULL;
          l_acho_ref := NULL;
          l_rac_ref  := NULL;
          l_org_ref  := NULL;
          l_acpe_ref := NULL;
          l_land_ref := NULL;
--
-- Main processing
--
--
-- Get acho_reference
--
          IF (p1.lppyt_acas_alternate_ref IS NOT NULL) THEN
--
            OPEN get_acas_ref(p1.lppyt_acas_alternate_ref);
           FETCH get_acas_ref INTO l_acas_ref;
           CLOSE get_acas_ref;
--
          END IF;
--
--
-- Get ipp_refno
--
--
          IF (p1.lppyt_ipp_shortname IS NOT NULL) THEN
--
            OPEN get_ipp_ref(p1.lppyt_ipp_shortname);
           FETCH get_ipp_ref INTO l_ipp_ref;
           CLOSE get_ipp_ref;
--
          END IF;
--
--
-- Get par_refno
--
--        
          IF (p1.lppyt_par_per_alt_ref IS NOT NULL) THEN
--
            IF (p1.lppyt_par_per_alt_ind = 'P')
              THEN
               OPEN get_par_ref(p1.lppyt_par_per_alt_ref);
              FETCH get_par_ref INTO l_par_ref;
              CLOSE get_par_ref;
            END IF;
--
            IF (p1.lppyt_par_per_alt_ind = 'A')
              THEN
               OPEN get_par_alt_ref(p1.lppyt_par_per_alt_ref);
              FETCH get_par_alt_ref INTO l_par_ref;
              CLOSE get_par_alt_ref;
            END IF;
--
          END IF;
--
--
-- Get acho_reference
--
--
          IF (p1.lppyt_acho_legacy_ref IS NOT NULL) THEN
--
            OPEN get_acho_ref(p1.lppyt_acho_legacy_ref);
           FETCH get_acho_ref INTO l_acho_ref;
           CLOSE get_acho_ref;
--
          END IF;
--
--
-- Get rac_accno
--
--
          IF (p1.lppyt_pay_ref IS NOT NULL) THEN
--
            OPEN get_rac_ref(p1.lppyt_pay_ref);
           FETCH get_rac_ref INTO l_rac_ref;
           CLOSE get_rac_ref;
--
          END IF;
--
-- Get org_ref
--
--
          IF (p1.lppyt_par_org_alt_ref IS NOT NULL) THEN
--
            OPEN get_org_ref(p1.lppyt_par_org_alt_ref);
           FETCH get_org_ref INTO l_org_ref;
           CLOSE get_org_ref; 
--
          END IF;
--
--
-- Get acpe_ref
--
--
          IF (p1.lppyt_acpe_par_per_alt_ref IS NOT NULL) THEN
--
            OPEN get_acpe_ref(p1.lppyt_acpe_par_per_alt_ref);
           FETCH get_acpe_ref INTO l_acpe_ref;
           CLOSE get_acpe_ref;
--
          END IF;
--
--
-- Get land_ref
--
--
          IF (p1.lppyt_land_par_per_alt_ref IS NOT NULL) THEN
--
           IF (p1.lppyt_land_par_per_alt_ind = 'A')
            THEN
              OPEN get_land_ref(p1.lppyt_land_par_per_alt_ref);
             FETCH get_land_ref INTO l_land_ref;
             CLOSE get_land_ref;
           END IF;
--
           IF (p1.lppyt_land_par_per_alt_ind = 'P')
            THEN
              OPEN get_land_par_ref(p1.lppyt_land_par_per_alt_ref);
             FETCH get_land_par_ref INTO l_land_ref;
             CLOSE get_land_par_ref;
           END IF;
--
          END IF;
--
--
--
-- Insert into relevent table
--
--
-- Insert into PREVENTION_PAYMENTS
--
--
INSERT /* +APPEND */ INTO PREVENTION_PAYMENTS(PPYT_REFNO,
                                PPYT_ACAS_REFERENCE,
                                PPYT_PAYMENT_AMOUNT,
                                PPYT_PAYMENT_DATE,
                                PPYT_PPTP_CODE,
                                PPYT_PAYEE_TYPE,
                                PPYT_HRV_HHPF_CODE,
                                PPYT_HRV_HPPM_CODE,
                                PPYT_REUSABLE_REFNO,
                                PPYT_SCO_CODE,
                                PPYT_CREATED_DATE,
                                PPYT_CREATED_BY,
                                PPYT_STATUS_DATE,
                                PPYT_PER_PAR_REFNO,
                                PPYT_ORG_PAR_REFNO,
                                PPYT_ACPE_ACAS_REFERENCE,
                                PPYT_ACPE_PAR_REFNO,
                                PPYT_PLD_REFNO,
                                PPYT_COMMENTS, 
                                PPYT_ALTERNATIVE_REFERENCE,
                                PPYT_RAC_ACCNO,
                                PPYT_TRA_REFNO,
                                PPYT_AUTHORISED_DATE,
                                PPYT_AUTHORISED_BY,
                                PPYT_HRV_HPCR_CODE,
                                PPYT_REVIEW_DATE,
                                PPYT_ACHO_REFERENCE,
                                PPYT_AUN_CODE,
                                PPYT_IPP_REFNO 
                           )
--
                        VALUES (P1.LPPYT_REFNO,
                                l_acas_ref,
                                P1.LPPYT_PAYMENT_AMOUNT,
                                P1.LPPYT_PAYMENT_DATE,
                                P1.LPPYT_PPTP_CODE,
                                P1.LPPYT_PAYEE_TYPE,
                                P1.LPPYT_HRV_HHPF_CODE,
                                P1.LPPYT_HRV_HPPM_CODE,
                                reusable_refno_seq.nextval,
                                P1.LPPYT_SCO_CODE,
                                P1.LPPYT_CREATED_DATE,
                                P1.LPPYT_CREATED_BY,
                                P1.LPPYT_STATUS_DATE,
                                l_par_ref,
                                l_org_ref,
                                l_acas_ref,
                                l_acpe_ref,
                                l_land_ref,
                                P1.LPPYT_COMMENTS,
                                P1.LPPYT_ALTERNATIVE_REFERENCE,
                                l_rac_ref,
                                NULL,
                                P1.LPPYT_AUTHORISED_DATE,
                                P1.LPPYT_AUTHORISED_BY,
                                P1.LPPYT_HRV_HPCR_CODE,
                                P1.LPPYT_REVIEW_DATE,
                                l_acho_ref,
                                P1.LPPYT_AUN_CODE,
                                l_ipp_ref
                           );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
   i := i+1; 
--
   IF MOD(i,50000)=0 THEN 
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('PREVENTION_PAYMENTS');
--
   execute immediate 'alter trigger PPYT_BR_I enable';
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
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT ROWID REC_ROWID,
       LPPYT_DLB_BATCH_ID,
       LPPYT_DL_SEQNO,
       LPPYT_DL_LOAD_STATUS,
       LPPYT_ACAS_ALTERNATE_REF,
       LPPYT_PAYMENT_AMOUNT,
       LPPYT_PAYMENT_DATE,
       LPPYT_PPTP_CODE,
       LPPYT_PAYEE_TYPE,
       LPPYT_HRV_HHPF_CODE,
       LPPYT_HRV_HPPM_CODE,
       LPPYT_SCO_CODE,
       LPPYT_CREATED_BY,
       LPPYT_CREATED_DATE,SYSDATE,
       LPPYT_STATUS_DATE,
       LPPYT_PAR_PER_ALT_REF,
       LPPYT_PAR_ORG_ALT_REF,
       LPPYT_ACPE_PAR_PER_ALT_REF,
       LPPYT_LAND_PAR_PER_ALT_REF,
       LPPYT_COMMENTS,
       LPPYT_ALTERNATIVE_REFERENCE,
       LPPYT_PAY_REF,
       LPPYT_TRA_EFFECTIVE_DATE,
       LPPYT_TRA_TRT_CODE,
       LPPYT_TRA_TRS_CODE,
       LPPYT_AUTHORISED_DATE,
       LPPYT_AUTHORISED_BY,
       LPPYT_HRV_HPCR_CODE,
       LPPYT_REVIEW_DATE,
       LPPYT_ACHO_LEGACY_REF,
       LPPYT_AUN_CODE,
       LPPYT_IPP_SHORTNAME,
       LPPYT_REFNO,
       LPPYT_PAR_PER_ALT_IND,
       LPPYT_LAND_PAR_PER_ALT_IND
  FROM dl_had_prevention_payments
 WHERE lppyt_dlb_batch_id   = p_batch_id
   AND lppyt_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR chk_acho_exists(p_acho_reference VARCHAR2) 
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = TO_CHAR(p_acho_reference);
--
--
-- ***********************************************************************
--
CURSOR chk_aun_exists(p_aun_code VARCHAR2 )
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_aun_code ;
--
-- ***********************************************************************
--
CURSOR chk_acas_exists(p_acas_reference VARCHAR2 )
IS
SELECT 'X'
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_acas_reference);
--
-- ***********************************************************************
--
CURSOR chk_par_exists(p_par_alt_ref VARCHAR2)
IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR chk_par_org_exists(p_org_short_name VARCHAR2)
IS
SELECT 'X'
  FROM parties
 WHERE par_org_short_name = TO_CHAR(p_org_short_name);
--
-- ***********************************************************************
--
CURSOR chk_ipp_exists(p_ipp_reference VARCHAR2 )
IS
SELECT 'X'
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_reference;
--
-- ***********************************************************************
--
CURSOR chk_sco_exists(p_sco_code VARCHAR2 )
IS
SELECT 'X'
  FROM status_codes
 WHERE sco_code = p_sco_code;
--
-- ***********************************************************************
--
CURSOR chk_ptp_exists(p_ptp_code VARCHAR2 )
IS
SELECT 'X'
  FROM prevention_payment_types
 WHERE pptp_code = p_ptp_code;
--
-- ***********************************************************************
--
CURSOR chk_par_alt_exists(p_par_alt_ref VARCHAR2)
IS
SELECT 'X'
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR chk_land_par_alt_exists(p_par_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
CURSOR chk_land_par_exists(p_land_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_land_alt_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HAD_PREVENTION_PAYMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         	VARCHAR2(1);
l_acho_reference       	NUMBER(10);
l_aun_exists            VARCHAR2(1);
l_acas_exists           VARCHAR2(1);
l_par_exists            VARCHAR2(1);
l_par_org_exists        VARCHAR2(1);
l_ipp_exists            VARCHAR2(1);
l_sco_exists            VARCHAR2(1);
l_ptp_exists            VARCHAR2(1);
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_prevention_payments.dataload_validate');
    fsc_utils.debug_message('s_dl_had_prevention_payments.dataload_validate',3);
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
          cs   := p1.lppyt_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check Housing Options Reference LACHH_ACHO_LEGACY_REF has been supplied
-- and valid
--
--  
          IF (p1.lppyt_acho_legacy_ref IS NOT NULL) THEN
--
             l_acho_reference := NULL;
--
              OPEN chk_acho_exists(p1.lppyt_acho_legacy_ref);
             FETCH chk_acho_exists INTO l_acho_reference;
             CLOSE chk_acho_exists;
--
             IF (l_acho_reference IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',185);
             END IF;
--
          END IF;
--
-- ***********
--
-- Check Admin Unit code is valid
--
          IF (p1.lppyt_aun_code IS NOT NULL ) THEN
--
           l_aun_exists :=NULL;
--
            OPEN chk_aun_exists (p1.lppyt_aun_code );
           FETCH chk_aun_exists into l_aun_exists;
           CLOSE chk_aun_exists;
--
           IF (l_aun_exists IS NULL ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',127);
           END IF;
--
         END IF;

--
-- ****************
--
-- Check Advise case
--
          l_acas_exists :=NULL;
--
          IF (p1.lppyt_acas_alternate_ref IS NOT NULL) THEN   
--      
            OPEN chk_acas_exists (p1.lppyt_acas_alternate_ref);
           FETCH chk_acas_exists into l_acas_exists;
           CLOSE chk_acas_exists;
--
           IF (l_acas_exists IS NULL ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',34);
           END IF;
--
          END IF;
--
-- ***************
--
-- Check Party exists
--
          IF (p1.lppyt_par_per_alt_ref IS NOT NULL ) THEN
--
           IF (p1.lppyt_par_per_alt_ind IS NULL ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',849);
           END IF;
--           
           IF (p1.lppyt_par_per_alt_ind IS NOT NULL ) THEN
              IF (p1.lppyt_par_per_alt_ind NOT IN ('P','A')) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',850);
              END IF;
           END IF;
--		   
           l_par_exists :=NULL;
--
           IF nvl(p1.lppyt_par_per_alt_ind,'N') = 'P'
            THEN
             OPEN chk_par_exists (p1.lppyt_par_per_alt_ref);
            FETCH chk_par_exists into l_par_exists;
            CLOSE chk_par_exists;
           END IF;
--
           IF nvl(p1.lppyt_par_per_alt_ind,'N') = 'A'
            THEN
             OPEN chk_par_alt_exists (p1.lppyt_par_per_alt_ref);
            FETCH chk_par_alt_exists into l_par_exists;
            CLOSE chk_par_alt_exists;
           END IF;
--
           IF (l_par_exists IS NULL ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',331);
           END IF;
--
          END IF;
--
-- ***************
--
-- Check Land Party exists
--
          IF (p1.lppyt_land_par_per_alt_ref IS NOT NULL ) THEN
--
           IF (p1.lppyt_land_par_per_alt_ind IS NULL ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',851);
           END IF;
--           
           IF (p1.lppyt_land_par_per_alt_ind IS NOT NULL ) THEN
              IF (p1.lppyt_land_par_per_alt_ind NOT IN ('P','A')) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',850);
              END IF;
           END IF;
--		   
           l_par_exists :=NULL;
--
           IF (p1.lppyt_land_par_per_alt_ind = 'A')
            THEN
              OPEN chk_land_par_alt_exists (p1.lppyt_land_par_per_alt_ref);
             FETCH chk_land_par_alt_exists INTO l_par_exists;
             CLOSE chk_land_par_alt_exists;
           END IF;
--
           IF (p1.lppyt_land_par_per_alt_ind = 'P')
            THEN
              OPEN chk_land_par_exists (p1.lppyt_land_par_per_alt_ref);
             FETCH chk_land_par_exists INTO l_par_exists;
             CLOSE chk_land_par_exists;
           END IF;
--
           IF (l_par_exists IS NULL ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',331);
           END IF;
--
          END IF;
--
-- ***************
--
-- Check Party Organisation exists
--
          IF (p1.lppyt_par_org_alt_ref IS NOT NULL ) THEN
--
           l_par_org_exists :=NULL;
--
            OPEN chk_par_org_exists (p1.lppyt_par_org_alt_ref);
           FETCH chk_par_org_exists into l_par_org_exists;
           CLOSE chk_par_org_exists;
--
           IF (l_par_org_exists IS NULL ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',332);
           END IF;
--
          END IF;
--
-- ***************
--
-- Check interested Party exists
--
          IF (p1.lppyt_ipp_shortname IS NOT NULL ) THEN
--
           l_ipp_exists :=NULL;
--
            OPEN chk_ipp_exists (p1.lppyt_ipp_shortname);
           FETCH chk_ipp_exists into l_ipp_exists;
           CLOSE chk_ipp_exists;
--
           IF (l_ipp_exists IS NULL ) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',246);
           END IF;
--
          END IF;
--
-- ***************
--
-- Check Payee Type is valid
--
          IF (p1.lppyt_payee_type NOT IN ('CLIENT','LANDLORD','OTHER PERSON'
                                         ,'ORGANISATION','INTERESTED PARTY')) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',247);
--
          END IF;
--
-- ***************
--
-- Check Status Code exists and is valid
--
          IF (p1.lppyt_sco_code IS NULL ) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',248);
--
          ELSE
--
              OPEN chk_sco_exists (p1.lppyt_sco_code );
             FETCH chk_sco_exists into l_sco_exists;
             CLOSE chk_sco_exists;
--
             IF (l_sco_exists IS NULL ) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',249);
             END IF;
--
          END IF;
-- 
-- ***************
--
-- Check Payment type Code exists and is valid
--
          IF (p1.lppyt_pptp_code IS NULL ) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',250);
--
          ELSE
--
              OPEN chk_ptp_exists (p1.lppyt_pptp_code );
             FETCH chk_ptp_exists into l_ptp_exists;
             CLOSE chk_ptp_exists;
--
             IF (l_ptp_exists IS NULL ) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',251);
             END IF;
--
          END IF;
-- 
-- ***************
--  
-- Check Payment Amount is supplied
--
          IF (p1.lppyt_payment_amount IS NULL ) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',252); 
          END IF;
-- 
-- ***************
--
-- Check Payment date is supplied
--
          IF (p1.lppyt_payment_date IS NULL ) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',253); 
          END IF;
-- 
-- ***************
--
-- Check Status date is supplied
--
          IF (p1.lppyt_status_date IS NULL ) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',254); 
          END IF;   
-- 
-- ***************
--  
-- Check Homeless prevention fund code LPPYT_HRV_HHPF_CODE is valid if supplied
--
          IF (p1.LPPYT_HRV_HHPF_CODE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',255); 
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('HLESSPREVFUN',p1.lppyt_hrv_hhpf_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',256);
--
          END IF;     
-- 
-- ***************
--
-- Check Homeless prevention payment method LPPYT_HRV_HPPM_CODE is valid if supplied
--
          IF (p1.LPPYT_HRV_HPPM_CODE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',257);
--
           IF (NOT s_dl_hem_utils.exists_frv('HLESSPREVPAYMTHD',p1.lppyt_hrv_hppm_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',258);
           END IF;
--
          END IF;  
-- 
-- ***************
--
-- Cancel reason code LPPYT_HRV_HPCR_CODE is valid if supplied
--
          IF (p1.LPPYT_HRV_HPCR_CODE IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('PREV_PAY_CANCEL_RSN',p1.lppyt_hrv_hpcr_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',259);
           END IF;
--
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
PROCEDURE dataload_delete(p_batch_id       IN VARCHAR2,
                          p_date           IN date) IS
--
CURSOR c1 is
SELECT ROWID REC_ROWID,
       LPPYT_DLB_BATCH_ID,
       LPPYT_DL_SEQNO,
       LPPYT_DL_LOAD_STATUS,
       LPPYT_REFNO
  FROM dl_had_prevention_payments
 WHERE lppyt_dlb_batch_id   = p_batch_id
   AND lppyt_dl_load_status = 'C';
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
cb         VARCHAR2(30);
cd         DATE;
cp         VARCHAR2(30) := 'DELETE';
ct         VARCHAR2(30) := 'DL_HAD_PREVENTION_PAYMENTS';
cs         INTEGER;
ce         VARCHAR2(200);
l_id       ROWID;
l_an_tab   VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_had_prevention_payments.dataload_delete');
    fsc_utils.debug_message('s_dl_had_prevention_payments.dataload_delete',3 );
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
          cs   := p1.lppyt_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
--
-- Delete from bonds table
--
          DELETE 
            FROM prevention_payments
           WHERE ppyt_refno = p1.lppyt_refno;
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
          IF MOD(i,5000) = 0 THEN 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PREVENTION_PAYMENTS');
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
END s_dl_had_prevention_payments;
/
