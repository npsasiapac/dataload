--
CREATE OR REPLACE PACKAGE BODY s_dl_had_advice_case_hsg_optn
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   16-JAN-2009  Initial Creation.
--
--
--  2.0     5.15.0    VS   27-APR-2009  Add additional validation to 
--                                      overcome CREATE constraint failure
--                                      on ACHO_HOAU_FK. Acho_hoop_code and
--                                      acho_hoau_aun_code combination must 
--                                      exist in housing_option_admin_units 
--                                      table.
--
--  3.0     5.16.0    IR   21-SEP-2009  Added SAS Indicator and additional code
--
--  4.0     5.16.0    VS   09-OCT-2009  SAS Indicator validation HD2 316 only 
--                                      required if SAS Indicator is 'Y'. 
--                                      Defect ID 2387.
--
--  5.0     5.16.0    VS   30-NOV-2009  Revenue Account Payment Reference 
--                                      required for SAS housing options
--                                      Defect ID 2751.
--
--  6.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      ACHO_BR_I in CREATE Process
--
--  7.0     5.15.0    VS   11-FEB-2010  Defect 3501 Fix. Add 2 more columns
--                                      acho_start_date and acho_next_pay_period_start
--
--  8.0     5.15.0    VS   17-FEB-2010  Add TO_CHAR to cursors to make sure
--                                      indexes are used correctly
--
--                                      Changed commit 500000 to 50000
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
  UPDATE dl_had_advice_case_hsg_optn
  SET    lacho_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
       dbms_output.put_line('Error updating status of dl_had_advice_case_hsg_optn');
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
       LACHO_DLB_BATCH_ID,
       LACHO_DL_SEQNO,
       LACHO_DL_LOAD_STATUS,
       LACHO_LEGACY_REF,
       LACHO_ACAS_ALTERNATE_REF,
       LACHO_ARCS_ARSN_CODE,
       LACHO_HOOP_CODE,
       LACHO_HODS_HRV_DEST_CODE,
       LACHO_STATUS_DATE,
       LACHO_HOAU_AUN_CODE,
       NVL(LACHO_CREATED_BY, 'DATALOAD') LACHO_CREATED_BY,
       NVL(LACHO_CREATED_DATE, SYSDATE)  LACHO_CREATED_DATE,
       LACHO_COMMENTS,
       LACHO_SAS_IND,
       LACHO_RAC_PAY_REF,
       LACHO_START_DATE,
       LACHO_NEXT_PAY_PERIOD_START,
       LACHO_REFERENCE
  FROM dl_had_advice_case_hsg_optn
 WHERE lacho_dlb_batch_id   = p_batch_id
   AND lacho_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR get_acas_reference(p_acas_alt_reference VARCHAR2)
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_acas_alt_reference);
--
-- ***********************************************************************
--
CURSOR get_hou_refno(p_acas_reference    VARCHAR2,
                     p_acho_reference    VARCHAR2) 
IS
SELECT lhou_refno
  FROM dl_had_households
 WHERE lhou_acas_alternate_ref = p_acas_reference
   AND lhou_acho_alternate_ref = p_acho_reference
   AND lhou_dl_load_status = 'C';
--
-- ***********************************************************************
--
CURSOR get_rac_accno(p_rac_pay_ref    VARCHAR2) 
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_rac_pay_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_HSG_OPTN';
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
l_acas_reference  NUMBER(10);
l_hou_refno       NUMBER(10);
l_rac_accno       NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger ACHO_BR_I disable';
--
    fsc_utils.proc_start('s_dl_had_advice_case_hsg_optn.dataload_create');
    fsc_utils.debug_message('s_dl_had_advice_case_hsg_optn.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lacho_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Get hou_refno
--
          l_hou_refno := NULL;
--
          IF (p1.lacho_sas_ind = 'Y') THEN
--
            OPEN get_hou_refno (p1.lacho_acas_alternate_ref, p1.lacho_legacy_ref);
           FETCH get_hou_refno into l_hou_refno;
           CLOSE get_hou_refno;
--
          END IF;
--
-- Get acas_reference
--
          l_acas_reference := NULL;
--
           OPEN get_acas_reference(p1.lacho_acas_alternate_ref);
          FETCH get_acas_reference INTO l_acas_reference;
          CLOSE get_acas_reference;
--
-- Get rac_accno
--
          l_rac_accno := NULL;
--
          IF (p1.LACHO_RAC_PAY_REF IS NOT NULL) THEN
--
            OPEN get_rac_accno(p1.lacho_rac_pay_ref);
           FETCH get_rac_accno INTO l_rac_accno;
           CLOSE get_rac_accno;
--
          END IF;
--
-- Insert into relevent table
--
--
-- Insert into ADVICE_CASE_HOUSING_OPTIONS
--
--
          INSERT /* +APPEND */ INTO advice_case_housing_options(ACHO_REFERENCE,
                                                                ACHO_ACAS_REFERENCE,
                                                                ACHO_HOOP_CODE,
                                                                ACHO_HODS_HRV_DEST_CODE,
                                                                ACHO_STATUS_DATE,
                                                                ACHO_HOAU_AUN_CODE,
                                                                ACHO_CREATED_BY,
                                                                ACHO_CREATED_DATE, 
                                                                ACHO_COMMENTS,
                                                                ACHO_ALTERNATIVE_REFERENCE,
                                                                ACHO_HOU_REFNO,
                                                                ACHO_RAC_ACCNO,
                                                                ACHO_START_DATE,
                                                                ACHO_NEXT_PAY_PERIOD_START
                                                               )
--
                                                        VALUES (p1.lacho_reference,
                                                                l_acas_reference,
                                                                p1.lacho_hoop_code,
                                                                p1.LACHO_HODS_HRV_DEST_CODE,
                                                                p1.LACHO_STATUS_DATE,
                                                                p1.LACHO_HOAU_AUN_CODE,
                                                                p1.LACHO_CREATED_BY,
                                                                p1.LACHO_CREATED_DATE,
                                                                p1.LACHO_COMMENTS,
                                                                p1.LACHO_LEGACY_REF,
                                                                l_hou_refno,
                                                                l_rac_accno,
                                                                p1.LACHO_START_DATE,
                                                                p1.LACHO_NEXT_PAY_PERIOD_START
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_HOUSING_OPTIONS');
--
    execute immediate 'alter trigger ACHO_BR_I enable';
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
       LACHO_DLB_BATCH_ID,
       LACHO_DL_SEQNO,
       LACHO_DL_LOAD_STATUS,
       LACHO_LEGACY_REF,
       LACHO_ACAS_ALTERNATE_REF,
       LACHO_ARCS_ARSN_CODE,
       LACHO_HOOP_CODE,
       LACHO_HODS_HRV_DEST_CODE,
       LACHO_STATUS_DATE,
       LACHO_HOAU_AUN_CODE,
       NVL(LACHO_CREATED_BY, 'DATALOAD') LACHO_CREATED_BY,
       NVL(LACHO_CREATED_DATE, SYSDATE)  LACHO_CREATED_DATE,
       LACHO_SAS_IND,
       LACHO_RAC_PAY_REF,
       LACHO_START_DATE,
       LACHO_NEXT_PAY_PERIOD_START,
       LACHO_COMMENTS
  FROM dl_had_advice_case_hsg_optn
 WHERE lacho_dlb_batch_id    = p_batch_id
   AND lacho_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_acas_exists(p_alternate_reference VARCHAR2) 
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = TO_CHAR(p_alternate_reference);
--
--
-- ***********************************************************************
--
CURSOR chk_hou_exists(p_acas_reference VARCHAR2
                     ,p_acho_reference VARCHAR2 )
IS
SELECT 'X'
  FROM dl_had_households
 WHERE lhou_acas_alternate_ref = p_acas_reference
   AND lhou_acho_alternate_ref = p_acho_reference
   AND lhou_dl_load_status = 'C';
--
-- ***********************************************************************
--
CURSOR chk_arsn_exists(p_arsn_code VARCHAR2) 
IS
SELECT 'X'
  FROM advice_reasons
 WHERE arsn_code = p_arsn_code;
--
--
-- ***********************************************************************
--
CURSOR chk_hoop_exists(p_hoop_code VARCHAR2) 
IS
SELECT 'X'
  FROM housing_options
 WHERE hoop_code = p_hoop_code;
--
--
-- ***********************************************************************
--
CURSOR chk_hoop_hods_exists (p_hoop_code           VARCHAR2,
                             p_hods_hrv_dest_code  VARCHAR2) 
IS
SELECT 'X'
  FROM housing_option_delivery_status
 WHERE hods_hoop_code     = p_hoop_code
   AND hods_hrv_dest_code = p_hods_hrv_dest_code;
--
--
-- ***********************************************************************
--
CURSOR chk_acas_acrs_exists(p_acas_reference NUMBER, 
                            p_arsn_code      VARCHAR2)
IS
SELECT 'X'
  FROM advice_case_reasons
 WHERE acrs_acas_reference = p_acas_reference
   AND acrs_arsn_code      = p_arsn_code;
--
--
-- ***********************************************************************
--
CURSOR get_acas_aun_type
IS
SELECT TRIM(pva.pva_char_value)
  FROM parameter_values            pva,
       area_codes                  arc,
       parameter_definition_usages pdu
 WHERE pdu.pdu_pdf_param_type  = 'SYSTEM'
   AND arc.arc_pgp_refno       = pdu.pdu_pgp_refno
   AND pdu.pdu_pdf_name        = pva.pva_pdu_pdf_name
   AND pdu.pdu_pdf_param_type  = pva.pva_pdu_pdf_param_type
   AND pdu.pdu_pob_table_name  = pva.pva_pdu_pob_table_name
   AND pdu.pdu_pgp_refno       = pva.pva_pdu_pgp_refno
   AND pdu.pdu_display_seqno   = pva.pva_pdu_display_seqno
   AND pdu.pdu_pdf_name        = 'ADVCASE_AUN_TYPE';
--
--
-- ***********************************************************************
--
CURSOR chk_acas_aun_exists(p_aun_code VARCHAR2, 
                           p_aun_type VARCHAR2)
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code     = p_aun_code
   AND aun_auy_code = p_aun_type;
--
--
-- ***********************************************************************
--
CURSOR chk_hoau_exists(p_hoop_code VARCHAR2,
                       p_aun_code  VARCHAR2) 
IS
SELECT 'X'
  FROM housing_option_admin_units
 WHERE hoau_hoop_code = p_hoop_code
   AND hoau_aun_code  = p_aun_code;
--
-- ***********************************************************************
--
CURSOR chk_rac_exists(p_rac_pay_ref    VARCHAR2) 
IS
SELECT 'X'
  FROM revenue_accounts
 WHERE rac_pay_ref = p_rac_pay_ref;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_HSG_OPTN';
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
l_acas_reference       	NUMBER(10);
l_arsn_exists           VARCHAR2(1);
l_hoop_exists           VARCHAR2(1);
l_hoop_hods_exists      VARCHAR2(1);
l_hods_exists           VARCHAR2(1);
l_acas_acrs_exists      VARCHAR2(1);
l_acas_aun_type         VARCHAR2(255);
l_acas_aun_exists       VARCHAR2(1);
l_hoau_exists           VARCHAR2(1);
l_hou_exists            VARCHAR2(1);
l_rac_exists            VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_had_advice_case_hsg_optn.dataload_validate');
    fsc_utils.debug_message('s_dl_had_advice_case_hsg_optn.dataload_validate',3);
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
          cs   := p1.lacho_dl_seqno;
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
-- Check Housing Options Reference LACHO_LEGACY_REF has been supplied
--
--  
          IF (p1.lacho_legacy_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',157);
          END IF;
--
-- ***********
--
-- Check household reference exists on households table
-- 
--
          l_hou_exists := NULL;
--
          IF (p1.lacho_sas_ind = 'Y') THEN

            OPEN chk_hou_exists (p1.LACHO_ACAS_ALTERNATE_REF, p1.LACHO_LEGACY_REF);
           FETCH chk_hou_exists INTO l_hou_exists;
           CLOSE chk_hou_exists;
--
           IF (l_hou_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',316);
           END IF;
--
          END IF;
--
-- 
-- *************************************************************************
--
-- Check Advice Case Alt Reference LACHO_ACAS_ALTERNATE_REF has been supplied 
-- and exists on advice_cases.
--
--  
          IF (p1.lacho_acas_alternate_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
          ELSE
--
             l_acas_reference := NULL;
--
              OPEN chk_acas_exists(p1.lacho_acas_alternate_ref);
             FETCH chk_acas_exists INTO l_acas_reference;
             CLOSE chk_acas_exists;
--
             IF (l_acas_reference IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',144);
             END IF;
--            
          END IF;
--
-- ***********
--
-- Check Advice Case Reason Code LACHO_ARCS_ARSN_CODE is supplied and valid
-- 
--
          IF (p1.lacho_arcs_arsn_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',128);
--
          ELSE
--
             l_arsn_exists := NULL;
--
              OPEN chk_arsn_exists (p1.lacho_arcs_arsn_code);
             FETCH chk_arsn_exists INTO l_arsn_exists;
             CLOSE chk_arsn_exists;
--
             IF (l_arsn_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',129);
             END IF;
-- 
          END IF;
--
-- ***********
--
-- Check Housing Options Code LACHO_HOOP_CODE is supplied and valid
-- 
--
          IF (p1.lacho_hoop_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',158);
--
          ELSE
--
             l_hoop_exists := NULL;
--
              OPEN chk_hoop_exists (p1.lacho_hoop_code);
             FETCH chk_hoop_exists INTO l_hoop_exists;
             CLOSE chk_hoop_exists;
--
             IF (l_hoop_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',159);
             END IF;
-- 
          END IF;
--
-- ***********
--
-- Check Delivery Status LACHO_HODS_HRV_DEST_CODE is supplied and valid
--
          l_hods_exists := NULL;
--
          IF (p1.LACHO_HODS_HRV_DEST_CODE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',160);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('DELIVERYSTATUS',p1.lacho_hods_hrv_dest_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',161);
--
          ELSE
--
             l_hods_exists := 'X';
--
          END IF;
--
-- ***********
--
-- Check Status date LACHO_STATUS_DATE has been supplied
--
-- 
         IF (p1.lacho_status_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',162);
         END IF;
--
-- ***********
--
-- Check Case Admin Unit LACHO_HOAU_AUN_CODE is valid if supplied
-- 
-- The admin unit code supplied must be a current valid Admin Unit and 
-- be of the type matching the parameter ‘ADVCASE_AUN_TYPE’ – Admin Unit Type 
-- for Advice Cases.
--
          IF (p1.lacho_hoau_aun_code IS NOT NULL) THEN
--
           l_acas_aun_type := NULL;
--
            OPEN get_acas_aun_type;
           FETCH get_acas_aun_type INTO l_acas_aun_type;
           CLOSE get_acas_aun_type;
--
           IF (l_acas_aun_type IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',143);
-- 
           ELSE
--
              l_acas_aun_exists := NULL;
--
               OPEN chk_acas_aun_exists (p1.lacho_hoau_aun_code, l_acas_aun_type);
              FETCH chk_acas_aun_exists INTO l_acas_aun_exists;
              CLOSE chk_acas_aun_exists;
--
              IF (l_acas_aun_exists IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',127);
              END IF;
--
           END IF; 
--
          END IF;
--
-- ***********
--
-- The combination of Advice Case and Reason must exist on 
-- Advice Case Reason Table
-- 
--
          IF (    l_acas_reference IS NOT NULL
              AND l_arsn_exists    IS NOT NULL) THEN
--
           l_acas_acrs_exists := NULL;
--
            OPEN chk_acas_acrs_exists (l_acas_reference, p1.lacho_arcs_arsn_code);
           FETCH chk_acas_acrs_exists INTO l_acas_acrs_exists;
           CLOSE chk_acas_acrs_exists;
--
           IF (l_acas_acrs_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',163);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- There must be an entry on Housing Option Delivery Status table 
-- for the combination of Housing Option Code and Delivery Status Code. 
-- These must be current.
-- 
--
          IF (    l_hoop_exists IS NOT NULL
              AND l_hods_exists IS NOT NULL) THEN
--
           l_hoop_hods_exists := NULL;
--
            OPEN chk_hoop_hods_exists (p1.lacho_hoop_code, p1.lacho_hods_hrv_dest_code);
           FETCH chk_hoop_hods_exists INTO l_hoop_hods_exists;
           CLOSE chk_hoop_hods_exists;
--
           IF (l_hoop_hods_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',164);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- There must be an entry on Housing Option admin Units table 
-- for the combination of Housing Option Code and admin unit Code.
-- This is to detect CREATE constraint failure ACHO_HOAU_FK.
-- 
--
          IF (    l_hoop_exists     IS NOT NULL
              AND l_acas_aun_exists IS NOT NULL) THEN
--
           l_hoau_exists := NULL;
--
            OPEN chk_hoau_exists (p1.lacho_hoop_code, p1.lacho_hoau_aun_code);
           FETCH chk_hoau_exists INTO l_hoau_exists;
           CLOSE chk_hoau_exists;
--
           IF (l_hoau_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',270);
           END IF;
-- 
          END IF;
--
-- ***********
--
-- Check revenue_account payment reference exists on revenue accounts table
-- 
--
          l_rac_exists := NULL;
--
          IF (p1.lacho_rac_pay_ref IS NOT NULL) THEN

            OPEN chk_rac_exists(p1.LACHO_RAC_PAY_REF);
           FETCH chk_rac_exists INTO l_rac_exists;
           CLOSE chk_rac_exists;
--
           IF (l_rac_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',323);
           END IF;
--
          END IF;
--
--
-- ***********************************************************************
--
-- All reference values supplied are valid
-- 
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
       LACHO_DLB_BATCH_ID,
       LACHO_DL_SEQNO,
       LACHO_DL_LOAD_STATUS,
       LACHO_REFERENCE
  FROM dl_had_advice_case_hsg_optn
 WHERE lacho_dlb_batch_id   = p_batch_id
   AND lacho_dl_load_status = 'C';
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
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_HSG_OPTN';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
l_an_tab             VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_advice_case_hsg_optn.dataload_delete');
    fsc_utils.debug_message('s_dl_had_advice_case_hsg_optn.dataload_delete',3 );
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
          cs   := p1.lacho_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
--
-- Delete from advice_case_housing_options table
--
          DELETE 
            FROM advice_case_housing_options
           WHERE acho_reference = p1.lacho_reference;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_HOUSING_OPTIONS');
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
END s_dl_had_advice_case_hsg_optn;
/

