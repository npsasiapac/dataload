CREATE OR REPLACE PACKAGE BODY s_dl_hra_account_rent_limits
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   11-JAN-2009  Initial Creation.
--
--  1.1     5.15.0    VS   05-MAY-2009  Disable trigger ARLI_BR_I on
--                                      CREATE process to stop the supplied
--                                      created_by and created_date from 
--                                      being over written.
--
--  1.2     5.15.0    VS   18-MAY-2009  Use the supplied Subsidy Review Legacy
--                                      Reference to link it to right subsidy
--                                      review.
--
--  1.3     5.15.0    VS   05-NOV-2009  Defect Id : 2457 Fix. Only perform 
--                                      validation check 87 if 
--                                      LARLI_SURV_LEGACY_REF has been 
--                                      supplied.
--
--
--  1.4     5.15.0    VS   01-DEC-2009  Defect Id : 2703 Fix. The Tenancy Ref
--                                      should only be supplied for SUBSIDY and
--                                      ABATEMENTS. The Revenue Account Payment
--                                      reference should be supplied for SAS.
--
--  1.5     5.15.0    VS   11-FEB-2010  Config for Account Rent Limit Type has
--                                      changed from SAS to PRS. Changing code to
--                                      cater for both.
--
--
--  1.6     5.15.0    VS   09-JUL-2010  Fix for Defect id 4731 -  
--                                      LARLI_REFNO will populated in the VALIDATE process.
--                                      Cursor will select the date in larli_reference, larli_start_date
--                                      order.
--  1.7     6.4.0     PH   30-JUN-2011  LARLI_REFNO now populated by ctl file
--                                      so removed from validate.
--                                      Removed enable/disable of triggers now
--                                      run update after inserted.
--                                      Added cursor for subsidy_applications
--                                      to use legacy ref
--                                      Amended to use tcy_alt_ref not tcy_refno
--  1.8     6.15      AJ   17-OCT-2017  1) Validation amended for MB to check rent account is linked
--                                      to tenancy reference supplied in tenancy_holidings
--                                      2) General review of all validation done
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
  UPDATE dl_hra_account_rent_limits
     SET larli_dl_load_status = p_status
   WHERE rowid                = p_rowid;
--
  EXCEPTION
    WHEN OTHERS THEN
       dbms_output.put_line('Error updating status of dl_hra_account_rent_limits');
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
SELECT rowid REC_ROWID,
       LARLI_DLB_BATCH_ID,
       LARLI_DL_SEQNO,
       LARLI_DL_LOAD_STATUS,
       LARLI_SURV_LEGACY_REF,
       LARLI_REFERENCE,
       LARLI_SURV_EFFECTIVE_DATE,
       LARLI_RLTY_CODE,
       LARLI_START_DATE,
       LARLI_AMOUNT,
       NVL(LARLI_CREATED_DATE,SYSDATE)  LARLI_CREATED_DATE,
       NVL(LARLI_CREATED_BY,'DATALOAD') LARLI_CREATED_BY,
       LARLI_END_DATE,
       LARLI_REFNO
  FROM dl_hra_account_rent_limits
 WHERE larli_dlb_batch_id    = p_batch_id
   AND larli_dl_load_status  = 'V';
--
-- **********************************
-- Additional Cursors
--
CURSOR get_tcy_rac_accno(p_tcy_refno VARCHAR2)
IS
SELECT rac_accno
  FROM revenue_accounts, 
       tenancy_holdings,
       tenancies
 WHERE tcy_alt_ref      = p_tcy_refno
   AND tho_tcy_refno    = tcy_refno
   AND tho_rac_accno    = rac_accno
   AND rac_hrv_ate_code = 'REN';
--
-- **********************************
--
CURSOR get_pay_rac_accno(p_rac_pay_ref VARCHAR2)
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_rac_pay_ref;
--
-- **********************************
-- get subsidy reviews refno, held on the DL table
--
CURSOR get_surv_refno(p_surv_legacy_ref VARCHAR2) 
IS
SELECT surv_refno
  FROM subsidy_reviews
      ,dl_hra_subsidy_reviews
 WHERE surv_refno       = lsurv_refno
   AND lsurv_legacy_ref = p_surv_legacy_ref;
--
-- get subsidy reviews refno direct as now held on table
--
CURSOR get_surv_refno2(p_surv_legacy_ref VARCHAR2) 
IS
SELECT surv_refno
  FROM subsidy_reviews
 WHERE surv_legacy_ref = p_surv_legacy_ref ;
--
-- **********************************
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_ACCOUNT_RENT_LIMITS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
-- **********************************
-- Other variables
--
i                          INTEGER := 0;
l_exists                   VARCHAR2(1);
l_surv_refno               NUMBER(10);     
l_rac_accno                NUMBER(10);
--
-- **********************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_account_rent_limits.dataload_create');
    fsc_utils.debug_message('s_dl_hra_account_rent_limits.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.larli_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
          l_rac_accno  := NULL;
          l_surv_refno := NULL;
--
          IF (p1.LARLI_RLTY_CODE NOT IN ('SAS','PRS')) THEN
--
            OPEN get_tcy_rac_accno(p1.larli_reference);
           FETCH get_tcy_rac_accno into l_rac_accno;
           CLOSE get_tcy_rac_accno;
--
          ELSE
--
              OPEN get_pay_rac_accno(p1.larli_reference);
             FETCH get_pay_rac_accno into l_rac_accno;
             CLOSE get_pay_rac_accno;
--
          END IF;      
--
-- try getting surv_refno directly first as legacy ref may have been loaded
--
          OPEN get_surv_refno2(p1.larli_surv_legacy_ref);
          FETCH get_surv_refno2 INTO l_surv_refno;
          CLOSE get_surv_refno2;
--
-- if surv_refno not got directly try DL table as may not have been loaded
--
          IF l_surv_refno IS NULL  THEN
           OPEN get_surv_refno2(p1.larli_surv_legacy_ref);
           FETCH get_surv_refno2 INTO l_surv_refno;
           CLOSE get_surv_refno2;
          END IF;
--
-- Insert int relevent table
--
          INSERT /* +APPEND */ INTO account_rent_limits(ARLI_REFNO,
                                                        ARLI_RAC_ACCNO,
                                                        ARLI_RLTY_CODE,
                                                        ARLI_START_DATE,
                                                        ARLI_AMOUNT,
                                                        ARLI_CREATED_DATE,
                                                        ARLI_CREATED_BY,
                                                        ARLI_END_DATE,
                                                        ARLI_STATUS,
                                                        ARLI_SURV_REFNO
                                                       )
--
                                                 VALUES(p1.larli_refno,
                                                        l_rac_accno,
                                                        p1.larli_rlty_code,
                                                        p1.larli_start_date,
                                                        p1.larli_amount,
                                                        p1.larli_created_date,
                                                        p1.larli_created_by,
                                                        p1.larli_end_date,
                                                        'A',
                                                        l_surv_refno
                                                       );
--
-- Now update the record to set the correct created by and created date
-- to overcome the trigger
--
         UPDATE   account_rent_limits
            SET   arli_created_date = p1.larli_created_date
                , arli_created_by   = p1.larli_created_by
          WHERE   arli_refno        = p1.larli_refno;
--
-- **********************************
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,500000)=0 THEN 
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
-- **********************************
-- Section to analyse the table(s) populated by this Dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ACCOUNT_RENT_LIMITS');
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
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid REC_ROWID,
       LARLI_DLB_BATCH_ID,
       LARLI_DL_SEQNO,
       LARLI_DL_LOAD_STATUS,
       LARLI_SURV_LEGACY_REF,
       LARLI_REFERENCE,
       LARLI_SURV_EFFECTIVE_DATE,
       LARLI_RLTY_CODE,
       LARLI_START_DATE,
       LARLI_AMOUNT,
       NVL(LARLI_CREATED_DATE,SYSDATE)  LARLI_CREATED_DATE,
       NVL(LARLI_CREATED_BY,'DATALOAD') LARLI_CREATED_BY,
       LARLI_END_DATE,
       LARLI_REFNO
  FROM dl_hra_account_rent_limits
 WHERE larli_dlb_batch_id    = p_batch_id
   AND larli_dl_load_status in ('L','F','O')
 ORDER BY larli_reference,
          larli_start_date;
--
-- **********************************
-- Additional Cursors
--
CURSOR chk_tcy_exists(p_tcy_refno VARCHAR2) 
IS
SELECT 'X'
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_refno;
--
-- **********************************
--
CURSOR chk_rlty_code_exists(p_rlty_code VARCHAR2) 
IS
SELECT 'X'
  FROM rent_limit_types
 WHERE rlty_code = p_rlty_code;
--
-- **********************************
-- get subsidy reviews refno, held on the DL table
--
CURSOR chk_surv_exists(p_surv_legacy_ref VARCHAR2) 
IS
SELECT 'X'
  FROM subsidy_reviews
      ,dl_hra_subsidy_reviews
 WHERE surv_refno = lsurv_refno
   AND lsurv_legacy_ref = p_surv_legacy_ref;
--
-- get subsidy reviews refno direct as now held on table
--
CURSOR chk_surv_exists2(p_surv_legacy_ref VARCHAR2) 
IS
SELECT 'X'
  FROM subsidy_reviews
 WHERE surv_legacy_ref = p_surv_legacy_ref ;
--
-- **********************************
--
CURSOR chk_rac_exists(p_rac_pay_ref VARCHAR2) 
IS
SELECT 'X'
  FROM REVENUE_ACCOUNTS
 WHERE rac_pay_ref = p_rac_pay_ref;
--
-- **********************************
--
CURSOR chk_tcy_exists2(p_tcy_refno VARCHAR2)
IS
SELECT 'X'
  FROM revenue_accounts, 
       tenancy_holdings,
       tenancies
 WHERE tcy_alt_ref      = p_tcy_refno
   AND tho_tcy_refno    = tcy_refno
   AND tho_rac_accno    = rac_accno
   AND rac_hrv_ate_code = 'REN';
--
-- **********************************
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_ACCOUNT_RENT_LIMITS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- **********************************
-- Other variables
--
l_rac_exists        VARCHAR2(1);
l_tcy_exists        VARCHAR2(1);
l_tcy_rac_chk       VARCHAR2(1);
--
l_rlty_code_exists  VARCHAR2(1);
l_surv_exists       VARCHAR2(1);
l_errors            VARCHAR2(10);
l_error_ind         VARCHAR2(10);
i                   INTEGER :=0;
--
-- **********************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_account_rent_limits.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_account_rent_limits.dataload_validate',3);
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
          cs   := p1.larli_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- **********************************
-- Validation checks required
--
-- Check Subsidy Review exists where legacy reference supplied
--
          IF (p1.LARLI_SURV_LEGACY_REF IS NOT NULL) THEN
--
             l_surv_exists := NULL;
--
              OPEN chk_surv_exists2(p1.LARLI_SURV_LEGACY_REF);
             FETCH chk_surv_exists2 INTO l_surv_exists;
             CLOSE chk_surv_exists2;
--
             IF (l_surv_exists IS NULL)  THEN
--
                OPEN chk_surv_exists(p1.LARLI_SURV_LEGACY_REF);
               FETCH chk_surv_exists INTO l_surv_exists;
               CLOSE chk_surv_exists;
--
              END IF;
--
             IF (l_surv_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',33);
             END IF;
--
          END IF;
--
-- **********************************
-- Check Tenancy Refno/Payment reference LARLI_REFERENCE has been 
-- supplied and is valid.
--
-- For SAS - payment reference needs to be supplied
-- For SUBSIDY and ABATEMENTS - Tenancy refno needs to be supplied.
--
-- 
          l_tcy_exists  := NULL;
          l_rac_exists  := NULL;
          l_tcy_rac_chk := NULL;
--
          IF (p1.larli_reference IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',324);
          ELSE
--
            IF (p1.LARLI_RLTY_CODE NOT IN ('SAS','PRS')) THEN
--
               OPEN chk_tcy_exists(p1.larli_reference);
              FETCH chk_tcy_exists INTO l_tcy_exists;
              CLOSE chk_tcy_exists;
--
              IF (l_tcy_exists IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',41);
              END IF;
--
              IF (l_tcy_exists IS NOT NULL) THEN
--
                 OPEN chk_tcy_exists2(p1.larli_reference);
                FETCH chk_tcy_exists2 INTO l_tcy_rac_chk;
                CLOSE chk_tcy_exists2;
--
                IF (l_tcy_rac_chk IS NULL) THEN
                 l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',339);
                END IF;
              END IF;
--
            ELSE
--
               OPEN chk_rac_exists(p1.larli_reference);
              FETCH chk_rac_exists INTO l_rac_exists;
              CLOSE chk_rac_exists;
--
              IF (l_rac_exists IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',323);
              END IF;
--
            END IF;
--
          END IF;
--
-- **********************************
-- Check Rent Limit Type Code has been supplied and is valid
--
          IF (p1.larli_rlty_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',84);
          ELSE
--
             l_rlty_code_exists := NULL;
--
              OPEN chk_rlty_code_exists(p1.larli_rlty_code);
             FETCH chk_rlty_code_exists INTO l_rlty_code_exists;
             CLOSE chk_rlty_code_exists;
--
             IF (l_rlty_code_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',85);
             END IF;
--
          END IF;
--
-- ********************************************
-- 
-- Check Rent Limit Start Date LARLI_START_DATE has been supplied and is valid
--
-- The Rent Limit Start Date must not be before the Review Start Date
--
          IF (p1.larli_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',86);
          END IF;
--
          IF (p1.LARLI_SURV_LEGACY_REF IS NOT NULL) THEN
--
           IF (p1.larli_start_date < p1.larli_surv_effective_date) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',87);
           END IF;
-- 
          END IF;
--
-- ********************************************
-- Check Income Subsidy Amount LARLI_AMOUNT has been supplied
--
          IF (p1.larli_amount IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',88);
          END IF;
--
-- ********************************************
-- Check Limit End date LARLI_END_DATE is valid if supplied
-- 
-- If supplied, the end date must not be before the start date.
-- 
          IF (p1.larli_end_date IS NOT NULL) THEN
--
           IF (p1.larli_end_date < p1.larli_start_date) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',89);
           END IF;
--
          END IF;
--
-- ********************************************
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
                          p_date           IN date) 
IS
--
CURSOR c1 is
SELECT rowid REC_ROWID,
       LARLI_DLB_BATCH_ID,
       LARLI_DL_SEQNO,
       LARLI_DL_LOAD_STATUS,
       LARLI_surv_legacy_ref,
       LARLI_refno
  FROM dl_hra_account_rent_limits
 WHERE larli_dlb_batch_id   = p_batch_id
   AND larli_dl_load_status = 'C';
--
-- ********************************************
-- Additional Cursors
--
-- Constants FOR process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HRA_ACCOUNT_RENT_LIMITS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     	ROWID;
l_an_tab	VARCHAR2(1);
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
-- ********************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_account_rent_limits.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_account_rent_limits.dataload_delete',3 );
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
          cs   := p1.larli_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
-- Delete from table
--
          DELETE 
            FROM account_rent_limits
           WHERE arli_refno = p1.larli_refno;
--
-- ********************************************
-- Now UPDATE the record status and process count
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
-- ********************************************
-- Section to analyse the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ACCOUNT_RENT_LIMITS');
--
    fsc_utils.proc_end;
--
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_delete;
--
END s_dl_hra_account_rent_limits;
/

