--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_subsidy_applications
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   05-JAN-2009  Initial Creation.
--
--  1.1     5.15.0    VS   05-MAY-2009  Disable trigger SUAP_BR_IU on
--                                      CREATE process to stop the supplied
--                                      created_by and created_date from 
--                                      being over written.
--
--  1.2     5.15.0    VS   18-MAY-2009  Make the supplied Legacy Ref the 
--                                      primary key. This will replace the 
--                                      sequence value used to populate
--                                      lsuap_reference.
--
--                                      Remove the DL_SUB_APP_PERF1, we
--                                      nolonger need it.
--
--  1.3     5.15.0    VS   08-MAR-2010  Defect 3707 Fix. Reusable Refno needs to
--                                      be populated, to allow notepads against
--                                      subsidy application.
--
--  1.4     5.15.0    VS   13-MAY-2010  Defect 4517 Fix. Added Modified By/Date
--                                      to subsidy application.
--
--  1.5     6.4.0     PH   29-JUN-2011  Amended the way we handle suap_reference
--                                      Originally used ref supplied to be
--                                      suap_reference, now use actual sequence.
--                                      However, main table does not hold legacy
--                                      ref so all dependant dataloads will need
--                                      to reference this dl table.
--                                      Removed enabling/disabling of triggers
--                                      now do update after insert
--                                      Added new fields
--                                      Amended to use tcy_alt_ref not tcy_refno
--  1.6     6.14      AJ   27-SEP-2017  Amended lsurv_subp_asca_code so that it
--                                      checks assessment_categories, and not 
--                                      the FRV change was at v6.8 error amended from
--                                      50 to 82 in hd2 errors file
--  1.7     6.15      DB   13-FEB-2018  Main table now holds legacy reference
--                                     amended code to insert this value and
--                                     also validate that its unique.
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
  UPDATE dl_hra_subsidy_applications
  SET    lsuap_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_subsidy_applications');
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
       LSUAP_DLB_BATCH_ID,
       LSUAP_DL_SEQNO,
       LSUAP_DL_LOAD_STATUS,
       LSUAP_LEGACY_REF,
       LSUAP_REFERENCE,
       LSUAP_TCY_ALT_REF,
       LSUAP_START_DATE,
       LSUAP_RECEIVED_DATE,
       LSUAP_SCO_CODE,
       NVL(LSUAP_CREATED_DATE, SYSDATE) LSUAP_CREATED_DATE,
       NVL(LSUAP_CREATED_BY,'DATALOAD') LSUAP_CREATED_BY,
       LSUAP_END_DATE,
       LSUAP_CHECKED_DATE,
       LSUAP_CHECKED_BY,
       LSUAP_MODIFIED_DATE,
       LSUAP_MODIFIED_BY,
       LSUAP_HRV_ASCA_CODE,
       LSUAP_HRV_HSTR_CODE,
       LSUAP_APP_LEGACY_REF,
       LSUAP_NEXT_SCHED_REVIEW_DATE
  FROM dl_hra_subsidy_applications
 WHERE lsuap_dlb_batch_id    = p_batch_id
   AND lsuap_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_get_tcy  ( p_tcy_alt_ref   VARCHAR2 )  IS
SELECT  tcy_refno
FROM    tenancies
WHERE   tcy_alt_ref  = p_tcy_alt_ref;
--
CURSOR  c_get_app  ( p_app_legacy_ref   VARCHAR2 )  IS
SELECT  app_refno
FROM    applications
WHERE   app_legacy_ref  = p_app_legacy_ref;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_SUBSIDY_APPLICATIONS';
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
l_tcy_refno       NUMBER(10);
l_app_refno       NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_applications.dataload_create');
    fsc_utils.debug_message('s_dl_hra_subsidy_applications.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lsuap_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
       l_tcy_refno   := NULL;
       l_app_refno   := NULL;
--
    IF p1.lsuap_tcy_alt_ref IS NOT NULL
     THEN
--
      OPEN c_get_tcy ( p1.lsuap_tcy_alt_ref );
       FETCH c_get_tcy INTO l_tcy_refno;
      CLOSE c_get_tcy;
--
    ELSE
--
      OPEN c_get_app ( p1.lsuap_app_legacy_ref );
       FETCH c_get_app INTO l_app_refno;
      CLOSE c_get_app;
--
    END IF;
--
-- Insert into relevent table
--
          INSERT /* +APPEND */ INTO subsidy_applications(SUAP_REFERENCE,
                                                         SUAP_REUSABLE_REFNO,
                                                         SUAP_START_DATE,
                                                         SUAP_RECEIVED_DATE,
                                                         SUAP_TCY_REFNO,
                                                         SUAP_SCO_CODE,
                                                         SUAP_CREATED_DATE,
                                                         SUAP_CREATED_BY,
                                                         SUAP_END_DATE,
                                                         SUAP_CHECKED_DATE,
                                                         SUAP_CHECKED_BY,
                                                         SUAP_MODIFIED_DATE,
                                                         SUAP_MODIFIED_BY,
                                                         SUAP_HRV_ASCA_CODE,
                                                         SUAP_HRV_HSTR_CODE,
                                                         SUAP_APP_REFNO,
                                                         SUAP_NEXT_SCHED_REVIEW_DATE,
                                                         SUAP_LEGACY_REF
                                                        )
--
                                                  VALUES(p1.lsuap_reference,
                                                         reusable_refno_seq.nextval,
                                                         p1.lsuap_start_date,
                                                         p1.lsuap_received_date,
                                                         l_tcy_refno,
                                                         p1.lsuap_sco_code,
                                                         p1.lsuap_created_date,
                                                         p1.lsuap_created_by,
                                                         p1.lsuap_end_date,
                                                         p1.lsuap_checked_date,
                                                         p1.lsuap_checked_by,
                                                         p1.lsuap_modified_date,
                                                         p1.lsuap_modified_by,
                                                         p1.lsuap_hrv_asca_code,
                                                         p1.lsuap_hrv_hstr_code,
                                                         l_app_refno,
                                                         p1.LSUAP_NEXT_SCHED_REVIEW_DATE,
                                                         p1.LSUAP_LEGACY_REF
                                                        );
--
-- Now update the record to set the correct created by and created date
-- to ovecome the trigger
--
         UPDATE   subsidy_applications
            SET   suap_created_date = p1.lsuap_created_date
                , suap_created_by   = p1.lsuap_created_by
          WHERE   suap_reference    = p1.lsuap_reference;
--
--
--
-- ***********************************************************************
--
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
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_APPLICATIONS');
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
       LSUAP_DLB_BATCH_ID,
       LSUAP_DL_SEQNO,
       LSUAP_DL_LOAD_STATUS,
       LSUAP_LEGACY_REF,
       LSUAP_TCY_ALT_REF,
       LSUAP_START_DATE,
       LSUAP_RECEIVED_DATE,
       LSUAP_SCO_CODE,
       LSUAP_CREATED_DATE,
       LSUAP_CREATED_BY,
       LSUAP_END_DATE,
       LSUAP_CHECKED_DATE,
       LSUAP_CHECKED_BY,
       LSUAP_HRV_ASCA_CODE,
       LSUAP_HRV_HSTR_CODE,
       decode(TO_CHAR(LSUAP_START_DATE,'fmDAY'),'MONDAY',   'MON',
                                                'TUESDAY',  'TUE',
                                                'WEDNESDAY','WED',
                                                'THURSDAY', 'THU',
                                                'FRIDAY',   'FRI',
                                                'SATURDAY', 'SAT',
                                                'SUNDAY',   'SUN') LSUAP_START_DAY,
       LSUAP_APP_LEGACY_REF,
       LSUAP_NEXT_SCHED_REVIEW_DATE
  FROM dl_hra_subsidy_applications
 WHERE lsuap_dlb_batch_id    = p_batch_id
   AND lsuap_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_suap_exists(p_suap_legacy_ref NUMBER) 
IS
SELECT 'X'
  FROM subsidy_applications
 WHERE suap_reference = p_suap_legacy_ref;
--
--
-- ***********************************************************************
--
CURSOR chk_tcy_exists(p_tcy_alt_ref   VARCHAR2) 
IS
SELECT tcy_refno, tcy_act_start_date
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_alt_ref;
--
-- ***********************************************************************
--
CURSOR chk_app_exists(p_app_legacy_ref   VARCHAR2) 
IS
SELECT app_refno, app_received_date
  FROM applications
 WHERE app_legacy_ref  = p_app_legacy_ref;
--
--
-- ***********************************************************************
--
CURSOR get_admin_year_dets(p_tcy_refno       NUMBER,
                           p_suap_start_date DATE) 
IS
SELECT b.aye_rent_week_start
  FROM revenue_accounts a,
       admin_years      b
 WHERE a.rac_tcy_refno = p_tcy_refno
   AND SYSDATE BETWEEN a.rac_start_date
                   AND NVL(a.rac_end_date,SYSDATE + 1)
   AND b.aye_aun_code  = a.rac_aun_code
   AND p_suap_start_date BETWEEN b.aye_start_date
                             AND b.aye_end_date;
--
--
-- ***********************************************************************
--
CURSOR chk_sco_exists(p_sco_code VARCHAR2) 
IS
SELECT sco_code
  FROM status_codes
 WHERE sco_code = p_sco_code;
--
-- ***********************************************************************
--
-- Moved check from First Ref Values to SUBSIDY_ASSESSMENT_CATEGORIES, as
-- configuration of the data has changed
--
CURSOR chk_asca_exists(p_suap_hrv_asca_code VARCHAR2)
IS
SELECT 'X'
FROM SUBSIDY_ASSESSMENT_CATEGORIES
WHERE ASCA_CODE = p_suap_hrv_asca_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_APPLICATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists                VARCHAR2(1);
l_suap_exists           VARCHAR2(1);
l_tcy_refno             NUMBER(10);
l_tcy_act_start_date    DATE;
l_app_refno             NUMBER(10);
l_app_received_date     DATE;
l_force_subsidy_period  VARCHAR2(1);
l_rent_week_start       VARCHAR2(3);
l_sco_code              VARCHAR2(3);
l_asca_exists           VARCHAR2(1);
--
l_errors                VARCHAR2(10);
l_error_ind             VARCHAR2(10);
i                       INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_applications.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_subsidy_applications.dataload_validate',3);
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
          cs   := p1.lsuap_dl_seqno;
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
--
-- Check Subsidy Application Legacy Ref SUAP_REFERENCE has been supplied and 
-- does not already exists
--
-- Commented out second part of this check as we now use the
-- sequence number in the ctl file so will never already 
-- exist
--  
          IF (p1.lsuap_legacy_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',012);
/*          ELSE
--
             l_suap_exists := NULL;
--
              OPEN chk_suap_exists(p1.lsuap_legacy_ref);
             FETCH chk_suap_exists INTO l_suap_exists;
             CLOSE chk_suap_exists;
--
             IF (l_suap_exists IS NOT NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',273);
             END IF;
*/
--            
          END IF;

-- ***********************************************************************
--
-- Check Tenancy Refno SUAP_TCY_REFNO has been supplied and exists
-- Amended these checks as Tenancy Ref is no longer mandatory
-- but at least one of tenancy ref or app ref ust be supplied
--
--  
    l_tcy_refno          := NULL;
    l_tcy_act_start_date := NULL;
    l_app_refno          := NULL;
    l_app_received_date  := NULL;
    l_asca_exists        := NULL;
--
-- check both null, if so error
--
     IF   (p1.lsuap_tcy_alt_ref    IS NULL)
      AND (p1.lsuap_app_legacy_ref IS NULL)
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',460);
     END IF;
--
-- Check both not null, if so error
--
     IF   (p1.lsuap_tcy_alt_ref    IS NOT NULL)
      AND (p1.lsuap_app_legacy_ref IS NOT NULL)
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',461);
     END IF;
--
-- Now check tenancy exists
--
     IF   (p1.lsuap_tcy_alt_ref    IS NOT NULL)
      THEN
       OPEN chk_tcy_exists(p1.lsuap_tcy_alt_ref);
        FETCH chk_tcy_exists INTO l_tcy_refno, l_tcy_act_start_date;
       CLOSE chk_tcy_exists;
--
        IF (l_tcy_refno IS NULL)
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',41);
        END IF;
--            
     END IF;   /*   IF   (p1.lsuap_tcy_alt_ref    IS NOT NULL)   */
--
-- check application exists
--
     IF   (p1.lsuap_app_legacy_ref    IS NOT NULL)
      THEN
       OPEN chk_app_exists(p1.lsuap_app_legacy_ref);
        FETCH chk_app_exists INTO l_app_refno, l_app_received_date;
       CLOSE chk_app_exists;
--
        IF (l_app_refno IS NULL)
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',233);
        END IF;
--            
     END IF;   /*   IF   (p1.lsuap_app_legacy_ref    IS NOT NULL)   */
--
--
-- Check Application Start date SUAP_START_DATE has been supplied and is valid
-- 
-- Application Start Date must not be > Tenancy Start Date
--
-- If the System Parameter is set to force the Start Date to coincide with 
-- the Rent Week Start Day, 'FORCE_SUBSIDY_PERIOD' set to Y', then the 
-- following check will take place.
-- 
-- The Start Date must be set to a date which falls on the day of the week 
-- identified by ADMIN YEAR Rent Week Start for the ADMIN YEAR which spans 
-- the Start Date and is for the Rents Admin Unit that the associated Rent 
-- Account is in.
--
--
          IF (p1.lsuap_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',42);
--
          ELSIF (    l_tcy_refno IS NOT NULL
                 AND p1.lsuap_start_date < l_tcy_act_start_date) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',43);
--
          ELSIF (    l_app_refno IS NOT NULL
                 AND p1.lsuap_start_date < l_app_received_date) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',462);
--
          ELSE
--
             l_force_subsidy_period := NULL;
--
             l_force_subsidy_period := s_parameter_values.get_param('FORCE_SUBSIDY_PERIOD','SYSTEM');
--
             IF (l_force_subsidy_period = 'Y') THEN
--
              l_rent_week_start := NULL;
--
               OPEN get_admin_year_dets(l_tcy_refno,p1.lsuap_start_date);
              FETCH get_admin_year_dets INTO l_rent_week_start;
              CLOSE get_admin_year_dets;
--
              IF (l_rent_week_start != p1.lsuap_start_day) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',44);
              END IF;
--
             END IF; -- l_force_subsidy_period

          END IF;
--
--
--
-- Check Application Received date SUAP_RECEIVED_DATE has been supplied and is valid
-- 
-- Application RECEIVED Date must not be in the future
--
--  
          IF (p1.lsuap_received_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',45);
--
          ELSIF (p1.lsuap_received_date > TRUNC(SYSDATE)) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',46);
--
          END IF;
--
--
-- 
-- The status code SUAP_SCO_CODE has been supplied and is valid
--
--
          IF (p1.lsuap_sco_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',47);
--
          ELSE
--
             l_sco_code := NULL;
--
              OPEN chk_sco_exists(p1.lsuap_sco_code);
             FETCH chk_sco_exists INTO l_sco_code;
             CLOSE chk_sco_exists;
--
             IF (l_sco_code IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',014);
             END IF;
--
          END IF;
--
--
--
-- The Application end date must not be before the Application start date
--
-- 
          IF (nvl(p1.lsuap_end_date, p1.lsuap_start_date) < p1.lsuap_start_date) THEN 
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',003);
          END IF;
--
--
--
-- Check Application Checked date SUAP_CHECKED_DATE supplied is not in the future
-- 
--  
          IF (    p1.lsuap_checked_date IS NOT NULL
              AND p1.lsuap_checked_date > TRUNC(SYSDATE)) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',48);
--
          END IF;
--
-- ********************************************
-- All reference values supplied are valid
-- 
-- Subsidy Assessment Category
--
--          IF (p1.lsuap_hrv_asca_code IS NULL) THEN
--           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',49);
--
--          ELSIF (NOT s_dl_hem_utils.exists_frv('SUBASSCAT',p1.lsuap_hrv_asca_code,'Y')) THEN
--              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',50);
--
--          END IF;
--
-- changed from domain to table check amended to match (AJ)
--
          IF (p1.lsuap_hrv_asca_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',49);
          ELSE
--
             l_asca_exists := NULL;
--
          OPEN chk_asca_exists(p1.lsuap_hrv_asca_code);
         FETCH chk_asca_exists INTO l_asca_exists;
         CLOSE chk_asca_exists;
--
             IF (l_asca_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',82);
            END IF;
--
          END IF;
--
-- ********************************************
-- 
--
-- Subsidy Termination Reason
--
        IF (NOT s_dl_hem_utils.exists_frv('SUBTERMRSN',p1.lsuap_hrv_hstr_code,'Y'))
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',51);
        END IF;
--
--  If supplied the next scheduled review date must be in the future
--
    IF (    p1.lsuap_next_sched_review_date IS NOT NULL
        AND p1.lsuap_next_sched_review_date < TRUNC(SYSDATE)) 
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',463);
    END IF;
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
       LSUAP_DLB_BATCH_ID,
       LSUAP_DL_SEQNO,
       LSUAP_DL_LOAD_STATUS,
       LSUAP_TCY_ALT_REF,
       LSUAP_LEGACY_REF,
       LSUAP_REFERENCE
  FROM dl_hra_subsidy_applications
 WHERE lsuap_dlb_batch_id   = p_batch_id
   AND lsuap_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_get_tcy_refno(p_tcy_alt_ref   VARCHAR2) IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_alt_ref;
--
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_APPLICATIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
i                INTEGER :=0;
l_tcy_refno      tenancies.tcy_refno%type;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_applications.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_subsidy_applications.dataload_delete',3 );
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
          cs   := p1.lsuap_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
--
-- Delete from table
--
          DELETE 
            FROM subsidy_applications
           WHERE suap_reference = p1.lsuap_reference;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_APPLICATIONS');
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
END s_dl_hra_subsidy_applications;
/
