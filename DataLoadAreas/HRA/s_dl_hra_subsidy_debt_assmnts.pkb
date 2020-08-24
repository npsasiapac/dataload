--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_subsidy_debt_assmnts
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.4.0     PH   01-JUL-2011  Initial Creation.
--  1.1     6.5.0     PH   24-FEB-2012  Legacy Ref now held in subsidy
--                                      applications, removed call to dl table
--
--
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag
        ( p_rowid  IN ROWID
        , p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_hra_subsidy_debt_assmnts
  SET    lsuda_dl_load_status  = p_status
  WHERE  rowid                 = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of s_dl_hra_subsidy_debt_assmnts');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_create
        ( p_batch_id          IN VARCHAR2
        , p_date              IN DATE)
AS
--
CURSOR  c1 IS
SELECT  rowid rec_rowid
      , lsuda_dlb_batch_id
      , lsuda_dl_seqno
      , lsuda_dl_load_status
      , lsuda_legacy_ref
      , lsuda_suap_legacy_ref
      , lsuda_start_date
      , lsuda_sco_code
      , nvl(lsuda_created_date, trunc(sysdate))  lsuda_created_date
      , nvl(lsuda_created_by, 'DATALOAD')        lsuda_created_by
      , lsuda_pay_ref
      , lsuda_comments
      , lsuda_total_debt
      , lsuda_total_accrued_debt
      , lsuda_total_non_accrued_debt
      , lsuda_calculated_date
      , lsuda_calculated_by
      , lsuda_established_date
      , lsuda_established_by
      , lsuda_sdar_code
      , lsuda_sdwr_code
      , lsuda_refno
FROM    dl_hra_subsidy_debt_assmnts
WHERE   lsuda_dlb_batch_id    = p_batch_id
AND     lsuda_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_get_suap( p_suap_legacy_ref    VARCHAR2 ) IS
SELECT  suap_reference
FROM    subsidy_applications
WHERE   suap_legacy_ref      =  p_suap_legacy_ref;
--
CURSOR  c_get_rac( p_rac_payref    VARCHAR2 ) IS
SELECT  rac_accno
FROM    revenue_accounts
WHERE   rac_pay_ref           =  p_rac_payref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                 VARCHAR2(30);
cd                 DATE;
cp                 VARCHAR2(30) := 'CREATE';
ct                 VARCHAR2(30) := 'DL_HRA_SUBSIDY_DEBT_ASSMNTS';
cs                 INTEGER;
ce	               VARCHAR2(200);
l_id               ROWID;
l_an_tab           VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                  INTEGER := 0;
l_exists           VARCHAR2(1);
l_suap_reference   subsidy_applications.suap_reference%type;
l_reusable_refno   subsidy_applications.suap_reusable_refno%type;
l_rac_accno        revenue_accounts.rac_accno%type;
--
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_subsidy_debt_assmnts.dataload_create');
fsc_utils.debug_message('s_dl_hra_subsidy_debt_assmnts.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1 LOOP
--
    BEGIN
--
   cs   := p1.lsuda_dl_seqno;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
-- Main processing
--
   l_suap_reference   := NULL;
   l_rac_accno        := NULL;
--
-- Open any cursors
--
   OPEN c_get_suap ( p1.lsuda_suap_legacy_ref );
    FETCH c_get_suap INTO l_suap_reference;
   CLOSE c_get_suap;
--
   OPEN c_get_rac ( p1.lsuda_pay_ref );
    FETCH c_get_rac INTO l_rac_accno;
   CLOSE c_get_rac;
--
-- Get the re-usable refno
--
   l_reusable_refno := fsc_utils.f_dynamic_value('reusable_refno_seq.NEXTVAL');
--
-- Insert into relevent table
--
        INSERT into  subsidy_debt_assessments
              ( suda_refno
              , suda_start_date
              , suda_sco_code
              , suda_suap_reference
              , suda_reusable_refno
              , suda_created_date
              , suda_created_by
              , suda_rac_accno
              , suda_comments
              , suda_total_debt
              , suda_total_accrued_debt
              , suda_total_non_accrued_debt
              , suda_calculated_date
              , suda_calculated_by
              , suda_established_date
              , suda_established_by
              , suda_sdar_code
              , suda_sdwr_code
              )
        VALUES
              ( p1.lsuda_refno
              , p1.lsuda_start_date
              , p1.lsuda_sco_code
              , l_suap_reference
              , l_reusable_refno
              , p1.lsuda_created_date
              , p1.lsuda_created_by
              , l_rac_accno
              , p1.lsuda_comments
              , p1.lsuda_total_debt
              , p1.lsuda_total_accrued_debt
              , p1.lsuda_total_non_accrued_debt
              , p1.lsuda_calculated_date
              , p1.lsuda_calculated_by
              , p1.lsuda_established_date
              , p1.lsuda_established_by
              , p1.lsuda_sdar_code
              , p1.lsuda_sdwr_code
              );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
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
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_DEBT_ASSESSMENTS');
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
PROCEDURE dataload_validate
        ( p_batch_id          IN VARCHAR2
        , p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT  rowid rec_rowid
      , lsuda_dlb_batch_id
      , lsuda_dl_seqno
      , lsuda_dl_load_status
      , lsuda_legacy_ref
      , lsuda_suap_legacy_ref
      , lsuda_start_date
      , lsuda_sco_code
      , nvl(lsuda_created_date, trunc(sysdate))  lsuda_created_date
      , nvl(lsuda_created_by, 'DATALOAD')        lsuda_created_by
      , lsuda_pay_ref
      , lsuda_comments
      , lsuda_total_debt
      , lsuda_total_accrued_debt
      , lsuda_total_non_accrued_debt
      , lsuda_calculated_date
      , lsuda_calculated_by
      , lsuda_established_date
      , lsuda_established_by
      , lsuda_sdar_code
      , lsuda_sdwr_code
      , lsuda_refno
FROM    dl_hra_subsidy_debt_assmnts
WHERE   lsuda_dlb_batch_id    = p_batch_id
AND     lsuda_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_get_suap( p_suap_legacy_ref    VARCHAR2 ) IS
SELECT  'X'
FROM    subsidy_applications
WHERE   suap_legacy_ref      =  p_suap_legacy_ref;
--
CURSOR  c_get_rac( p_rac_payref    VARCHAR2 ) IS
SELECT  rac_accno
      , rac_hrv_ate_code
FROM    revenue_accounts
WHERE   rac_pay_ref           =  p_rac_payref;
--
CURSOR  c_get_suda( p_suda_legacy_ref    VARCHAR2 ) IS
SELECT  'X'
FROM    subsidy_debt_assessments
      , dl_hra_subsidy_debt_assmnts
WHERE   suda_refno            =  lsuda_refno
AND     lsuda_legacy_ref      =  p_suda_legacy_ref;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                 VARCHAR2(30);
cd                 DATE;
cp                 VARCHAR2(30) := 'VALIDATE';
ct                 VARCHAR2(30) := 'DL_HRA_SUBSIDY_DEBT_ASSMNTS';
cs                 INTEGER;
ce                 VARCHAR2(200);
l_id               ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists           VARCHAR2(1);
l_pro_refno        NUMBER(10);
l_pro_aun          VARCHAR2(20);
l_errors           VARCHAR2(10);
l_error_ind        VARCHAR2(10);
i                  INTEGER :=0;
l_rac_accno        revenue_accounts.rac_accno%type;
l_hrv_ate_code     revenue_accounts.rac_hrv_ate_code%type;
l_suda_ate_code    VARCHAR2(10);
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_subsidy_debt_assmnts.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_subsidy_debt_assmnts.dataload_validate',3);
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
   cs              := p1.lsuda_dl_seqno;
   l_id            := p1.rec_rowid;
--
   l_errors        := 'V';
   l_error_ind     := 'N';
   l_rac_accno     := NULL;
   l_hrv_ate_code  := NULL;
   l_suda_ate_code := NULL;
--
--
-- Validation checks required
--
-- Check a record doesn't already exist
--
   OPEN c_get_suda ( p1.lsuda_legacy_ref );
    FETCH c_get_suda INTO l_exists;
     IF c_get_suda%FOUND
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',455);
     END IF;
   CLOSE c_get_suda;  
--
-- Check the Subsidy Application
--
   OPEN c_get_suap ( p1.lsuda_suap_legacy_ref );
    FETCH c_get_suap INTO l_exists;
     IF c_get_suap%NOTFOUND
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',32);
     END IF;
   CLOSE c_get_suap;
--
-- Check Status Code
-- must be one of ‘RAI’, ‘GEN’, ‘ASS’, ‘DIS’, ‘CAD’, ‘CAC’, ‘COM’, ‘CAN’, ‘REJ’
--
   IF nvl(p1.lsuda_sco_code, 'XYZ') NOT IN
           ( 'RAI', 'GEN', 'ASS', 'DIS', 'CAD'
           , 'CAC', 'COM', 'CAN', 'REJ' )
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',456);
   END IF;
--
-- If the Status code  is  ‘DIS’, ‘CAD’, ‘CAC’, ‘COM’ then total_debt
-- must be supplied
--
   IF p1.lsuda_sco_code IN  ( 'DIS', 'CAD', 'CAC', 'COM' ) 
    AND
     p1.lsuda_total_debt IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',457);
   END IF;
--
-- If the status is COM then the revenue account must be supplied
--
   IF p1.lsuda_sco_code = 'COM'
    THEN
     OPEN c_get_rac ( p1.lsuda_pay_ref );
      FETCH c_get_rac INTO l_rac_accno, l_hrv_ate_code;
       IF l_rac_accno IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',458);
       END IF;
     CLOSE c_get_rac;
   END IF;
--
-- If there is a Revenue Account, then this must be of the Account Type held 
-- as system parameter SUB_DEBT_ACCT_TYPE.
--
   l_suda_ate_code := s_parameter_values.get_param('SUB_DEBT_ACCT_TYPE','SYSTEM');
--
   IF l_rac_accno IS NOT NULL
    THEN
      IF l_hrv_ate_code != l_suda_ate_code
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',459);
      END IF;
   END IF;
--
-- Check other mandatory fields
--
-- Start Date
--
   IF p1.lsuda_start_date IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',42);
   END IF;
-- 
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
PROCEDURE dataload_delete
        ( p_batch_id          IN VARCHAR2
        , p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT  rowid rec_rowid
      , lsuda_dlb_batch_id
      , lsuda_dl_seqno
      , lsuda_dl_load_status
      , lsuda_refno 
FROM    dl_hra_subsidy_debt_assmnts
WHERE   lsuda_dlb_batch_id    = p_batch_id
AND     lsuda_dl_load_status  = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                 VARCHAR2(30);
cd                 DATE;
cp                 VARCHAR2(30) := 'DELETE';
ct                 VARCHAR2(30) := 'DL_HRA_SUBSIDY_DEBT_ASSMNTS';
cs                 INTEGER;
ce                 VARCHAR2(200);
l_id               ROWID;
l_an_tab           VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists           VARCHAR2(1);
i                  INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_subsidy_debt_assmnts.dataload_delete');
fsc_utils.debug_message('s_dl_hra_subsidy_debt_assmnts.dataload_delete',3 );
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
   cs   := p1.lsuda_dl_seqno;
   l_id := p1.rec_rowid;
   i    := i +1;
--
-- Delete from table
--
   DELETE FROM subsidy_debt_assessments
   WHERE  suda_refno = p1.lsuda_refno
   ;
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
IF mod(i,5000) = 0 THEN commit; END IF;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_DEBT_ASSESSMENTS');
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
END s_dl_hra_subsidy_debt_assmnts;
/


