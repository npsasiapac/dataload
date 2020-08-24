CREATE OR REPLACE PACKAGE BODY s_dl_hem_income_header_usages
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.5.0     VS   23-JUN-2011 	Initial Creation.
--
--  2.0     6.5.0     VS   19-JUL-2011 	surv_legacy_ref and
--                                      acas_alternate_reference change.
--  1.1     6.5.0     PH   16-MAR-2012  Legacy Ref now held in subsidy
--                                      reviews, removed call to dl table
--  1.2     6.11      AJ   22-DEC-2015  error message 491(Subsidy Review Ref Check)
--                                      and 492(Advice Case ref check) amended to
--                                      474 and 144 so generic hd2_errs_in can be used
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
  UPDATE dl_hem_income_header_usages
     SET lihu_dl_load_status = p_status
   WHERE rowid               = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_income_header_usages');
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
SELECT rowid rec_rowid,
       LIHU_DLB_BATCH_ID,
       LIHU_DL_SEQNO,
       LIHU_DL_LOAD_STATUS,
       LIHU_INH_LEGACY_REF,
       NVL(LIHU_CREATED_BY,'DATALOAD') LIHU_CREATED_BY,
       NVL(LIHU_CREATED_DATE,SYSDATE)  LIHU_CREATED_DATE,
       LIHU_REFERENCE_TYPE,
       LIHU_REFERENCE_VALUE,
       LIHU_REFNO
  FROM dl_hem_income_header_usages
 WHERE lihu_dlb_batch_id   = p_batch_id
   AND lihu_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_tar_refno (p_tcy_alt_ref VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_alr_refno (p_app_legacy_ref VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;
--
-- ***********************************************************************
--
CURSOR c_inh_refno (p_inh_legacy_ref NUMBER) 
IS
SELECT linh_refno
  FROM dl_hem_income_headers,
       income_headers
 WHERE linh_legacy_ref     = p_inh_legacy_ref
   AND linh_dl_load_status = 'C'
   AND inh_refno           = linh_refno;
--
-- ***********************************************************************
--
CURSOR c_surv_refno(p_surv_legacy_ref    VARCHAR2)
IS
SELECT surv_refno
  FROM subsidy_reviews
 WHERE surv_legacy_ref     = p_surv_legacy_ref;
--
-- ************************************************************************************
--
CURSOR c_acas_reference(p_acas_alternate_reference VARCHAR2) 
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = p_acas_alternate_reference;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_INCOME_HEADER_USAGES';
cs                   INTEGER;
ce	             VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                    INTEGER := 0;
l_exists             VARCHAR2(1);
l_tcy_refno          NUMBER(10);
l_app_refno          NUMBER(10);
l_surv_refno         NUMBER(10);
l_acas_reference     NUMBER(10);
l_inh_refno          NUMBER(10);

--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger IHU_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_income_header_usages.dataload_create');
    fsc_utils.debug_message('s_dl_hem_income_header_usages.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lihu_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
          l_tcy_refno      := NULL;
          l_app_refno      := NULL;
          l_surv_refno     := NULL;
          l_acas_reference := NULL;
          l_inh_refno      := NULL;
--
--
-- Main processing
--
-- Open any cursors
--
--
-- Get the Relevent Object
--
          IF (p1.LIHU_REFERENCE_TYPE = 'TAR') THEN
--
               OPEN c_tar_refno(p1.LIHU_REFERENCE_VALUE);
              FETCH c_tar_refno INTO l_tcy_refno;
              CLOSE c_tar_refno;
--
          ELSIF (p1.LIHU_REFERENCE_TYPE = 'TCY') THEN 
--
              l_tcy_refno := p1.LIHU_REFERENCE_VALUE;
--
          ELSIF (p1.LIHU_REFERENCE_TYPE = 'ALR') THEN
--
               OPEN c_alr_refno(p1.LIHU_REFERENCE_VALUE);
              FETCH c_alr_refno INTO l_app_refno;
              CLOSE c_alr_refno;
--
          ELSIF (p1.LIHU_REFERENCE_TYPE = 'APP') THEN 
--
              l_app_refno := p1.LIHU_REFERENCE_VALUE;
--
          ELSIF (p1.LIHU_REFERENCE_TYPE = 'SURV') THEN 
--
               OPEN c_surv_refno(p1.LIHU_REFERENCE_VALUE);
              FETCH c_surv_refno INTO l_surv_refno;
              CLOSE c_surv_refno;
--
          ELSIF (p1.LIHU_REFERENCE_TYPE = 'ACAS') THEN 
--
               OPEN c_acas_reference(p1.LIHU_REFERENCE_VALUE);
              FETCH c_acas_reference INTO l_acas_reference;
              CLOSE c_acas_reference;
--
          END IF;
--
--
-- Get internal income_header refno linkinh into the DL table
--
           OPEN c_inh_refno(p1.LIHU_INH_LEGACY_REF);
          FETCH c_inh_refno INTO l_inh_refno;
          CLOSE c_inh_refno;
--
--
-- Insert into income_headers
--
           INSERT /* +APPEND */ into  income_header_usages(IHU_REFNO,
                                                           IHU_INH_REFNO,
                                                           IHU_CREATED_BY,
                                                           IHU_CREATED_DATE,
                                                           IHU_APP_REFNO,
                                                           IHU_BAN_REFERENCE,
                                                           IHU_TCY_REFNO,
                                                           IHU_SURV_REFNO,
                                                           IHU_ACAS_REFERENCE
                                                          )
--
                                                   VALUES (p1.LIHU_REFNO,
                                                           l_inh_refno,
                                                           p1.LIHU_CREATED_BY,
                                                           p1.LIHU_CREATED_DATE,
                                                           l_app_refno,
                                                           NULL,
                                                           l_tcy_refno,
                                                           l_surv_refno,
                                                           l_acas_reference
                                                          );
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,50000) = 0 THEN 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_HEADER_USAGES');
--
    execute immediate 'alter trigger IHU_BR_I enable';
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
SELECT rowid rec_rowid,
       LIHU_DLB_BATCH_ID,
       LIHU_DL_SEQNO,
       LIHU_DL_LOAD_STATUS,
       LIHU_INH_LEGACY_REF,
       NVL(LIHU_CREATED_BY,'DATALOAD') LIHU_CREATED_BY,
       NVL(LIHU_CREATED_DATE,SYSDATE)  LIHU_CREATED_DATE,
       LIHU_REFERENCE_TYPE,
       LIHU_REFERENCE_VALUE,
       LIHU_REFNO
  FROM dl_hem_income_header_usages
 WHERE lihu_dlb_batch_id   = p_batch_id
   AND lihu_dl_load_status in ('L','F','O');
--
-- ************************************************************************************
--
-- Additional Cursors
--
CURSOR c_chk_inh_exists(p_inh_legacy_ref NUMBER) 
IS
SELECT linh_refno
  FROM dl_hem_income_headers,
       income_headers
 WHERE linh_legacy_ref     = p_inh_legacy_ref
   AND linh_dl_load_status = 'C'
   AND inh_refno           = linh_refno;
--
-- ************************************************************************************
--
CURSOR c_inh_refno(p_inh_refno NUMBER) 
IS
SELECT inh_refno
  FROM income_headers
 WHERE inh_refno = p_inh_refno;
--
-- ************************************************************************************
--
CURSOR c_tar_refno(p_tcy_alt_ref VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_alt_ref;
--
-- ************************************************************************************
--
CURSOR c_tcy_refno(p_tcy_refno VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_refno = p_tcy_refno;
--
-- ************************************************************************************
--
CURSOR c_app_refno(p_app_refno VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_refno = p_app_refno;
--
-- ************************************************************************************
--
CURSOR c_alr_refno(p_app_legacy_ref VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;
--
-- ***********************************************************************
--
CURSOR c_surv_refno(p_surv_legacy_ref    VARCHAR2)
IS
SELECT surv_refno
  FROM subsidy_reviews
 WHERE surv_legacy_ref     = p_surv_legacy_ref;
--
-- ************************************************************************************
--
CURSOR c_acas_reference(p_acas_alternate_reference VARCHAR2) 
IS
SELECT acas_reference
  FROM advice_cases
 WHERE acas_alternate_reference = p_acas_alternate_reference;
--
-- ************************************************************************************
--
CURSOR c_ihu_exists(p_inh_refno     NUMBER,
                    p_app_refno     NUMBER,
                    p_ban_reference NUMBER,
                    p_surv_refno    NUMBER,
                    p_tcy_refno     NUMBER) 
IS
SELECT 'X'
  FROM income_header_usages
 WHERE ihu_inh_refno                    = p_inh_refno
   AND NVL(ihu_app_refno,111222111)     = NVL(p_app_refno,111222111)
   AND NVL(ihu_ban_reference,111222111) = NVL(p_ban_reference,111222111)
   AND NVL(ihu_surv_refno,111222111)    = NVL(p_surv_refno,111222111)
   AND NVL(ihu_tcy_refno,111222111)     = NVL(p_tcy_refno,111222111);
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                      VARCHAR2(30);
cd                      DATE;
cp                      VARCHAR2(30) := 'VALIDATE';
ct                      VARCHAR2(30) := 'DL_HEM_INCOME_HEADER_USAGES';
cs                      INTEGER;
ce                      VARCHAR2(200);
l_id                    ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists                VARCHAR2(1);
l_inh_refno             NUMBER(10);
l_tcy_refno             NUMBER(10);
l_app_refno             NUMBER(10);
l_surv_refno            NUMBER(10);
l_acas_reference        NUMBER(10);
l_ihu_exists            VARCHAR2(1);
l_errors                VARCHAR2(10);
l_error_ind             VARCHAR2(10);
i                       INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_income_header_usages.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_income_header_usages.dataload_validate',3);
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
          cs   := p1.lihu_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
          l_inh_refno      := NULL;
          l_tcy_refno      := NULL;
          l_app_refno      := NULL;
          l_surv_refno     := NULL;
          l_acas_reference := NULL;

--
-- ***********************************************************************
--
-- Check Income Header Legacy Reference exists in INCOME_HEADERS
--
--
           OPEN c_chk_inh_exists(p1.LIHU_INH_LEGACY_REF);
          FETCH c_chk_inh_exists INTO l_inh_refno;
--
          IF (c_chk_inh_exists%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',473);
          END IF;
--
          CLOSE c_chk_inh_exists;
--
-- ************************************************************************************
--
-- Check the Reference Type is Valid
--
          IF p1.LIHU_REFERENCE_TYPE NOT IN ('TCY', 'TAR', 'APP', 'ALR', 'SURV', 'ACAS') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',865);
          END IF;
--
-- ************************************************************************************
--
-- Check Tenancy reference supplied is valid
--
--
          IF (p1.LIHU_REFERENCE_TYPE = 'TCY') THEN
--
            OPEN c_tcy_refno(p1.LIHU_REFERENCE_VALUE);
           FETCH c_tcy_refno INTO l_tcy_refno;
--
           IF (c_tcy_refno%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',844);
           END IF;
--
           CLOSE c_tcy_refno;
--
          END IF;
--
-- *********************
--
          IF (p1.LIHU_REFERENCE_TYPE = 'TAR') THEN
--
            OPEN c_tar_refno(p1.LIHU_REFERENCE_VALUE);
           FETCH c_tar_refno INTO l_tcy_refno;
--
           IF (c_tar_refno%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',845);
           END IF;
--
           CLOSE c_tar_refno;
--
          END IF;
--
-- *********************
--
          IF (p1.LIHU_REFERENCE_TYPE = 'APP') THEN
--
            OPEN c_app_refno(p1.LIHU_REFERENCE_VALUE);
           FETCH c_app_refno INTO l_app_refno;
--
           IF (c_app_refno%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',595);
           END IF;
--
           CLOSE c_app_refno;
--
          END IF;
--
-- *********************
--
          IF (p1.LIHU_REFERENCE_TYPE = 'ALR') THEN
--
            OPEN c_alr_refno(p1.LIHU_REFERENCE_VALUE);
           FETCH c_alr_refno INTO l_app_refno;
--
           IF (c_alr_refno%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',049);
           END IF;
--
           CLOSE c_alr_refno;
--
          END IF;
--
-- *********************
--
          IF (p1.LIHU_REFERENCE_TYPE = 'SURV') THEN
--
            OPEN c_surv_refno(p1.LIHU_REFERENCE_VALUE);
           FETCH c_surv_refno INTO l_surv_refno;
--
           IF (c_surv_refno%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',474);
           END IF;
--
           CLOSE c_surv_refno;
--
          END IF;
--
-- *********************
--
          IF (p1.LIHU_REFERENCE_TYPE = 'ACAS') THEN
--
            OPEN c_acas_reference(p1.LIHU_REFERENCE_VALUE);
           FETCH c_acas_reference INTO l_acas_reference;
--
           IF (c_acas_reference%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',144);
           END IF;
--
           CLOSE c_acas_reference;
--
          END IF;
--
-- ***********************************************************************
--
-- check Income header usage record doesn't already exist for combination of
-- values supplied
--
--
            OPEN c_ihu_exists(l_inh_refno,
                              l_app_refno,
                              NULL,
                              l_surv_refno,
                              l_tcy_refno);
--
           FETCH c_ihu_exists INTO l_ihu_exists;
           CLOSE c_ihu_exists;
--
           IF (l_ihu_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',476);
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
                          p_date           IN date) 
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LIHU_DLB_BATCH_ID,
       LIHU_DL_SEQNO,
       LIHU_DL_LOAD_STATUS,
       LIHU_INH_LEGACY_REF,
       LIHU_REFNO
  FROM dl_hem_income_header_usages
 WHERE lihu_dlb_batch_id   = p_batch_id
   AND lihu_dl_load_status = 'C';
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
cb              VARCHAR2(30);
cd              DATE;
cp              VARCHAR2(30) := 'DELETE';
ct              VARCHAR2(30) := 'DL_HEM_INCOME_HEADER_USAGES';
cs              INTEGER;
ce              VARCHAR2(200);
l_id            ROWID;
l_an_tab        VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i               INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_income_header_usages.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_income_header_usages.dataload_delete',3 );
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
          cs   := p1.lihu_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;

--
-- Delete from income_headers table
--
          DELETE 
            FROM income_header_usages
           WHERE ihu_refno = p1.LIHU_REFNO;
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
          IF mod(i,10000) = 0 THEN 
           commit; 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_HEADER_USAGES');
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
END s_dl_hem_income_header_usages;
/