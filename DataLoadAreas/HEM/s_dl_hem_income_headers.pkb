CREATE OR REPLACE PACKAGE BODY s_dl_hem_income_headers
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         	WHY
--
--  1.0     6.5.0     VS   23-JUNE-2011 	Initial Creation.
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
  UPDATE dl_hem_income_headers
     SET linh_dl_load_status = p_status
   WHERE rowid               = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_income_headers');
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
       LINH_DLB_BATCH_ID,
       LINH_DL_SEQNO,
       LINH_DL_LOAD_STATUS,
       LINH_LEGACY_REF,
       LINH_PAR_REF_TYPE,
       LINH_PAR_REFERENCE,
       LINH_START_DATE,
       LINH_SCO_CODE,
       LINH_STATUS_DATE,
       NVL(LINH_CREATED_BY,'DATALOAD') LINH_CREATED_BY,
       NVL(LINH_CREATED_DATE,SYSDATE)  LINH_CREATED_DATE,
       LINH_END_DATE,
       LINH_MODIFIED_DATE,
       LINH_MODIFIED_BY,
       LINH_HRV_OVRD_CODE,
       LINH_INDR_REFERENCE,
       LINH_INH_LEGACY_REF,
       LINH_REFNO
  FROM dl_hem_income_headers
 WHERE linh_dlb_batch_id    = p_batch_id
   AND linh_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_prf_refno (p_par_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_INCOME_HEADERS';
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
l_par_refno          NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger INH_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_income_headers.dataload_create');
    fsc_utils.debug_message('s_dl_hem_income_headers.dataload_create',3);
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
          cs   := p1.linh_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
          l_par_refno := NULL;
--
-- Main processing
--
-- Open any cursors
--
--
-- Get the Relevent Object
--
          IF (p1.LINH_PAR_REF_TYPE = 'PRF') THEN
--
               OPEN c_prf_refno(p1.LINH_PAR_REFERENCE);
              FETCH c_prf_refno INTO l_par_refno;
              CLOSE c_prf_refno;
--
          ELSIF (p1.LINH_PAR_REF_TYPE = 'PAR') THEN 
--
              l_par_refno := p1.LINH_PAR_REFERENCE;
--
          END IF;
--
--
-- Insert into income_headers
--
           INSERT /* +APPEND */ into  income_headers(INH_REFNO,
                                                     INH_START_DATE,
                                                     INH_SCO_CODE,
                                                     INH_STATUS_DATE,
                                                     INH_CREATED_DATE,
                                                     INH_CREATED_BY,
                                                     INH_PAR_REFNO,
                                                     INH_END_DATE,
                                                     INH_MODIFIED_DATE,
                                                     INH_MODIFIED_BY,
                                                     INH_INDR_REFNO,
                                                     INH_INH_REFNO,
                                                     INH_HRV_OVRD_CODE
                                                    )
--
                                             VALUES (p1.LINH_REFNO,
                                                     p1.LINH_START_DATE,
                                                     p1.LINH_SCO_CODE,
                                                     p1.LINH_STATUS_DATE,
                                                     p1.LINH_CREATED_DATE,
                                                     p1.LINH_CREATED_BY,
                                                     l_par_refno,
                                                     p1.LINH_END_DATE,
                                                     p1.LINH_MODIFIED_DATE,
                                                     p1.LINH_MODIFIED_BY,
                                                     p1.LINH_INDR_REFERENCE,
                                                     p1.LINH_INH_LEGACY_REF,
                                                     p1.LINH_HRV_OVRD_CODE
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_HEADERS');
--
    execute immediate 'alter trigger INH_BR_I enable';
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
       LINH_DLB_BATCH_ID,
       LINH_DL_SEQNO,
       LINH_DL_LOAD_STATUS,
       LINH_LEGACY_REF,
       LINH_PAR_REF_TYPE,
       LINH_PAR_REFERENCE,
       LINH_START_DATE,
       LINH_SCO_CODE,
       LINH_STATUS_DATE,
       NVL(LINH_CREATED_BY,'DATALOAD') LINH_CREATED_BY,
       NVL(LINH_CREATED_DATE,SYSDATE)  LINH_CREATED_DATE,
       LINH_END_DATE,
       LINH_MODIFIED_DATE,
       LINH_MODIFIED_BY,
       LINH_HRV_OVRD_CODE,
       LINH_INDR_REFERENCE,
       LINH_INH_LEGACY_REF,
       LINH_REFNO
  FROM dl_hem_income_headers
 WHERE linh_dlb_batch_id    = p_batch_id
   AND linh_dl_load_status in ('L','F','O');
--
-- ************************************************************************************
--
-- Additional Cursors
--
CURSOR c_chk_inh_exists(p_inh_legacy_ref NUMBER) 
IS
SELECT 'X'
  FROM dl_hem_income_headers,
       income_headers
 WHERE linh_dl_load_status = 'C'
   AND linh_legacy_ref     = p_inh_legacy_ref
   AND inh_refno           = linh_refno;
--
-- ************************************************************************************
--
CURSOR c_prf_refno(p_par_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ************************************************************************************
--
CURSOR c_par_refno(p_par_refno VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_refno;
--
-- ************************************************************************************
--
CURSOR chk_indr_ref_exists(p_indr_refno   NUMBER) 
IS
SELECT 'X'
  FROM income_detail_requests
 WHERE indr_refno = p_indr_refno;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                      VARCHAR2(30);
cd                      DATE;
cp                      VARCHAR2(30) := 'VALIDATE';
ct                      VARCHAR2(30) := 'DL_HEM_INCOME_HEADERS';
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
l_inh_exists            VARCHAR2(1);
l_inh_inh_exists        VARCHAR2(1);
l_par_refno             NUMBER(10);
l_errors                VARCHAR2(10);
l_error_ind             VARCHAR2(10);
i                       INTEGER :=0;
l_indr_ref_exists       VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_income_headers.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_income_headers.dataload_validate',3);
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
          cs   := p1.linh_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
          l_par_refno      := NULL;
          l_inh_exists     := NULL;
          l_inh_inh_exists := NULL;

--
-- ***********************************************************************
--
-- Check Income Header Legacy Reference doesn't already exists in 
-- INCOME_HEADERS
--
           OPEN c_chk_inh_exists(p1.LINH_LEGACY_REF);
          FETCH c_chk_inh_exists INTO l_inh_exists;
--
          IF (c_chk_inh_exists%FOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',468);
          END IF;
--
          CLOSE c_chk_inh_exists;
--
-- ************************************************************************************
--
-- Check the Object Source Type is Valid
--
          IF p1.LINH_PAR_REF_TYPE NOT IN ('PAR', 'PRF') THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',864);
          END IF;
--
-- ************************************************************************************
--
-- Check Party reference supplied is valid
--
--
          IF (p1.LINH_PAR_REF_TYPE = 'PAR') THEN
--
            OPEN c_par_refno(p1.LINH_PAR_REFERENCE);
           FETCH c_par_refno INTO l_par_refno;
--
           IF (c_par_refno%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',208);
           END IF;
--
           CLOSE c_par_refno;
--
          END IF;
--
-- *********************
--
          IF (p1.LINH_PAR_REF_TYPE = 'PRF') THEN
--
            OPEN c_prf_refno(p1.LINH_PAR_REFERENCE);
           FETCH c_prf_refno INTO l_par_refno;
--
           IF (c_prf_refno%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',207);
           END IF;
--
           CLOSE c_prf_refno;
--
          END IF;
--
-- ***********************************************************************
-- 
-- The status code must be a valid code
--
          IF (p1.linh_sco_code NOT IN ( 'RAI', 'PEN', 'TBV', 'VER', 'CAN' )) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',469);
          END IF;
--
-- ***********************************************************************
-- 
-- The end date must not be before the start date
-- 
          IF (nvl(p1.linh_end_date, p1.linh_start_date) < p1.linh_start_date) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',003);
          END IF;
--
-- ***********************************************************************
--
-- If Supplied, check Income Detail Request Reference exists
--
          IF (p1.LINH_INDR_REFERENCE IS NOT NULL) THEN
--
           l_indr_ref_exists := NULL;
--
            OPEN chk_indr_ref_exists(p1.LINH_INDR_REFERENCE);
           FETCH chk_indr_ref_exists INTO l_indr_ref_exists;
           CLOSE chk_indr_ref_exists;
--
           IF (l_indr_ref_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',470);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If override Reason is supplied then the Override Income Header Reference
-- must be supplied
--
          IF (    p1.LINH_HRV_OVRD_CODE  IS NOT NULL
              AND p1.LINH_INH_LEGACY_REF IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',471);
--
          END IF;
--
-- ***********************************************************************
--
-- If supplied, check Override Income Header Legacy Reference  exists 
-- in INCOME_HEADERS
--
          IF (p1.LINH_INH_LEGACY_REF IS NOT NULL) THEN
--
            OPEN c_chk_inh_exists(p1.LINH_INH_LEGACY_REF);
           FETCH c_chk_inh_exists INTO l_inh_inh_exists;
--
          IF (c_chk_inh_exists%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',472);
          END IF;
--
          CLOSE c_chk_inh_exists;
--
         END IF;
--
-- ***********************************************************************
--
-- All reference values are valid
-- 
-- Override Reason
--
          IF (NOT s_dl_hem_utils.exists_frv('OVERRIDERSN',p1.linh_hrv_ovrd_code,'Y')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',15);
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
       LINH_DLB_BATCH_ID,
       LINH_DL_SEQNO,
       LINH_DL_LOAD_STATUS,
       LINH_LEGACY_REF,
       LINH_REFNO
  FROM dl_hem_income_headers
 WHERE linh_dlb_batch_id   = p_batch_id
   AND linh_dl_load_status = 'C';
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
ct              VARCHAR2(30) := 'DL_HEM_INCOME_HEADERS';
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
    fsc_utils.proc_start('s_dl_hem_income_headers.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_income_headers.dataload_delete',3 );
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
          cs   := p1.linh_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;

--
-- Delete from income_headers table
--
          DELETE 
            FROM income_headers
           WHERE inh_refno = p1.LINH_REFNO;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_HEADERS');
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
END s_dl_hem_income_headers;
/