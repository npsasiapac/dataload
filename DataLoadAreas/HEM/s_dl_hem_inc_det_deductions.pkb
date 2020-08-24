CREATE OR REPLACE PACKAGE BODY s_dl_hem_inc_det_deductions
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.5.0     VS   04-JUL-2011  Initial Creation.
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
    UPDATE dl_hem_inc_det_deductions
       SET lindd_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_inc_det_deductions');
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
       LINDD_DLB_BATCH_ID,
       LINDD_DL_SEQNO,
       LINDD_DL_LOAD_STATUS,
       LINDD_INDT_LEGACY_REF,
       LINDD_INCO_CODE,
       LINDD_HRV_DDCO_CODE,
       LINDD_AMOUNT,
       LINDD_REGULAR_AMOUNT,
       NVL(LINDD_CREATED_BY,'DATALOAD') LINDD_CREATED_BY,
       NVL(LINDD_CREATED_DATE,SYSDATE)  LINDD_CREATED_DATE,
       LINDD_COMMENTS,
       LINDD_MODIFIED_BY,
       LINDD_MODIFIED_DATE
  FROM dl_hem_inc_det_deductions
 WHERE lindd_dlb_batch_id   = p_batch_id
   AND lindd_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR get_indt_refno(p_indt_legacy_ref   NUMBER) 
IS
SELECT indt_refno
  FROM dl_hem_income_details,
       income_details
 WHERE lindt_legacy_ref      = p_indt_legacy_ref
   AND lindt_dl_load_status  = 'C'
   AND lindt_refno           = indt_refno;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_INC_DET_DEDUCTIONS';
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
--
l_indt_refno         NUMBER(10);
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger INDD_BR_I disable';
    execute immediate 'alter trigger INDD_BR_IU disable';
--
    fsc_utils.proc_start('s_dl_hem_inc_det_deductions.dataload_create');
    fsc_utils.debug_message('s_dl_hem_inc_det_deductions.dataload_create',3);
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
          cs   := p1.lindd_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
--
          l_indt_refno := NULL;
--
           OPEN get_indt_refno(p1.LINDD_INDT_LEGACY_REF);
          FETCH get_indt_refno INTO l_indt_refno;
          CLOSE get_indt_refno;
--
--
-- Now insert into income_detail_deductions
--
          INSERT /* +APPEND */ INTO  income_detail_deductions(INDD_INDT_REFNO,
                                                              INDD_INCO_CODE,
                                                              INDD_HRV_DDCO_CODE,
                                                              INDD_AMOUNT,
                                                              INDD_REGULAR_AMOUNT,
                                                              INDD_CREATED_BY,
                                                              INDD_CREATED_DATE,
                                                              INDD_COMMENTS,
                                                              INDD_MODIFIED_BY,
                                                              INDD_MODIFIED_DATE
                                                             )
--
                                                      VALUES (l_indt_refno,
                                                              p1.LINDD_INCO_CODE,
                                                              p1.LINDD_HRV_DDCO_CODE,
                                                              p1.LINDD_AMOUNT,
                                                              p1.LINDD_REGULAR_AMOUNT,
                                                              p1.LINDD_CREATED_BY,
                                                              p1.LINDD_CREATED_DATE,
                                                              p1.LINDD_COMMENTS,
                                                              p1.LINDD_MODIFIED_BY,
                                                              p1.LINDD_MODIFIED_DATE
                                                             );
-- 
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,5000) = 0 THEN 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_DETAIL_DEDUCTIONS');
--
    execute immediate 'alter trigger INDD_BR_I enable';
    execute immediate 'alter trigger INDD_BR_IU enable';
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
PROCEDURE dataload_validate(p_batch_id       IN VARCHAR2,
                            p_date           IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LINDD_DLB_BATCH_ID,
       LINDD_DL_SEQNO,
       LINDD_DL_LOAD_STATUS,
       LINDD_INDT_LEGACY_REF,
       LINDD_INCO_CODE,
       LINDD_HRV_DDCO_CODE,
       LINDD_AMOUNT,
       LINDD_REGULAR_AMOUNT,
       NVL(LINDD_CREATED_BY,'DATALOAD') LINDD_CREATED_BY,
       NVL(LINDD_CREATED_DATE,SYSDATE)  LINDD_CREATED_DATE,
       LINDD_COMMENTS,
       LINDD_MODIFIED_BY,
       LINDD_MODIFIED_DATE
  FROM dl_hem_inc_det_deductions
 WHERE lindd_dlb_batch_id   = p_batch_id
   AND lindd_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR chk_indt_refno(p_indt_legacy_ref NUMBER) 
IS
SELECT indt_refno
  FROM dl_hem_income_details,
       income_details
 WHERE lindt_legacy_ref     = p_indt_legacy_ref
   AND lindt_dl_load_status = 'C'
   AND lindt_refno          = indt_refno;
--
-- ***********************************************************************
--
CURSOR chk_indt_exists(p_indt_refno     NUMBER,
                       p_indt_inco_code VARCHAR2) 
IS
SELECT 'X'
  FROM income_details
 WHERE indt_refno     = p_indt_refno
   AND indt_inco_code = p_indt_inco_code;
--
-- ***********************************************************************
--
CURSOR chk_incd_exists(p_incd_inco_code     VARCHAR2,
                       p_incd_hrv_ddco_code VARCHAR2) 
IS
SELECT 'X'
  FROM income_code_deductions
 WHERE incd_inco_code     = p_incd_inco_code
   AND incd_hrv_ddco_code = p_incd_hrv_ddco_code;
--
-- ***********************************************************************
--
CURSOR chk_inco_exists(p_incd_inco_code   VARCHAR2) 
IS
SELECT 'X'
  FROM income_codes
 WHERE inco_code = p_incd_inco_code;
--
-- ***********************************************************************
--
CURSOR chk_indd_exists(p_indd_indt_refno    NUMBER,
                       p_indd_inco_code     VARCHAR2,
                       p_indd_hrv_ddco_code VARCHAR2) 
IS
SELECT 'X'
  FROM income_detail_deductions
 WHERE indd_indt_refno    = p_indd_indt_refno
   AND indd_inco_code     = p_indd_inco_code
   AND indd_hrv_ddco_code = p_indd_hrv_ddco_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                       VARCHAR2(30);
cd                       DATE;
cp                       VARCHAR2(30) := 'VALIDATE';
ct                       VARCHAR2(30) := 'DL_HEM_INC_DET_DEDUCTIONS';
cs                       INTEGER;
ce                       VARCHAR2(200);
l_id                     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_indt_refno             NUMBER(10);
l_indt_exists            VARCHAR2(1);
l_incd_exists            VARCHAR2(1);
l_indd_exists            VARCHAR2(1);
l_inco_exists            VARCHAR2(1);
--
l_ded_code_valid         VARCHAR2(1);
--
l_errors                 VARCHAR2(10);
l_error_ind              VARCHAR2(10);
i                        INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_inc_det_deductions.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_inc_det_deductions.dataload_validate',3);
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
          cs   := p1.lindd_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
          l_ded_code_valid := 'Y';
--
--
-- ***********************************************************************
--
-- check to see if we can get the income detail reference from the income detail
-- data load table for the IHS unique income id.
--
-- If we get a reference check to see that it has been created successfully in 
-- income_details table.
--
--
          l_indt_refno  := NULL;
          l_indt_exists := NULL;
--
           OPEN chk_indt_refno(p1.LINDD_INDT_LEGACY_REF);
          FETCH chk_indt_refno INTO l_indt_refno;
          CLOSE chk_indt_refno;
--
          IF (l_indt_refno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',490);
--
          ELSE
--
              OPEN chk_indt_exists(l_indt_refno,p1.LINDD_INCO_CODE);
             FETCH chk_indt_exists INTO l_indt_exists;
             CLOSE chk_indt_exists;
--
             IF (l_indt_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',444);
             END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check the income code supplied is valid
--
          l_inco_exists := NULL;
--
           OPEN chk_inco_exists(p1.lindd_inco_code);
          FETCH chk_inco_exists INTO l_inco_exists;
          CLOSE chk_inco_exists;
--
          IF (l_inco_exists IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',018);
          END IF;

--
-- ***********************************************************************
--
--
--  DEDUCTION CODE
--
          IF (NOT s_dl_hem_utils.exists_frv('DEDUCTCODE',p1.lindd_hrv_ddco_code,'Y')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',445);
           l_ded_code_valid := 'N';
          END IF;
--
-- ***********************************************************************
--
-- Check the income code deductions combination supplied is valid
--
          l_incd_exists := NULL;
--
          IF (    l_inco_exists IS NOT NULL
              AND l_ded_code_valid = 'Y') THEN

            OPEN chk_incd_exists(p1.lindd_inco_code, p1.lindd_hrv_ddco_code);
           FETCH chk_incd_exists INTO l_incd_exists;
           CLOSE chk_incd_exists;
--
           IF (l_incd_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',446);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check record doesn't already exist in income_detail_deduction for
-- indt_refno/incd_inco_code/incd_hrv_ddco_code combination supplied
--
          l_indd_exists := NULL;
--
          IF (    l_indt_refno     IS NOT NULL
              AND l_inco_exists    IS NOT NULL
              AND l_ded_code_valid = 'Y') THEN
--
            OPEN chk_indd_exists(l_indt_refno, p1.lindd_inco_code, p1.lindd_hrv_ddco_code);
           FETCH chk_indd_exists INTO l_indd_exists;
           CLOSE chk_indd_exists;
--
           IF (l_indd_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',447);
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
PROCEDURE dataload_delete(p_batch_id  IN VARCHAR2,
                          p_date      IN DATE) 
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LINDD_DLB_BATCH_ID,
       LINDD_DL_SEQNO,
       LINDD_DL_LOAD_STATUS,
       LINDD_INDT_LEGACY_REF,
       LINDD_INCO_CODE,
       LINDD_HRV_DDCO_CODE
  FROM dl_hem_inc_det_deductions
 WHERE lindd_dlb_batch_id   = p_batch_id
   AND lindd_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR get_indt_refno(p_indt_legacy_ref NUMBER) 
IS
SELECT indt_refno
  FROM dl_hem_income_details,
       income_details
 WHERE lindt_legacy_ref     = p_indt_legacy_ref
   AND lindt_dl_load_status = 'C'
   AND lindt_refno          = indt_refno;
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'DELETE';
ct               VARCHAR2(30) := 'DL_HEM_INC_DET_DEDUCTIONS';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
l_an_tab         VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
l_indt_refno     NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_inc_det_deductions.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_inc_det_deductions.dataload_delete',3 );
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
          cs   := p1.lindd_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Open any cursors
--
--
          l_indt_refno := NULL;
--
           OPEN get_indt_refno(p1.LINDD_INDT_LEGACY_REF);
          FETCH get_indt_refno INTO l_indt_refno;
          CLOSE get_indt_refno;
--
--
-- Delete from tables
--
          DELETE 
            FROM income_detail_deductions
           WHERE indd_indt_refno    = l_indt_refno
             AND indd_inco_code     = p1.LINDD_INCO_CODE
             AND indd_hrv_ddco_code = p1.LINDD_HRV_DDCO_CODE;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_DETAIL_DEDUCTIONS');
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
END s_dl_hem_inc_det_deductions;
/