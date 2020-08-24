CREATE OR REPLACE PACKAGE s_dl_hra_lwr_assessments
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER  DB Ver   WHO  WHEN          WHY
  --  1.0  5.17.0   VRS  27-APR-2010   INITIAL Version
  --
  --  declare package variables AND constants


  --***********************************************************************
  --  DESCRIPTION
  --
  --  1:  ...
  --  2:  ...
  --  REFERENCES FUNCTION
  --
  --
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END s_dl_hra_lwr_assessments;
--
/


CREATE OR REPLACE PACKAGE BODY s_dl_hra_lwr_assessments
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.17.0    VS   28-APR-2010  Initial Creation.
--
--  2.0     5.17.0    VS   30-AUG-2010  CR101 Changes.
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
    UPDATE dl_hra_lwr_assessments
       SET lwra_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hra_lwr_assessments');
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
       LWRA_DLB_BATCH_ID,
       LWRA_DL_SEQNO,
       LWRA_DL_LOAD_STATUS,
       LWRA_REFNO,
       LWRA_LWRB_REFNO,
       LWRA_TYPE,
       LWRA_SCO_CODE,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LWRA_CURRENT_ASSESSMENT_REF,
       LWRA_LOT_NUMBER,
       LWRA_PLAN_NUMBER,
       LWRA_PLAN_TYPE,
       LWRA_TOTAL_PAYABLE_AMOUNT,
       LWRA_CR_DR_INDICATOR,
       LWRA_ADDRESS,
       NVL(LWRA_CREATED_BY, 'DATALOAD') LWRA_CREATED_BY,
       NVL(LWRA_CREATED_DATE, SYSDATE)  LWRA_CREATED_DATE,
       LWRA_SECTION_NUMBER,
       LWRA_VG_INSERT_NUMBER,
       LWRA_VG_DISTRICT_NUMBER,
       LWRA_LAND_RATEABLE_VALUE,
       LWRA_INSTALMENT_AMOUNT,
       LWRA_INSTALMENT_DUE_DATE,
       LWRA_PREV_ASSESSMENT_REF,
       LWRA_NO_OF_EXTRA_CHARGES,
       LWRA_INSTALMENT_1_AMOUNT,
       LWRA_INSTALMENT_2_AMOUNT,
       LWRA_INSTALMENT_3_AMOUNT,
       LWRA_INSTALMENT_4_AMOUNT,
       LWRA_CATEGORY_RECORD_COUNT,
       LWRA_CURRENT_ANNUAL_RATES,
       LWRA_FAOR_CODE,
       LWRA_LGA_CODE,
       LWRA_MODIFIED_BY,
       LWRA_MODIFIED_DATE,
       LWRA_PRO_PROPREF,
       LWRA_PREV_LWRA_REFNO
  FROM dl_hra_lwr_assessments
 WHERE lwra_dlb_batch_id   = p_batch_id
   AND lwra_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_ltl_refno(p_lltl_ref          VARCHAR2,
                   p_ltl_hrv_fltt_type VARCHAR2)
IS
SELECT ltl_refno
  FROM land_titles
 WHERE ltl_reference     = p_lltl_ref
   AND ltl_hrv_fltt_type = p_ltl_hrv_fltt_type;
--
-- ***********************************************************************
--
CURSOR c_pro_refno(p_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_ASSESSMENTS';
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
l_ltl_reference      VARCHAR2(17);
l_ltl_refno          NUMBER(10);
--
l_pro_refno          NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger LWRA_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hra_lwr_assessments.dataload_create');
    fsc_utils.debug_message('s_dl_hra_lwr_assessments.dataload_create',3);
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
          cs   := p1.lwra_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- get Land title reference
--
          l_ltl_refno     := NULL;
          l_ltl_reference := NULL;
--
--
          IF p1.lwra_plan_type = 'SP' THEN
--
           l_ltl_reference := p1.lwra_lot_number||'/'||p1.lwra_plan_number;
--
          ELSIF p1.lwra_plan_type = 'DP' THEN
--
              IF (p1.lwra_section_number IS NULL) THEN
               l_ltl_reference := p1.lwra_lot_number||'/'||p1.lwra_plan_number;
              ELSE
                 l_ltl_reference := p1.lwra_lot_number||'/'||p1.lwra_section_number||'/'||p1.lwra_plan_number;
              END IF;
--
          END IF;
--
--
           OPEN c_ltl_refno(l_ltl_reference, p1.lwra_plan_type);
          FETCH c_ltl_refno into l_ltl_refno;
          CLOSE c_ltl_refno;
--
-- get Pro Refno if lwra_pro_propref is supplied
--
          l_pro_refno := NULL;
--
          IF (p1.lwra_pro_propref IS NOT NULL) THEN
--
            OPEN c_pro_refno(p1.lwra_pro_propref);
           FETCH c_pro_refno INTO l_pro_refno;
           CLOSE c_pro_refno;
--
          END IF;
--
--
--
-- Insert into LWR_ASSESSMENTS table
--
        INSERT /* +APPEND */ INTO  lwr_assessments(LWRA_REFNO,
                                                   LWRA_TYPE,
                                                   LWRA_LWRB_REFNO,
                                                   LWRA_SCO_CODE,
                                                   LWRA_RATE_PERIOD_START_DATE,
                                                   LWRA_RATE_PERIOD_END_DATE,
                                                   LWRA_CURRENT_ASSESSMENT_REF,
                                                   LWRA_LOT_NUMBER,
                                                   LWRA_PLAN_NUMBER,
                                                   LWRA_PLAN_TYPE,
                                                   LWRA_TOTAL_PAYABLE_AMOUNT,
                                                   LWRA_CR_DR_INDICATOR,
                                                   LWRA_ADDRESS,
                                                   LWRA_CREATED_DATE,
                                                   LWRA_CREATED_BY,
                                                   LWRA_LTL_REFNO,
                                                   LWRA_SECTION_NUMBER,
                                                   LWRA_VG_INSERT_NUMBER,
                                                   LWRA_VG_DISTRICT_NUMBER,
                                                   LWRA_LAND_RATEABLE_VALUE,
                                                   LWRA_INSTALMENT_AMOUNT,
                                                   LWRA_INSTALMENT_DUE_DATE,
                                                   LWRA_PREV_ASSESSMENT_REF,
                                                   LWRA_NO_OF_EXTRA_CHARGES,
                                                   LWRA_INSTALMENT_1_AMOUNT,
                                                   LWRA_INSTALMENT_2_AMOUNT,
                                                   LWRA_INSTALMENT_3_AMOUNT,
                                                   LWRA_INSTALMENT_4_AMOUNT,
                                                   LWRA_CATEGORY_RECORD_COUNT,
                                                   LWRA_CURRENT_ANNUAL_RATES,
                                                   LWRA_FAOR_CODE,
                                                   LWRA_LGA_CODE,
                                                   LWRA_MODIFIED_BY,
                                                   LWRA_MODIFIED_DATE,
                                                   LWRA_PRO_REFNO,
                                                   LWRA_PREV_LWRA_REFNO
                                                  )
--
                                           VALUES (p1.LWRA_REFNO,
                                                   p1.LWRA_TYPE,
                                                   p1.LWRA_LWRB_REFNO,
                                                   p1.LWRA_SCO_CODE,
                                                   p1.LWRA_RATE_PERIOD_START_DATE,
                                                   p1.LWRA_RATE_PERIOD_END_DATE,
                                                   p1.LWRA_CURRENT_ASSESSMENT_REF,
                                                   p1.LWRA_LOT_NUMBER,
                                                   p1.LWRA_PLAN_NUMBER,
                                                   p1.LWRA_PLAN_TYPE,
                                                   p1.LWRA_TOTAL_PAYABLE_AMOUNT,
                                                   p1.LWRA_CR_DR_INDICATOR,
                                                   p1.LWRA_ADDRESS,
                                                   p1.LWRA_CREATED_DATE,
                                                   p1.LWRA_CREATED_BY,
                                                   l_ltl_refno,
                                                   p1.LWRA_SECTION_NUMBER,
                                                   p1.LWRA_VG_INSERT_NUMBER,
                                                   p1.LWRA_VG_DISTRICT_NUMBER,
                                                   p1.LWRA_LAND_RATEABLE_VALUE,
                                                   p1.LWRA_INSTALMENT_AMOUNT,
                                                   p1.LWRA_INSTALMENT_DUE_DATE,
                                                   p1.LWRA_PREV_ASSESSMENT_REF,
                                                   p1.LWRA_NO_OF_EXTRA_CHARGES,
                                                   p1.LWRA_INSTALMENT_1_AMOUNT,
                                                   p1.LWRA_INSTALMENT_2_AMOUNT,
                                                   p1.LWRA_INSTALMENT_3_AMOUNT,
                                                   p1.LWRA_INSTALMENT_4_AMOUNT,
                                                   p1.LWRA_CATEGORY_RECORD_COUNT,
                                                   p1.LWRA_CURRENT_ANNUAL_RATES,
                                                   p1.LWRA_FAOR_CODE,
                                                   p1.LWRA_LGA_CODE,
                                                   p1.LWRA_MODIFIED_BY,
                                                   p1.LWRA_MODIFIED_DATE,
                                                   l_pro_refno,
                                                   p1.LWRA_PREV_LWRA_REFNO
                                                  );
--
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_ASSESSMENTS');
--
    execute immediate 'alter trigger LWRA_BR_I enable';
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
SELECT rowid rec_rowid,
       LWRA_DLB_BATCH_ID,
       LWRA_DL_SEQNO,
       LWRA_DL_LOAD_STATUS,
       LWRA_REFNO,
       LWRA_LWRB_REFNO,
       LWRA_TYPE,
       LWRA_SCO_CODE,
       LWRA_RATE_PERIOD_START_DATE,
       LWRA_RATE_PERIOD_END_DATE,
       LWRA_CURRENT_ASSESSMENT_REF,
       LWRA_LOT_NUMBER,
       LWRA_PLAN_NUMBER,
       LWRA_PLAN_TYPE,
       LWRA_TOTAL_PAYABLE_AMOUNT,
       LWRA_CR_DR_INDICATOR,
       LWRA_ADDRESS,
       NVL(LWRA_CREATED_BY, 'DATALOAD') LWRA_CREATED_BY,
       NVL(LWRA_CREATED_DATE, SYSDATE)  LWRA_CREATED_DATE,
       LWRA_SECTION_NUMBER,
       LWRA_VG_INSERT_NUMBER,
       LWRA_VG_DISTRICT_NUMBER,
       LWRA_LAND_RATEABLE_VALUE,
       LWRA_INSTALMENT_AMOUNT,
       LWRA_INSTALMENT_DUE_DATE,
       LWRA_PREV_ASSESSMENT_REF,
       LWRA_NO_OF_EXTRA_CHARGES,
       LWRA_INSTALMENT_1_AMOUNT,
       LWRA_INSTALMENT_2_AMOUNT,
       LWRA_INSTALMENT_3_AMOUNT,
       LWRA_INSTALMENT_4_AMOUNT,
       LWRA_CATEGORY_RECORD_COUNT,
       LWRA_CURRENT_ANNUAL_RATES,
       LWRA_FAOR_CODE,
       LWRA_LGA_CODE,
       LWRA_MODIFIED_BY,
       LWRA_MODIFIED_DATE,
       LWRA_PRO_PROPREF,
       LWRA_PREV_LWRA_REFNO
  FROM dl_hra_lwr_assessments
 WHERE lwra_dlb_batch_id   = p_batch_id
   AND lwra_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Check Batch id doesn't already exist on lwr_batches
--
CURSOR chk_batch_id(p_lwrb_refno NUMBER)
IS
SELECT lwrb_refno
  FROM lwr_batches
 WHERE lwrb_refno = p_lwrb_refno;
--
-- ***********************************************************************
--
CURSOR chk_ltl_exists(p_ltl_reference     VARCHAR2,
                      p_ltl_hrv_fltt_type VARCHAR2)
IS
SELECT ltl_refno
  FROM land_titles
 WHERE ltl_reference     = p_ltl_reference
   AND ltl_hrv_fltt_type = p_ltl_hrv_fltt_type;
--
-- ***********************************************************************
--
CURSOR chk_pro_exists(p_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ***********************************************************************
--
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_ASSESSMENTS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists             VARCHAR2(1);
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
i                    INTEGER :=0;
--
l_lwrb_refno         NUMBER(10);
l_lwrb_lwrb_refno    NUMBER(10);
l_ltl_reference      VARCHAR2(17);
l_ltl_refno          NUMBER(10);
--
l_pro_refno          NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_assessments.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_lwr_assessments.dataload_validate',3);
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
          cs   := p1.lwra_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
-- Validation checks required
--
-- Check batch_id exists
--
          l_lwrb_refno := NULL;
--
           OPEN chk_batch_id(p1.lwra_lwrb_refno);
          FETCH chk_batch_id INTO l_lwrb_refno;
          CLOSE chk_batch_id;
--
          IF (l_lwrb_refno IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',352);
          END IF;
--
-- ***********************************************************************
--
-- Check Assessment Type is valid
--
          IF (p1.lwra_type NOT IN ('SURI','WAAS','ANRH')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',353);
          END IF;
--
-- ***********************************************************************
--
-- Check Batch Status is valid
--
          IF (p1.lwra_sco_code NOT IN ('NEW','PAY','DNP','INV')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',354);
          END IF;
--
-- ***********************************************************************
--
-- Check Assessment LWRA_RATE_PERIOD_START_DATE < LWRA_RATE_PERIOD_END_DATE
--
          IF (p1.lwra_rate_period_start_date > p1.lwra_rate_period_end_date) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',355);
          END IF;
--
-- ***********************************************************************
--
-- Check Land title exists
--
--
          l_ltl_reference := NULL;
--
--
          IF p1.lwra_plan_type = 'SP' THEN
--
           l_ltl_reference := p1.lwra_lot_number||'/'||TO_CHAR(p1.lwra_plan_number);
--
          ELSIF p1.lwra_plan_type = 'DP' THEN
--
              IF (p1.lwra_section_number IS NULL) THEN
               l_ltl_reference := p1.lwra_lot_number||'/'|| TO_CHAR(p1.lwra_plan_number);
              ELSE
                 l_ltl_reference := p1.lwra_lot_number||'/'||p1.lwra_section_number||'/'|| TO_CHAR(p1.lwra_plan_number);
              END IF;
--
          END IF;
--
--
           OPEN chk_ltl_exists(l_ltl_reference, p1.lwra_plan_type);
          FETCH chk_ltl_exists INTO l_ltl_refno;
--
          IF (chk_ltl_exists%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',459);
          END IF;
--
          CLOSE chk_ltl_exists;
--
-- ***********************************************************************
--
-- Check Credit Debit Indicator is valid
--
          IF (p1.lwra_cr_dr_indicator NOT IN ('CR','DR')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',360);
          END IF;
--
-- ***********************************************************************
--
-- If supplied, validate the override reason code
--
          IF (p1.lwra_faor_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('LWR_ASS_ORRIDE_RSN',p1.lwra_faor_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',356);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If the Assessment is for an Annual Batch(ANRH) then the following must
-- be provided: Total Amount, Category Record Count, Instalment Amount,
-- Instalment Due Date, Instalment1 and Rateable Value. Instalment2,
-- Instalment3 and Instalment4 if available
--
--
          IF (p1.lwra_type = 'ANRH') THEN
--
           IF (   p1.LWRA_TOTAL_PAYABLE_AMOUNT  IS NULL
               OR p1.LWRA_CATEGORY_RECORD_COUNT IS NULL
               OR p1.LWRA_INSTALMENT_AMOUNT     IS NULL
               OR p1.LWRA_INSTALMENT_DUE_DATE   IS NULL
               OR p1.LWRA_INSTALMENT_1_AMOUNT   IS NULL
               OR p1.LWRA_LAND_RATEABLE_VALUE   IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',357);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If the Assessment is for an Instalment Batch(SURI) the following must
-- be supplied: Instalment Amount, Instalment Due Date and No of Extra Charges
--
--
          IF (p1.lwra_type = 'SURI') THEN
--
           IF (   p1.LWRA_INSTALMENT_AMOUNT   IS NULL
               OR p1.LWRA_INSTALMENT_DUE_DATE IS NULL
               OR p1.LWRA_NO_OF_EXTRA_CHARGES IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',358);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- If the Assessment is for a Water Batch(WAAS) then the following must
-- be provided: Period Start Date, Period End Date, Category Record Count, Rateable Value
--
--
          IF (p1.lwra_type = 'WAAS') THEN
--
           IF (   p1.LWRA_RATE_PERIOD_START_DATE IS NULL
               OR p1.LWRA_RATE_PERIOD_END_DATE   IS NULL
               OR p1.LWRA_CATEGORY_RECORD_COUNT  IS NULL
               OR p1.LWRA_LAND_RATEABLE_VALUE    IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',359);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
          l_pro_refno := NULL;
--
          IF (p1.lwra_pro_propref IS NOT NULL) THEN
--
            OPEN chk_pro_exists(p1.lwra_pro_propref);
           FETCH chk_pro_exists INTO l_pro_refno;
           CLOSE chk_pro_exists;
--
           IF (l_pro_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',55);
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
CURSOR c1
IS
SELECT rowid rec_rowid,
       LWRA_DLB_BATCH_ID,
       LWRA_DL_SEQNO,
       LWRA_DL_LOAD_STATUS,
       LWRA_REFNO
  FROM dl_hra_lwr_assessments
 WHERE lwra_dlb_batch_id   = p_batch_id
   AND lwra_dl_load_status = 'C';
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
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_LWR_ASSESSMENTS';
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
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_lwr_assessments.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_lwr_assessments.dataload_delete',3 );
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
          cs   := p1.lwra_dl_seqno;
          l_id := p1.rec_rowid;
--
--
-- Delete from LWR_ASSESSMENTS table
--
--
          DELETE
            FROM lwr_assessments
           WHERE lwra_refno = p1.lwra_refno;
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
          i := i + 1;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('LWR_ASSESSMENTS');
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
END s_dl_hra_lwr_assessments;
/
