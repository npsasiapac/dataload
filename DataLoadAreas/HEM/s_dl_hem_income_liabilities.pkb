CREATE OR REPLACE PACKAGE BODY s_dl_hem_income_liabilities
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.11      AJ   22-DEC-2015  Initial Creation.
--  1.1     6.11      AJ   21-APR-2016  syntax error at line 224 fixed
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
    UPDATE dl_hem_income_liabilities
       SET linli_dl_load_status = p_status
     WHERE rowid                = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_income_liabilities');
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
CURSOR  c1 
IS
SELECT rowid rec_rowid
       ,LINLI_DLB_BATCH_ID
       ,LINLI_DL_SEQNO
       ,LINLI_DL_LOAD_STATUS
       ,LINLI_LEGACY_REF
       ,LINLI_INH_LEGACY_REF
       ,LINLI_ILR_CODE
       ,LINLI_LIABLE_PERCENT
       ,NVL(LINLI_CREATED_BY,'DATALOAD') LINLI_CREATED_BY
       ,NVL(LINLI_CREATED_DATE, SYSDATE) LINLI_CREATED_DATE
       ,LINLI_PAYMENT_AMOUNT
       ,LINLI_INF_CODE
       ,LINLI_REGULAR_AMOUNT
       ,LINLI_HRV_VETY_CODE
       ,LINLI_CREDITOR
       ,LINLI_SECURED_IND
       ,LINLI_BALANCE
       ,LINLI_MATURITY_DATE
       ,LINLI_COMMENTS
       ,LINLI_MODIFIED_BY
       ,LINLI_MODIFIED_DATE
       ,LINLI_REFNO
  FROM dl_hem_income_liabilities
 WHERE linli_dlb_batch_id   = p_batch_id
   AND linli_dl_load_status = 'V';
--
-- ***********************************************************************
-- Additional Cursors
--
CURSOR c_get_inh(p_inh_legacy_ref NUMBER) 
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
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_INCOME_LIABILITIES';
cs                   INTEGER;
ce	                 VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
-- ***********************************************************************
-- Other variables
--
i                    INTEGER := 0;
l_exists             VARCHAR2(1);
l_inh_refno          NUMBER(10);
--
-- ***********************************************************************
--
BEGIN
--
 execute immediate 'alter trigger INLI_BR_I disable';
--
 fsc_utils.proc_start('s_dl_hem_income_liabilities.dataload_create');
 fsc_utils.debug_message('s_dl_hem_income_liabilities.dataload_create',3);
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
    cs   := p1.linli_dl_seqno;
    l_id := p1.rec_rowid;
--
    SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
-- Get the income_header_refno
--
    OPEN c_get_inh(p1.LINLI_INH_LEGACY_REF);
    FETCH c_get_inh INTO l_inh_refno;
    CLOSE c_get_inh;
--
-- Insert into relevant table
--
    INSERT /* +APPEND */ into  income_liabilities(INLI_REFNO
                                                 ,INLI_INH_REFNO
                                                 ,INLI_ILR_CODE
                                                 ,INLI_INF_CODE
                                                 ,INLI_PAYMENT_AMOUNT
                                                 ,INLI_LIABLE_PERCENT
                                                 ,INLI_CREDITOR
                                                 ,INLI_SECURED_IND
                                                 ,INLI_CREATED_BY
                                                 ,INLI_CREATED_DATE
                                                 ,INLI_HRV_VETY_CODE
                                                 ,INLI_REGULAR_AMOUNT
                                                 ,INLI_BALANCE
                                                 ,INLI_MATURITY_DATE
                                                 ,INLI_COMMENTS
                                                 ,INLI_MODIFIED_BY
                                                 ,INLI_MODIFIED_DATE     
                                                  )
--
                                          VALUES (p1.linli_refno
                                                 ,l_inh_refno
                                                 ,p1.LINLI_ILR_CODE
                                                 ,p1.LINLI_INF_CODE
                                                 ,p1.LINLI_PAYMENT_AMOUNT
                                                 ,p1.LINLI_LIABLE_PERCENT
                                                 ,p1.LINLI_CREDITOR
                                                 ,p1.LINLI_SECURED_IND
                                                 ,p1.LINLI_CREATED_BY
                                                 ,p1.LINLI_CREATED_DATE
                                                 ,p1.LINLI_HRV_VETY_CODE
                                                 ,p1.LINLI_REGULAR_AMOUNT
                                                 ,p1.LINLI_BALANCE
                                                 ,p1.LINLI_MATURITY_DATE
                                                 ,p1.LINLI_COMMENTS
                                                 ,p1.LINLI_MODIFIED_BY
                                                 ,p1.LINLI_MODIFIED_DATE   
                                                  );
--
-- ***********************************************************************
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
  execute immediate 'alter trigger INLI_BR_I enable';
-- 
  COMMIT;
--
-- ***********************************************************************
-- Section to analyse the table(s) populated by this Dataload
--
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_LIABILITIES');
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
PROCEDURE dataload_validate(p_batch_id  IN VARCHAR2,
                            p_date      IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid
       ,LINLI_DLB_BATCH_ID
       ,LINLI_DL_SEQNO
       ,LINLI_DL_LOAD_STATUS
       ,LINLI_LEGACY_REF
       ,LINLI_INH_LEGACY_REF
       ,LINLI_ILR_CODE
       ,LINLI_LIABLE_PERCENT
       ,NVL(LINLI_CREATED_BY,'DATALOAD') LINLI_CREATED_BY
       ,NVL(LINLI_CREATED_DATE, SYSDATE) LINLI_CREATED_DATE
       ,LINLI_PAYMENT_AMOUNT
       ,LINLI_INF_CODE
       ,LINLI_REGULAR_AMOUNT
       ,LINLI_HRV_VETY_CODE
       ,LINLI_CREDITOR
       ,LINLI_SECURED_IND
       ,LINLI_BALANCE
       ,LINLI_MATURITY_DATE
       ,LINLI_COMMENTS
       ,LINLI_MODIFIED_BY
       ,LINLI_MODIFIED_DATE
       ,LINLI_REFNO
  FROM dl_hem_income_liabilities
 WHERE linli_dlb_batch_id    = p_batch_id
   AND linli_dl_load_status IN ('L','F','O');
--
-- ******************
-- Additional Cursors
--
CURSOR c_chk_inh_exists(p_inh_legacy_ref NUMBER) 
IS
SELECT inh_refno,
       inh_sco_code
  FROM dl_hem_income_headers,
       income_headers
 WHERE linh_dl_load_status = 'C'
   AND linh_legacy_ref     = p_inh_legacy_ref
   AND inh_refno           = linh_refno;
--
-- ******************
--
CURSOR c_chk_ilr(p_ilr_code  VARCHAR2) 
IS
SELECT ilr_code
  FROM income_liability_reasons
  WHERE ilr_code = p_ilr_code;
--
-- ******************
--
CURSOR c_chk_inf(p_inf_code  VARCHAR2) 
IS
SELECT 'X'
  FROM income_frequencies
 WHERE inf_code = p_inf_code;
--
-- ******************
--
CURSOR c_chk_vety(p_ilr_code    VARCHAR2,
                  p_vety_Code   VARCHAR2) 
IS
SELECT 'X'
  FROM income_liability_verifications
 WHERE ilv_ilr_code      = p_ilr_code
   AND ilv_hrv_vety_code = p_vety_code;
--
-- ******************
--
CURSOR c_itngr(p_liable_percent NUMBER) 
IS
SELECT FLOOR(p_liable_percent) floor_val
FROM dual;
--
-- ******************
--
CURSOR chk_inli(p_refno NUMBER
               ,p_inh_refno NUMBER
               ,p_ilr_code VARCHAR2
               ,p_inf_code VARCHAR2
               ,p_payment_amount NUMBER
               ,p_liable_percent NUMBER
               ,p_creditor VARCHAR2
               ,p_secured_ind VARCHAR2) 
IS
SELECT 'X'
 FROM income_liabilities
WHERE inli_refno = p_refno
  AND inli_inh_refno = p_inh_refno
  AND inli_ilr_code = p_ilr_code
  AND inli_inf_code = p_inf_code
  AND inli_payment_amount = p_payment_amount
  AND inli_liable_percent = p_liable_percent
  AND inli_creditor = p_creditor
  AND inli_secured_ind = p_secured_ind;
--
-- ******************
--
-- Constants for process_summary
--
cb                         VARCHAR2(30);
cd                         DATE;
cp                         VARCHAR2(30) := 'VALIDATE';
ct                         VARCHAR2(30) := 'DL_HEM_INCOME_LIABILITIES';
cs                         INTEGER;
ce                         VARCHAR2(200);
l_id                       ROWID;
--
-- ******************
-- Other variables
--
l_exists                   VARCHAR2(1);
l_errors                   VARCHAR2(10);
l_error_ind                VARCHAR2(10);
i                          INTEGER :=0;
l_vety                     VARCHAR2(1);
l_inh_refno                NUMBER(10);
l_inh_sco_code             VARCHAR2(3);
l_ilr_code                 VARCHAR2(10);
l_mult_allowed_cnt_1       INTEGER;
l_mult_allowed_cnt_2       INTEGER;
l_floor_val                NUMBER(3);
--
-- ******************
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hem_income_liabilities.dataload_validate');
  fsc_utils.debug_message( 's_dl_hem_income_liabilities.dataload_validate',3);
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
    cs   := p1.linli_dl_seqno;
    l_id := p1.rec_rowid;
    l_errors := 'V';
    l_error_ind := 'N';
    l_exists                   := NULL;
    l_inh_refno                := NULL;
    l_inh_sco_code             := NULL;
    l_ilr_code                 := NULL;
    l_vety                     := NULL;
    l_mult_allowed_cnt_1       := 0;
    l_mult_allowed_cnt_2       := 0;
    l_floor_val                := 0;
--
-- ******************
-- Other mandatory fields supplied
--
    IF p1.linli_payment_amount IS NULL
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',875);
	END IF;
--
    IF p1.linli_creditor IS NULL
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',877);
	END IF;
--
    IF p1.linli_secured_ind IS NULL
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',878);
	END IF;
--
    IF p1.linli_legacy_ref IS NULL
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',880);
	END IF;
--
-- Check Income Header Legacy Reference has been supplied
-- and exists in INCOME_HEADERS
--
    IF p1.linli_inh_legacy_ref IS NULL
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',819);
    ELSE	 
     OPEN c_chk_inh_exists(p1.linli_inh_legacy_ref);
     FETCH c_chk_inh_exists INTO l_inh_refno, l_inh_sco_code;
      IF (c_chk_inh_exists%NOTFOUND) THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',473);
      END IF;
     CLOSE c_chk_inh_exists;
	END IF;
--
-- Check Income Liability Reason Code has been supplied
-- and exists on the Income Liability Reasons Table
--
    IF p1.linli_ilr_code IS NULL
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',820);
    ELSE	
     OPEN c_chk_ilr(p1.linli_ilr_code);
     FETCH c_chk_ilr INTO l_ilr_code;
      IF (c_chk_ilr%NOTFOUND) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',818);
      END IF;
     CLOSE c_chk_ilr;
    END IF;
--
-- Check Percentage liable has been supplied as is between 1-100
--
    IF p1.linli_liable_percent IS NULL
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',820);
    END IF;
--
    IF p1.linli_liable_percent IS NOT NULL
	 THEN
--
      IF (p1.linli_liable_percent < 1 OR p1.linli_liable_percent > 100)
	   THEN	 
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',874);
      END IF;
--
      OPEN c_itngr(p1.linli_liable_percent);
      FETCH c_itngr INTO l_floor_val;
      CLOSE c_itngr;	 
-- 
      IF (p1.linli_liable_percent != l_floor_val)
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',874);
      END IF;	 
    END IF;
--
-- Check Income Frequency Code has been supplied and also must
-- exist on the Income Frequencies Table
--
    l_exists := NULL;
--
    IF p1.linli_inf_code IS NULL
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',876);
    ELSE	
     OPEN c_chk_inf(p1.linli_inf_code);
     FETCH c_chk_inf INTO l_exists;
      IF (c_chk_inf%NOTFOUND) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',337);
      END IF;
     CLOSE c_chk_inf;
    END IF;
--
--  Income Verification Type only mandatory if income header has been
--  verified (status of VER)
--
    IF (l_inh_sco_code = 'VER' AND p1.linli_hrv_vety_code IS NULL)
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',876);
    END IF;
--
    IF p1.linli_hrv_vety_code IS NOT NULL
	 THEN
      IF (NOT s_dl_hem_utils.exists_frv('VERIFIEDTYPE',p1.linli_hrv_vety_code,'Y'))
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',23);
      END IF;
    END IF;
--
-- Check Reason Code and Verifications Type Combination                                   
--
    IF (    p1.linli_ilr_code      IS NOT NULL
        AND p1.linli_hrv_vety_code IS NOT NULL)
     THEN
      OPEN c_chk_vety(p1.linli_ilr_code,
                      p1.linli_hrv_vety_code);
      FETCH c_chk_vety into l_vety;
       IF (c_chk_vety%NOTFOUND) THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',821); 
       END IF;
      CLOSE c_chk_vety;
    END IF;
--
-- ************************************************************************************
--
-- Other reference values are valid
--
    IF nvl(p1.linli_secured_ind,'X') NOT IN ('Y','N')
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',879);
	END IF;
--
-- Check if income liabilities record already exists against income header
-- PK is only inli_refno so could duplicate records if you wished but tried
-- to stop this on data load as assumed duplicates not wanted
--
    IF (p1.linli_refno          IS NOT NULL
    AND l_inh_refno             IS NOT NULL
    AND p1.linli_ilr_code       IS NOT NULL
    AND p1.linli_inf_code       IS NOT NULL
    AND p1.linli_payment_amount IS NOT NULL
    AND p1.linli_liable_percent IS NOT NULL
    AND p1.linli_creditor       IS NOT NULL
    AND p1.linli_secured_ind    IS NOT NULL)
     THEN
--
    l_exists := NULL;
--
      OPEN chk_inli(p1.linli_refno
                   ,l_inh_refno
                   ,p1.linli_ilr_code
                   ,p1.linli_inf_code
                   ,p1.linli_payment_amount
                   ,p1.linli_liable_percent
                   ,p1.linli_creditor
                   ,p1.linli_secured_ind);
      FETCH chk_inli into l_exists;
       IF (chk_inli%FOUND) THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',881); 
       END IF;
      CLOSE chk_inli;
    END IF;
--
-- ************************************************************************************
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
CURSOR c1 is
SELECT rowid rec_rowid
      ,LINLI_DLB_BATCH_ID
      ,LINLI_DL_SEQNO
      ,LINLI_DL_LOAD_STATUS
      ,LINLI_REFNO
  FROM dl_hem_income_liabilities
 WHERE linli_dlb_batch_id   = p_batch_id
   AND linli_dl_load_status = 'C';
--
-- ***********************************************************************
-- Additional Cursors
-- None
--
-- Constants FOR process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'DELETE';
ct               VARCHAR2(30) := 'DL_HEM_INCOME_LIABILITIES';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
l_an_tab         VARCHAR2(1);
--
-- ***********************************************************************
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
-- ***********************************************************************
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hem_income_liabilities.dataload_delete');
  fsc_utils.debug_message('s_dl_hem_income_liabilities.dataload_delete',3 );
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
     cs   := p1.linli_dl_seqno;
     l_id := p1.rec_rowid;
     i    := i +1;
--
-- Delete from income_liabilities table
--
     DELETE 
     FROM income_liabilities
     WHERE inli_refno = p1.linli_refno;
--
-- ***********************************************************************
-- Now UPDATE the record status and process count
--
     s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
     set_record_status_flag(l_id,'V');
--
     IF mod(i,5000) = 0 THEN 
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
-- ***********************************************************************
-- Section to analyse the table(s) populated by this dataload
--
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('INCOME_LIABILITIES');
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
END s_dl_hem_income_liabilities;
/

