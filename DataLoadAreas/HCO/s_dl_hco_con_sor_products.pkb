CREATE OR REPLACE PACKAGE BODY s_dl_hco_con_sor_products
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Vers   WHO  WHEN         WHY
--      1.0  5.12.0    PH   20-JUL-2007  Initial Creation
--      2.0  5.13.0    PH   06-FEB-2008  Now includes its own 
--                                       set_record_status_flag procedure.
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hco_con_sor_products
  SET lcsph_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_con_sor_products');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--  declare package variables AND constants
--
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT rowid rec_rowid
,      lcsph_dlb_batch_id
,      lcsph_dl_seqno
,      lcsph_dl_load_status
,      lcsph_cos_code
,      lcsph_sor_code
,      lcsps_start_date
,      lcsps_end_date
,      lcspr_prod_code
,      lcspr_default_quantity
,      lcspr_hrv_uom_code
FROM   dl_hco_con_sor_products
WHERE  lcsph_dlb_batch_id    = p_batch_id
AND    lcsph_dl_load_status  = 'V';
--
CURSOR c_csph_exists(p_cos_code   VARCHAR2,
                     p_sor_code   VARCHAR2) IS
SELECT 'X'
FROM   cos_sor_product_headers
WHERE  csph_cos_code  = p_cos_code
AND    csph_sor_code  = p_sor_code;
--
CURSOR c_csps_exists(p_cos_code   VARCHAR2,
                     p_sor_code   VARCHAR2,
                     p_start_date DATE) IS
SELECT csps_refno
FROM   con_sor_prdt_specifications
WHERE  csps_csph_cos_code = p_cos_code
AND    csps_csph_sor_code = p_sor_code
AND    csps_start_date    = p_start_date;
--
CURSOR c_csps_refno IS
SELECT csps_refno_seq.nextval
FROM   dual;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_CON_SOR_PRODUCTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
i            INTEGER := 0;
l_exists     VARCHAR2(1);
l_csps_refno INTEGER;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_con_sor_products.dataload_create');
  fsc_utils.debug_message( 's_dl_hco_con_sor_products.dataload_create',3);
--
  cb := p_batch_id;
  cd := p_date;
--
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1(p_batch_id) 
  LOOP
--
    BEGIN
--
      cs := p1.lcsph_dl_seqno;
      l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_csps_refno := NULL;
--
-- Check and see if Headers exists
--
   OPEN c_csph_exists(p1.lcsph_cos_code, p1.lcsph_sor_code);
    FETCH c_csph_exists into l_exists;
     IF c_csph_exists%NOTFOUND
      THEN
--
       INSERT INTO cos_sor_product_headers
       (csph_cos_code
       ,csph_sor_code
       ,csph_created_by
       ,csph_created_date
       )
       VALUES
       (p1.lcsph_cos_code
       ,p1.lcsph_sor_code
       ,'DATALOAD'
       ,sysdate
       );
--
     END IF;
   CLOSE c_csph_exists;
--
-- Now check and see if the Spec Exists, if it does
-- just insert into con_sor_products if not
-- insert into con_sor_prdt_specifications as well
--
  OPEN c_csps_exists(p1.lcsph_cos_code, p1.lcsph_sor_code, p1.lcsps_start_date);
   FETCH c_csps_exists INTO l_csps_refno;
  CLOSE c_csps_exists;
--
  IF l_csps_refno IS NOT NULL
--
   THEN
      --
      -- Create Con Sor Products
      --
      INSERT INTO con_sor_products
      (cspr_csps_refno
      ,cspr_prod_code
      ,cspr_default_quantity
      ,cspr_hrv_uom_code
      ,cspr_created_by
      ,cspr_created_date
      )
      VALUES
      (l_csps_refno
      ,p1.lcspr_prod_code
      ,p1.lcspr_default_quantity
      ,p1.lcspr_hrv_uom_code
      ,'DATALOAD'
      ,sysdate
      );
--
  ELSE
      --
      -- Create CON Sor Specification and Con SOR Products
      --
    OPEN c_csps_refno;
     FETCH c_csps_refno INTO l_csps_refno;
    CLOSE c_csps_refno;
--
      INSERT INTO con_sor_prdt_specifications
      (csps_refno
      ,csps_csph_cos_code
      ,csps_csph_sor_code
      ,csps_start_date
      ,csps_created_by
      ,csps_created_date
      ,csps_end_date
      )
      VALUES
      (l_csps_refno
      ,p1.lcsph_cos_code
      ,p1.lcsph_sor_code
      ,p1.lcsps_start_date
      ,'DATALOAD'
      ,sysdate
      ,p1.lcsps_end_date
      );
      --
      -- Create Sor Products
      --
      INSERT INTO con_sor_products
      (cspr_csps_refno
      ,cspr_prod_code
      ,cspr_default_quantity
      ,cspr_hrv_uom_code
      ,cspr_created_by
      ,cspr_created_date
      )
      VALUES
      (l_csps_refno
      ,p1.lcspr_prod_code
      ,p1.lcspr_default_quantity
      ,p1.lcspr_hrv_uom_code
      ,'DATALOAD'
      ,sysdate
      );
--
  END IF; -- l_csps_refno IS NOT NULL
--
      --
      -- keep a count of the rows processed and commit after every 1000
      --
      i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'C');
      --
    EXCEPTION
    WHEN OTHERS 
    THEN
      ROLLBACK TO SP1;
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
    END;
--
  END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('COS_SOR_PRODUCT_HEADERS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SOR_PRDT_SPECIFICATIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SOR_PRODUCTS');
--
  fsc_utils.proc_end;
--
  commit;
--
EXCEPTION
WHEN OTHERS 
THEN
  set_record_status_flag(l_id,'O');
  s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
END dataload_create;
--
--
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT rowid rec_rowid
,      lcsph_dlb_batch_id
,      lcsph_dl_seqno
,      lcsph_dl_load_status
,      lcsph_cos_code
,      lcsph_sor_code
,      lcsps_start_date
,      lcsps_end_date
,      lcspr_prod_code
,      lcspr_default_quantity
,      lcspr_hrv_uom_code
FROM   dl_hco_con_sor_products
WHERE  lcsph_dlb_batch_id    = p_batch_id
AND    lcsph_dl_load_status in ('L','F','O');
--
-- Other Cursors Start here
--
CURSOR c_prod_exists(p_prod_code  VARCHAR2) IS
SELECT 'X'
FROM   products
WHERE  prod_code = p_prod_code;
--
--
-- Constants for process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'VALIDATE';
ct               VARCHAR2(30) := 'DL_HCO_CON_SOR_PRODUCTS';
cs               INTEGER;
ce               VARCHAR2(200);
l_id     ROWID;
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
--
-- Other variables
--
l_dummy             VARCHAR2(2000);
l_date              DATE;
l_is_inactive       BOOLEAN DEFAULT FALSE; 
l_exists            VARCHAR2(1);
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_con_sor_products.dataload_validate');
  fsc_utils.debug_message( 's_dl_hco_con_sor_products.dataload_validate',3);
--
  cb := p_batch_id;
  cd := p_date;
--
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 IN c1 
  LOOP
--
    BEGIN
--
      cs := p1.lcsph_dl_seqno;
      l_errors := 'V';
      l_error_ind := 'N';
      l_id := p1.rec_rowid;
--
-- Check the Mandatory Fields have been supplied
--
      IF p1.lcsph_cos_code IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',276);
      END IF;
--
      IF p1.lcsph_sor_code IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',180);
      END IF;
--
      IF p1.lcsps_start_date IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',181);
      END IF;
--
      IF p1.lcspr_prod_code IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',182);
      END IF;
--
      IF p1.lcspr_default_quantity IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',183);
      END IF;
--
      IF p1.lcspr_hrv_uom_code IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',184);
      END IF;

--
-- Referential Integrity
--
--
-- COS Code
--
      IF p1.lcsph_cos_code IS NOT NULL
       THEN 
         IF NOT s_contractor_sites.check_cos_exists(p1.lcsph_cos_code)
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',251);
         END IF;
      END IF;
--
-- SOR Code
--
      IF p1.lcsph_sor_code IS NOT NULL
       THEN 
         IF NOT s_schedule_of_rates.check_sor_exists(p1.lcsph_sor_code)                      
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',140);
         END IF;
      END IF;
--
-- Product Code
--
    IF p1.lcspr_prod_code IS NOT NULL
     THEN 
       OPEN c_prod_exists(p1.lcspr_prod_code);
        FETCH c_prod_exists INTO l_exists;
         IF c_prod_exists%NOTFOUND
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',186);
         END IF;
       CLOSE c_prod_exists;
    END IF;
--
-- Unit of Measure
--
      IF (NOT s_dl_hem_utils.exists_frv('UNITS',p1.lcspr_hrv_uom_code,'N'))
       THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',749);
      END IF;
--
-- Default Quantity must not be less that zero
-- 
      IF p1.lcspr_default_quantity < 0
       THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',187);
      END IF;
--
-- Check that if the end date is supplied it's after the start date
--
     IF p1.lcsps_end_date IS NOT NULL
       THEN
        IF p1.lcsps_end_date < p1.lcsps_start_date
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',188);
        END IF;
     END IF; 
      --  
      --  
      --
      -- Now UPDATE the record count and error code 
      IF l_errors = 'F' THEN
        l_error_ind := 'Y';
      ELSE
        l_error_ind := 'N';
      END IF;
      --
      -- keep a count of the rows processed and commit after every 1000
      --
      i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
      set_record_status_flag(l_id,l_errors);
      --
    EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
    --
    END;
  --
  END LOOP;
  --
  COMMIT;
  --
  fsc_utils.proc_END;
  --
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
CURSOR c1 is
SELECT rowid rec_rowid
,      lcsph_dlb_batch_id
,      lcsph_dl_seqno
,      lcsph_dl_load_status
,      lcsph_cos_code
,      lcsph_sor_code
,      lcsps_start_date
,      lcsps_end_date
,      lcspr_prod_code
,      lcspr_default_quantity
,      lcspr_hrv_uom_code
FROM   dl_hco_con_sor_products
WHERE  lcsph_dlb_batch_id    = p_batch_id
AND    lcsph_dl_load_status  = 'C';
--
CURSOR c_csps_refno (p_cos_code   VARCHAR2,
                     p_sor_code   VARCHAR2,
                     p_start_date DATE) IS
SELECT csps_refno
FROM   con_sor_prdt_specifications
WHERE  csps_csph_cos_code = p_cos_code
AND    csps_csph_sor_code = p_sor_code
AND    csps_start_date    = p_start_date;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_CON_SOR_PRODUCTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i        INTEGER := 0;
l_csps_refno INTEGER;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_con_sor_products.dataload_delete');
  fsc_utils.debug_message( 's_dl_hco_con_sor_products.dataload_delete',3 );
--
  cp := p_batch_id;
  cd := p_date;
--
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 IN c1 LOOP
--
BEGIN
--
    cs := p1.lcsph_dl_seqno;
    l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the csps_refno
-- 
l_csps_refno := NULL;
--
  OPEN c_csps_refno(p1.lcsph_cos_code, p1.lcsph_sor_code, p1.lcsps_start_date);
   FETCH c_csps_refno INTO l_csps_refno;
  CLOSE c_csps_refno;
--
    DELETE FROM con_sor_products
    WHERE  cspr_csps_refno = l_csps_refno
    AND    cspr_prod_code  = p1.lcspr_prod_code;
--
    DELETE FROM con_sor_prdt_specifications
    WHERE  csps_refno = l_csps_refno
    AND not exists (select null
                    from   con_sor_products
                    where  csps_refno = cspr_csps_refno
                   );
--

    --
    s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
    set_record_status_flag(l_id,'V');
    --
    -- keep a count of the rows processed and commit after every 1000
    --
    i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
    --
--
EXCEPTION
WHEN OTHERS 
THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
  END LOOP;
--
-- Delete the Headers now we are out of the loop
--
    DELETE FROM cos_sor_product_headers
    WHERE  csph_created_by = 'DATALOAD'
    AND not exists (select null
                    from   con_sor_prdt_specifications
                    where  csph_cos_code = csps_csph_cos_code
                    and    csph_sor_code = csps_csph_cos_code
                   );
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('COS_SOR_PRODUCT_HEADERS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SOR_PRODUCTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CON_SOR_PRDT_SPECIFICATIONS');
--
  fsc_utils.proc_end;
--
  commit;
--
  EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
--
END s_dl_hco_con_sor_products;
/
