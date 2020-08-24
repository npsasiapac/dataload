CREATE OR REPLACE PACKAGE BODY s_dl_hco_sor_prdt_specificatn
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Vers   WHO  WHEN         WHY
--      1.0  5.10.0    PH   15-AUG-2006  Initial Creation
--      2.0  5.12.0    PH   20-JUL-2007  Amended Create to check if 
--                                       specification exists as it's a
--                                       one to many relationship, also
--                                       amended validate and delete to
--                                       cater for above.
--      3.0 5.13.0   PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--      3.1 5.15.1   PH   27-NOV-2009 Amended error code on Product check
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
  UPDATE dl_hco_sor_prdt_specificatn
  SET lspsn_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_sor_prdt_specificatn');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--
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
,      lspsn_dlb_batch_id
,      lspsn_dl_seqno
,      lspsn_dl_load_status
,      lspsn_sor_code
,      lspsn_start_date
,      lspsn_end_date 
,      lspro_prod_code
,      lspro_default_quantity
,      lspro_hrv_uom_code
FROM   dl_hco_sor_prdt_specificatn
WHERE  lspsn_dlb_batch_id    = p_batch_id
AND    lspsn_dl_load_status = 'V';
--
CURSOR c_spsn_exists(p_sor_code   VARCHAR2,
                     p_start_date DATE) IS
SELECT spsn_refno
FROM   sor_prdt_specifications
WHERE  spsn_sor_code = p_sor_code
AND    spsn_start_date = p_start_date;
--
CURSOR c_spsn_refno IS
SELECT spsn_refno_seq.nextval
FROM   dual;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_SOR_PRDT_SPECIFICATN';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
-- Other variables
--
i            INTEGER := 0;
l_spsn_refno INTEGER;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_sor_prdt_specificatn.dataload_create');
  fsc_utils.debug_message( 's_dl_hco_sor_prdt_specificatn.dataload_create',3);
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
      cs := p1.lspsn_dl_seqno;
      l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
l_spsn_refno := NULL;
--
-- Check and see if Specification exists
--
  OPEN c_spsn_exists(p1.lspsn_sor_code, p1.lspsn_start_date);
   FETCH c_spsn_exists INTO l_spsn_refno;
  CLOSE c_spsn_exists;
--
  IF l_spsn_refno IS NOT NULL
--
   THEN
--
      --
      -- Create Sor Products
      --
      INSERT INTO sor_products
      (spro_spsn_refno
      ,spro_prod_code
      ,spro_default_quantity
      ,spro_hrv_uom_code
      ,spro_created_by
      ,spro_created_date
      )
      VALUES
      (l_spsn_refno
      ,p1.lspro_prod_code
      ,p1.lspro_default_quantity
      ,p1.lspro_hrv_uom_code
      ,'DATALOAD'
      ,sysdate
      );
--
  ELSE
--
      --
      -- Create Sor Specification and Products
      --
    OPEN c_spsn_refno;
     FETCH c_spsn_refno INTO l_spsn_refno;
    CLOSE c_spsn_refno;
--
      INSERT INTO sor_prdt_specifications 
      (spsn_refno
      ,spsn_sor_code
      ,spsn_start_date
      ,spsn_created_by
      ,spsn_created_date
      ,spsn_end_date
      )
      VALUES
      (l_spsn_refno
      ,p1.lspsn_sor_code
      ,p1.lspsn_start_date
      ,'DATALOAD'
      ,sysdate
      ,p1.lspsn_end_date
      );
--
      --
      -- Create Sor Products
      --
      INSERT INTO sor_products
      (spro_spsn_refno
      ,spro_prod_code
      ,spro_default_quantity
      ,spro_hrv_uom_code
      ,spro_created_by
      ,spro_created_date
      )
      VALUES
      (l_spsn_refno
      ,p1.lspro_prod_code
      ,p1.lspro_default_quantity
      ,p1.lspro_hrv_uom_code
      ,'DATALOAD'
      ,sysdate
      );
--
  END IF; -- l_spsn_refno IS NOT NULL
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SOR_PRDT_SPECIFICATIONS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SOR_PRODUCTS');
--
  fsc_utils.proc_end;
--
  commit;
--
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
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
,      lspsn_dlb_batch_id
,      lspsn_dl_seqno
,      lspsn_dl_load_status
,      lspsn_sor_code
,      lspsn_start_date
,      lspsn_end_date 
,      lspro_prod_code
,      lspro_default_quantity
,      lspro_hrv_uom_code
,      lspsn_refno
FROM   dl_hco_sor_prdt_specificatn
WHERE  lspsn_dlb_batch_id    = p_batch_id
AND    lspsn_dl_load_status in ('L','F','O');
--
-- Other Cursors Start here
--
CURSOR c_spsn_exists(p_spsn_sor_code   VARCHAR2
                    ,p_spsn_start      DATE)  IS
SELECT 'X'
FROM   sor_prdt_specifications
WHERE  spsn_sor_code   = p_spsn_sor_code
AND    spsn_start_date = p_spsn_start;
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
ct               VARCHAR2(30) := 'DL_HCO_SOR_PRDT_SPECIFICATN';
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
  fsc_utils.proc_start('s_dl_hco_sor_prdt_specificatn.dataload_validate');
  fsc_utils.debug_message( 's_dl_hco_sor_prdt_specificatn.dataload_validate',3);
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
      cs := p1.lspsn_dl_seqno;
      l_errors := 'V';
      l_error_ind := 'N';
      l_id := p1.rec_rowid;
--
-- Check the Mandatory Fields have been supplied
--
      IF p1.lspsn_sor_code IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',180);
      END IF;
--
      IF p1.lspsn_start_date IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',181);
      END IF;
--
      IF p1.lspro_prod_code IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',182);
      END IF;
--
      IF p1.lspro_default_quantity IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',183);
      END IF;
--
      IF p1.lspro_hrv_uom_code IS NULL
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',184);
      END IF;

--
-- Validate Unique Key
--
-- Commented this out as this dataloads does the specification
-- and the products which is a header with many details
-- The code allows for this in the create
--
--      OPEN c_spsn_exists(p1.lspsn_sor_code, p1.lspsn_start_date);
--       FETCH c_spsn_exists INTO l_exists;
--        IF c_spsn_exists%FOUND
--         THEN
--         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',185);
--        END IF;
--      CLOSE c_spsn_exists;
--
-- Referential Integrity
--
-- SOR Code
--
      IF p1.lspsn_sor_code IS NOT NULL
       THEN 
         IF NOT s_schedule_of_rates.check_sor_exists(p1.lspsn_sor_code)                      
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',140);
         END IF;
      END IF;
--
-- Product Code
--
    IF p1.lspro_prod_code IS NOT NULL
     THEN 
       OPEN c_prod_exists(p1.lspro_prod_code);
        FETCH c_prod_exists INTO l_exists;
         IF c_prod_exists%NOTFOUND
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',375);
         END IF;
       CLOSE c_prod_exists;
    END IF;
--
-- Unit of Measure
--
      IF (NOT s_dl_hem_utils.exists_frv('UNITS',p1.lspro_hrv_uom_code,'N'))
       THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',749);
      END IF;
--
-- Default Quantity must not be less that zero
-- 
      IF p1.lspro_default_quantity < 0
       THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',187);
      END IF;
--
-- Check that if the end date is supplied it's after the start date
--
      IF p1.lspsn_end_date IS NOT NULL
       THEN
        IF p1.lspsn_end_date < p1.lspsn_start_date
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',188);
        END IF;
      END IF;
--  
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
,      lspsn_dlb_batch_id
,      lspsn_dl_seqno
,      lspsn_dl_load_status
,      lspsn_sor_code
,      lspsn_start_date
,      lspsn_end_date 
,      lspro_prod_code
,      lspro_default_quantity
,      lspro_hrv_uom_code
FROM   dl_hco_sor_prdt_specificatn
WHERE  lspsn_dlb_batch_id    = p_batch_id
AND    lspsn_dl_load_status = 'C';
--
CURSOR c_spsn_refno(p_sor_code   VARCHAR2,
                    p_start_date DATE) IS
SELECT spsn_refno
FROM   sor_prdt_specifications
WHERE  spsn_sor_code   = p_sor_code
AND    spsn_start_date = p_start_date;
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_SOR_PRDT_SPECIFICATN';
cs       INTEGER;
ce       VARCHAR2(200);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
i        INTEGER := 0;
l_spsn_refno INTEGER;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hco_sor_prdt_specificatn.dataload_delete');
  fsc_utils.debug_message( 's_dl_hco_sor_prdt_specificatn.dataload_delete',3 );
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
    cs := p1.lspsn_dl_seqno;
    l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- get the spsn_refno
-- 
l_spsn_refno := NULL;
--
    OPEN c_spsn_refno(p1.lspsn_sor_code, p1.lspsn_start_date);
     FETCH c_spsn_refno INTO l_spsn_refno;
    CLOSE c_spsn_refno;
--
    DELETE FROM sor_products
    WHERE  spro_spsn_refno = l_spsn_refno
    AND    spro_prod_code  = p1.lspro_prod_code;
--
    DELETE FROM sor_prdt_specifications
    WHERE  spsn_refno = l_spsn_refno
    AND not exists (select null
                    from   sor_products
                    where  spsn_refno = spro_spsn_refno
                   );
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
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SOR_PRODUCTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SOR_PRDT_SPECIFICATIONS');
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
END s_dl_hco_sor_prdt_specificatn;
/
