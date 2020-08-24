 CREATE OR REPLACE PACKAGE BODY s_dl_hco_products
AS
   --**************************************************************************
   -- System      : Costractors
   -- Sub-System  : Stock Control
   -- Author      : Karen Shannon
   -- Date        : 20 May 2005
   -- Description : Dataload Script for Products
   --**************************************************************************
   -- Change Control
   -- Version  DB Vers  Who   Date         Description
   -- 1.0               KS    20-May-2005  Initial Creation.
   -- 2.0               PJD   09-Jun-2006  Re-written in a similar format to 
   --                                      other V5 dataloads
   -- 2.1      5.10.0   PH    27-JUL-2006  Added Rollback to savepoint
   --
   -- 2.2      5.10.0   VRS   08-JAN-2007  Changed DLO 987 error code to HD1 225
   -- 2.3      5.10.0   PH    30-MAR-2007  Corrected validate on Lead time, added
   --                                      call to generate error.
   -- 3.0      5.13.0   PH    06-FEB-2008  Now includes its own 
   --                                      set_record_status_flag procedure.
   -- 3.1      5.15.1   PH    06-APR-2009  Corrected insert on prod_prod_code
   --                                      previously populated with lprod_code.
   -- 3.2      6.11     AJ    17-AUG-2015  added lprod_code_mlang, lprod_description_mlang
   --                                      lprod_short_desc_mlang to create and validate sections
   --
   --***************************************************************************
   --***************************************************************************
   -- Procedure  : dataload_create
   -- Type       : PUBLIC
   -- Author     : Karen Shannon
   -- Date       : 20 May 2005
   -- Description: Create a record into the Products table
   --              after each record has passed validation
   --              Amended June 2006 to usual format for housing dataloads
   -- ***********************************************************************
   --
   --
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hco_products
  SET lprod_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_products');
     RAISE;
  --
END set_record_status_flag;
   --
   --***************************************************************************
   PROCEDURE dataload_create(p_batch_id  IN VARCHAR2,
                             p_date      IN DATE)
   IS
      CURSOR c1
      IS
         SELECT ROWID rec_rowid,
                lprod_dlb_batch_id,
                lprod_dl_seqno, 
                lprod_dl_load_status, 
                lprod_code, 
                lprod_description,
                lprod_short_desc, 
                lprod_type_ind, 
                lprod_current_ind, 
                lprod_reusable_refno, 
                lprod_prod_code, 
                lprod_pdg_hrv_fpg_code,
                lprod_pdg_hrv_fpsg_code, 
                lprod_hrv_manu_code, 
                lprod_hrv_uom_bought_in, 
                lprod_hrv_uom_issued_in, 
                lprod_hazardous_ind, 
                lprod_manufactured_ind, 
                lprod_superceded_date, 
                lprod_quality_regno, 
                lprod_comments, 
                lprod_recyclable_ind, 
                lprod_inspection_reqd_ind, 
                lprod_service_rqd_ind, 
                lprod_safe_cloth_reqd_ind, 
                lprod_special_instruction,
                lprod_abc_rank, 
                lprod_last_invoiced_cost, 
                lprod_contract_price, 
                lprod_handling_ohead_amt, 
                lprod_handling_percentage, 
                lprod_estimated_ind,  
                lprod_fraction_ind, 
                lprod_stocked_ind, 
                lprod_hrv_tun_lead_time, 
                lprod_minimum_lead_time, 
                lprod_hrv_tun_shelf_life,
                lprod_shelf_life,
                lprod_code_mlang, 
                lprod_description_mlang,
                lprod_short_desc_mlang
         FROM   dl_hco_products
         WHERE  lprod_dlb_batch_id = p_batch_id
         AND    lprod_dl_load_status IN ('V');
      --
      CURSOR c2 (p_fpg_code varchar2, p_fpsc_code varchar2) IS
      SELECT pdg_refno
      FROM product_groupings
      WHERE pdg_hrv_fpg_code  = p_fpg_code
        AND nvl(pdg_hrv_fpsg_code,'~') = nvl(p_fpsc_code,'~')
      ;
      --
      -- Constants for process_summary
      cb       VARCHAR2(30);
      cd       DATE;
      cp       VARCHAR2(30) := 'CREATE';
      ct       VARCHAR2(30) := 'DL_HCO_PRODUCTS';
      cs       INTEGER;
      ce       VARCHAR2(200);
      l_id     ROWID;
      l_an_tab VARCHAR2(1);

      -- Other variables
      i       INTEGER := 0;
      l_count INTEGER := 1;
      l_pdg_refno number(8);

      -- Collections for use in bulk select
      
BEGIN
--
fsc_utils.proc_start('s_dl_hco_products.dataload_create');
      fsc_utils.debug_message( 's_dl_hco_products.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lprod_dl_seqno;
l_id := p1.rec_rowid;
--
 
      SAVEPOINT SP2;
      --
      l_pdg_refno := NULL;
      OPEN c2(p1.lprod_pdg_hrv_fpg_code,p1.lprod_pdg_hrv_fpsg_code);
      FETCH c2 INTO l_pdg_refno;
      CLOSE c2;
      --
      -- Insert into PRODUCTS table
       
               INSERT INTO products(prod_code,
                                    prod_description,
                                    prod_short_desc,
                                    prod_type_ind,
                                    prod_current_ind,
                                    prod_reusable_refno,
                                    prod_prod_code,
                                    prod_pdg_refno,
                                    prod_hrv_manu_code,
                                    prod_hrv_uom_bought_in,
                                    prod_hrv_uom_issued_in,
                                    prod_hazardous_ind,
                                    prod_manufactured_ind,
                                    prod_superceded_date,
                                    prod_quality_regno,
                                    prod_comments,
                                    prod_recycleable_ind,
                                    prod_inspection_required_ind,
                                    prod_service_required_ind,
                                    prod_safety_clothing_reqd_ind,
                                    prod_special_instructions,
                                    prod_abc_rank,
                                    prod_last_invoiced_cost,
                                    prod_contract_price,
                                    prod_handling_overhead_amount,
                                    prod_handling_percentage,
                                    prod_estimated_ind,
                                    prod_hrv_tun_shelf_life,
                                    prod_shelf_life,                                    
                                    prod_minimum_lead_time,
                                    prod_hrv_tun_lead_time,
                                    prod_fraction_ind,
                                    prod_stocked_ind,
                                    prod_created_date,
                                    prod_created_by,
                                    prod_code_mlang,
                                    prod_description_mlang,
                                    prod_short_desc_mlang)
               VALUES(p1.lprod_code,
                      p1.lprod_description,
                      nvl(p1.lprod_short_desc,substr(p1.lprod_description,1,100)),
                      p1.lprod_type_ind,
                      nvl(p1.lprod_current_ind,'Y'),
                      reusable_refno_seq.nextval,
                      p1.lprod_prod_code,
                      l_pdg_refno,
                      p1.lprod_hrv_manu_code,
                      p1.lprod_hrv_uom_bought_in,
                      p1.lprod_hrv_uom_issued_in,
                      nvl(p1.lprod_hazardous_ind,'N') ,
                      nvl(p1.lprod_manufactured_ind,'N') ,
                      p1.lprod_superceded_date,
                      p1.lprod_quality_regno,
                      p1.lprod_comments,
                      nvl(p1.lprod_recyclable_ind,'N') ,
                      nvl(p1.lprod_inspection_reqd_ind,'N') ,
                      nvl(p1.lprod_service_rqd_ind,'N') ,
                      nvl(p1.lprod_safe_cloth_reqd_ind,'N') ,
                      p1.lprod_special_instruction,
                      p1.lprod_abc_rank,
                      p1.lprod_last_invoiced_cost,
                      p1.lprod_contract_price,
                      p1.lprod_handling_ohead_amt,
                      p1.lprod_handling_percentage,
                      nvl(p1.lprod_estimated_ind,'N') ,
                      p1.lprod_hrv_tun_shelf_life,
                      p1.lprod_shelf_life,
                      p1.lprod_minimum_lead_time,
                      p1.lprod_hrv_tun_lead_time,
                      nvl(p1.lprod_fraction_ind,'N') ,
                      nvl(p1.lprod_stocked_ind,'N') ,
                      SYSDATE,
                      'DATALOAD',
                      p1.lprod_code_mlang,
                      p1.lprod_description_mlang,
                      nvl(p1.lprod_short_desc_mlang,substr(p1.lprod_description_mlang,1,100)));

        -- Update record status and process count
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed AND COMMIT after every 500
--
i := i+1; IF MOD(i,500)=0 THEN COMMIT; END IF;
--
  EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP2;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
 END;
--
END LOOP;
COMMIT;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PRODUCTS');
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


   --**************************************************************************
   -- Procedure  : dataload_validate
   -- Type       : PUBLIC 
   -- Author     : Karen Shannon - rewritten Peter Davies
   -- Date       : 20 May 2005   - June 2006
   -- Description: Validate a record in dl_hco_products
   --**************************************************************************
   PROCEDURE dataload_validate (p_batch_id  IN VARCHAR2,
                                p_date      IN DATE)
   AS
      CURSOR c1
      IS
         SELECT ROWID rec_rowid,
                lprod_dlb_batch_id,
                lprod_dl_seqno, 
                lprod_dl_load_status, 
                lprod_code, 
                lprod_description,
                lprod_short_desc, 
                lprod_type_ind, 
                lprod_current_ind,
                lprod_reusable_refno, 
                lprod_prod_code, 
                lprod_pdg_hrv_fpg_code,
                lprod_pdg_hrv_fpsg_code, 
                lprod_hrv_manu_code, 
                lprod_hrv_uom_bought_in, 
                lprod_hrv_uom_issued_in, 
                lprod_hazardous_ind, 
                lprod_manufactured_ind, 
                lprod_superceded_date, 
                lprod_quality_regno, 
                lprod_comments, 
                lprod_recyclable_ind, 
                lprod_inspection_reqd_ind, 
                lprod_service_rqd_ind, 
                lprod_safe_cloth_reqd_ind, 
                lprod_special_instruction,
                lprod_abc_rank, 
                lprod_last_invoiced_cost, 
                lprod_contract_price, 
                lprod_handling_ohead_amt, 
                lprod_handling_percentage, 
                lprod_estimated_ind,  
                lprod_fraction_ind, 
                lprod_stocked_ind, 
                lprod_hrv_tun_lead_time, 
                lprod_minimum_lead_time, 
                lprod_hrv_tun_shelf_life,
                lprod_shelf_life,
                lprod_code_mlang, 
                lprod_description_mlang,
                lprod_short_desc_mlang				
         FROM   dl_hco_products
         WHERE  lprod_dlb_batch_id = p_batch_id
         AND    lprod_dl_load_status IN ('L','F','O');

      -- Cursor to validate that the record already exists within lprod
      CURSOR c_dup_exists(cp_prod_code VARCHAR2)
      IS
         SELECT 'X'
         FROM   products
         WHERE  prod_code = cp_prod_code;

      -- Cursor to validate that the record already exists within lprod
      CURSOR c_parent(cp_prod_code products.prod_code%TYPE) 
      IS
         SELECT 'X'
         FROM   products
         WHERE  prod_code = cp_prod_code;         

      -- Cursor to validate the Product Group
      CURSOR c_group(cp_pdg_refno product_groupings.pdg_refno%TYPE) 
      IS
         SELECT 'X'
         FROM   product_groupings
         WHERE  pdg_refno = cp_pdg_refno;

      -- Cursor to validate the Manufacturer Code
      CURSOR c_manu(cp_manu VARCHAR2) 
      IS
         SELECT 'X'
         FROM   hrv_manufacturer
         WHERE  frv_code = cp_manu;
         
      -- Cursor to validate the Unit of Measure
      CURSOR c_uom(cp_uom VARCHAR2) 
      IS
         SELECT 'X'
         FROM   hrv_uom_stock
         WHERE  frv_code = cp_uom;

      -- Cursor to validate the Lead time/ Shelf Life
      CURSOR c_time(cp_time VARCHAR2) 
      IS
         SELECT 'X'
         FROM   hrv_time_units
         WHERE  frv_code = cp_time;                                        

      -- Cursor to check for valid product group and sub group combo
      CURSOR c_pdg (p_fpg_code varchar2, p_fpsc_code varchar2) IS
      SELECT    pdg_refno
      FROM product_groupings
      WHERE pdg_hrv_fpg_code  = p_fpg_code
        AND nvl(pdg_hrv_fpsg_code,'~') = nvl(p_fpsc_code,'~')
      ;

      -- Cursor to validate that the record already exists within lprod
      CURSOR c_dup_exists_mlang(cp_prod_code_mlang VARCHAR2)
      IS
         SELECT 'X'
         FROM   products
         WHERE  prod_code_mlang = cp_prod_code_mlang;
		 
      -- Cursor to validate that the record already exists within lprod
      CURSOR c_dup2_exists_mlang(cp_prod_code_mlang VARCHAR2, cp_prod_code VARCHAR2)
      IS
         SELECT 'X'
         FROM   products
         WHERE  prod_code_mlang = cp_prod_code_mlang
         AND    prod_code != cp_prod_code;

      -- constants for error process
      cb VARCHAR2(30);
      cd DATE;
      cp VARCHAR2(30) := 'VALIDATE';
      ct VARCHAR2(30) := 'DL_HCO_PRODUCTS';
      cs INTEGER      := 0;
      ce VARCHAR2(200);
      l_id     ROWID;

      -- necessary variables
      l_errors            VARCHAR2(10);
      l_error_ind         VARCHAR2(10);
      l_mode              VARCHAR2(10);
      i                   INTEGER := 0;
      li                  PLS_INTEGER := 0;
      
      -- other variables
      l_exists        VARCHAR2(1);
      l_pdg_refno     NUMBER;
      l_exists_mlang  VARCHAR2(1);
      l_exists_mlang2 VARCHAR2(1);
      --   
       
   BEGIN   
      fsc_utils.proc_start('s_dl_hco_products.dataload_validate');
      fsc_utils.debug_message( 's_dl_hco_products.dataload_validate',3 );

      cb := p_batch_id;
      cd := p_date;

      s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
      
      FOR p1 IN c1 LOOP
      --
      BEGIN
      --
      cs := p1.lprod_dl_seqno;
      l_id := p1.rec_rowid;
      --
      l_errors := 'V';
      l_error_ind := 'N';
      l_exists_mlang := NULL;
      l_exists_mlang2 := NULL;
      --
      -- Check if matching product code already exists
      l_exists := NULL;
      OPEN  c_dup_exists (p1.lprod_code);
      FETCH c_dup_exists INTO l_exists;
      CLOSE c_dup_exists;
      --
      -- Check if matching mlang product code already exists
      IF p1.lprod_code_mlang IS NOT NULL
      THEN
        OPEN  c_dup_exists_mlang (p1.lprod_code_mlang);
        FETCH c_dup_exists_mlang INTO l_exists_mlang;
        CLOSE c_dup_exists_mlang;
      --     
        IF l_exists_mlang IS NOT NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',279);
        END IF;
      --
        IF p1.lprod_description_mlang IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',280);
        END IF;
      --
        OPEN  c_dup2_exists_mlang (p1.lprod_code_mlang, p1.lprod_code);
        FETCH c_dup2_exists_mlang INTO l_exists_mlang2;
        CLOSE c_dup2_exists_mlang;
      --     
        IF l_exists_mlang2 IS NOT NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',281);
        END IF;
      --	  
      END IF;  --end of mlang checks
	  --
      IF l_exists IS NOT NULL 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',223);
      END IF;
      --
      IF p1.lprod_type_ind NOT IN ('C','N')
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',224);               
      END IF;
      --
      IF p1.lprod_current_ind NOT IN ('Y','N')
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',159);               
      END IF;
      --
      -- Check the manufacturer code
      IF p1.lprod_hrv_manu_code IS NOT NULL
      THEN
        l_exists := NULL;
        OPEN  c_manu (p1.lprod_hrv_manu_code);
        FETCH c_manu INTO l_exists;
        CLOSE c_manu;
     
        IF l_exists IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',226);
        END IF;
      --
      END IF;
      --
      -- check Product Group /Sub Group Combo
      l_pdg_refno := NULL;
      OPEN  c_pdg (p1.lprod_pdg_hrv_fpg_code, p1.lprod_pdg_hrv_fpsg_code);
      FETCH c_pdg INTO l_pdg_refno;
      CLOSE c_pdg;
      --
      IF l_pdg_refno IS NULL 
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',225);
      END IF;
      --
      -- Ensure that the UOM Bought In Code entered is Valid
      IF p1.lprod_hrv_uom_bought_in IS NOT NULL
      THEN
        l_exists := NULL;
        OPEN  c_uom (p1.lprod_hrv_uom_bought_in);
        FETCH c_uom INTO l_exists;
        CLOSE c_uom;
        IF l_exists IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',227);
        END IF;                
      END IF;
      --
      -- Ensure that the UOM Issued In Code entered is Valid
      IF p1.lprod_hrv_uom_issued_in IS NOT NULL
      THEN
        l_exists := NULL;
        OPEN  c_uom (p1.lprod_hrv_uom_issued_in);
        FETCH c_uom INTO l_exists;
        CLOSE c_uom;
        IF l_exists IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',225);
        END IF;                
      END IF;
      --
      -- Ensure that the Hazardous Ind is either 'Y' or 'N'
      IF (NOT s_dl_hem_utils.yornornull(p1.lprod_hazardous_ind))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',229);               
      END IF;
      --
      -- Ensure that the Manufactured Ind is either 'Y' or 'N'
      IF (NOT s_dl_hem_utils.yornornull(p1.lprod_manufactured_ind))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',230);               
      END IF;
      --
      -- Ensure that the Recyclable Ind is either 'Y' or 'N'
      IF p1.lprod_type_ind = 'N'
      THEN
        IF p1.lprod_recyclable_ind IS NOT NULL
        THEN
          -- Recyclable Ind can not be populated when type is non-consumable
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',232);                  
        END IF;
      ELSE -- Consumable
        IF (NOT s_dl_hem_utils.yorn(p1.lprod_recyclable_ind))
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',231);  
        END IF;
      END IF;
      --         
      -- Ensure that the Inspection Required  is either 'Y' or 'N'
      IF (NOT s_dl_hem_utils.yornornull(p1.lprod_inspection_reqd_ind))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',233);               
      END IF;
      --
      -- Ensure that the Service Required is either 'Y' or 'N'
      IF (NOT s_dl_hem_utils.yornornull(p1.lprod_service_rqd_ind))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',234); 
      END IF;
      --
      -- Ensure that the Safety Clothing Required is either 'Y' or 'N'
      IF (NOT s_dl_hem_utils.yornornull(p1.lprod_safe_cloth_reqd_ind))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',235); 
      END IF;
      --                        
      -- Ensure that the Invoiced Cost is between £0 and £999999999.99
      IF (p1.lprod_last_invoiced_cost IS NOT NULL 
          AND p1.lprod_last_invoiced_cost NOT BETWEEN 0 AND 999999999.99
         )
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',239);
      END IF;
      --                                
      -- Ensure that the Contract Price is between £0 and £999999999.99
      IF (p1.lprod_contract_price IS NOT NULL 
          AND p1.lprod_contract_price NOT BETWEEN 0 AND 999999999.99
         )
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',240);
      END IF; 
      --
      -- Ensure that the Overhead Handling Amount is between £0 and £999999999.99
      IF (p1.lprod_handling_ohead_amt IS NOT NULL 
          AND p1.lprod_handling_ohead_amt NOT BETWEEN 0 AND 999999999.99
          )
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',241);
      END IF;
      --
      -- Ensure that the handling Percentage is between £0 and £999.99
      IF (p1.lprod_handling_percentage IS NOT NULL
          AND p1.lprod_handling_percentage NOT BETWEEN 0 AND 999.99
         )
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',241);
      END IF;
      --
      -- Ensure that the Estimated Ind is either 'Y' or 'N'
      IF (NOT s_dl_hem_utils.yornornull(p1.lprod_estimated_ind))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',236); 
      END IF;
      --
      -- Ensure that the Fraction Allowed is either 'Y' or 'N'
      IF (NOT s_dl_hem_utils.yornornull(p1.lprod_fraction_ind))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',237); 
      END IF;
      --
      -- Ensure that the Stocked Ind is either 'Y' or 'N'
      IF (NOT s_dl_hem_utils.yornornull(p1.lprod_stocked_ind))
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',238); 
      END IF;
      --
      -- Ensure that the Time Unit code entered is Valid
      IF p1.lprod_hrv_tun_lead_time IS NOT NULL
      THEN
        l_exists := NULL;
        OPEN  c_time (p1.lprod_hrv_tun_lead_time);
        FETCH c_time INTO l_exists;
         IF c_time%NOTFOUND
          THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',278);
         END IF;
        CLOSE c_time;
      END IF;
      --
      -- check both or neither lead time fields supplied         
      IF (p1.lprod_hrv_tun_lead_time IS NOT NULL 
          AND p1.lprod_minimum_lead_time IS NULL
         ) 
         OR
         (    p1.lprod_hrv_tun_lead_time IS NULL 
          AND p1.lprod_minimum_lead_time IS NOT NULL
         )
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',228);               
      END IF;
      --
      -- Shelf Life Unit can not be populated when type is non-consumable
      IF p1.lprod_type_ind = 'N'
      THEN    
        IF (p1.lprod_hrv_tun_shelf_life IS NOT NULL 
            OR
            p1.lprod_shelf_life IS NOT NULL
           )
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',245);
         END IF;                 
       ELSE  -- Consumable                  
       -- Ensure that the Time Unit code entered is Valid
         IF p1.lprod_hrv_tun_shelf_life IS NOT NULL
         THEN 
           l_exists := NULL;                    
           OPEN c_time (p1.lprod_hrv_tun_shelf_life);
           FETCH c_time INTO l_exists;
           CLOSE c_time;
           IF l_exists IS NULL
           THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',228);
           END IF;
         END IF;
       END IF;
       -- 
       -- check that either both or neither of the shelf life fields are supplied.
       --
       IF (p1.lprod_hrv_tun_shelf_life IS NOT NULL 
           AND p1.lprod_shelf_life IS NULL
          ) 
          OR
          (p1.lprod_hrv_tun_shelf_life IS NULL 
           AND p1.lprod_shelf_life IS NOT NULL
          )
       THEN 
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',244);               
       END IF;
       --
       -- Special instructions only allowed on non-consumables?
       IF (p1.lprod_type_ind = 'C' 
           AND p1.lprod_special_instruction IS NOT NULL
           )
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',246);               
       END IF;
--
-- Now UPDATE the record count and error code
IF l_errors = 'F' THEN
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
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--   EXCEPTION
--      WHEN OTHERS THEN
--      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
--      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--      set_record_status_flag(l_id,'O');

 END;
--
END LOOP;
--
COMMIT;
--
fsc_utils.proc_END;
--
--   EXCEPTION
--      WHEN OTHERS THEN
--      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
   --***************************************************************************
   -- Procedure  : dataload_delete
   -- Type       : PUBLIC
   -- Author     : Karen Shannon
   -- Date       : 20 May 2005
   -- Description: Delete Products
   -- Notes      : NOT USED, BUT CODED NEVERTHELESS 
   --            : June 2006 - Wil definately be used, so just as well coded!
   --***************************************************************************   
   PROCEDURE dataload_delete(p_batch_id IN VARCHAR2,
                             p_date     IN DATE)
   IS
      CURSOR c1
      IS
         SELECT ROWID rec_rowid,
                lprod_dlb_batch_id,
                lprod_dl_seqno, 
                lprod_dl_load_status, 
                lprod_code
         FROM   dl_hco_products
         WHERE  lprod_dlb_batch_id = p_batch_id
         AND    lprod_dl_load_status IN ('C')
         ORDER BY rec_rowid DESC;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_PRODUCTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hco_products.dataload_delete');
fsc_utils.debug_message('s_dl_hco_products.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lprod_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
DELETE FROM products
WHERE prod_code   = p1.lprod_code
;
--
-- Update record status and process count
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
-- keep a count of the rows processed AND COMMIT after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
EXCEPTION
   WHEN OTHERS THEN
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PRODUCTS');
--
fsc_utils.proc_END;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_DELETE;
--
END s_dl_hco_products;
/
show errors

