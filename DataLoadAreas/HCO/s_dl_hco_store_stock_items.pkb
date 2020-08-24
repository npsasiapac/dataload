CREATE OR REPLACE PACKAGE BODY s_dl_hco_store_stock_items
AS
--**************************************************************************
-- System      : Constractors
-- Sub-System  : Stock Control
-- Author      : Karen Shannon
-- Date        : 24 May 2005
-- Description : Dataload Script for stores
--**************************************************************************
-- Change Control
-- Version  DB Vers  Who  Date         Description
--
-- 1.0               KS   24-May-2005  Initial Creation.
-- 
-- 2.0               VS   28-Jun-2006  Updating to ISG Format.
-- 
-- 3.0               VS   04-Jul-2006  Removing MIN/MAx Validation on Reorder Quantity 
--                                     as it is not required. 0 is a valid value.
--
-- 3.1       5.10.0  PH   27-JUL-2006  Added Db Version to Change control
--
-- 3.2       5.10.0  VS   02-FEB-2007  default 0 quantity fields now taken out
-- 4.0       5.13.0  PH   06-FEB-2008 Now includes its own 
--                                    set_record_status_flag procedure.
--**************************************************************************
--
-- **************************************************************************************
--
--***************************************************************************
-- Procedure  : dataload_create
-- Type       : PUBLIC
-- Author     : Karen Shannon
-- Date       : 24 May 2305
-- Description: Create a record into the store_stock_items table
--              after each record has passed validation
--***************************************************************************
--
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hco_store_stock_items
  SET lssi_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_store_stock_items');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id  IN VARCHAR2,
                          p_date      IN DATE)
IS
CURSOR c1 IS
SELECT ROWID rec_rowid,
       lssi_dlb_batch_id,
       lssi_dl_seqno, 
       lssi_dl_load_status, 
       lssi_sto_location, 
       lssi_prod_code,
       lssi_quantity,
       lssi_reorder_level,
       lssi_reorder_quantity, 
       lssi_on_order_quantity,
       lssi_minimum_quantity, 
       lssi_maximum_quantity, 
       lssi_ideal_quantity, 
       lssi_reserved_quantity, 
       lssi_sco_code
  FROM dl_hco_store_stock_items
 WHERE lssi_dlb_batch_id    = p_batch_id
   AND lssi_dl_load_status IN ('V');
--
-- *************************************************************
--
-- Cursor to return the fraction allowed ind
--
CURSOR c_prod(cp_prod products.prod_code%TYPE) 
IS
SELECT prod_fraction_ind
  FROM products
 WHERE prod_code = cp_prod;
--
-- *************************************************************
--         
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'CREATE';
ct       	VARCHAR2(30) := 'DL_HCO_STORE_STOCK_ITEMS';
cs       	INTEGER;
ce       	VARCHAR2(230);
l_an_tab 	VARCHAR2(1);
l_id     ROWID;
--
-- Other variables
--
i       	INTEGER := 0;
l_count 	INTEGER := 1;
li      	PLS_INTEGER := 0;
--     
-- Additional Variables
--
l_fraction 	products.prod_fraction_ind%TYPE;
--
--
BEGIN
    fsc_utils.proc_start('s_dl_hco_store_stock_items.dataload_create');
    fsc_utils.debug_message('s_dl_hco_store_stock_items.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
-- **************************
-- START MAIN LOOP PROCESSING
-- **************************
--

    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lssi_dl_seqno;
          l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get the Fraction Indicator
--
          IF (p1.lssi_prod_code IS NOT NULL) THEN
--           
           l_fraction := NULL;
--
            OPEN c_prod(p1.lssi_prod_code);
           FETCH c_prod INTO l_fraction;
           CLOSE c_prod;
--
          END IF;
--
--               
-- If Fraction Allowed is set to No then all the Quantities must be rounded
-- 
          IF (NVL(l_fraction,'Y') = 'N') THEN
           --
           -- Insert into store_stock_items table   
           --
           INSERT INTO store_stock_items(ssi_sto_location,
                                         ssi_prod_code,
                                         ssi_quantity,
                                         ssi_reorder_level,
                                         ssi_reorder_quantity,
                                         ssi_on_order_quantity,
                                         ssi_minimum_quantity,
                                         ssi_maximum_quantity,
                                         ssi_ideal_quantity,
                                         ssi_reserved_quantity,
                                         ssi_sco_code)
--
                                  VALUES(p1.lssi_sto_location, 
                                         p1.lssi_prod_code,
                                         ROUND(p1.lssi_quantity),
                                         ROUND(p1.lssi_reorder_level),
                                         ROUND(p1.lssi_reorder_quantity),
                                         ROUND(p1.lssi_on_order_quantity),
                                         ROUND(p1.lssi_minimum_quantity),
                                         ROUND(p1.lssi_maximum_quantity),
                                         ROUND(p1.lssi_ideal_quantity),
                                         ROUND(p1.lssi_reserved_quantity),
                                         NVL(p1.lssi_sco_code,'CUR')
                                        );
--                         
          ELSE -- Fractions Allowed ie 'Y'
             --                  
             -- Insert into store_stock_items table   
             --
             INSERT INTO store_stock_items(ssi_sto_location,
                                           ssi_prod_code,
                                           ssi_quantity,
                                           ssi_reorder_level,
                                           ssi_reorder_quantity,
                                           ssi_on_order_quantity,
                                           ssi_minimum_quantity,
                                           ssi_maximum_quantity,
                                           ssi_ideal_quantity,
                                           ssi_reserved_quantity,
                                           ssi_sco_code)
--
                                    VALUES(p1.lssi_sto_location, 
                                           p1.lssi_prod_code,
                                           p1.lssi_quantity,
                                           p1.lssi_reorder_level,
                                           p1.lssi_reorder_quantity,
                                           p1.lssi_on_order_quantity,
                                           p1.lssi_minimum_quantity,
                                           p1.lssi_maximum_quantity,
                                           p1.lssi_ideal_quantity,
                                           p1.lssi_reserved_quantity,
                                           NVL(p1.lssi_sco_code,'CUR')
                                          );
          END IF;
--
-- **************************************************************
-- keep a count of the rows processed and commit after every 1000
-- **************************************************************
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
-- *************************************************************
-- Section to analyze the table(s) populated by this dataload
-- *************************************************************
--
    l_an_tab := s_dl_hou_utils.dl_comp_stats('STORE_STOCK_ITEMS');          
--
    fsc_utils.proc_end;
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
         RAISE;
--
END dataload_create;
--
-- **************************************************************************************
--
--**************************************************************************
-- Procedure  : dataload_validate
-- Type       : PUBLIC 
-- Author     : Karen Shannon
-- Date       : 24 May 2005
-- Description: Validate a record in dl_hco_store_stock_items
--**************************************************************************
--
PROCEDURE dataload_validate (p_batch_id  IN VARCHAR2,
                             p_date      IN DATE)
AS
CURSOR c1 IS
SELECT ROWID rec_rowid,
       lssi_dlb_batch_id,
       lssi_dl_seqno, 
       lssi_dl_load_status, 
       lssi_sto_location, 
       lssi_prod_code,
       lssi_quantity,
       lssi_reorder_level,
       lssi_reorder_quantity, 
       lssi_on_order_quantity,
       lssi_minimum_quantity, 
       lssi_maximum_quantity, 
       lssi_ideal_quantity, 
       lssi_reserved_quantity, 
       lssi_sco_code
  FROM dl_hco_store_stock_items
 WHERE lssi_dlb_batch_id    = p_batch_id
   AND lssi_dl_load_status IN ('L','F','O');
--
-- *************************************************************
--
-- Cursor to check that a record already exists within stores
--
CURSOR c_sto(cp_sto stores.sto_location%TYPE) 
IS
SELECT 'X'
  FROM stores
 WHERE sto_location = cp_sto;
--
-- *************************************************************
--
-- Cursor to check that a record already exists within products
--
CURSOR c_prod(cp_prod products.prod_code%TYPE) 
IS
SELECT 'X'
  FROM products
 WHERE prod_code = cp_prod;
--
-- *************************************************************
--
-- Cursor to check that a record already exists within Status Codes
--
CURSOR c_sco(cp_sco status_codes.sco_code%TYPE) 
IS
SELECT 'X'
  FROM status_codes
 WHERE sco_code = cp_sco;                  
--
-- *************************************************************
--
-- constants for error process
--
cb 		VARCHAR2(30);
cd 		DATE;
cp 		VARCHAR2(30) := 'VALIDATE';
ct 		VARCHAR2(30) := 'DL_HCO_STORE_STOCK_ITEMS';
cs 		INTEGER;
ce 		VARCHAR2(230);
l_id     ROWID;
--
-- necessary variables
--
l_errors    	VARCHAR2(10);
l_error_ind 	VARCHAR2(10);
l_mode     	VARCHAR2(10);
i           	INTEGER := 0;
li          	PLS_INTEGER := 0;
--      
-- other variable
--
l_sto        	VARCHAR2(1);
l_prod       	VARCHAR2(1);
l_sco        	VARCHAR2(1);
--
--
BEGIN   
    fsc_utils.proc_start('s_dl_hco_store_stock_items.dataload_validate');
    fsc_utils.debug_message('s_dl_hco_store_stock_items.dataload_validate',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--   
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lssi_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
-- *********************************
--                                 *
-- VALIDATE FIELDS       	   *
--                                 *
-- *********************************
--
--
-- *******************************************************************************
-- 
-- Re-Set Values at beginning for new record
--
          l_sto        := NULL;
          l_prod       := NULL;
          l_sco        := NULL;
--
-- *******************************************************************************
--   
-- Validate the Store Locations. Check to see it is supplied and valid.
--
          IF (p1.lssi_sto_location IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',001);
          ELSE
--
              OPEN c_sto(p1.lssi_sto_location);
             FETCH c_sto INTO l_sto;
             CLOSE c_sto;
--
             IF (l_sto IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',020);
             END IF;
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Product Code. Check to see it is supplied and Valid
--
          IF (p1.lssi_prod_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',010);
          ELSE
--
              OPEN c_prod(p1.lssi_prod_code);
             FETCH c_prod INTO l_prod;
             CLOSE c_prod;
--
             IF (l_prod IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',257);
             END IF;
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Quantity. Check to see that it has been supplied and
-- ensure that the Quantity is between 0 and 9999.99
--
          IF (p1.lssi_quantity IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',011);
--
          ELSIF (p1.lssi_quantity NOT BETWEEN 0 AND 9999.99) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',259);
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Re-Order Level. Check to see that it has been supplied and
-- ensure that the Re-Order level is between 0 and 9999.99.
-- Ensure that the Re-Order level is between the minimum and
-- maximum quantities               
--
          IF (p1.lssi_reorder_level IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',012);
--
          ELSIF (p1.lssi_reorder_level NOT BETWEEN 0 AND 9999.99) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',261);
--
          ELSIF (p1.lssi_reorder_level NOT BETWEEN p1.lssi_minimum_quantity 
                                               AND p1.lssi_maximum_quantity) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',268);                  
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Re-Order Quantity. Check to see that it has been supplied and
-- ensure that the Re-Order Quantity is between 0 and 9999.99.
-- 
--
          IF (p1.lssi_reorder_quantity IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',013);
--
          ELSIF (p1.lssi_reorder_quantity NOT BETWEEN 0 AND 9999.99) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',262);
--
          END IF;           
--
-- *******************************************************************************
--
-- Validate the On Order Quantity. Check to see that it has been supplied and
-- ensure that the On Order Quantity is between 0 and 9999.99.
-- 
          IF (p1.lssi_on_order_quantity IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',014);
--
          ELSIF (p1.lssi_on_order_quantity NOT BETWEEN 0 AND 9999.99) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',263);
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Minimum Quantity. Check to see that it has been supplied and
-- ensure that the Minimum Quantity is between 0 and 9999.99.
-- Ensure Minimum Quantity is less than or equal to Maximum Quantity.
--
          IF (p1.lssi_minimum_quantity IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',015);
--
          ELSIF (p1.lssi_minimum_quantity NOT BETWEEN 0 AND 9999.99) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',264);
--
          ELSIF (p1.lssi_minimum_quantity > p1.lssi_maximum_quantity) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',260);
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Maximum Quantity. Check to see that it has been supplied and
-- ensure that the Maximum Quantity is between 0 and 9999.99.
--
          IF (p1.lssi_maximum_quantity IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',016);
--
          ELSIF (p1.lssi_maximum_quantity NOT BETWEEN 0 AND 9999.99) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',265);
--
          END IF;
--
-- *******************************************************************************
--
-- Validate the Ideal Quantity. Check to see that it has been supplied and
-- ensure that the Ideal Quantity is between 0 and 9999.99.
-- Ensure that the Ideal Quantity is between the minimum and maximum quantities
--               
         IF (p1.lssi_ideal_quantity IS NULL) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',017);
--
         ELSIF (p1.lssi_ideal_quantity NOT BETWEEN 0 AND 9999.99) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',266);
--
         ELSIF (p1.lssi_ideal_quantity NOT BETWEEN p1.lssi_minimum_quantity
                                               AND p1.lssi_maximum_quantity) THEN
--
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',270);
--
         END IF;
--
-- *******************************************************************************
--
-- Validate the Reserved Quantity. Check to see that it has been supplied and
-- ensure that the Reserved Quantity is between 0 and 9999.99
--
         IF (p1.lssi_reserved_quantity IS NULL) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',018);
--
         ELSIF (p1.lssi_reserved_quantity NOT BETWEEN 0 AND 9999.99) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',267);
--
         END IF;
--
-- *******************************************************************************
--                                                                      
-- Validate the Status Code. Check to see that it has been supplied and Valid
--
         IF (p1.lssi_sco_code IS NULL) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',019);
--       
         ELSE
--
             OPEN c_sco(p1.lssi_sco_code);
            FETCH c_sco INTO l_sco;
            CLOSE c_sco;
--
            IF (l_sco IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',258);
            END IF;
--     
         END IF; 
--
--
-- *******************************************************************************
--
-- Now UPDATE the record count and error code
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
-- *******************************************************************************
-- 
--***************************************************************************
-- Procedure  : dataload_delete
-- Type       : PUBLIC
-- Author     : Karen Shannon
-- Date       : 23 May 2305
-- Description: Delete stores
-- Notes      : NOT USED, BUT CODED NEVERTHELESS 
--***************************************************************************   
PROCEDURE dataload_delete(p_batch_id IN VARCHAR2,
                          p_date     IN DATE)
IS
CURSOR c1 IS
SELECT ROWID rec_rowid,
       lssi_dlb_batch_id,
       lssi_dl_seqno, 
       lssi_dl_load_status, 
       lssi_sto_location, 
       lssi_prod_code
  FROM dl_hco_store_stock_items
 WHERE lssi_dlb_batch_id    = p_batch_id
   AND lssi_dl_load_status IN ('C');
--
-- *******************************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HCO_STORE_STOCK_ITEMS';
cs       INTEGER;
ce       VARCHAR2(230);
l_id     ROWID;
--
-- Declare variables
--
i          INTEGER  := 0;
li         PLS_INTEGER := 0;
l_count    INTEGER  := 1;
l_an_tab   VARCHAR2(1);
--
-- Other variables
--
BEGIN
    fsc_utils.proc_start('s_dl_hco_store_stock_items.dataload_delete');
    fsc_utils.debug_message('s_dl_hco_store_stock_items.dataload_delete',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--   
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lssi_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
--
-- Delete from store stock items table, using the store location, product code
--
          DELETE 
            FROM store_stock_items
           WHERE ssi_sto_location = p1.lssi_sto_location
             AND ssi_prod_code    = p1.lssi_prod_code;
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i +1;
--
          IF MOD(i,1000) = 0 THEN
           COMMIT;
          END IF;
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
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
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab := s_dl_hou_utils.dl_comp_stats('STORE_STOCK_ITEMS');
--
    COMMIT;
--
    fsc_utils.proc_end;

    EXCEPTION
         WHEN OTHERS THEN
         s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
         RAISE;
--
END dataload_delete;
--
END s_dl_hco_store_stock_items;
/