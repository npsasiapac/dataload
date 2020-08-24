CREATE OR REPLACE PACKAGE BODY s_dl_hco_stores
AS
--**************************************************************************
-- System      : Constractors
-- Sub-System  : Stock Control
-- Author      : Karen Shannon
-- Date        : 23 May 2005
-- Description : Dataload Script for stores
--**************************************************************************
-- Change Control
-- Version DB Vers  Who   Date         Description
-- 1.0              KS    23-May-2005  Initial Creation.
-- 1.1     5.10.0   PH    27-JUL-2006  Added DB Vesrion to Change Control
-- 1.2     5.10.0   PH    10-MAY-2007  Amended Validate on Store Types by
--                                     removing call to parameter definitions
--                                     Also added nvl to lsto_suspended_ind
--                                     on Create process
-- 2.0     5.13.0   PH    06-FEB-2008  Now includes its own 
--                                     set_record_status_flag procedure.
--
--
--**************************************************************************
--
--***************************************************************************
-- Procedure  : dataload_create
-- Type       : PUBLIC
-- Author     : Karen Shannon
-- Date       : 23 May 2005
-- Description: Create a record into the stores table
--              for each record that has passed validation
--***************************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hco_stores
  SET lsto_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hco_stores');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_create(p_batch_id  IN VARCHAR2,
                             p_date      IN DATE)
IS
CURSOR c1 IS
SELECT ROWID rec_rowid,
       lsto_dlb_batch_id,
       lsto_dl_seqno, 
       lsto_dl_load_status, 
       lsto_location, 
       lsto_type,
       lsto_description, 
       lsto_start_date, 
       lsto_strt_code,
       lsto_end_date, 
       lsto_comments, 
       lsto_cdep_dep_code, 
       lsto_cdep_cos_code, 
       lsto_cos_code, 
       lsto_vehicle_reg, 
       lsto_hrv_ffty_code, 
       lsto_hrv_vehi_code, 
       lsto_hrv_vmm_code, 
       lsto_tax_due_date, 
       lsto_insurance_due_date,
       lsto_insurance_reference, 
       lsto_first_registered_date, 
       lsto_cc, 
       lsto_mot_due_date, 
       lsto_service_due_date, 
       nvl(lsto_suspended_ind, 'N') lsto_suspended_ind
--                
  FROM dl_hco_stores
--
 WHERE lsto_dlb_batch_id = p_batch_id
   AND lsto_dl_load_status IN ('V');
--
--***************************************************************************
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCO_STORES';
cs       INTEGER;
ce       VARCHAR2(230);
l_an_tab VARCHAR2(1);
l_id     ROWID;
--
-- Other variables
--
i       INTEGER := 0;
l_count INTEGER := 1;
li      PLS_INTEGER := 0;
--
--
--***************************************************************************
-- 
BEGIN
    fsc_utils.proc_start('s_dl_hco_stores.dataload_create');
    fsc_utils.debug_message('s_dl_hco_stores.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Ensure cursor is closed
--
    IF (c1%ISOPEN) THEN
     CLOSE c1;
    END IF;
--
-- **************************
-- START MAIN LOOP PROCESSING
-- **************************
--

    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lsto_dl_seqno;
          l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Insert into stores table
--
          INSERT INTO stores(sto_location,
                             sto_type,
                             sto_description,
                             sto_start_date,
                             sto_strt_code,
                             sto_end_date,
                             sto_comments,
                             sto_cdep_dep_code,
                             sto_cdep_cos_code,
                             sto_cos_code,
                             sto_vehicle_reg,
                             sto_hrv_ffty_code,
                             sto_hrv_vehi_code,
                             sto_hrv_vmm_code,
                             sto_tax_due_date,
                             sto_insurance_due_date,
                             sto_insurance_reference,           
                             sto_first_registered_date,
                             sto_cc,
                             sto_mot_due_date,                    
                             sto_service_due_date,
                             sto_reusable_refno,
                             sto_suspended_ind)
--
                      VALUES(p1.lsto_location, 
                             p1.lsto_type,
                             p1.lsto_description, 
                             p1.lsto_start_date,
                             p1.lsto_strt_code,
                             p1.lsto_end_date,
                             p1.lsto_comments,
                             p1.lsto_cdep_dep_code,
                             p1.lsto_cdep_cos_code,
                             p1.lsto_cos_code,
                             p1.lsto_vehicle_reg,
                             p1.lsto_hrv_ffty_code,
                             p1.lsto_hrv_vehi_code,
                             p1.lsto_hrv_vmm_code,
                             p1.lsto_tax_due_date,
                             p1.lsto_insurance_due_date,
                             p1.lsto_insurance_reference,
                             p1.lsto_first_registered_date,
                             p1.lsto_cc,
                             p1.lsto_mot_due_date,
                             p1.lsto_service_due_date,
                             reusable_refno_seq.nextval,
                             p1.lsto_suspended_ind);
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
    l_an_tab := s_dl_hou_utils.dl_comp_stats('STORES');          
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
--**************************************************************************
-- Procedure  : dataload_validate
-- Type       : PUBLIC 
-- Author     : Karen Shannon
-- Date       : 23 May 2005
-- Description: Validate a record in dl_hou_stores
--**************************************************************************
--
PROCEDURE dataload_validate (p_batch_id  IN VARCHAR2,
                             p_date      IN DATE)
AS
CURSOR c1 IS
SELECT ROWID rec_rowid,
       lsto_dlb_batch_id,
       lsto_dl_seqno, 
       lsto_dl_load_status, 
       lsto_location, 
       lsto_type,
       lsto_description, 
       lsto_start_date, 
       lsto_strt_code,
       lsto_end_date, 
       lsto_comments, 
       lsto_cdep_dep_code, 
       lsto_cdep_cos_code, 
       lsto_cos_code, 
       lsto_vehicle_reg, 
       lsto_hrv_ffty_code, 
       lsto_hrv_vehi_code, 
       lsto_hrv_vmm_code, 
       lsto_tax_due_date, 
       lsto_insurance_due_date,
       lsto_insurance_reference, 
       lsto_first_registered_date, 
       lsto_cc, 
       lsto_mot_due_date, 
       lsto_service_due_date, 
       lsto_suspended_ind
--
  FROM dl_hco_stores
--
 WHERE lsto_dlb_batch_id = p_batch_id
   AND lsto_dl_load_status IN ('L','F','O');
--
--**************************************************************************
--
-- Cursor to validate that the location record doesn't already exists within stores
--
CURSOR c_loc_exists(cp_sto_location stores.sto_location%TYPE) 
IS
SELECT 'X'
  FROM stores
 WHERE sto_location = cp_sto_location;
--
--**************************************************************************
--
-- Cursor to check that a record already exists within store types
--
CURSOR c_strt(cp_strt store_types.strt_code%TYPE) 
IS
SELECT 'X', 
       strt_pgp_refno
  FROM store_types
 WHERE strt_code = cp_strt;
--
--**************************************************************************
--
-- Cursor to check that a record already exists within store types
--
CURSOR c_para(cp_para parameter_definition_usages.pdu_pgp_refno%TYPE) 
IS
SELECT 'x'
  FROM parameter_definitions, 
       parameter_definition_usages
 WHERE pdf_name         = pdu_pdf_name
   AND pdf_param_type   = pdu_pdf_param_type
   AND pdf_required_ind = 'Y'
   AND pdu_pgp_refno    = cp_para;
--
--**************************************************************************
--
-- Cursor to check that a record already exists within COS Depots
--
CURSOR c_dep(cp_dep depots.dep_code%TYPE,
             cp_cos contractor_sites.cos_code%TYPE) 
IS
SELECT 'X'
  FROM cos_depots
 WHERE cdep_dep_code = cp_dep
   AND cdep_cos_code = cp_cos;
--
--**************************************************************************
--       
-- Cursor to check that a record already exists within Contractor Sites
--
CURSOR c_cos(cp_cos contractor_sites.cos_code%TYPE) 
IS
SELECT 'X'
  FROM contractor_sites
 WHERE cos_code = cp_cos;
--
--**************************************************************************
--
-- Cursor to validate the Fuel Type Code
--
CURSOR c_ffty(cp_ffty VARCHAR2) 
IS
SELECT 'X'
  FROM hrv_fuel_types
 WHERE frv_code = cp_ffty;
--
--**************************************************************************
--
-- Cursor to validate the Vehicle Insurance Code
--
CURSOR c_vehi(cp_vehi VARCHAR2) 
IS
SELECT 'X'
  FROM hrv_vehicle_ins
 WHERE frv_code = cp_vehi;
--
--**************************************************************************
--
-- Cursor to validate the Vehicle Registration
--
CURSOR c_vehr(cp_vehr VARCHAR2) 
IS
SELECT 'X'
  FROM stores
 WHERE sto_vehicle_reg = cp_vehr;
--
--**************************************************************************
--
-- Cursor to validate the Vehicle Make/Model Code
--
CURSOR c_vmm(cp_vmm VARCHAR2) 
IS
SELECT 1
  FROM hrv_vehicle_make
 WHERE frv_code = cp_vmm;                           
--
--**************************************************************************
--
--        
-- constants for error process
--
cb 		VARCHAR2(30);
cd 		DATE;
cp 		VARCHAR2(30) := 'VALIDATE';
ct 		VARCHAR2(30) := 'DL_HCO_STORES';
cs 		INTEGER;
ce 		VARCHAR2(230);
l_id     ROWID;
--
-- necessary variables
--
l_errors    	VARCHAR2(10);
l_error_ind 	VARCHAR2(10);
l_mode      	VARCHAR2(10);
i           	INTEGER := 0;
li          	PLS_INTEGER := 0;
--      
-- other variables
--
l_loc_exists 		VARCHAR2(1);
l_strt       		VARCHAR2(1);
l_strt_pgp_refno	NUMBER(10);
l_para       		VARCHAR2(1);
l_dep_cos    		VARCHAR2(1);
l_cos        		VARCHAR2(1);
l_ffty       		VARCHAR2(1);
l_vehi       		VARCHAR2(1);
l_vmm        		VARCHAR2(1);
l_vehr       		VARCHAR2(1);
l_date       		DATE;
--
--
BEGIN   
    fsc_utils.proc_start('s_dl_hco_stores.dataload_validate');
    fsc_utils.debug_message('s_dl_hco_stores.dataload_validate',3 );
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
          cs := p1.lsto_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
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
          l_loc_exists 	   := NULL;
          l_strt       	   := NULL;
          l_strt_pgp_refno := NULL;
          l_para       	   := NULL;
          l_dep_cos        := NULL;
          l_cos            := NULL;
          l_ffty           := NULL;
          l_vehi           := NULL;
          l_vmm            := NULL;
          l_vehr           := NULL;
          l_date           := NULL;
--
-- *******************************************************************************
-- 
-- Validate the Store Location. Check to see that it has been supplied and doesn't
-- already exist.
--
          IF (p1.lsto_location IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',001);
          ELSE
              OPEN c_loc_exists(p1.lsto_location);
             FETCH c_loc_exists INTO l_loc_exists;
             CLOSE c_loc_exists;
--
             IF (l_loc_exists IS NOT NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',002);
             END IF;
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Store Type. Check to see that it has been supplied and ensure that 
-- the Store Type is either 'D' or 'V'
--
          IF (p1.lsto_type IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',003);
          ELSE
--               
             IF (p1.lsto_type NOT IN ('V','D')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',247);               
             END IF;
-- 
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the description. Check to see that it has been supplied
--
          IF (p1.lsto_description IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',004);
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Store Start_date. Check to see that it has been supplied
--
          IF (p1.lsto_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',005);
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the store Type. Check to see that it has been supplied and ensure that 
-- Store Type is Valid. If valid check to see that there is an entry in parameter
-- definitions.
--
-- PH - Removed the call to parameter definition as not required
-- As long as exists on STORE_TYPES it's good enough
--
         IF (p1.lsto_strt_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',006);
         ELSE
--
             OPEN c_strt (p1.lsto_strt_code);
            FETCH c_strt INTO l_strt, l_strt_pgp_refno;
            CLOSE c_strt;
--
            IF (l_strt IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',249);
--            ELSE
--  
--                OPEN c_para (l_strt_pgp_refno);
--               FETCH c_para INTO l_para;
--               CLOSE c_para;
--
--               IF (l_para IS NULL) THEN
--                l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',007);
--               END IF;
--
            END IF;     
--
         END IF;
--
-- *******************************************************************************
-- 
-- Validate the Suspended Ind. Check to see that it is either 'Y' or 'N'. If left
-- blank this will default to 'N' anyway.
--
          IF (p1.lsto_suspended_ind IS NOT NULL) THEN
--
           IF (p1.lsto_suspended_ind NOT IN ('Y','N')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',248);               
           END IF;
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the stores start and end dates. Ensure that the Start date is not 
-- greater than the end date.
--
          IF (    p1.lsto_start_date IS NOT NULL
              AND p1.lsto_end_date   IS NOT NULL) THEN
--
           IF (p1.lsto_start_date > p1.lsto_end_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',250);                  
           END IF;
--
          END IF;
--
-- *******************************************************************************
-- 
-- Validate the Depot fields. Ensure that both Depot field are poulated and valid.
--
--
         IF (p1.lsto_type = 'D') THEN
--
          IF   ((    p1.lsto_cdep_dep_code IS NOT NULL 
                 AND p1.lsto_cdep_cos_code IS NULL)
             OR
                (    p1.lsto_cdep_dep_code IS NULL
                 AND p1.lsto_cdep_cos_code IS NOT NULL)) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',251);
--
          END IF;
--
--                  
          IF (    p1.lsto_cdep_dep_code IS NOT NULL 
              AND p1.lsto_cdep_cos_code IS NOT NULL) THEN
               
            OPEN c_dep(p1.lsto_cdep_dep_code,
                       p1.lsto_cdep_cos_code);
           FETCH c_dep INTO l_dep_cos;
           CLOSE c_dep;
--
           IF (l_dep_cos IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',202);
           END IF;
--
          END IF;
--
         END IF;
--
-- *******************************************************************************
-- 
-- Validate Dept Cos code and Cos code. Both cannot be populated at the same time
-- ie mutually exclusive
--
         IF (    p1.lsto_cdep_cos_code IS NOT NULL 
             AND p1.lsto_cos_code      IS NOT NULL) THEN
--
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',252);
--
         END IF;
--
-- *******************************************************************************
-- 
-- Validate the Contractor Site. Check to see that it is Valid
--
         IF (p1.lsto_cos_code IS NOT NULL) THEN
--
             OPEN c_cos (p1.lsto_cos_code);
            FETCH c_cos INTO l_cos;
            CLOSE c_cos;
--
            IF (l_cos IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',008);
            END IF;     
--
         END IF;
--
-- *******************************************************************************
-- 
-- Validate. Only type 'V' can have the vehicle details populated
--
         IF (p1.lsto_type = 'D') THEN
--
          IF (   p1.lsto_vehicle_reg         	IS NOT NULL
              OR p1.lsto_hrv_ffty_code       	IS NOT NULL
              OR p1.lsto_hrv_vehi_code       	IS NOT NULL
              OR p1.lsto_hrv_vmm_code        	IS NOT NULL
              OR p1.lsto_tax_due_date        	IS NOT NULL
              OR p1.lsto_insurance_due_date  	IS NOT NULL
              OR p1.lsto_insurance_reference 	IS NOT NULL
              OR p1.lsto_first_registered_date  IS NOT NULL
              OR p1.lsto_cc                  	IS NOT NULL
              OR p1.lsto_mot_due_date        	IS NOT NULL
              OR p1.lsto_service_due_date     	IS NOT NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',253);
--
          END IF;
--
         END IF;
--
-- *******************************************************************************
-- 
-- For 'V'ehicle Type, if the Fuel Type is entered ensure this is a valid code
--
         IF (    p1.lsto_type          = 'V' 
             AND p1.lsto_hrv_ffty_code IS NOT NULL) THEN
--
           OPEN c_ffty (p1.lsto_hrv_ffty_code);
          FETCH c_ffty INTO l_ffty;
          CLOSE c_ffty;
--
          IF (l_ffty IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',254);
          END IF;
--
         END IF;
--
-- *******************************************************************************
-- 
-- For 'V'ehicle Type, if the Vehicle Insurance is entered ensure this is a valid code
--
         IF (    p1.lsto_type          = 'V'
             AND p1.lsto_hrv_vehi_code IS NOT NULL) THEN
--
           OPEN c_vehi (p1.lsto_hrv_vehi_code);
          FETCH c_vehi INTO l_vehi;
          CLOSE c_vehi;
--
          IF (l_vehi IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',255);
          END IF;
--
         END IF;
--
-- *******************************************************************************
-- 
-- For 'V'ehicle Type, if the Vehicle Make/Model is entered ensure this is a valid code
--
         IF (    p1.lsto_type         = 'V'
             AND p1.lsto_hrv_vmm_code IS NOT NULL) THEN
--
           OPEN c_vmm (p1.lsto_hrv_vmm_code);
          FETCH c_vmm INTO l_vmm;
          CLOSE c_vmm;
--
          IF (l_vmm IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'DLO',256);
          END IF;             
--
         END IF;
--
-- *******************************************************************************
-- 
-- Validate the Vehicle Registration. This should be unique in the stores table. 
-- Check for duplicates.
--
         IF (    p1.lsto_type         = 'V'
             AND p1.lsto_vehicle_reg  IS NOT NULL) THEN
--
           OPEN c_vehr (p1.lsto_vehicle_reg);
          FETCH c_vehr INTO l_vehr;
          CLOSE c_vehr;
--
          IF (l_vehr IS NOT NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'CDL',009);
          END IF;
--
         END IF;
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
-- Date       : 23 May 2005
-- Description: Delete stores
-- Notes      : NOT USED, BUT CODED NEVERTHELESS 
--***************************************************************************   
PROCEDURE dataload_delete(p_batch_id IN VARCHAR2,
                          p_date     IN DATE)
IS
CURSOR c1
IS
SELECT ROWID rec_rowid,
       lsto_dlb_batch_id,
       lsto_dl_seqno, 
       lsto_dl_load_status, 
       lsto_location
  FROM dl_hco_stores
 WHERE lsto_dlb_batch_id    = p_batch_id
   AND lsto_dl_load_status IN ('C');
--
--***************************************************************************
--
--
-- Constants for process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HCO_STORES';
cs       	INTEGER;
ce       	VARCHAR2(230);
l_id     ROWID;
--
-- Declare variables
--
i          	INTEGER  := 0;
li         	PLS_INTEGER := 0;
l_count    	INTEGER  := 1;
l_an_tab   	VARCHAR2(1);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hco_stores.dataload_delete');
    fsc_utils.debug_message('s_dl_hco_stores.dataload_delete',3);
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
          cs := p1.lsto_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Delete from stores table
--
          DELETE 
            FROM stores
           WHERE sto_location = p1.lsto_location;
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
    l_an_tab:= s_dl_hou_utils.dl_comp_stats('STORES');
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
END s_dl_hco_stores;
/
