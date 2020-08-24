CREATE OR REPLACE PACKAGE BODY s_dl_hra_tenant_allowances
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  VERSION  DB Ver   WHO            WHEN         WHY
--      1.0  6.18     Rob Heath      11-FEB-2019  Initial Creation for SAHT
--                                                New Data Load
--                                    
-- 
-- ***********************************************************************
--
--  declare package variables AND constants
--
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
   UPDATE dl_hra_tenant_allowances
   SET    ltall_dl_load_status = p_status
   WHERE  rowid = p_rowid;
--
EXCEPTION
   WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_tenant_allowances');
      RAISE;
--
END set_record_status_flag;
--
--
PROCEDURE dataload_create
   (p_batch_id          IN VARCHAR2
   ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       ltall_dlb_batch_id,
       ltall_dl_seqno,
       ltall_dl_load_status,
       ltall_tcy_refno,  
       ltall_talt_code,     
       ltall_start_date,
       ltall_created_date,
       ltall_created_by,       
       ltall_amount,
       ltall_end_date,
       ltall_approved_date,
       --ltall_last_paid_date,
       ltall_next_payment_due_date,
       ltall_refno       
FROM  dl_hra_tenant_allowances
WHERE ltall_dlb_batch_id   = p_batch_id
AND   ltall_dl_load_status = 'V';
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_TENANT_ALLOWANCES';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- Other variables
--
l_id            ROWID;
l_an_tab        VARCHAR2(1);
i               INTEGER := 0;
l_tall_refno    INTEGER;
l_tcy_refno     INTEGER;

--
-- *****************************
--
BEGIN
--
   fsc_utils.proc_start('s_'||ct||'.dataload_create');
   fsc_utils.debug_message('s_'||ct||'.dataload_create',3);
--
   cb := p_batch_id;
   cd := p_date;
--
   s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
   FOR p1 in c1 LOOP
      BEGIN
--
         cs   := p1.ltall_dl_seqno;
         l_id := p1.rec_rowid;
--
         SAVEPOINT SP1;
--       
         l_tall_refno := tall_seq.nextval; 
--
         INSERT INTO tenant_allowances
            (tall_refno,
             tall_tcy_refno,  
             tall_talt_code,     
             tall_start_date,
             tall_created_date,
             tall_created_by,       
             tall_amount,
             tall_end_date,
             tall_approved_date,
             --tall_last_paid_date,
             tall_next_payment_due_date)
         VALUES
            (l_tall_refno,
             p1.ltall_tcy_refno,  
             p1.ltall_talt_code,     
             p1.ltall_start_date,
             p1.ltall_created_date,
             p1.ltall_created_by,       
             p1.ltall_amount,
             p1.ltall_end_date,
             p1.ltall_approved_date,
             --p1.ltall_last_paid_date,
             p1.ltall_next_payment_due_date);
--
-- Now update record with created date as trigger sets it to sysdate
-- only needed if creation date supplied otherwise sysdate is fine   
--
         IF p1.ltall_created_date IS NOT NULL AND p1.ltall_created_by IS NOT NULL
         THEN
            UPDATE tenant_allowances
            SET    tall_created_date = p1.ltall_created_date,
                   tall_created_by = p1.ltall_created_by
            WHERE  tall_refno = l_tall_refno;
         ELSE
            UPDATE tenant_allowances
            SET    tall_created_by = 'DATALOAD'
            WHERE  tall_refno = l_tall_refno;               
         END IF;
--       
         UPDATE dl_hra_tenant_allowances
         SET    ltall_refno = l_tall_refno
         WHERE  rowid = p1.rec_rowid;               
--
--       Now UPDATE the record status and process count
--
         i := i + 1;
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
      END;
--
   END LOOP;
--
   COMMIT;
--
-- ***********************************************************************
-- Section to analyze the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANT_ALLOWANCES');
--
   fsc_utils.proc_END;
--
EXCEPTION
   WHEN OTHERS THEN
     s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
     RAISE;
END dataload_create;
--
-- ***********************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       ltall_dlb_batch_id,
       ltall_dl_seqno,
       ltall_dl_load_status,
       ltall_tcy_refno,  
       ltall_talt_code,     
       ltall_start_date,
       ltall_created_date,
       ltall_created_by,       
       ltall_amount,
       ltall_end_date,
       ltall_approved_date,
       --ltall_last_paid_date,
       ltall_next_payment_due_date
FROM   dl_hra_tenant_allowances
WHERE  ltall_dlb_batch_id   = p_batch_id
AND    ltall_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
-- Check Tenancy is valid
--
CURSOR chk_tcy_refno(p_tcy_refno VARCHAR2)
IS
SELECT 'X'
  FROM tenancies
 WHERE tcy_refno = p_tcy_refno;
--
-- ***********************************************************************
-- Check Allowance Type Code is valid
--
CURSOR chk_talt_code(p_talt_code VARCHAR2)
IS
SELECT 'X', talt_automatic_ind, talt_water_usage_ind
  FROM tenant_allowance_types
 WHERE talt_code = p_talt_code
   AND talt_current_ind = 'Y';
--
-- ***********************************************************************
-- Check for overlapping records
--
CURSOR chk_overlap(p_tcy_refno     INTEGER
                  ,p_talt_code     VARCHAR2)
IS
      
SELECT 'X'
FROM   tenant_allowances
WHERE  tall_tcy_refno = p_tcy_refno
AND    tall_talt_code = p_talt_code
AND    (tall_end_date IS NULL
              OR
        tall_end_date >= TRUNC(SYSDATE));
--
-- ***********************************************************************
-- Check for duplicates /overlapping dates in data load batch
CURSOR chk_dup_dl (p_dlb_batch_id  VARCHAR2
                  ,p_dl_seqno      INTEGER
                  ,p_tcy_refno     INTEGER
                  ,p_talt_code     VARCHAR2
                  ,p_start_date    DATE
                  ,p_end_date      DATE )
                 
IS
SELECT count(*)
FROM   dl_hra_tenant_allowances
WHERE  ltall_dlb_batch_id = p_dlb_batch_id
AND    ltall_tcy_refno = p_tcy_refno
AND    ltall_talt_code = p_talt_code
AND    NVL(p_end_date,TO_DATE('31129999','DDMMYYYY')) >= ltall_start_date
AND    p_start_date <= NVL(ltall_end_date,TO_DATE('31129999','DDMMYYYY'))
AND    ltall_dl_seqno < p_dl_seqno
AND    ltall_dl_load_status != 'F';
--  
-- ***********************************************************************
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HRA_TENANT_ALLOWANCES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors          VARCHAR2(10);
l_error_ind       VARCHAR2(10);
i                 INTEGER := 0;
l_chk          	VARCHAR2(1);
l_chk_dup_dl      INTEGER;
l_main_cnt        INTEGER;
l_automatic_ind   VARCHAR2(1);
l_water_usage_ind VARCHAR2(1);
--
-- ***********************************************************************
--
BEGIN
--
   fsc_utils.proc_start('s_'||ct||'.dataload_validate');
   fsc_utils.debug_message('s_'||ct||'.dataload_validate',3 );
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
         cs := p1.ltall_dl_seqno;
         l_id := p1.rec_rowid;
--
         l_errors := 'V';
         l_error_ind := 'N';
--
--       Mandatory field check and content
--      
--       Tenancy Reference Number
         IF p1.ltall_tcy_refno IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',766);
            --'Tenancy Reference Number must be supplied'
         ELSE
            l_chk := NULL;

            OPEN chk_tcy_refno(p1.ltall_tcy_refno);
            FETCH chk_tcy_refno INTO l_chk;
            CLOSE chk_tcy_refno;

            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',767);
               --'Tenancy Reference Number supplied is invalid'               
            END IF;  
               
         END IF;
--        
--       Allowance Type Code 
         IF p1.ltall_talt_code IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',768);
            --'Allowance Type Code must be supplied'
         ELSE
            l_chk := NULL;
            l_automatic_ind := NULL;
            l_water_usage_ind := NULL;

            OPEN chk_talt_code(p1.ltall_talt_code);
            FETCH chk_talt_code INTO l_chk, l_automatic_ind, l_water_usage_ind;
            CLOSE chk_talt_code;

            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',769);
               --'Allowance Type Code supplied is invalid' 
            ELSIF l_automatic_ind = 'Y'
            THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',770);
               --'Automatic indicator of Allowance Type must be N'             
            END IF;
         END IF;
--                        
--       Start Date
         IF p1.ltall_start_date IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',771);
            --'Start Date must be supplied'
         ELSIF p1.ltall_start_date < TRUNC(SYSDATE)
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',772);
            --'The Start Date cannot be before today'   
         END IF;   
--                    
--       Check the End Date if supplied
         IF p1.ltall_end_date IS NOT NULL
         THEN
            IF p1.ltall_end_date < p1.ltall_start_date
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',773);
               --'The End Date cannot be less than Start Date'
            END IF;
         END IF;  
         
         IF p1.ltall_amount IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',774);  
            --'Amount must be supplied'
         ELSIF p1.ltall_amount NOT BETWEEN 0.01 AND 99999999.99
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',775);         
            --'Amount must be between 0.01 and 99999999.99'
         END IF;
         
         IF p1.ltall_approved_date IS NULL
         THEN   
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',776);
            --'Approved Date must be supplied'
         ELSE            
            IF p1.ltall_approved_date > TRUNC(SYSDATE)
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',777);
               --'Approved Date cannot be after today' 
            END IF;
         END IF; 

         IF p1.ltall_next_payment_due_date IS NULL
         THEN   
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',778);
            --'Next Payment Due Date must be supplied'
         ELSE            
            IF p1.ltall_next_payment_due_date < p1.ltall_start_date
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',779);
               --'Next Payment Due Date must be on or after Start Date' 
            END IF;
         END IF;
         
         IF l_water_usage_ind = 'Y'
            AND NVL(s_twc_water_charge_statuses.get_water_charge_status(p1.ltall_tcy_refno,p1.ltall_start_date),'Z') <> 'ACTUAL'
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',780);
            --'This allowance can only be paid to a tenant who is being charged based on actual water usage'
         END IF;
                          
         --chk_overlapping dates   
         IF p1.ltall_tcy_refno IS NOT NULL AND
            p1.ltall_talt_code  IS NOT NULL AND     
            p1.ltall_start_date IS NOT NULL
         THEN   
            l_chk := NULL;
                                 
            OPEN chk_overlap (p1.ltall_tcy_refno
                             ,p1.ltall_talt_code);
            FETCH chk_overlap INTO l_chk;
            CLOSE chk_overlap ;

            IF l_chk IS NOT NULL
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',781);
               -- 'Dates overlap with an existing record'
            END IF;            
 
         END IF;
                  
         --Check data load batch for duplicates         
         IF p1.ltall_tcy_refno IS NOT NULL AND
            p1.ltall_talt_code  IS NOT NULL AND     
            p1.ltall_start_date IS NOT NULL
         THEN   
            l_chk_dup_dl := 0;
                                 
            OPEN  chk_dup_dl(p1.ltall_dlb_batch_id
                            ,p1.ltall_dl_seqno
                            ,p1.ltall_tcy_refno
                            ,p1.ltall_talt_code
                            ,p1.ltall_start_date
                            ,p1.ltall_end_date);

            FETCH chk_dup_dl INTO l_chk_dup_dl;
            CLOSE chk_dup_dl;

            IF l_chk_dup_dl > 0
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',782);
               --'Record is a duplicate in the data load batch'
            END IF; 
         END IF;  
                           
         --Check the Created date if supplied
         IF (NVl(p1.ltall_created_date,TRUNC(SYSDATE)) > TRUNC(SYSDATE))
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',783);
            --'The Created date cannot be later than today (truncated sysdate)'
         END IF;

--       *****************************************
--       Now UPDATE the record count and error code
         IF l_errors = 'F' THEN
            l_error_ind := 'Y';
         ELSE
            l_error_ind := 'N';
         END IF;
--
--       keep a count of the rows processed and commit after every 1000
         i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
         s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
         set_record_status_flag(l_id,l_errors);
--
      EXCEPTION
        WHEN OTHERS THEN
           ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
           s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
           set_record_status_flag(l_id,'O');
      END;
   END LOOP;
--
--
   fsc_utils.proc_end;
   COMMIT;
--
EXCEPTION
   WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
END dataload_validate;
--
--
-- ***********************************************************************
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       ltall_dl_seqno,
       ltall_refno
FROM   dl_hra_tenant_allowances
WHERE  ltall_dlb_batch_id   = p_batch_id
AND    ltall_dl_load_status = 'C';
--
--
-- *******************************
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'dl_hra_tenant_allowances';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- ***********************************************************************
--
-- Other variables
--
--
i        INTEGER := 0;
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
BEGIN
--
    fsc_utils.proc_start('s_'||ct||'.dataload_delete');
    fsc_utils.debug_message('s_'||ct||'.dataload_delete',3 );
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
          cs   := p1.ltall_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from TENANT_ALLOWANCES table
--
          DELETE
            FROM tenant_allowances
           WHERE tall_refno = p1.ltall_refno;
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
      END;
--
    END LOOP;
--
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANT_ALLOWANCES');
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

END s_dl_hra_tenant_allowances;
/

show errors
