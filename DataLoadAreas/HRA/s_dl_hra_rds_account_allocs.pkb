--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_account_allocs
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    PH   03-FEB-2008  Initial Creation.
--
--  2.0     5.15.0    VS   13-NOV-2009  Fix for Defect Id 2494.
--
--  3.0     5.15.0    VS   19-NOV-2009  Fix for Defect Id 2494. Allocated
--                                      Requested Amount := account deduction
--                                      requested amount and not the combined
--                                      amount.
--
--  4.0     5.15.0    VS   30-NOV-2009  Fix for Defect Id 2494. Remove validation
--                                      check for a PAY transaction 
--
--  5.0     5.15.0    VS   09-DEC-2009  Fix for Defect Id 2847. Use the instruction
--                                      effective date to get right allocation 
--                                      reference.
--
--  6.0     5.15.0    MT   09-FEB-2010  Shipped with MIN0061d version 9.  More fix for Defect Id 2847. Removed the trunc 
--                                      on the instruction effective date.  There were small number of records with 
--                                      more than 1 instruction  the same eff date
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
  UPDATE dl_hra_rds_account_allocs
     SET lraal_dl_load_status = p_status
   WHERE rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
       dbms_output.put_line('Error updating status of dl_hra_rds_account_allocs');
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
CURSOR  c1 IS
SELECT rowid rec_rowid,
       lraal_dlb_batch_id,
       lraal_dl_seqno,
       lraal_dl_load_status,
       lraal_rdsa_ha_reference,
       lraal_raud_start_date,
       lraal_hrv_dedt_code,
       lraal_hrv_rbeg_code,
       lraal_racd_pay_ref,
       lraal_racd_start_date,
       lraal_racd_radt_code,
       lraal_rdal_effective_date,
       lraal_requested_amount,
       lraal_fixed_amount_ind,
       lraal_allocated_amount,
       lraal_priority,
       lraal_requested_percentage,
       LRAAL_RACD_REQUESTED_AMOUNT,
       LRAAL_RDAL_DEDUCT_ACTION_TYPE,
       LRAAL_RDIN_EFFECTIVE_DATE
  FROM dl_hra_rds_account_allocs
 WHERE lraal_dlb_batch_id    = p_batch_id 
   AND lraal_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_get_racd(p_ha_reference       VARCHAR2,
                  p_dedt_code          VARCHAR2,
                  p_rbeg_code          VARCHAR2,
                  p_rac_accno          VARCHAR2,
                  p_racd_start_date    DATE,
                  p_raud_start_date    DATE,
                  p_radt_code          VARCHAR2) 
IS
SELECT racd_refno
  FROM rds_account_deductions,
       rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno         = raud_rdsa_refno
   AND racd_raud_refno    = raud_refno
   AND racd_rac_accno     = p_rac_accno  
   AND racd_start_date    = p_racd_start_date
   AND rdsa_ha_reference  = p_ha_reference
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_hrv_rbeg_code = p_rbeg_code
   AND raud_start_date    = p_raud_start_date
   AND racd_radt_code     = p_radt_code;
--
-- ***********************************************************************
--
CURSOR c_get_rdal(p_ha_reference     	  VARCHAR2,
                  p_dedt_code        	  VARCHAR2,
                  p_rbeg_code        	  VARCHAR2,
                  p_start_date       	  DATE,
                  p_effect_date      	  DATE,
                  --p_allocated_amount 	  NUMBER,
                  p_rdin_effect_date 	  DATE,
                  p_deduction_action_type VARCHAR2) 
IS
SELECT rdal_refno
  FROM rds_allocations,
       rds_instructions,
       rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno                                             = raud_rdsa_refno
   AND rdin_raud_refno                                        = raud_refno
   AND rdin_refno                                             = rdal_rdin_refno
   AND rdin_effective_date                                    = p_rdin_effect_date
   AND TRUNC(rdal_effective_date)                             = TRUNC(p_effect_date)
   AND rdsa_ha_reference                                      = p_ha_reference
   AND raud_hrv_dedt_code                                     = p_dedt_code
   AND raud_hrv_rbeg_code                                     = p_rbeg_code
   AND TRUNC(raud_start_date)                                 = TRUNC(p_start_date)
   --AND rdal_allocated_amount                                  = p_allocated_amount
   AND rdal_deduction_action_type                             = p_deduction_action_type;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   	VARCHAR2(30);
cd                   	DATE;
cp                   	VARCHAR2(30) := 'CREATE';
ct                   	VARCHAR2(30) := 'DL_HRA_RDS_ACCOUNT_ALLOCS';
cs                   	INTEGER;
ce	               	VARCHAR2(200);
l_id                 	ROWID;
l_an_tab             	VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                       INTEGER := 0;
l_exists                VARCHAR2(1);
l_rac_accno             revenue_accounts.rac_accno%type;
l_racd_refno            rds_account_deductions.racd_refno%type;
l_rdal_refno		rds_allocations.rdal_refno%type;
--
l_allocated_amount	NUMBER(11,2);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_account_allocs.dataload_create');
    fsc_utils.debug_message('s_dl_hra_rds_account_allocs.dataload_create',3);
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
          cs   := p1.lraal_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lraal_racd_pay_ref);
--
          l_racd_refno       := NULL;
          l_rdal_refno       := NULL;
          l_allocated_amount := NULL;
--
           OPEN c_get_racd(p1.lraal_rdsa_ha_reference, 
                           p1.lraal_hrv_dedt_code,
                           p1.lraal_hrv_rbeg_code, 
                           l_rac_accno,
                           p1.lraal_racd_start_date, 
                           p1.lraal_raud_start_date,
                           p1.lraal_racd_radt_code);
--
          FETCH c_get_racd INTO l_racd_refno;
          CLOSE c_get_racd;
--
--
           OPEN c_get_rdal(p1.lraal_rdsa_ha_reference, 
                           p1.lraal_hrv_dedt_code,
                           p1.lraal_hrv_rbeg_code, 
                           p1.lraal_raud_start_date,
                           p1.lraal_rdal_effective_date, 
                           --p1.lraal_allocated_amount,
                           p1.lraal_rdin_effective_date,
                           p1.lraal_rdal_deduct_action_type);
--
          FETCH c_get_rdal INTO l_rdal_refno;
          CLOSE c_get_rdal;
--
-- ***********************************************************************
--
-- Calculate the requested amount depending on the deduction
-- action type. If PAF (paid in full) then the allocated amount = requested
-- deduction amount. If PAP (part paid) then we calculate the allocated amount.
--
--
          IF (p1.lraal_rdal_deduct_action_type = 'PAF') THEN
--
           l_allocated_amount := p1.LRAAL_RACD_REQUESTED_AMOUNT;
--
          ELSIF (p1.lraal_rdal_deduct_action_type = 'PAP') THEN
--
              l_allocated_amount := ROUND(((p1.LRAAL_ALLOCATED_AMOUNT / p1.LRAAL_REQUESTED_AMOUNT) 
                                           * p1.LRAAL_RACD_REQUESTED_AMOUNT), 2);
--
          ELSE
--
             l_allocated_amount := 0;
--
          END IF;
--
--
-- Insert into relevent table
--
          INSERT /* +APPEND */ into  rds_account_allocations(raal_racd_refno,
                                                             raal_rdal_refno,
                                                             raal_requested_amount,
                                                             raal_fixed_amount_ind,
                                                             raal_allocated_amount,
                                                             raal_priority,
                                                             raal_requested_percentage
                                                            )
--
                                                      VALUES(l_racd_refno,
                                                             l_rdal_refno,
                                                             p1.lraal_racd_requested_amount,
                                                             p1.lraal_fixed_amount_ind,
                                                             l_allocated_amount,
                                                             p1.lraal_priority,
                                                             p1.lraal_requested_percentage
                                                            );
--
--
-- ***********************************************************************
--
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
    COMMIT;
--
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_ACCOUNT_ALLOCATIONS');
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
CURSOR c1 is
SELECT rowid rec_rowid,
       lraal_dlb_batch_id,
       lraal_dl_seqno,
       lraal_dl_load_status,
       lraal_rdsa_ha_reference,
       lraal_raud_start_date,
       lraal_hrv_dedt_code,
       lraal_hrv_rbeg_code,
       lraal_racd_pay_ref,
       lraal_racd_start_date,
       lraal_racd_radt_code,
       lraal_rdal_effective_date,
       lraal_requested_amount,
       lraal_fixed_amount_ind,
       lraal_allocated_amount,
       lraal_priority,
       lraal_requested_percentage,
       LRAAL_RACD_REQUESTED_AMOUNT,
       LRAAL_RDAL_DEDUCT_ACTION_TYPE,
       LRAAL_RDIN_EFFECTIVE_DATE
  FROM dl_hra_rds_account_allocs
 WHERE lraal_dlb_batch_id    = p_batch_id
   AND lraal_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_get_racd(p_ha_reference       VARCHAR2,
                  p_dedt_code          VARCHAR2,
                  p_rbeg_code          VARCHAR2,
                  p_rac_accno          VARCHAR2,
                  p_racd_start_date    DATE,
                  p_raud_start_date    DATE,
                  p_radt_code          VARCHAR2) 
IS
SELECT racd_refno
  FROM rds_account_deductions,
       rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno         = raud_rdsa_refno
   AND racd_raud_refno    = raud_refno
   AND racd_rac_accno     = p_rac_accno
   AND racd_start_date    = p_racd_start_date
   AND rdsa_ha_reference  = p_ha_reference
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_hrv_rbeg_code = p_rbeg_code
   AND raud_start_date    = p_raud_start_date
   AND racd_radt_code     = p_radt_code;
--
-- ***********************************************************************
--
CURSOR c_get_rdal(p_ha_reference     	  VARCHAR2,
                  p_dedt_code        	  VARCHAR2,
                  p_rbeg_code        	  VARCHAR2,
                  p_start_date       	  DATE,
                  p_effect_date      	  DATE,
                  p_allocated_amount      NUMBER,
                  p_rdin_effect_date 	  DATE,
                  p_deduction_action_type VARCHAR2)
IS
SELECT rdal_refno
  FROM rds_allocations,
       rds_instructions,
       rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno                                             = raud_rdsa_refno
   AND rdin_raud_refno                                        = raud_refno
   AND rdin_refno                                             = rdal_rdin_refno
   AND rdin_effective_date                                    = p_rdin_effect_date
   AND TRUNC(rdal_effective_date)                             = TRUNC(p_effect_date)
   AND rdsa_ha_reference                                      = p_ha_reference
   AND raud_hrv_dedt_code                                     = p_dedt_code
   AND raud_hrv_rbeg_code                                     = p_rbeg_code
   AND TRUNC(raud_start_date)                                 = TRUNC(p_start_date)
   AND rdal_allocated_amount                                  = p_allocated_amount
   AND rdal_deduction_action_type                             = p_deduction_action_type;
   
CURSOR C_GET_RDAL_ALLOC_AMOUNT(p_ha_reference     	  VARCHAR2,
                               p_dedt_code        	  VARCHAR2,
                               p_rbeg_code        	  VARCHAR2,
                               p_start_date       	  DATE,
                               p_effect_date      	  DATE,
                               p_rdin_effect_date 	  DATE,
                               p_deduction_action_type VARCHAR2)
IS
SELECT rdal_allocated_amount
  FROM rds_allocations,
       rds_instructions,
       rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno                                             = raud_rdsa_refno
   AND rdin_raud_refno                                        = raud_refno
   AND rdin_refno                                             = rdal_rdin_refno
   AND rdin_effective_date                                    = p_rdin_effect_date
   AND TRUNC(rdal_effective_date)                             = TRUNC(p_effect_date)
   AND rdsa_ha_reference                                      = p_ha_reference
   AND raud_hrv_dedt_code                                     = p_dedt_code
   AND raud_hrv_rbeg_code                                     = p_rbeg_code
   AND TRUNC(raud_start_date)                                 = TRUNC(p_start_date)
   AND rdal_deduction_action_type                             = p_deduction_action_type;
   
l_rdal_alloc_amount   NUMBER(11,2);
   
CURSOR C_ALLOC_AMOUNT_SUM(p_ha_reference     	      VARCHAR2,
                          p_dedt_code        	      VARCHAR2,
                          p_rbeg_code        	      VARCHAR2,
                          p_start_date       	      DATE,
                          p_effect_date      	      DATE,
                          p_rdin_effect_date 	      DATE,
                          p_deduction_action_type   VARCHAR2) IS
SELECT SUM(lraal_allocated_amount)
  FROM dl_hra_rds_account_allocs
 WHERE lraal_dlb_batch_id = p_batch_id
   AND lraal_rdsa_ha_reference = p_ha_reference
   AND lraal_hrv_dedt_code = p_dedt_code
   AND lraal_hrv_rbeg_code = p_rbeg_code
   AND TRUNC(lraal_raud_start_date) = TRUNC(p_start_date)
   AND TRUNC(lraal_rdal_effective_date) = TRUNC(p_effect_date)
   AND TRUNC(lraal_rdin_effective_date) = TRUNC(p_rdin_effect_date)
   AND lraal_rdal_deduct_action_type = p_deduction_action_type;

l_alloc_amount_sum   NUMBER(11,2);
--
-- ***********************************************************************
--
CURSOR c_check_rac(p_pay_ref  VARCHAR2) 
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_pay_ref;
--
-- ***********************************************************************
--
--CURSOR chk_trans_exists(p_rac_accno  		NUMBER,
--                        p_effective_date 	DATE,
--                        p_allocated_amount	NUMBER) 
--IS
--SELECT 'X'
--  FROM transactions
-- WHERE tra_rac_accno      = p_rac_accno
--   AND tra_effective_date = p_effective_date
--   AND tra_cr             = p_allocated_amount
--   AND tra_trt_code       = 'PAY';
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_RDS_ACCOUNT_ALLOCS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists                   VARCHAR2(1);
l_pro_refno                NUMBER(10);
l_errors                   VARCHAR2(10);
l_error_ind                VARCHAR2(10);
i                          INTEGER :=0;
l_rac_accno                revenue_accounts.rac_accno%type;
l_racd_refno               rds_account_deductions.racd_refno%type;
l_rdal_refno               rds_allocations.rdal_refno%type;
--
l_allocated_amount         NUMBER(11,2);
l_trans_exists             VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_account_allocs.dataload_validate');
    fsc_utils.debug_message( 's_dl_hra_rds_account_allocs.dataload_validate',3);
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
          cs   := p1.lraal_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- ***********************************************************************
--
--
-- Validation checks required
--
-- Check the account exists
-- 
          OPEN c_check_rac(p1.lraal_racd_pay_ref);
         FETCH c_check_rac INTO l_rac_accno;
--
         IF c_check_rac%NOTFOUND THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
         END IF;
--
         CLOSE c_check_rac;
--
--
-- ***********************************************************************
--
         l_racd_refno   := NULL;
         l_rdal_refno   := NULL;
--
-- Check the Account Deduction Exists
--
           OPEN c_get_racd(p1.lraal_rdsa_ha_reference, 
                           p1.lraal_hrv_dedt_code,
                           p1.lraal_hrv_rbeg_code,
                           l_rac_accno,
                           p1.lraal_racd_start_date, 
                           p1.lraal_raud_start_date,
                           p1.lraal_racd_radt_code
                          );

          FETCH c_get_racd INTO l_racd_refno;
--
          IF c_get_racd%NOTFOUND THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',170);
          END IF;
--
          CLOSE c_get_racd;
--
-- ***********************************************************************
--
-- Check the RDS Allocation Exists
--
           OPEN c_get_rdal(p1.lraal_rdsa_ha_reference, 
                           p1.lraal_hrv_dedt_code,
                           p1.lraal_hrv_rbeg_code,
                           p1.lraal_raud_start_date, 
                           p1.lraal_rdal_effective_date, 
                           p1.lraal_allocated_amount,
                           p1.lraal_rdin_effective_date,
                           p1.lraal_rdal_deduct_action_type);
--
          FETCH c_get_rdal INTO l_rdal_refno;
--
          IF c_get_rdal%NOTFOUND THEN
           OPEN C_ALLOC_AMOUNT_SUM(p1.lraal_rdsa_ha_reference, 
                                   p1.lraal_hrv_dedt_code,
                                   p1.lraal_hrv_rbeg_code,
                                   p1.lraal_raud_start_date, 
                                   p1.lraal_rdal_effective_date, 
                                   p1.lraal_rdin_effective_date,
                                   p1.lraal_rdal_deduct_action_type);
           FETCH C_ALLOC_AMOUNT_SUM INTO l_alloc_amount_sum;
           CLOSE C_ALLOC_AMOUNT_SUM;
          
           OPEN C_GET_RDAL_ALLOC_AMOUNT(p1.lraal_rdsa_ha_reference, 
                                        p1.lraal_hrv_dedt_code,
                                        p1.lraal_hrv_rbeg_code,
                                        p1.lraal_raud_start_date, 
                                        p1.lraal_rdal_effective_date, 
                                        p1.lraal_rdin_effective_date,
                                        p1.lraal_rdal_deduct_action_type);
           FETCH C_GET_RDAL_ALLOC_AMOUNT INTO l_rdal_alloc_amount;
           CLOSE C_GET_RDAL_ALLOC_AMOUNT;
          
           IF l_alloc_amount_sum != l_rdal_alloc_amount
           THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',171);
           END IF;
          END IF;
--        
          CLOSE c_get_rdal;
--
-- ***********************************************************************
--
-- Priority must be between 1 and 99
--
          IF nvl(p1.lraal_priority, 0) NOT BETWEEN 1 and 99 THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',172);
          END IF;
--
-- ***********************************************************************
--
-- Mandatory Fields (except those in previous checks)
--
-- Requested Amount
--
          IF p1.lraal_requested_amount IS NULL THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',173);
          END IF;
--
-- ***********************************************************************
--
-- Fixed Amount Indicator
--
          IF nvl(p1.lraal_fixed_amount_ind, 'X') NOT IN ( 'Y', 'N' ) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',174);
          END IF;
--
-- ***********************************************************************
--
-- Allocated Amount
--
          IF p1.lraal_allocated_amount IS NULL THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',175);
          END IF;
--
--
-- ******************************************************************************
--
-- Check the deduction Requested amount has been supplied
--
          IF (p1.lraal_racd_requested_amount IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',110);
          END IF;
--
-- ***********************************************************************
--
-- Deduction Action Type
--
          IF (p1.lraal_rdal_deduct_action_type IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',169);
--
          ELSIF (p1.lraal_rdal_deduct_action_type NOT IN ('PAF','PAP','PAZ',
                                                          'PBC','PBD','PBS','PBR')) THEN 
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',239);
--
          END IF;
--
-- ***********************************************************************
--
-- Calculate and check the requested amount depending on the deduction
-- action type. If PAF (paid in full) then the allocated amount = requested
-- deduction amount. If PAP (part paid) then we calculate the allocated amount.
--
-- Once we have calculated the allocated amount we now want to check against the
-- transaction record to make sure it matches. The rds account allocation should be
-- the same as the posted PAY transaction.
--
--
          l_allocated_amount := NULL;
--
          IF (p1.lraal_rdal_deduct_action_type = 'PAF') THEN
--
           l_allocated_amount := p1.LRAAL_RACD_REQUESTED_AMOUNT;
--
          ELSIF (p1.lraal_rdal_deduct_action_type = 'PAP') THEN
--
              l_allocated_amount := ROUND(((p1.LRAAL_ALLOCATED_AMOUNT / p1.LRAAL_REQUESTED_AMOUNT) 
                                           * p1.LRAAL_RACD_REQUESTED_AMOUNT), 2);
--
          ELSE
--
             l_allocated_amount := 0;
--
          END IF;
--
--
--          l_trans_exists := NULL;
--
--          IF (p1.lraal_rdal_deduct_action_type IN ('PAF','PAP')) THEN
--
--            OPEN chk_trans_exists(l_rac_accno, p1.LRAAL_RDAL_EFFECTIVE_DATE, l_allocated_amount);
--           FETCH chk_trans_exists INTO l_trans_exists;
--           CLOSE chk_trans_exists;
--
--           IF (l_trans_exists IS NULL) THEN
--            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',321);
--           END IF;
--
--          END IF;
--
-- ******************************************************************************
--
-- Check the Instruction Effective Date has been supplied
--
          IF (p1.lraal_rdin_effective_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',325);
          END IF;
--
-- ***********************************************************************
--
-- Reference Values
--
-- Deduction Type -- Mandatory as used in a link
--
          IF (NOT s_dl_hem_utils.exists_frv('RDS_DED_TYPE',p1.lraal_hrv_dedt_code,'N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',103);
          END IF;
--
-- **************************
--
-- Benefit Group -- Mandatory as used in a link
--
          IF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lraal_hrv_rbeg_code,'N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',104);
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
       lraal_dlb_batch_id,
       lraal_dl_seqno,
       lraal_dl_load_status,
       lraal_rdsa_ha_reference,
       lraal_raud_start_date,
       lraal_hrv_dedt_code,
       lraal_hrv_rbeg_code,
       lraal_racd_pay_ref,
       lraal_racd_start_date,
       lraal_racd_radt_code,
       lraal_rdal_effective_date,
       lraal_requested_amount,
       lraal_fixed_amount_ind,
       lraal_allocated_amount,
       lraal_priority,
       lraal_requested_percentage,
       LRAAL_RACD_REQUESTED_AMOUNT,
       LRAAL_RDAL_DEDUCT_ACTION_TYPE,
       LRAAL_RDIN_EFFECTIVE_DATE
  FROM dl_hra_rds_account_allocs
 WHERE lraal_dlb_batch_id   = p_batch_id
   AND lraal_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_get_racd(p_ha_reference       VARCHAR2,
                  p_dedt_code          VARCHAR2,
                  p_rbeg_code          VARCHAR2,
                  p_rac_accno          VARCHAR2,
                  p_racd_start_date    DATE,
                  p_raud_start_date    DATE,
                  p_radt_code          VARCHAR2) 
IS
SELECT racd_refno
  FROM rds_account_deductions,
       rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno         = raud_rdsa_refno
   AND racd_raud_refno    = raud_refno
   AND racd_rac_accno     = p_rac_accno
   AND racd_start_date    = p_racd_start_date
   AND rdsa_ha_reference  = p_ha_reference
   AND raud_hrv_dedt_code = p_dedt_code
   AND raud_hrv_rbeg_code = p_rbeg_code
   AND raud_start_date    = p_raud_start_date;
--
-- ***********************************************************************
--
CURSOR c_get_rdal(p_ha_reference     	  VARCHAR2,
                  p_dedt_code        	  VARCHAR2,
                  p_rbeg_code        	  VARCHAR2,
                  p_start_date       	  DATE,
                  p_effect_date      	  DATE,
                  p_allocated_amount      NUMBER,
                  p_rdin_effect_date 	  DATE,
                  p_deduction_action_type VARCHAR2)
IS
SELECT rdal_refno
  FROM rds_allocations,
       rds_instructions,
       rds_authorised_deductions,
       rds_authorities
 WHERE rdsa_refno                                             = raud_rdsa_refno
   AND rdin_raud_refno                                        = raud_refno
   AND rdin_refno                                             = rdal_rdin_refno
   AND rdin_effective_date                                    = p_rdin_effect_date
   AND TRUNC(rdal_effective_date)                             = TRUNC(p_effect_date)
   AND rdsa_ha_reference                                      = p_ha_reference
   AND raud_hrv_dedt_code                                     = p_dedt_code
   AND raud_hrv_rbeg_code                                     = p_rbeg_code
   AND TRUNC(raud_start_date)                                 = TRUNC(p_start_date)
   AND rdal_allocated_amount                                  = p_allocated_amount
   AND rdal_deduction_action_type                             = p_deduction_action_type;
--
-- ***********************************************************************
--
CURSOR c_check_rac(p_pay_ref  VARCHAR2) 
IS
SELECT rac_accno
  FROM revenue_accounts
 WHERE rac_pay_ref = p_pay_ref;
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_RDS_ACCOUNT_ALLOCS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists		VARCHAR2(1);
l_pro_refno             NUMBER(10);
i                       INTEGER :=0;
l_an_tab                VARCHAR2(1);
l_rac_accno             revenue_accounts.rac_accno%type;
l_racd_refno            rds_account_deductions.racd_refno%type;
l_rdal_refno            rds_allocations.rdal_refno%type;
--
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_rds_account_allocs.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_rds_account_allocs.dataload_delete',3 );
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
          cs   := p1.lraal_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
          l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref(p1.lraal_racd_pay_ref);
--
          l_racd_refno   := NULL;
          l_rdal_refno   := NULL;
--
           OPEN c_get_racd(p1.lraal_rdsa_ha_reference, 
                           p1.lraal_hrv_dedt_code, 
                           p1.lraal_hrv_rbeg_code, 
                           l_rac_accno, 
                           p1.lraal_racd_start_date,
                           p1.lraal_raud_start_date,
                           p1.lraal_racd_radt_code);
--
          FETCH c_get_racd INTO l_racd_refno;
          CLOSE c_get_racd;
--
--
           OPEN c_get_rdal(p1.lraal_rdsa_ha_reference, 
                           p1.lraal_hrv_dedt_code,
                           p1.lraal_hrv_rbeg_code, 
                           p1.lraal_raud_start_date,
                           p1.lraal_rdal_effective_date, 
                           p1.lraal_allocated_amount,
                           p1.lraal_rdin_effective_date,
                           p1.lraal_rdal_deduct_action_type);
--
          FETCH c_get_rdal INTO l_rdal_refno;
          CLOSE c_get_rdal;
--
-- Delete from table
--
          DELETE 
            FROM rds_account_allocations
           WHERE raal_racd_refno = l_racd_refno
             AND raal_rdal_refno = l_rdal_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_ACCOUNT_ALLOCATIONS');
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
END s_dl_hra_rds_account_allocs;
/