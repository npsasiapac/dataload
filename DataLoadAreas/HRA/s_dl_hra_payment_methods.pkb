CREATE OR REPLACE PACKAGE BODY s_dl_hra_payment_methods       
AS      
-- ***********************************************************************      
        
  --  DESCRIPTION:      
  --            
  --  CHANGE CONTROL    
  --  VERSION DB VER    WHO  WHEN       WHY       
  --      1.0   PJD          05-DEC-01  Dataload     
  --      1.2 5.1.4     PJD  27-FEB-02  Insert into payment contracts before methods
  --                                    Change validation on bde_bty_code      
  --                                    Add Insert into pm_destination_accounts (including
  --                                    changing various cursors to get the required info)   
  --      1.3 5.1.4     PJD  02-APR-02  Correct insert into Bank Details   
  -- 
  --      1.4 5.2.0     MH   16-OCT-02  Added section to insert into
  --                                    Payment Contracts and Methods
  --                                    even if bank details not supplied.
  --      1.5 5.2.0     PJD  28-NOV-02  Changes for NCCW to allow for multiple
  --                                    Payment Contracts
  --      2.0 5.2.0     PJD  09-DEC-02  Add pme_end_date into insert proc
  --      3.0 5.3.0     PJD  04-FEB-03  Default 'Created By' field to DATALOAD
  --      3.1 5.3.0     PH   20-MAR-03  Removed 'p1.lpme_aun_bad_account_no IS NULL'
  --                                    from validate (error 109) as if null defaults
  --      3.2 5.3.0     PH   25-MAR-03  Changed cp variable on delete process to DELETE
  --                                    from CREATE
  --      3.3 5.3.0     PH   17-MAR-03  Changed c_bde_exists cursor on create by adding nvl.
  --      3.4 5.3.0     PJD  05-JUL-03  Changed logic relating to payment contracts to allow
  --                                    for multiple methods per contract.
  --      3.5 5.3.0     PJD  04-SEP-03  Changed the field used to determine whether to
  --                                    create/link to a payment contract to be the 
  --                                    lpme_pct_start_date
  --      3.6 5.4.0     PJD  03-NOV-03  l_pct_refno now set to null earlier in 
  --                                    the create procedure. 
  --      3.7 5.4.0     PJD  11-NOV-03  When bank details not supplied the code
  --                                    now uses the lpct_start_date
  --                                    to decide if a contract needs creating.
  --                                    If lpct_start_date is supplied but
  --                                    neither lpct_amount or lpct_percentage
  --                                    are supplied then the code won't try
  --                                    to create a payment contract although
  --                                    it will link to an existing one.
  --      3.8 5.5.0    MH   15-MAR-04   pme_end_date insert missing when bad account null
  --      3.9 5.6.0    PH   10-NOV-04   Amended validate on regular payment methods. Now
  --                                    pass in pme_start as you Cannot have a pme record 
  --                                    without relevant payment_profile for the period.
  --      4.0 5.13.0   PH   06-FEB-2008 Now includes its own 
  --                                    set_record_status_flag procedure.
  --      4.1 5.13.0   PH   06-FEB-2008 Amended validate on Bank Details, now checks
  --                                    bank types table.
  --      4.2 6.1      PJD  20-SEP-2012 Corrected cursor c_null_aub_aun_code
  --                                    which needed missing brackets adding back in
  --      6.11 6.11    AJ   28-JUL-2015 Updated for v6.11 multi-language changes
  --                                    in insert bank_details and validate bank details
--        6.13        PJD   13-JUN-2017 Consolidated validation on
--                                      bank details to prevent 
--                                      duplication of error 109
--
--        6.13.1      PJD   16-OCT-2017 Put and end date on
--                                      party_bank_acct_pymt_mthds if
--                                      payment method has an end date
--
-- *******************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hra_payment_methods         
  SET lpme_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_payment_methods');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
  --           
  --  declare package variables AND constants       
        
PROCEDURE dataload_create       
(p_batch_id  IN VARCHAR2,           
 p_date      IN DATE)       
AS      
--      
CURSOR c1(p_batch_id VARCHAR2) is           
SELECT          
rowid rec_rowid         
,lpme_dlb_batch_id      
,lpme_dl_seqno          
,lpme_pay_ref           
,lpme_pme_pmy_code      
,lpme_pme_start_date    
,lpme_pme_end_date    
,lpme_pme_hrv_ppc_code  
,lpme_pme_first_dd_taken_ind        
,lpme_bde_bank_name     
,lpme_bad_account_no    
,lpme_bad_account_name  
,lpme_bad_sort_code     
,lpme_bde_branch_name   
,lpme_bad_start_date    
,lpme_aun_bad_account_no        
,lpme_source_acc_ind    
,lpme_bad_par_per_alt_ref       
,lpme_pct_amount        
,lpme_pct_percentage    
,lpme_bde_bty_code
,lpme_pct_start_date
,lpme_pct_end_date
,lpme_bde_bank_name_mlang
,lpme_bde_branch_name_mlang
FROM dl_hra_payment_methods         
WHERE lpme_dlb_batch_id    = p_batch_id     
AND   lpme_dl_load_status = 'V';            
--      
CURSOR c_pro_refno(l_pro_propref VARCHAR2)          
IS      
SELECT pro_refno        
FROM   properties       
WHERE  pro_propref = l_pro_propref;         
--      
CURSOR c_par_refno(l_par_alt_ref VARCHAR2)          
IS      
SELECT par_refno        
FROM   parties          
WHERE  par_per_alt_ref = l_par_alt_ref;     
--      
CURSOR c_org_refno(p_par_alt_ref VARCHAR2)          
IS      
SELECT par_refno        
FROM   parties          
WHERE  par_com_short_name = p_par_alt_ref           
OR     par_org_short_name = p_par_alt_ref;          
--      
CURSOR c_pme_refno      
IS      
SELECT pme_refno_seq.nextval        
FROM dual;      
--   
CURSOR c_pct(p_par_refno NUMBER, 
             p_rac_accno NUMBER,
             p_start     DATE)
IS      
SELECT pct_refno        
FROM   payment_contracts
WHERE  pct_par_refno     = p_par_refno
AND    pct_rac_accno     = p_rac_accno
AND    p_start          >= pct_start_date
AND    p_start          <= NVL(pct_end_date,p_start);
--         
CURSOR c_pct_refno      
IS      
SELECT pct_refno_seq.nextval        
FROM dual;      
--      
CURSOR c_pmt (p_pmy_code VARCHAR2)          
IS      
SELECT pmy_regular_payment_ind,pmy_extract_ind      
FROM   payment_method_types         
WHERE  pmy_code = p_pmy_code;       
--      
CURSOR c_bde_exists(p_bank_name VARCHAR2, p_branch_name VARCHAR2) IS    
select bde_refno        
FROM   bank_details     
WHERE  bde_bank_name   = p_bank_name        
  AND  nvl(bde_branch_name,'~') = nvl(p_branch_name,'~');     
--      
CURSOR c_bde_refno      
IS      
SELECT bde_refno_seq.nextval        
FROM dual;      
--      
CURSOR c_bad_refno      
IS      
SELECT bad_refno_seq.nextval        
FROM dual;      
--      
CURSOR c_rac_details (p_pay_ref VARCHAR2)           
IS      
SELECT rac_accno,rac_aun_code,rac_class_code,rac_par_refno,rac_report_pro_refno      
FROM   revenue_accounts         
WHERE  rac_pay_ref = p_pay_ref;             
--      
CURSOR c_get_tenant (p_rac_accno NUMBER) is         
SELECT hop_par_refno    
FROM   household_persons, tenancy_instances, revenue_accounts           
WHERE  hop_refno = tin_hop_refno            
AND    tin_tcy_refno = rac_tcy_refno        
AND    rac_accno = p_rac_accno      
ORDER BY nvl(tin_end_date,sysdate) desc ,tin_start_date, tin_hop_refno;         
--      
CURSOR c_aub_aun_code(p_aun_bad_account_no VARCHAR2
     ,p_pro_refno  NUMBER
     ,p_rac_aun_code       VARCHAR2) 
IS
SELECT aub_aun_code,aub_bad_refno,aub_start_date
FROM  admin_unit_bank_accounts a,bank_account_details b WHERE b.bad_type = 'ORG'
AND   a.aub_bad_refno = b.bad_refno
AND   b.bad_account_no = nvl(p_aun_bad_account_no,b.bad_account_no)
AND   (
       (p_pro_refno IS NULL
        AND a.aub_aun_code = p_rac_aun_code
       ) 
       OR
       (p_pro_refno IS NOT NULL
        AND
        EXISTS (SELECT NULL
        FROM admin_properties p,admin_groupings g
        WHERE (   a.aub_aun_code      = g.agr_aun_code_parent
       OR a.aub_aun_code      = g.agr_aun_code_child)
          AND p.apr_aun_code  = g.agr_aun_code_child    
          AND p.apr_pro_refno         = p_pro_refno
       )
        )
       )
ORDER BY aub_default_account_ind DESC;  -- Y before N
--             
--      
-- Constants for process_summary            
cb       VARCHAR2(30);  
cd       DATE;          
cp       VARCHAR2(30) := 'CREATE';          
ct       VARCHAR2(30) := 'DL_HRA_PAYMENT_METHODS';  
cs       INTEGER;       
ce	      VARCHAR2(200);        
l_id     ROWID; 
--      
-- Other variables      
--      
        
l_exists  VARCHAR2(1);      
l_tcy_refno       tenancies.tcy_refno%TYPE;         
l_rac_accno       revenue_accounts.rac_accno%TYPE;  
l_rac_aun_code    revenue_accounts.rac_aun_code%TYPE; 
l_rac_pro_refno   revenue_accounts.rac_report_pro_refno%TYPE;      
l_rac_start_date  DATE;         
l_class_code      revenue_accounts.rac_hrv_ate_code%TYPE;       
l_pro_refno       properties.pro_refno%TYPE;        
l_par_refno       parties.par_refno%TYPE;           
l_pme_refno       payment_methods.pme_refno%type;   
l_pct_refno       payment_contracts.pct_refno%type;         
l_pmy_reg_pay_ind payment_method_types.pmy_regular_payment_ind%TYPE;    
l_pmy_extract_ind payment_method_types.pmy_extract_ind%TYPE;            
l_bad_refno       NUMBER(8);        
l_bde_refno       NUMBER(8);        
l_is_configured   VARCHAR(1);       
l_an_tab          VARCHAR2(1);  
l_aub_aun_code    VARCHAR2(20);
l_aub_bad_refno   NUMBER(10);
l_aub_start_date  DATE;    
l_pct_start       DATE;
l_pct_end         DATE;        
i                 INTEGER := 0;             
--      
BEGIN           
--      
fsc_utils.proc_start('s_dl_hra_payment_methods.dataload_create');       
fsc_utils.debug_message( 's_dl_hra_payment_methods.dataload_create',3);         
--      
cb := p_batch_id;       
cd := p_date;           
--      
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');      
--      
for p1 in c1(p_batch_id) loop       
--      
BEGIN           
  --            
  cs := p1.lpme_dl_seqno;
  l_id := p1.rec_rowid;
  SAVEPOINT SP1;        
     --            
     -- Get Revenue Account number             
     --            
     OPEN  c_rac_details(p1.lpme_pay_ref);          
     FETCH c_rac_details into l_rac_accno, l_rac_aun_code,       
                              l_class_code,l_par_refno,
                              l_rac_pro_refno;     
     CLOSE c_rac_details;       
     --            
     --            
     OPEN  c_pme_refno;         
     FETCH c_pme_refno into l_pme_refno;    
     CLOSE c_pme_refno;         
     --            
     -- If bank details have been supplied, get the next sequence number
     -- and insert into BANK_DETAILS and BANK_HOLDINGS     
     --            
     IF (p1.lpme_bad_account_no IS NOT NULL)           
     THEN          
     --            
     --   Need to see if a party has been supplied on the record             
     --            
       IF p1.lpme_bad_par_per_alt_ref IS NULL          
       THEN        
     --   is it a sundry party account - in which case use that party    
         IF l_par_refno IS NULL        
         THEN      
           OPEN  c_get_tenant(l_rac_accno);     
           FETCH c_get_tenant into l_par_refno;        
           CLOSE c_get_tenant;         
         END IF; -- l_par_refno          
       ELSE
     --        
     --   If a party has been supplied then...       
     --   is it a Person
     --         
         IF (NVL(p1.lpme_source_acc_ind,'P') = 'P')    
         THEN      
           OPEN  c_par_refno(p1.lpme_bad_par_per_alt_ref);      
           FETCH c_par_refno INTO l_par_refno;         
           CLOSE c_par_refno;      
         ELSE      
     --        
     --     or else it must be an organisation         
     --
           OPEN  c_org_refno(p1.lpme_bad_par_per_alt_ref);      
           FETCH c_org_refno INTO l_par_refno;         
           CLOSE c_org_refno;      
         END IF;           
     --          
       END IF; -- had the bad_par_per_alt_ref been supplied        
     --          
     --   If the Payment Contract Details have been supplied       
     --   then create a payment contract
     --       
     --   Changed to use lpme_pct_start_date as the relevant column 
     --   to decide whether a payment contract should be created/linked to
     --
     --   v3.6 - Moved the next line up above the following IF statement
     --
       l_pct_refno:= NULL;
     --
       IF p1.lpme_pct_start_date IS NOT NULL            
       THEN   
     --
     --   check to see if there is an existing contract to link to
     --
         l_pct_start := NULL;
         l_pct_start := NVL(p1.lpme_pct_start_date,p1.lpme_pme_start_date);  
     --
         OPEN  c_pct(l_par_refno, l_rac_accno, l_pct_start); 
         FETCH c_pct into l_pct_refno;          
         CLOSE c_pct;
     --    
     --     else get the next refno
     -- 
        IF (l_pct_refno IS NULL
            AND  (    p1.lpme_pct_amount IS NOT NULL           
                   OR p1.lpme_pct_percentage IS NOT NULL ))     
         THEN
           OPEN  c_pct_refno;      
           FETCH c_pct_refno into l_pct_refno;          
           CLOSE c_pct_refno;
     --       
           INSERT INTO payment_contracts        
           (pct_refno          
           ,pct_rac_accno   
           ,pct_par_refno   
           ,pct_start_date 
           ,pct_end_date 
           ,pct_status      
           ,pct_created_date        
           ,pct_created_by  
           ,pct_amount      
           ,pct_percentage)         
           VALUES           
           (l_pct_refno     
           ,l_rac_accno     
           ,l_par_refno     
           ,nvl(p1.lpme_pct_start_date,p1.lpme_pme_start_date)
           ,p1.lpme_pct_end_date      
           ,'A'     
           ,sysdate         
           ,'DATALOAD' 
           ,p1.lpme_pct_amount      
           ,p1.lpme_pct_percentage);
      --            
         END IF; -- l_pct_refno
      --       
       END IF; -- payment contract info has been supplied     
      --         
      --   insert a row into PAYMENT_METHODS    
      --            
       INSERT into payment_methods             
       (pme_refno          
       ,pme_pmy_code       
       ,pme_start_date 
       ,pme_end_date    
       ,pme_first_dd_taken_ind         
       ,pme_pct_refno      
       ,pme_rac_accno      
       ,pme_hrv_ppc_code   
       )           
       values      
       (l_pme_refno        
       ,p1.lpme_pme_pmy_code       
       ,p1.lpme_pme_start_date  
       ,p1.lpme_pme_end_date       
       ,p1.lpme_pme_first_dd_taken_ind         
       ,l_pct_refno        
       ,l_rac_accno        
       ,p1.lpme_pme_hrv_ppc_code);             
       --        
       --         
       -- Now we need to get insert into bank_details,        
       -- bank account details, party_bank_accounts and       
       -- party bank acct pymt_mthds as appropriate;  
       --         
       -- So does the bank detail already exist       
       l_exists := NULL;  
       OPEN c_bde_exists(p1.lpme_bde_bank_name,       
       p1.lpme_bde_branch_name);    
       FETCH c_bde_exists into l_bde_refno;           
       CLOSE c_bde_exists;        
       --         
       IF l_bde_refno IS NULL         
       THEN       
         OPEN c_bde_refno;        
         FETCH c_bde_refno into l_bde_refno;          
         CLOSE c_bde_refno;       
         --
         -- used when mlang not supplied (v6.11)
         --
         IF (p1.lpme_bde_bank_name_mlang   IS NULL) 
         THEN
         --
         INSERT INTO BANK_DETAILS             
         (bde_refno       
         ,bde_bank_name   
         ,bde_created_by  
         ,bde_created_date        
         ,bde_branch_name         
         ,bde_bty_code)   
         VALUES       --      
         (l_bde_refno     
         ,p1.lpme_bde_bank_name       
         ,'DATALOAD'   
         ,sysdate         
         ,p1.lpme_bde_branch_name             
         ,p1.lpme_bde_bty_code);      
         --
         END IF;
         --
         IF (p1.lpme_bde_bank_name_mlang IS NOT NULL)
         THEN
         --
         -- used when mlang supplied (v6.11)
         --
         INSERT INTO BANK_DETAILS
         (bde_refno,
         bde_bank_name,
         bde_created_by,
         bde_created_date,
         bde_branch_name,
         bde_bty_code,
         bde_bank_name_mlang,
         bde_branch_name_mlang
         ) VALUES 
         (l_bde_refno,
         p1.lpme_bde_bank_name,
         'DATALOAD',
         sysdate,
         p1.lpme_bde_branch_name,
         p1.lpme_bde_bty_code,
         p1.lpme_bde_bank_name_mlang,
         p1.lpme_bde_branch_name_mlang);
         --
         END IF;
         --	
       END IF;    
       --
       -- Now insert into bank_account_details        
       l_bad_refno := NULL;       
       OPEN c_bad_refno;  
       FETCH c_bad_refno into l_bad_refno;    
       CLOSE c_bad_refno;         
       --         
       INSERT INTO BANK_ACCOUNT_DETAILS       
       (bad_refno         
       ,bad_type          
       ,bad_sort_code     
       ,bad_bde_refno     
       ,bad_account_no    
       ,bad_account_name  
       ,bad_start_date    
       ,bad_created_by    
       ,bad_created_date  
       ,bad_end_date      
       ,bad_user_alt_ref)         
       VALUES     
       (l_bad_refno       
       ,'CUS'     
       ,p1.lpme_bad_sort_code         
       ,l_bde_refno       
       ,p1.lpme_bad_account_no        
       ,p1.lpme_bad_account_name      
       ,p1.lpme_pme_start_date        
       ,'DATALOAD'
       ,sysdate           
       ,NULL      
       ,NULL);    
       --         
       -- and now into party bank_accounts    
       INSERT INTO PARTY_BANK_ACCOUNTS        
       (pba_par_refno     
       ,pba_bad_refno     
       ,pba_start_date    
       ,pba_created_date  
       ,pba_created_by    
       ,pba_end_date)     
       VALUES     
       (l_par_refno       
       ,l_bad_refno       
       ,p1.lpme_pme_start_date        
       ,sysdate           
       ,'DATALOAD'
       ,NULL);    
       --         
       -- and now into PARTY BANK ACCT PYMT MTHDS     
       --
       INSERT INTO party_bank_acct_pymt_mthds         
       (pbp_pba_par_refno         
       ,pbp_pba_bad_refno         
       ,pbp_pba_start_date        
       ,pbp_pme_refno     
       ,pbp_start_date    
       ,pbp_created_date  
       ,pbp_created_by    
       ,pbp_end_date)     
       VALUES     
       (l_par_refno       
       ,l_bad_refno       
       ,p1.lpme_pme_start_date        
       ,l_pme_refno       
       ,p1.lpme_pme_start_date        
       ,sysdate           
       ,'DATALOAD'      
       ,p1.lpme_pme_end_date);    
       --     
       -- and finally do an insert into pm_destination_accounts
       --
       l_aub_aun_code   := NULL;
       l_aub_bad_refno  := NULL;
       l_aub_start_date := NULL;
       --
       OPEN  c_aub_aun_code(p1.lpme_aun_bad_account_no,l_rac_pro_refno,
                            l_rac_aun_code);
       FETCH c_aub_aun_code INTO l_aub_aun_code,l_aub_bad_refno,
				 l_aub_start_date;
       CLOSE c_aub_aun_code;
       --
       IF l_aub_bad_refno IS NOT NULL
       THEN
         INSERT INTO pm_destination_accounts
         (pda_aub_aun_code
         ,pda_aub_bad_refno
         ,pda_aub_start_date
         ,pda_pme_refno
         ,pda_start_date
         ,pda_created_date
         ,pda_created_by)
         VALUES
         (l_aub_aun_code
         ,l_aub_bad_refno
         ,l_aub_start_date
         ,l_pme_refno
         ,p1.lpme_pme_start_date
         ,sysdate
         ,'DATALOAD');
       --
       END IF;
      
    END IF; -- p1.lpme_bad_account_no IS NOT NULL     
    --            
    --  
    --            
    -- If bank details have been NOT been supplied, 
    -- We can still insert into PAYMENT CONTRACTS and 
    -- PAYMENT METHODS
    -- get the next sequence number and insert into 
    -- BANK_DETAILS and BANK_HOLDINGS     
    --            
    IF (p1.lpme_bad_account_no IS NULL)           
    THEN          
    --            
    -- Need to see if a party has been supplied on the record             
    --            
      IF p1.lpme_bad_par_per_alt_ref IS NULL          
      THEN        
    -- is it a sundry party account - in which case use that party    
        IF l_par_refno IS NULL        
        THEN      
          OPEN c_get_tenant(l_rac_accno);     
          FETCH c_get_tenant into l_par_refno;        
          CLOSE c_get_tenant;         
         END IF;          
      ELSE        
        -- If a party has been supplied then...       
        -- is it a Person         
        IF (NVL(p1.lpme_source_acc_ind,'P') = 'P')    
        THEN      
          OPEN c_par_refno(p1.lpme_bad_par_per_alt_ref);      
          FETCH c_par_refno INTO l_par_refno;         
          CLOSE c_par_refno;      
        ELSE      
          --        
          -- or else it must be an organisation         
          OPEN c_org_refno(p1.lpme_bad_par_per_alt_ref);      
          FETCH c_org_refno INTO l_par_refno;         
          CLOSE c_org_refno;      
        END IF;           
        --          
      END IF; -- had the bad_par_per_alt_ref been supplied        
      --          
      -- If the Payment Contract Details have been supplied       
      -- then create a payment contract
      --       
      IF    p1.lpme_pct_start_date IS  NOT NULL           
      THEN   
        --
        --   check to see if there is an existing contract to link to
        --
        l_pct_refno := NULL;
        l_pct_start := NULL;
        l_pct_start := NVL(p1.lpme_pct_start_date,p1.lpme_pme_start_date);  
        OPEN  c_pct(l_par_refno, l_rac_accno, l_pct_start); 
        FETCH c_pct into l_pct_refno;          
        CLOSE c_pct;
        --    
        --     else get the next refno
        -- 
        IF (l_pct_refno IS NULL
            AND  (    p1.lpme_pct_amount IS NOT NULL           
                   OR p1.lpme_pct_percentage IS NOT NULL ))     
        THEN
          OPEN  c_pct_refno;      
          FETCH c_pct_refno into l_pct_refno;          
          CLOSE c_pct_refno;
       --       
          INSERT INTO payment_contracts        
          (pct_refno          
          ,pct_rac_accno   
          ,pct_par_refno   
          ,pct_start_date 
          ,pct_end_date 
          ,pct_status      
          ,pct_created_date        
          ,pct_created_by  
          ,pct_amount      
          ,pct_percentage)         
          VALUES           
          (l_pct_refno     
          ,l_rac_accno     
          ,l_par_refno     
          ,nvl(p1.lpme_pct_start_date,p1.lpme_pme_start_date)
          ,p1.lpme_pct_end_date
          ,'A'     
          ,sysdate         
          ,'DATALOAD' 
          ,p1.lpme_pct_amount      
          ,p1.lpme_pct_percentage);
          --            
        END IF; -- l_pct_refno
        --       
      ELSE 
        l_pct_start := NULL;
      END IF; -- payment contract info has been supplied     
      --
      --   insert a row into PAYMENT_METHODS    
      --            
      INSERT into payment_methods             
      (pme_refno          
      ,pme_pmy_code       
      ,pme_start_date
      ,pme_end_date     
      ,pme_first_dd_taken_ind         
      ,pme_pct_refno      
      ,pme_rac_accno      
      ,pme_hrv_ppc_code   
      )           
      values      
      (l_pme_refno        
      ,p1.lpme_pme_pmy_code       
      ,p1.lpme_pme_start_date
      ,p1.lpme_pme_end_date         
      ,p1.lpme_pme_first_dd_taken_ind         
      ,l_pct_refno        
      ,l_rac_accno        
      ,p1.lpme_pme_hrv_ppc_code);             
      --        
      --         
    END IF;
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
  WHEN OTHERS THEN      
  ROLLBACK TO SP1;      
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);         
  set_record_status_flag(l_id,'O');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y'); 
  --          
 END;           
END LOOP;       
--      
-- Section to anayze the table(s) populated by this dataload            
--      
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_METHODS');      
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_CONTRACTS');            
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCT_PYMT_MTHDS');           
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCOUNTS');          
l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_HOLDINGS');        
l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');     
l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_ACCOUNT_DETAILS');  
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PM_DESTINATION_ACCOUNTS');         
--      
fsc_utils.proc_end;     
commit;         
--      
EXCEPTION    
WHEN OTHERS THEN     
s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');      
--      
--      
END dataload_create;    
--      
--      
PROCEDURE dataload_validate         
     (p_batch_id  IN VARCHAR2,      
      p_date      IN DATE)          
AS      
--      
CURSOR c1 is    
SELECT          
rowid rec_rowid         
,lpme_dlb_batch_id      
,lpme_dl_seqno          
,lpme_pay_ref           
,lpme_pme_pmy_code      
,lpme_pme_start_date    
,lpme_pme_end_date
,lpme_pme_hrv_ppc_code  
,lpme_pme_first_dd_taken_ind        
,lpme_bde_bank_name     
,lpme_bad_account_no    
,lpme_bad_account_name  
,lpme_bad_sort_code     
,lpme_bde_branch_name   
,lpme_bad_start_date    
,lpme_aun_bad_account_no        
,lpme_source_acc_ind    
,lpme_bad_par_per_alt_ref       
,lpme_pct_amount        
,lpme_pct_percentage    
,lpme_bde_bty_code   
,lpme_pct_start_date
,lpme_pct_end_date
,lpme_bde_bank_name_mlang
,lpme_bde_branch_name_mlang   
FROM dl_hra_payment_methods         
WHERE lpme_dlb_batch_id  = p_batch_id       
AND   lpme_dl_load_status       in ('L','F','O');   
--      
CURSOR c_ppr(c_lrac_aun_code VARCHAR2,      
     c_lrac_pme_hrv_ppc_code VARCHAR2,      
     c_year VARCHAR2)       
IS      
SELECT ppr_aye_year     
  FROM payment_profiles         
 WHERE ppr_aye_aun_code = c_lrac_aun_code           
   AND ppr_aye_year     = c_year            
   AND ppr_hrv_ppc_code = c_lrac_pme_hrv_ppc_code;  
--      
CURSOR c_get_pro_refno (c_pro_propref VARCHAR2)     
IS      
SELECT pro_refno FROM properties            
WHERE  pro_propref = c_pro_propref;         
--      
CURSOR c_par_refno(l_par_alt_ref VARCHAR2)          
IS      
SELECT par_refno        
FROM   parties          
WHERE  par_per_alt_ref = l_par_alt_ref;           
--      
CURSOR c_org_refno(l_par_alt_ref VARCHAR2)          
IS      
SELECT par_refno        
FROM   parties          
WHERE  par_com_short_name = l_par_alt_ref           
OR     par_org_short_name = l_par_alt_ref;          
--      
--      
CURSOR c_rac_accno(p_pay_ref VARCHAR2)      
IS      
SELECT rac_accno,rac_aun_code,rac_start_date,rac_report_pro_refno       
  FROM revenue_accounts         
 WHERE rac_pay_ref = p_pay_ref;             
--      
CURSOR c_pmy_dets(p_pmy_code VARCHAR2)      
IS      
SELECT PMY_CODE,PMY_REGULAR_PAYMENT_IND     
FROM   payment_method_types         
WHERE  pmy_code = p_pmy_code;       
--      
CURSOR c_aub_aun_code(p_aun_bad_account_no VARCHAR2         
     ,p_pro_refno  NUMBER)  
IS      
SELECT 'X'      
FROM bank_account_details       
WHERE bad_type = 'ORG'  
AND   bad_account_no = p_aun_bad_account_no         
AND   EXISTS (SELECT NULL       
      FROM admin_properties,admin_groupings         
           ,admin_unit_bank_accounts        
      WHERE aub_bad_refno      = bad_refno  
        AND (aub_aun_code      = agr_aun_code_parent            
     OR aub_aun_code   = agr_aun_code_child)            
        AND agr_aun_code_child = apr_aun_code       
        AND apr_pro_refno      = p_pro_refno);      
--      
CURSOR c_null_aub_aun_code(p_pro_refno NUMBER)      
IS      
SELECT 'X'      
FROM bank_account_details       
WHERE bad_type = 'ORG'  
AND   EXISTS (SELECT NULL       
              FROM admin_properties,admin_groupings         
                  ,admin_unit_bank_accounts        
              WHERE aub_bad_refno      = bad_refno  
                AND (aub_aun_code       = agr_aun_code_parent            
                     OR    
                     aub_aun_code       = agr_aun_code_child  
                    )           
                AND agr_aun_code_child = apr_aun_code       
                AND apr_pro_refno      = p_pro_refno);      
--      
CURSOR c_bty_code(p_bde_bty_code VARCHAR2) IS       
SELECT bty_code
      ,bty_branch_code_mandatory_flag
  FROM bank_types
 WHERE bty_code = p_bde_bty_code;      
--      
CURSOR c_year(p_aun_code   VARCHAR2,
              p_start_date DATE) IS
SELECT aye_year
FROM   admin_years
WHERE  p_aun_code = aye_aun_code
AND    p_date between aye_start_date AND aye_end_date;     
--      
CURSOR c_pct (p_rac_accno NUMBER, p_par_refno NUMBER, p_start DATE) IS SELECT pct_start_date, pct_end_date
FROM   payment_contracts
WHERE  pct_rac_accno  = p_rac_accno
AND    pct_par_refno  = p_par_refno
AND    p_start       >= pct_start_date
AND    p_start       <= NVL(pct_end_date,p_start);
--      
-- Constants for process_summary            
cb       VARCHAR2(30);  
cd       DATE;          
cp       VARCHAR2(30) := 'VALIDATE';        
ct       VARCHAR2(30) := 'DL_HRA_PAYMENT_METHODS';  
cs       INTEGER;       
ce       VARCHAR2(200);
l_id     ROWID;
--      
l_exists           VARCHAR2(1);
l_pro_refno        NUMBER(10);
l_par_refno        NUMBER(10);
l_errors           VARCHAR2(10);
l_error_ind        VARCHAR2(10);
i                  INTEGER :=0;
-- Other Variables
l_ppr              NUMBER(4);
l_year             NUMBER(4);
l_priority         NUMBER(8);
l_aun_current      VARCHAR2(1);
l_aun_code         VARCHAR2(20);
l_rac_accno        VARCHAR2(8);
l_rac_aun_code     VARCHAR2(20);
l_rac_start_date   DATE;
l_tcy_refno        tenancies.tcy_refno%TYPE;
l_lrac_report_pro_refno NUMBER(25);
l_dummy            VARCHAR(1);
epo_found	       VARCHAR(1);
l_pmy_code         VARCHAR2(2);
l_pmy_reg_pay      VARCHAR2(1);
l_pct_start        DATE;
l_pct_end          DATE;
l_bty_code         bank_types.bty_code%type;
l_branch_mand      bank_types.bty_branch_code_mandatory_flag%type;             
--      
BEGIN           
--      
fsc_utils.proc_start('s_dl_hra_payment_methods.dataload_validate');     
fsc_utils.debug_message('s_dl_hra_payment_methods.dataload_validate',3);        
--      
cb := p_batch_id;       
cd := p_date;           
--      
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');      
--      
FOR p1 IN c1 LOOP       
--      
BEGIN           
  cs := p1.lpme_dl_seqno;
  l_id := p1.rec_rowid;
  --            
  l_errors := 'V';      
  l_error_ind := 'N';   
  --            
  --            
  -- Check the revenue account exists on    
  -- REVENUE_ACCOUNTS   
  --            
  l_rac_accno      := NULL;         
  l_aun_code       := NULL;         
  l_rac_start_date := NULL;         
  l_pro_refno      := NULL;         
  --            
  OPEN c_rac_accno(p1.lpme_pay_ref);        
  FETCH c_rac_accno into l_rac_accno, l_aun_code,   
         l_rac_start_date, l_pro_refno;     
  CLOSE c_rac_accno;    
  IF l_rac_accno IS NULL        
  THEN          
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);       
  END IF;       
  --            
  -- Check the Y/N column       
  --            
  -- First direct debit taken       
  --            
  IF ( p1.lpme_pme_first_dd_taken_ind NOT IN ('Y','N') )        
  THEN          
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',101);       
  END IF;       
  --            
  -- Check all the reference value columns          
  
  -- Payment method     
  --            
  l_pmy_code    := NULL;        
  l_pmy_reg_pay := NULL;        
  --            
  OPEN  c_pmy_dets(p1.lpme_pme_pmy_code);           
  FETCH c_pmy_dets into l_pmy_code,l_pmy_reg_pay;   
  CLOSE c_pmy_dets;     
  --            
  IF l_pmy_code IS NULL         
  THEN          
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',103);      
  END IF;       
  --            
  --            
  -- Check payment method start date is not before account start date           
  --            
  IF (p1.lpme_pme_start_date < l_rac_start_date)    
  THEN          
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',105);       
  END IF;       
  --            
  -- If the payment method is by a reg payment method, check            
  -- that a corresponding entry exists in PAYMENT_PROFILES
  --
    -- If the payment method is by a reg payment method, check
    -- that a corresponding entry exists in PAYMENT_PROFILES
    -- Amended to include the payment method start as we need
    -- to make sure the profile exists for the period of the method
    --    
  IF (l_pmy_reg_pay ='Y')       
  THEN          
    open  c_year(l_aun_code, p1.lpme_pme_start_date);      
    fetch c_year into l_year;       
    close c_year;       
--      
--    l_year := s_admin_years.get_aye_year(l_aun_code, TRUNC(SYSDATE));         
--      
    OPEN c_ppr(l_aun_code,      
       p1.lpme_pme_hrv_ppc_code,    
       l_year);         
    FETCH c_ppr INTO l_ppr;         
    CLOSE c_ppr;        
    IF l_ppr IS NULL    
    THEN        
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',104);     
    END IF;                      
    --          
    -- If a bank account number has been supplied, check that all the           
    -- other bank details have been supplied, otherwise check that no           
    -- bank details have been supplied
--
-- Move the other regular pay checks to before the bank details
-- as it's possible to supply a bank name and no branch name
-- depending on system build. Therefore need to open this cursor
-- first
--
-- *********************************************************************
--
-- OTHER REG PAY CHECKS  ADDED
--
-- Check the bank type is valid - if supplied
--
l_bty_code        := NULL;
l_branch_mand     := NULL;
--
           IF (p1.lpme_bde_bty_code IS NOT NULL) THEN
-- 
             OPEN c_bty_code(p1.lpme_bde_bty_code);
            FETCH c_bty_code into l_bty_code, l_branch_mand;
--
            IF (c_bty_code%NOTFOUND) THEN
--
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',866);
--
            END IF;
--
            CLOSE c_bty_code;
--
           END IF;
--          
    IF (p1.lpme_bad_account_no IS NOT NULL)         
    THEN
--
             IF l_branch_mand = 'MAN'
              THEN
               IF ((   p1.lpme_bad_account_name IS NULL 
                    OR p1.lpme_bad_sort_code    IS NULL 
                    OR p1.lpme_bde_branch_name  IS NULL 
                    OR p1.lpme_bde_bank_name    IS NULL 
                    OR p1.lpme_bde_bty_code     IS NULL )) THEN
--
                l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',109);
-- 
               ELSIF (     p1.lpme_bde_bank_name_mlang   IS NOT NULL
                    AND p1.lpme_bde_branch_name_mlang IS NULL    ) 
                THEN
--
                l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',109);
-- 
               END IF;
--
             ELSE
--
               IF ((   p1.lpme_bad_account_name IS NULL 
                    OR p1.lpme_bad_sort_code    IS NULL 
                    OR p1.lpme_bde_bank_name    IS NULL 
                    OR p1.lpme_bde_bty_code     IS NULL )) 
               THEN
                l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',109);
-- 
               ELSIF (     p1.lpme_bde_bank_name_mlang IS NULL
                    AND p1.lpme_bde_branch_name_mlang IS NOT NULL    ) 
               THEN
                l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',109);
-- 
               END IF;
--
             END IF;  /* l_branch_mand = 'MAN'  */
--    
    END IF;
    --
    IF (p1.lpme_bad_account_no IS NULL)
    THEN  
      IF (p1.lpme_bad_account_name   IS NOT NULL or
          p1.lpme_bad_sort_code      IS NOT NULL or         
          p1.lpme_bad_start_date     IS NOT NULL or         
          p1.lpme_bde_branch_name    IS NOT NULL or         
          p1.lpme_bde_bank_name      IS NOT NULL or    
          p1.lpme_bde_bty_code       IS NOT NULL or     
          p1.lpme_aun_bad_account_no IS NOT NULL or
          p1.lpme_bde_bank_name_mlang IS NOT NULL or
          p1.lpme_bde_branch_name_mlang IS NOT NULL)   
      THEN      
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',110);         
      END IF;           
    END IF;     
    --          
    -- OTHER REG PAY CHECKS  ADDED          
    --          
    --          
    -- Check the organisation bank account is valid         
    IF p1.lpme_aun_bad_account_no IS NOT NULL       
    THEN        
      l_exists := NULL;         
      OPEN c_aub_aun_code(p1.lpme_aun_bad_account_no        
         ,l_pro_refno);     
      FETCH c_aub_aun_code INTO l_exists;           
      CLOSE c_aub_aun_code;         
      IF l_exists IS NULL       
      THEN      
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',867);           
      END IF;           
    ELSE        
      l_exists := NULL;         
      OPEN c_null_aub_aun_code(l_pro_refno);        
      FETCH c_null_aub_aun_code INTO l_exists;      
      CLOSE c_null_aub_aun_code;            
      IF l_exists IS NULL       
      THEN      
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',867);           
      END IF;           
    END IF;     
    --          
    IF p1.lpme_bad_par_per_alt_ref IS NOT NULL      
    THEN         
      -- Check the par_per_alt_ref is valid (if supplied)       
      --        
      IF (nvl(p1.lpme_source_acc_ind,'P') = 'P' ) /* people */
      THEN      
        OPEN  c_par_refno(p1.lpme_bad_par_per_alt_ref);     
        FETCH c_par_refno into l_par_refno;         
        CLOSE c_par_refno;      
        --      
        IF (l_par_refno IS NULL)            
        THEN    
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);         
        END IF;         
      ELSE   /* organisation */        
        OPEN  c_org_refno(p1.lpme_bad_par_per_alt_ref);     
        FETCH c_org_refno into l_par_refno;         
        CLOSE c_org_refno;      
        --      
        IF (l_par_refno IS NULL)            
        THEN    
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);         
        END IF;         
      END IF;           
    END IF;     
  END IF;       
--
-- Validate the Payment Contract details if supplied
--
  IF (   p1.lpme_pct_amount IS NOT NULL           
      OR p1.lpme_pct_percentage IS NOT NULL )   
  THEN
--
-- Check that this contract doesn't conflict with an existing payment contract
-- If it matches exactly then that is fine
--
    l_pct_start := NULL;
    l_pct_end   := NULL;
--  
    OPEN  c_pct(l_par_refno, l_rac_accno, NVL(p1.lpme_pct_start_date,p1.lpme_pme_end_date)); 
    FETCH c_pct into l_pct_start, l_pct_end;          
    CLOSE c_pct;
--
    IF     l_pct_start < nvl(p1.lpme_pct_start_date,p1.lpme_pme_start_date)
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',679);           
    ELSIF  (    l_pct_end IS NOT NULL
            AND l_pct_end < NVL(p1.lpme_pct_end_date,l_pct_end))
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',679);           
    END IF;
  END IF;
--     
-- Now UPDATE the record count AND error code       
IF l_errors = 'F' THEN  
  l_error_ind := 'Y';   
ELSE            
  l_error_ind := 'N';   
END IF;         
--      
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
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);     
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');        
      set_record_status_flag(l_id,'O');
END;            
--      
END LOOP;       
--      
COMMIT;         
--      
fsc_utils.proc_END;     
--      
   EXCEPTION    
      WHEN OTHERS THEN  
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');             
--      
END dataload_validate;  
--      
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,       
           p_date    IN DATE) IS        
--      
CURSOR c1 is    
SELECT          
rowid rec_rowid         
,lpme_dlb_batch_id      
,lpme_dl_seqno          
,lpme_pay_ref           
,lpme_pme_pmy_code      
,lpme_pme_start_date    
,lpme_pme_end_date    
,lpme_pme_hrv_ppc_code  
,lpme_pme_first_dd_taken_ind        
,lpme_bde_bank_name     
,lpme_bad_account_no    
,lpme_bad_account_name  
,lpme_bad_sort_code     
,lpme_bde_branch_name   
,lpme_bad_start_date    
,lpme_aun_bad_account_no        
,lpme_source_acc_ind    
,lpme_bad_par_per_alt_ref       
,lpme_pct_amount        
,lpme_pct_percentage    
,lpme_pct_start_date
,lpme_pct_end_date
FROM dl_hra_payment_methods         
WHERE lpme_dlb_batch_id     = p_batch_id    
  AND lpme_dl_load_status   = 'C';          
--      
CURSOR c_rac_details (p_pay_ref VARCHAR2)           
IS      
SELECT rac_accno,rac_aun_code,rac_class_code,rac_par_refno      
FROM   revenue_accounts         
WHERE  rac_pay_ref = p_pay_ref;             
--      
CURSOR c_par_refno(l_par_alt_ref VARCHAR2)          
IS      
SELECT par_refno        
FROM   parties          
WHERE  par_per_alt_ref = l_par_alt_ref;     
--      
CURSOR c_org_refno(l_par_alt_ref VARCHAR2)          
IS      
SELECT par_refno        
FROM   parties          
WHERE  par_com_short_name = l_par_alt_ref           
OR     par_org_short_name = l_par_alt_ref;          
--      
CURSOR c_get_pme_refno (p_rac_accno NUMBER,p_pmy_code VARCHAR2,         
        p_start_date DATE)          
IS      
SELECT pme_refno        
FROM   payment_methods  
WHERE  pme_rac_accno   = p_rac_accno        
  AND   pme_pmy_code   = p_pmy_code         
  AND   pme_start_date = p_start_date;      
--      
CURSOR c_get_pba_details (l_par_refno NUMBER, l_pme_refno NUMBER)       
IS      
SELECT pbp_pba_bad_refno        
  FROM party_bank_acct_pymt_mthds           
 WHERE pbp_pba_par_refno = l_par_refno      
   AND pbp_pme_refno     = l_pme_refno;     
--      
CURSOR c_get_tenant (p_rac_accno NUMBER) is         
SELECT hop_par_refno    
FROM   household_persons, tenancy_instances, revenue_accounts           
WHERE  hop_refno = tin_hop_refno            
AND    tin_tcy_refno = rac_tcy_refno        
AND    rac_accno = p_rac_accno      
ORDER BY nvl(tin_end_date,sysdate) desc ,tin_start_date, tin_hop_refno;         
--    
cursor c_del_bad is
select bad_refno
FROM bank_account_details            
WHERE NOT EXISTS (SELECT NULL FROM party_bank_accounts      
          WHERE  pba_bad_refno = bad_refno);
--
cursor c_del_bad2 is
select bad_refno 
FROM bank_account_details            
WHERE NOT EXISTS (SELECT NULL FROM party_bank_accounts      
                  WHERE  pba_bad_refno = bad_refno)
AND   NOT EXISTS (SELECT NULL FROM party_bank_acct_pymt_mthds
                  WHERE pbp_pba_bad_refno = bad_refno)
AND   NOT EXISTS (SELECT NULL FROM bank_acct_pymt_mthd_types
                  WHERE bpt_bad_refno = bad_refno);
--
--
-- Constants for process_summary            
cb       VARCHAR2(30);  
cd       DATE;          
cp       VARCHAR2(30) := 'DELETE';          
ct       VARCHAR2(30) := 'DL_HRA_PAYMENT_METHODS';  
cs       INTEGER;       
ce       VARCHAR2(200);
l_id     ROWID;
--      
-- Other variables      
--      
l_rac_accno       revenue_accounts.rac_accno%TYPE;  
l_par_refno       parties.par_refno%TYPE;           
l_pme_refno       payment_methods.pme_refno%TYPE;   
l_bad_refno       bank_details.bde_refno%TYPE;      
l_rac_aun_code    revenue_accounts.rac_aun_code%TYPE;       
l_rac_start_date  DATE;         
l_class_code      revenue_accounts.rac_hrv_ate_code%TYPE;       
i                 INTEGER := 0;             
l_an_tab          VARCHAR2(1);      
l_count           INTEGER;
--      
BEGIN           
--      
fsc_utils.proc_start('s_dl_hra_revenue_accounts.dataload_delete');      
fsc_utils.debug_message( 's_dl_hra_revenue_accounts.dataload_delete',3 );       
--      
cb := p_batch_id;       
cd := p_date;           
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');      
--      
FOR p1 IN c1 LOOP       
--      
BEGIN           
--      
  cs := p1.lpme_dl_seqno;
  l_id := p1.rec_rowid;
  SAVEPOINT SP1;        
  i := i +1;    
--      
-- get the rac_accno    
 --             
  OPEN  c_rac_details(p1.lpme_pay_ref);     
  FETCH c_rac_details into l_rac_accno,l_rac_aun_code,      
  l_class_code,l_par_refno;     
  CLOSE c_rac_details;  
  --            
  -- Now need to get the Par refno          
  --            
    IF p1.lpme_bad_par_per_alt_ref IS NULL          
    THEN        
      -- is it a sundry party account - in which case use that party    
      IF l_par_refno IS NULL        
      THEN      
        OPEN c_get_tenant(l_rac_accno);     
        FETCH c_get_tenant into l_par_refno;        
        CLOSE c_get_tenant;         
       END IF;          
    ELSE        
      -- If a party has been supplied then...       
      -- is it a Person         
      IF (NVL(p1.lpme_source_acc_ind,'P') = 'P')    
      THEN      
        OPEN c_par_refno(p1.lpme_bad_par_per_alt_ref);      
        FETCH c_par_refno INTO l_par_refno;         
        CLOSE c_par_refno;      
      ELSE      
      --        
      -- or else it must be an organisation         
        OPEN c_org_refno(p1.lpme_bad_par_per_alt_ref);      
        FETCH c_org_refno INTO l_par_refno;         
        CLOSE c_org_refno;      
      END IF;           
    --          
    END IF; -- had the bad_par_per_alt_ref been supplied        
  --            
  -- Get the PME refno
  l_pme_refno := NULL;  
  OPEN c_get_pme_refno(l_rac_accno,p1.lpme_pme_pmy_code,        
       p1.lpme_pme_start_date);     
  FETCH c_get_pme_refno into l_pme_refno;           
  CLOSE c_get_pme_refno;        
  --            
  -- Now Get the pba details        
  l_bad_refno := NULL;  
  OPEN  c_get_pba_details(l_par_refno,l_pme_refno);         
  FETCH c_get_pba_details into l_bad_refno;         
  CLOSE c_get_pba_details;      
  -- 
  DELETE FROM party_bank_acct_pymt_mthds    
  WHERE  pbp_pba_par_refno  = l_par_refno   
    AND  pbp_pba_bad_refno  = l_bad_refno   
    AND  pbp_pme_refno      = l_pme_refno   
    AND  pbp_pba_start_date = p1.lpme_pme_start_date;           
  --            
  DELETE FROM payment_contracts             
  WHERE  pct_rac_accno  = l_rac_accno       
    AND  pct_par_refno  = l_par_refno       
    AND  pct_start_date = nvl(p1.lpme_pct_start_date,p1.lpme_pme_start_date)
    AND NOT EXISTS (SELECT NULL FROM payment_methods
                                WHERE pme_pct_refno  = pct_refno
                                  AND pme_refno     != l_pme_refno);   
  --            
  DELETE FROM payment_methods       
  WHERE  pme_refno = l_pme_refno;           
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
-- Now tidy up the bank account details and bank details tables         
--      
DELETE FROM party_bank_accounts             
WHERE  NOT EXISTS (SELECT NULL FROM party_bank_acct_pymt_mthds          
                   WHERE pbp_pba_par_refno  = pba_par_refno             
                   AND   pbp_pba_bad_refno  = pba_bad_refno             
                   AND   pbp_pba_start_date = pba_start_date);          
--      
FOR p_del_bad IN c_del_bad LOOP
DELETE
FROM party_bank_acct_pymt_mthds
WHERE  pbp_pba_bad_refno = p_del_bad.bad_refno;
--
END LOOP;
--
FOR p_del_bad2 IN c_del_bad2 LOOP
DELETE FROM bank_account_details            
WHERE bad_refno = p_del_bad2.bad_refno;
--
END LOOP;
--      
DELETE FROM bank_details        
WHERE NOT EXISTS (SELECT NULL FROM bank_account_details     
          WHERE  bad_bde_refno = bde_refno);        
--      
-- Section to anayze the table(s) populated by this dataload            
--      
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_METHODS');      
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PAYMENT_CONTRACTS');            
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCT_PYMT_MTHDS');           
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PARTY_BANK_ACCOUNTS');          
l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');     
l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_ACCOUNT_DETAILS');  
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PM_DESTINATION_ACCOUNTS');    
--      
fsc_utils.proc_end;     
COMMIT;         
--      
   EXCEPTION    
      WHEN OTHERS THEN  
      set_record_status_flag(l_id,'O');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');             
--      
END dataload_delete;    
--      
--      
END s_dl_hra_payment_methods;       
        
/

