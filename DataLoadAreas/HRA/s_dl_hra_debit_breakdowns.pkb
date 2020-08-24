CREATE OR REPLACE PACKAGE BODY s_dl_hra_debit_breakdowns
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER DB Ver WHO  WHEN      WHY
  --  1.0        MTR  27/11/00  Dataload
  --  1.1 5.1.4  PJD  07/03/02  Added ROLLBACK to insert
  --                            Corrected parameters passed into 
  --                            is_current_element  
  --  1.2 5.1.4  SJB  11/03/02  Corrected p_els (now uses sysdate+999)
  --  1.3 5.1.5  PJD  15/04/02  Corrected c_dbr to include pro_refno 
  --  2.0 5.2.0  PJD  19/08/02  Corrected parameters passed through to 'is_current_element'
  --  2.1 5.2.0  PJD  24/09/02  Don't create summary rents after an account has ended
  --  3.0 5.3.0  PJD  04/02/03  Default 'Created By' field to DATALOAD
  --  3.1 5.4.0  PJD  19/02/04  Amended cursor c_dbr in valaidate to include dbr_status
  --  5.0 5.5.0  PH   26/02/04  Amended Validate and Create processes to cater for 
  --                            dataloading SP elements. New field added.
  --  5.1 5.5.0  PH   15/03/04  Amended create and delete by including 
  --                            SP_SERVICE_USAGES for sp_elements.
  --  5.2 5.5.0  PH   23/03/04  Amended above change to cater for numeric elements
  --                            where the att_code would then be 'NUL'
  --  5.3 5.6.0  PH   18/11/04  Added extra inserts on create process for if the
  --                            account has ended. If this is the case we need a
  --                            C and W record in case the tenancy is re-instated.
  --  5.4 5.6.0  PH   03/12/04  Amended create process for SP_SERVICE_USAGES to
  --                            cater for SPA accounts.
  --  5.5 5.7.0  PH   18/02/05  Added final commits to Create process.
  --  5.6 5.7.0  PJD  18/04/05  Added cursor c_tho in validate and create procs
  --                            Also added back in validation regarding admin years
  --                            but only if the record is still current.
  --  5.5 5.7.0  PJD  14/08/05  Added in missing variable declarations
  --  5.6 5.8.0  PH   02/12/05  Removed previously added change on C and W records
  --                            as not used and causes problems with dbr_ar_i trigger.
  --  5.7 5.8.0  PJD  05/01/06  Validation on Admin Years (hdl044) altered to use a
  --                            local cursor to avoid Oracle error where admin year
  --                            does not exist.
  --  5.8 5.10.0 PH   05/09/06  Added Batch Question to enable the ending
  --                            of existing elements. Changes to Create, Validate
  --                            and Delete processes
  --  5.9 5.10.0 PH   23/10/06  New Validation on dbr_start_date, cannot be before
  --                            account start date.
  --  6.0 5.10.0 PH   26/04/07  Added check on Element Type as you cannot have
  --                            a rent element of type DATE.
  --  7.0 5.12.0 PH   21/08/07  Added validate on current elements, making sure
  --                            that they don't start before rent_elemrates
  --                            If they do, summary_rents does not get populated
  --                            correctly.
  --  7.1 5.12.0 PH   02/11/07  Amended update to existing dbr to allow for
  --                            null values on att_code.
  --  8.0 5.13.0 PH 06-FEB-2008 Now includes its own 
  --                            set_record_status_flag procedure.
  --  8.1 6.1.1  PH 22-JUL-2010 Amended c_get_par_refno cursor in create/delete,
  --                            no longer uses tenancy holdings as may not
  --                            be a REN account
  --  8.2 6.13  PJD 13-JUN-2017 Error check 124 now only valid if 
  --                            account is still open. 
  --  8.3 6.13  PAH 01-MAR-2018 Added the re-calculation of the summary rent if the 
  --                            batch is deleted.
  -- ***********************************************************************
  --
  --  
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hra_debit_breakdowns
  SET ldbr_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_debit_breakdowns');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
  --
  --  declare package variables AND constants


PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
rowid rec_rowid
,ldbr_dlb_batch_id
,ldbr_dl_seqno
,ldbr_pay_ref
,ldbr_pro_refno
,ldbr_ele_code
,ldbr_start_date
,ldbr_end_date
,ldbr_att_code
,ldbr_ele_value
,ldbr_vca_code
,ldbr_par_alt_ref
FROM dl_hra_debit_breakdowns
WHERE ldbr_dlb_batch_id    = p_batch_id
AND   ldbr_dl_load_status = 'V';
--
CURSOR c_account(p_batch_id VARCHAR2) is
SELECT min(aye_start_date) aye_start,
MIN(ldbr.ldbr_start_date) dbr_start,ldbr_pay_ref,rac_end_date
FROM admin_years aye,
revenue_Accounts rac,
dl_hra_debit_breakdowns ldbr
WHERE ldbr.ldbr_dlb_batch_id = p_batch_id
AND ldbr.ldbr_dl_load_status = 'S'
AND ldbr.ldbr_pay_ref        = rac.rac_pay_ref
AND rac.rac_aun_code         = aye.aye_aun_code
GROUP BY ldbr.ldbr_pay_ref,rac_end_date;
--
CURSOR c_ele(p_ele_code VARCHAR2) is
SELECT ele_type,ele_value_type
FROM elements
WHERE ele_code = p_ele_code;
--
CURSOR c_sp_ele(p_ele_code VARCHAR2,
                p_att_code VARCHAR2) is
SELECT 'X'
FROM   sp_services
WHERE  spsv_ele_code  = p_ele_code
AND    spsv_att_code  = nvl(p_att_code, 'NUL');
--
CURSOR c_par_refno(p_par_alt_ref VARCHAR2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_alt_ref;
--
CURSOR c_tho (p_pro_refno in NUMBER, p_tcy_refno in NUMBER,
              p_date      in DATE) is
SELECT tho_rac_accno, rowid tho_rowid
FROM   tenancy_holdings
WHERE  tho_pro_refno = p_pro_refno
  AND  tho_tcy_refno = p_tcy_refno
  AND  p_date BETWEEN tho_start_date AND nvl(tho_end_date,p_date +1);
--
CURSOR c_get_par_refno(p_rac_accno NUMBER) IS
SELECT hop_par_refno
FROM   revenue_accounts,
       tenancy_instances,
       household_persons
WHERE  rac_accno           = p_rac_accno
AND    rac_tcy_refno       = tin_tcy_refno
AND    tin_hop_refno       = hop_refno
and    tin_main_tenant_ind = 'Y';
--
CURSOR c_get_rac_par_refno (p_rac_accno NUMBER) IS
SELECT rac_par_refno
FROM   revenue_accounts
WHERE  rac_accno = p_rac_accno;
--
CURSOR c_get_class_code (p_rac_accno NUMBER) IS
SELECT rac_class_code
FROM   revenue_accounts
WHERE  rac_accno = p_rac_accno;
--
CURSOR c_spsu_refno is
SELECT spsu_refno_seq.nextval
FROM   dual;
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HRA_DEBIT_BREAKDOWNS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_rac_accno          NUMBER;
l_rac_accno2         NUMBER;
l_rac_start_date     DATE;
l_rer_rebateable_ind VARCHAR2(1);
i                    INTEGER := 0;
l_answer             VARCHAR2(1);
l_answer2            VARCHAR2(1);
l_pro_refno          NUMBER;
l_ele_type           VARCHAR2(2);
l_ele_value_type     VARCHAR2(1);
l_sre_start          DATE;
l_an_tab             VARCHAR2(1);
l_par_refno          parties.par_refno%TYPE;
l_exists             VARCHAR2(1);
l_spsu_refno         NUMBER;
l_rac_end_date       DATE;
l_class_code         VARCHAR2(10);
l_existing_rac_accno NUMBER(11);
l_tho_rowid          ROWID;
l_tcy_refno          tenancies.tcy_refno%TYPE;
--
BEGIN
--
fsc_utils.proc_start('s_hra_debit_breakdowns.dataload_create');
fsc_utils.debug_message( 's_hra_debit_breakdowns.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Create Element Question'
-- and 'End existing Charges'
--
l_answer  := s_dl_batches.get_answer(p_batch_id, 1);
l_answer2 := s_dl_batches.get_answer(p_batch_id, 2);
--
for p1 in c1(p_batch_id) loop
--
BEGIN
  --
  cs := p1.ldbr_dl_seqno;
  l_id := p1.rec_rowid;
  --
  SAVEPOINT SP1;
  --
    l_pro_refno := null;
  --
  IF (p1.ldbr_pro_refno != '~NCT~' )
  THEN
    l_pro_refno := s_properties.get_refno_for_propref(p1.ldbr_pro_refno);
  END IF;
  --
  -- Get Revenue Account number
   l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                  ( p1.ldbr_pay_ref );
  --
  -- Get Revenue Account start
  -- WHY?
  --   l_rac_start_date := s_revenue_accounts.get_rac_start_date( l_rac_accno );
  --
  -- Get Rebateable Ind
   l_rer_rebateable_ind := s_rent_elemrates.get_rebateable_ind
                   ( p1.ldbr_ele_code, p1.ldbr_start_date, p1.ldbr_att_code );
  --
  -- Check to see if a CL element type
  --
  OPEN c_ele(p1.ldbr_ele_code);
  FETCH c_ele into l_ele_type,l_ele_value_type;
  CLOSE c_ele;
  --
  -- Check to see if it is a SP Element
  --
  l_exists    := null;
  l_par_refno  := null;
  l_class_code := null;
  --
  OPEN c_sp_ele(p1.ldbr_ele_code, p1.ldbr_att_code);
   FETCH c_sp_ele into l_exists;
  CLOSE c_sp_ele;
  --
  -- If it is a SP Element then get the PAR_REFNO
  -- if its been supplied, if not use the main tenant 
  -- for TSA accounts or the rac_par_refno for SPA
  --
  IF l_exists is not null
   THEN
    --
    -- Has the Person Alt Ref been supplied? If so get it.
    --
     IF p1.ldbr_par_alt_ref is not null
      THEN 
       OPEN c_par_refno(p1.ldbr_par_alt_ref);
        FETCH c_par_refno into l_par_refno;
       CLOSE c_par_refno;
     ELSE
       --
       -- Check what type of account it is
       --
       OPEN c_get_class_code(l_rac_accno);
        FETCH c_get_class_code INTO l_class_code;
       CLOSE c_get_class_code;
  --
         IF l_class_code != 'SPA'
          THEN
           OPEN c_get_par_refno(l_rac_accno);
            FETCH c_get_par_refno into l_par_refno;
           CLOSE c_get_par_refno;
         ELSE
  --
           OPEN c_get_rac_par_refno(l_rac_accno);
            FETCH c_get_rac_par_refno INTO l_par_refno;
           CLOSE c_get_rac_par_refno;
         END IF;
      END IF;
  --
  -- So its a SP Element, we therefore need to
  -- insert a row into sp_service_usages
  --
  l_spsu_refno := null;
  --
  OPEN c_spsu_refno;
   FETCH c_spsu_refno into l_spsu_refno;
  CLOSE c_spsu_refno;
  --
  INSERT INTO SP_SERVICE_USAGES
  (
   spsu_refno
  ,spsu_status
  ,spsu_rac_accno
  ,spsu_par_refno
  ,spsu_spsv_ele_code
  ,spsu_spsv_att_code
  ,spsu_start_date
  ,spsu_end_date
  ,spsu_created_by
  ,spsu_created_date
  )
  VALUES
  (l_spsu_refno
  ,'A'
  ,l_rac_accno
  ,l_par_refno
  ,p1.ldbr_ele_code
  ,nvl(p1.ldbr_att_code, 'NUL')
  ,p1.ldbr_start_date
  ,p1.ldbr_end_date
  ,'DATALOAD'
  ,trunc(sysdate)
  );
  --
   END IF;
  --
  -- If the question to 'End the DBR's is set to 'Y' then end 
  -- them the day before.
  --
      IF l_answer2 = 'Y'
        THEN
         UPDATE debit_breakdowns
            SET dbr_end_date  = p1.ldbr_start_date-1
          WHERE dbr_rac_accno = l_rac_accno
            AND dbr_ele_code  = p1.ldbr_ele_code
            AND nvl(dbr_att_code, 'NUL')  = nvl(p1.ldbr_att_code, nvl(dbr_att_code, 'NUL'))
            AND dbr_status    = 'A'
            AND (p1.ldbr_start_date BETWEEN dbr_start_date AND NVL(dbr_end_date,sysdate)
            OR
            NVL(p1.ldbr_end_date,p1.ldbr_start_date) BETWEEN dbr_start_date 
                               AND NVL(dbr_END_date,p1.ldbr_start_date)
           );
      END IF;
  --
  INSERT INTO DEBIT_BREAKDOWNS
  (
  dbr_rac_accno
  ,dbr_pro_refno
  ,dbr_ele_code
  ,dbr_start_date
  ,dbr_end_date
  ,dbr_att_code
  ,dbr_ele_value
  ,dbr_vca_code
  ,dbr_status
  ,dbr_rebateable_ind
  ,dbr_created_by
  ,dbr_created_date
  ,dbr_dispute_ind
  ,dbr_capped_ind
  ,dbr_par_refno
  )
  VALUES
  (l_rac_accno
  ,l_pro_refno
  ,p1.ldbr_ele_code
  ,p1.ldbr_start_date
  ,p1.ldbr_end_date
  ,p1.ldbr_att_code
  ,p1.ldbr_ele_value
  ,p1.ldbr_vca_code
  ,'A'
  ,NVL(l_rer_rebateable_ind,'N')
  ,'DATALOAD'
  ,trunc(sysdate)
  ,'N'
  ,'N'
  ,l_par_refno);
  --
  -- New code added April 2005, does the tenancy holding record
  -- need updating.
  l_existing_rac_accno := NULL;
  l_tho_rowid := NULL;
  OPEN  c_tho (l_pro_refno,l_tcy_refno,p1.ldbr_start_date);
  FETCH c_tho INTO l_existing_rac_accno, l_tho_rowid;
  CLOSE c_tho;
  IF l_existing_rac_accno IS NULL
  THEN
    UPDATE tenancy_holdings
    SET    tho_rac_accno = l_rac_accno
    WHERE  rowid         = l_tho_rowid;
  END IF;
  --
  -- 02/12/05 - Removed this code as not needed.
  --
  -- New code added 18/11/04. If the account is ended then
  -- there also needs to be a 'C' and a 'W' record in case the
  -- tenancy gets reinstated. The C records starts the day after
  -- the end date and the application uses this record to create 
  -- an Active entry when reinstated. Same must apply for 
  -- sp_service_usages
  --
/*
  IF p1.ldbr_end_date is not null
   THEN
      l_rac_end_date := s_revenue_accounts.get_rac_end_date( l_rac_accno );
  --
       IF l_rac_end_date = p1.ldbr_end_date
  --
  --  Then we need to create the additional entries
  --  First the 'C' record starting day after and no end date
  -- 
  --
     THEN
       IF l_exists is not null
       THEN
        l_spsu_refno := null;
  --
         OPEN c_spsu_refno;
         FETCH c_spsu_refno into l_spsu_refno;
         CLOSE c_spsu_refno;
 --
           INSERT INTO SP_SERVICE_USAGES
              (
               spsu_refno
              ,spsu_status
              ,spsu_rac_accno
              ,spsu_par_refno
              ,spsu_spsv_ele_code
              ,spsu_spsv_att_code
              ,spsu_start_date
              ,spsu_end_date
              ,spsu_created_by
              ,spsu_created_date
              )
         VALUES
              (l_spsu_refno
              ,'C'
              ,l_rac_accno
              ,l_par_refno
              ,p1.ldbr_ele_code
              ,nvl(p1.ldbr_att_code, 'NUL')
              ,p1.ldbr_end_date+1
              ,null
              ,'DATALOAD'
              ,trunc(sysdate)
              );
  --
       END IF;    -- l_exists is not null
  --
      INSERT INTO DEBIT_BREAKDOWNS
       (
        dbr_rac_accno
        ,dbr_pro_refno
        ,dbr_ele_code
        ,dbr_start_date
        ,dbr_end_date
        ,dbr_att_code
        ,dbr_ele_value
        ,dbr_vca_code
        ,dbr_status
        ,dbr_rebateable_ind
        ,dbr_created_by
        ,dbr_created_date
        ,dbr_dispute_ind
        ,dbr_capped_ind
        ,dbr_par_refno
        )
        VALUES
        (l_rac_accno
        ,l_pro_refno
        ,p1.ldbr_ele_code
        ,p1.ldbr_end_date+1
        ,null
        ,p1.ldbr_att_code
        ,p1.ldbr_ele_value
        ,p1.ldbr_vca_code
        ,'C'
        ,NVL(l_rer_rebateable_ind,'N')
        ,'DATALOAD'
        ,trunc(sysdate)
        ,'N'
        ,'N'
        ,l_par_refno);
  --
  --  Now one with a status of 'W'
  --
      INSERT INTO DEBIT_BREAKDOWNS
       (
        dbr_rac_accno
        ,dbr_pro_refno
        ,dbr_ele_code
        ,dbr_start_date
        ,dbr_end_date
        ,dbr_att_code
        ,dbr_ele_value
        ,dbr_vca_code
        ,dbr_status
        ,dbr_rebateable_ind
        ,dbr_created_by
        ,dbr_created_date
        ,dbr_dispute_ind
        ,dbr_capped_ind
        ,dbr_par_refno
        )
        VALUES
        (l_rac_accno
        ,l_pro_refno
        ,p1.ldbr_ele_code
        ,p1.ldbr_start_date
        ,null
        ,p1.ldbr_att_code
        ,p1.ldbr_ele_value
        ,p1.ldbr_vca_code
        ,'W'
        ,NVL(l_rer_rebateable_ind,'N')
        ,'DATALOAD'
        ,trunc(sysdate)
        ,'N'
        ,'N'
        ,l_par_refno);
   END IF;                  -- l_rac_end_date = p1.ldbr_end_date
  --
  END IF;                   -- p1.ldbr_end_date is not null
  --
*/
  --
  -- If the property element create flag is set to 'Y' and the
  -- property element does not already exist, create the new property
  -- element
  --
  --
IF ( l_answer = 'Y'
  AND  l_ele_type != 'CL'
  AND  l_pro_refno IS NOT NULL
  AND NOT s_property_elements.is_current_element(
    l_pro_refno,
    p1.ldbr_ele_code,
    p1.ldbr_att_code,
    null,
    p1.ldbr_start_date,
    p1.ldbr_end_date) ) -- amended 19/08/02
  THEN
    INSERT INTO PROPERTY_ELEMENTS
    (PEL_PRO_REFNO
    ,PEL_ELE_CODE
    ,PEL_START_DATE
    ,PEL_ATT_CODE
    ,PEL_HRV_ELO_CODE
    ,PEL_FAT_CODE
    ,PEL_CREATED_BY
    ,PEL_CREATED_DATE
    ,PEL_NUMERIC_VALUE
    ,PEL_END_DATE
    ,PEL_SOURCE
    )
    VALUES
    ( l_pro_refno,
      p1.ldbr_ele_code,
      p1.ldbr_start_date,
      NVL(p1.ldbr_att_code,'NUL'),
      'PRO',
      'NUL',
      'DATALOAD',
      sysdate,
      p1.ldbr_ele_value,
      p1.ldbr_end_date,
      'LDBR');
  END IF;
  --
  -- keep a count of the rows processed and commit after every 1000
  --
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
--
-- Set the status = 'S' so that it can be picked up by the second loop
--
set_record_status_flag(l_id,'S');
--
 EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK to SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
 END;
END LOOP;
COMMIT;
--
-- now the loop to calculate the summary_rents
--
i := 0;
--
  FOR r_account IN c_account(p_batch_id) LOOP
  BEGIN
  --
  l_sre_start := r_account.dbr_start;
  IF r_account.aye_start > r_account.dbr_start
  THEN
    l_sre_start := r_account.aye_start;
  END IF;
  -- 
  -- Don't need to create a Summary Rent for periods after the account end date
     IF l_sre_start < nvl(r_account.rac_end_date,l_sre_start+1) 
     THEN 
  --
  -- Get Revenue Account number
       l_rac_accno2 := s_revenue_accounts2.get_rac_accno_from_pay_ref
                     ( r_account.ldbr_pay_ref );
  --
       s_sum_rent.p_create_summary_rent(l_sre_start, l_rac_accno2);
  --
     END IF;
  -- 
  -- keep a count of the rows processed and commit after every 1000
  --
i := i+1; IF MOD(i,100)=0 THEN COMMIT; END IF;
--
update dl_hra_debit_breakdowns
set    ldbr_dl_load_status = 'C'
WHERE  ldbr_dl_load_status = 'S'
  and  ldbr_pay_ref        = r_account.ldbr_pay_ref
  and  ldbr_dlb_batch_id   = cb;
--
 EXCEPTION
   WHEN OTHERS THEN
 NULL;
END;
END LOOP;
COMMIT;
--
fsc_utils.proc_end;
commit;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PROPERTY_ELEMENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DEBIT_BREAKDOWNS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUMMARY_RENTS');
--
   EXCEPTION
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
--
--
END dataload_create;
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ldbr_dlb_batch_id
,ldbr_dl_seqno
,ldbr_pay_ref
,ldbr_pro_refno
,ldbr_ele_code
,ldbr_start_date
,ldbr_end_date
,ldbr_att_code
,ldbr_ele_value
,ldbr_vca_code
,ldbr_par_alt_ref
 FROM dl_hra_debit_breakdowns
WHERE ldbr_dlb_batch_id      = p_batch_id
  AND   ldbr_dl_load_status       in ('L','F','O');
--
CURSOR c_dbr(
         c_rac_accno   NUMBER,
         c_pro_refno   VARCHAR2,
         c_ele_code    VARCHAR2,
         c_att_code    VARCHAR2,
         c_start_date  DATE) is
SELECT 'x'
  FROM debit_breakdowns
 WHERE dbr_rac_accno         = c_rac_accno
   AND nvl(dbr_pro_refno,-1) = nvl(c_pro_refno,-1)
   AND dbr_ele_code          = c_ele_code
   AND c_start_date between dbr_start_date
                    and NVL(dbr_end_date,c_start_date+1)
   AND dbr_status = 'A';
--
CURSOR c_get_rac(p_payref VARCHAR2) IS
SELECT rac_accno
,      rac_aun_code
,      rac_tcy_refno
,      rac_start_date
,      rac_end_date
  FROM revenue_accounts
 WHERE rac_pay_ref = p_payref;
--
--
CURSOR c_att(p_ele_code VARCHAR2, p_att_code VARCHAR2) IS
SELECT null
  FROM attributes
 WHERE att_ele_code = p_ele_code
   AND   att_code     = p_att_code;
--
--
CURSOR c_ele(p_ele_code VARCHAR2) is
SELECT ele_type,ele_value_type
FROM elements
WHERE ele_code = p_ele_code;
--
CURSOR c_pels(p_pro_refno  NUMBER, p_ele_code  VARCHAR2
             ,p_att_code VARCHAR2, p_ele_value VARCHAR2
             ,p_start_date   DATE, p_end_date      DATE) IS
SELECT  'x'
FROM   property_elements
WHERE  pel_pro_refno                       = p_pro_refno
AND    pel_ele_code                        = p_ele_code
AND    pel_att_code                        = NVL(p_att_code,'NUL')
AND    NVL(pel_numeric_value,-1234567)     = NVL(p_ele_value,-1234567)
AND    p_start_date                       >= pel_start_date
AND    p_start_date                       <= NVL(pel_end_date,p_start_date)
AND    NVL(p_end_date,sysdate+999)        <= NVL(pel_end_date,sysdate+999);
--
CURSOR c_par_refno(p_par_alt_ref VARCHAR2,
                   p_tcy_refno   NUMBER,
                   p_dbr_start   DATE)    IS
SELECT hop_par_refno
FROM   parties,
       household_persons,
       tenancy_instances
WHERE  par_per_alt_ref = p_par_alt_ref
AND    tin_tcy_refno   = p_tcy_refno
AND    par_refno       = hop_par_refno
AND    tin_hop_refno   = hop_refno
and    p_dbr_start    between tin_start_date
                       and nvl(tin_end_date, p_dbr_start);
--
CURSOR c_sp_ele(p_ele_code VARCHAR2,
                p_att_code VARCHAR2) is
SELECT 'X'
FROM   sp_services
WHERE  spsv_ele_code  = p_ele_code
AND    spsv_att_code  = nvl(p_att_code, 'NUL');
--
CURSOR c_tho (p_pro_refno in NUMBER, p_tcy_refno in NUMBER,
              p_date      in DATE) is
SELECT 'X'
FROM   tenancy_holdings
WHERE  tho_pro_refno = p_pro_refno
  AND  tho_tcy_refno = p_tcy_refno
  AND  p_date BETWEEN tho_start_date AND nvl(tho_end_date,p_date +1);
--
CURSOR c_val_admin_year (p_aun_code VARCHAR2, p_date DATE) IS
SELECT 'Y'
FROM   admin_years
WHERE  aye_aun_code = p_aun_code
  AND  p_date BETWEEN aye_start_date
                  AND aye_end_date
  AND  (aye_end_date < sysdate
        OR
        aye_active_ind = 'Y')
;
--
CURSOR c_rer_check (p_ele_code   varchar2,
                    p_att_code   varchar2) IS
SELECT min(rer_start_date)
FROM   rent_elemrates rer
WHERE  rer_ele_code           = p_ele_code
AND    nvl(rer_att_code,'^')  = nvl(p_att_code,'^')
AND    rer_status     = 'A';
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_DEBIT_BREAKDOWNS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_ele_type       VARCHAR2(2);
l_ele_value_type VARCHAR2(1);
--
-- Other Variables
--
l_rac_accno       revenue_accounts.rac_accno%TYPE;
l_rac_aun_code    admin_units.aun_code%TYPE;
l_tcy_refno       tenancies.tcy_refno%TYPE;
l_pro_refno       properties.pro_refno%TYPE;
l_dbr             VARCHAR2(1);
l_ety_attr_type   elements.ele_usage%TYPE;
l_ety_type        elements.ele_type%TYPE;
l_answer          VARCHAR2(1);
l_par_refno       parties.par_refno%TYPE;
l_answer2         VARCHAR2(1);
l_rac_start_date  revenue_accounts.rac_start_date%TYPE;
l_rac_end_date    revenue_accounts.rac_end_date%TYPE;
l_rer_start       rent_elemrates.rer_start_date%TYPE;
--
BEGIN
--
-- dbms_output.put_line('STARTING');
--
fsc_utils.proc_start('s_hra_debit_breakdowns.dataload_validate');
fsc_utils.debug_message( 's_hra_debit_breakdowns.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- check if a property element may be created FROM this batch
-- .. and if an existing element should be ended.
--
l_answer  := s_dl_batches.get_answer(p_batch_id, 1);
l_answer2 := s_dl_batches.get_answer(p_batch_id, 2);
--
FOR p1 IN c1 LOOP
--
BEGIN
--
  cs := p1.ldbr_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
--
-- Get pro refno if propref;
--
IF (p1.ldbr_pro_refno IS NOT NULL)
        THEN
        l_pro_refno := null;
        l_pro_refno := s_properties.get_refno_for_propref
                       (p1.ldbr_pro_refno);
END IF;
--
  --
  -- Check the payment reference exists on REVENUE_ACCOUNTS and, if
  -- so, check the debit breakdown does not already exist on
  -- DEBIT_BREAKDOWNS
  --
  l_rac_accno := null;
--
  OPEN c_get_rac(p1.ldbr_pay_ref);
  FETCH c_get_rac into l_rac_accno,l_rac_aun_code,l_tcy_refno,
                       l_rac_start_date,l_rac_end_date;
  CLOSE c_get_rac;
-- 
-- Only do theis check if the answer to the End 
-- Existing Elements question is N
--
  IF l_rac_accno IS NOT NULL THEN
     IF nvl(l_answer2, 'N') = 'N'
       THEN
    OPEN c_dbr(
      l_rac_accno,
      l_pro_refno,
      p1.ldbr_ele_code,
      p1.ldbr_att_code,
      p1.ldbr_start_date);
    FETCH c_dbr INTO l_dbr;
    IF c_dbr%NOTFOUND
    THEN
       NULL;
    ELSE
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',118);
    END IF;
    CLOSE c_dbr;
    END IF;
  ELSE
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
  END IF;
  --
  -- Check the end date is null or later than the start date
  --
  IF (p1.ldbr_end_date IS NOT NULL AND 
      p1.ldbr_end_date < p1.ldbr_start_date)
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',119);
  END IF;
  --
  -- Check that a valid admin year exists for the debit breakdown
  -- start date
  --
  -- the next check reinstated April 2005.. but only if the breakdown is unended.
  -- (following helpdesk call 239466 for Birmingham)
  -- Changed to use a local cursor rather than s_admin_years 
  --
  IF (l_rac_aun_code IS NOT NULL AND p1.ldbr_end_Date IS NULL)
  THEN
    l_exists := 'N';
    OPEN  c_val_admin_year(l_rac_aun_code, p1.ldbr_start_date);
    FETCH c_val_admin_year INTO l_exists;
    CLOSE c_val_admin_year;
    -- 
    IF nvl(l_exists,'N') != 'Y'
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',044);
    END IF;
  END IF;
  --
  -- Check the element code exists on ELEMENTS
  --
  l_ele_type       := null;
  l_ele_value_type := null;
  --
  OPEN  c_ele(p1.ldbr_ele_code);
  FETCH c_ele into l_ele_type, l_ele_value_type;
  CLOSE c_ele;
  --
  IF l_ele_type IS NOT NULL
  THEN
    --
    -- New code to fail Date Elements
    --
    IF (l_ele_value_type = 'D')
      THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',246);
    END IF;
    --
    -- If an attribute code has been supplied, check that the atty/ety
    -- code combination exists on ATTRIBUTE_TYPES
    IF (l_ele_value_type = 'C')
      THEN
    --
      OPEN c_att(p1.ldbr_ele_code, p1.ldbr_att_code);
      FETCH c_att into l_exists;
      IF c_att%notfound then
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',041);
      END IF;
      CLOSE c_att;
    END IF;
    --
    -- Get the attribute type for this element
    --
    l_ety_attr_type := s_elements.chk_attribute_usage(p1.ldbr_ele_code);
    --
    -- Check that an element value is supplied for a numeric element
    --
    IF (l_ety_attr_type = 'N' AND p1.ldbr_ele_value IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',035);
    END IF;
    --
    -- Check that an attribute code is supplied for a coded element
    --
    IF (l_ety_attr_type = 'C' AND p1.ldbr_att_code IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',033);
    END IF;
  ELSE
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',031);
  END IF;
  --
  -- If the element type is NOT 'CL'
  --
  l_ety_type := s_elements.get_element_type( p1.ldbr_ele_code );
  --
  IF (l_ety_type IS NOT NULL)
  THEN
    IF (l_ety_type != 'CL')
    THEN
  --
  -- Check property reference, if supplied, exists on PROPERTIES and
  -- is in a rents admin unit
  --
        IF l_pro_refno is null
          THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
        END IF;
        IF (NVL(l_rac_end_date,sysdate+1) > sysdate
            AND NOT s_properties.chk_prop_in_rents_au(l_pro_refno) )
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',124);
        END IF;
      --
      -- Check the property exists against the tenancy and the
      -- debit breakdown start and end dates fall within the tcy_holding
      -- start and end dates
      --
      -- IF s_tenancy_holdings.get_pro_propref(l_tcy_refno) IS NULL
      -- now replaced with this cursor
      --
      l_exists := NULL;
      OPEN  c_tho (l_pro_refno,l_tcy_refno,p1.ldbr_start_date);
      FETCH c_tho INTO l_exists;
      CLOSE c_tho;
      --
      IF l_exists IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',125);
      END IF;
      --
      -- Check the property element exists and if so, check the value or
      -- code matches the debit breakdown. If it does not exist, only
      -- record an error if the element create flag is 'N'
      --
      IF l_answer != 'Y' THEN
        OPEN c_pels(l_pro_refno,
                    p1.ldbr_ele_code,p1.ldbr_att_code,p1.ldbr_ele_value,
                    p1.ldbr_start_date,p1.ldbr_end_date);
        FETCH c_pels into l_exists;
        IF c_pels%notfound THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',127);
        END IF;
        CLOSE c_pels;
      END IF;
    END IF;
  END IF;
  --
  --
  -- Check that if a VAT Code has been supplied it is valid and has a
  -- current rate on VAT_RATES
  --
  IF (p1.ldbr_vca_code IS NOT NULL)
  THEN
    IF ( s_vat_rates.get_vat_rate(p1.ldbr_vca_code) ) IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',123);
    END IF;
  END IF;
  --
  -- If a Person Alt Ref has been supplied make sure they exist
  -- in the household for the account and dbr start
  --
  l_par_refno := null;
  --
    IF p1.ldbr_par_alt_ref is not null
     THEN
      OPEN  c_par_refno(p1.ldbr_par_alt_ref, l_tcy_refno, p1.ldbr_start_date);
      FETCH c_par_refno into l_par_refno;
      CLOSE c_par_refno;
  --
      IF (l_par_refno IS NULL)
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',592);
  --
  -- If the Person Alt Ref has been supplied make sure that the
  -- element is a SP element
  --
      ELSE
       OPEN c_sp_ele(p1.ldbr_ele_code, nvl(p1.ldbr_att_code, 'NUL'));
        FETCH c_sp_ele into l_exists;
         IF c_sp_ele%notfound 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',593);
         END IF;
       CLOSE c_sp_ele;
      END IF;
    END IF;
  --  
  -- Check the start date is not before the account start date
  --
  IF p1.ldbr_start_date < l_rac_start_date
   THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',220);
  END IF;
  --
  -- Check if current debit breakdown there is a valid rent_elemrate
  -- otherwise summary rents will be incorrect
  --
  IF nvl(p1.ldbr_end_date, sysdate+1) > trunc(sysdate)
   THEN
  --
   l_rer_start := NULL;
  --
    OPEN c_rer_check (p1.ldbr_ele_code, p1.ldbr_att_code);
     FETCH c_rer_check INTO l_rer_start;
    CLOSE c_rer_check;
  --
      IF p1.ldbr_start_date < nvl(l_rer_start, sysdate)
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',277);
      END IF;
  END IF;
  --
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
-- dbms_output.put_line('count = '||i);
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
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,ldbr_dlb_batch_id
,ldbr_dl_seqno
,ldbr_pay_ref
,ldbr_ele_code
,ldbr_att_code
,ldbr_start_date
,ldbr_pro_refno
,ldbr_par_alt_ref
FROM  dl_hra_debit_breakdowns
WHERE ldbr_dlb_batch_id   = p_batch_id
AND   ldbr_dl_load_status in ('C','S');
--
CURSOR c_sp_ele(p_ele_code VARCHAR2,
                p_att_code VARCHAR2) is
SELECT 'X'
FROM   sp_services
WHERE  spsv_ele_code  = p_ele_code
AND    spsv_att_code  = nvl(p_att_code, 'NUL');
--
CURSOR c_par_refno(p_par_alt_ref VARCHAR2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_alt_ref;
--
CURSOR c_get_par_refno(p_rac_accno NUMBER) IS
SELECT hop_par_refno
FROM   revenue_accounts,
       tenancy_instances,
       household_persons
WHERE  rac_accno           = p_rac_accno
AND    rac_tcy_refno       = tin_tcy_refno
AND    tin_hop_refno       = hop_refno
and    tin_main_tenant_ind = 'Y';
--
CURSOR c_account(p_batch_id VARCHAR2) is
SELECT min(aye_start_date) aye_start,
MIN(ldbr.ldbr_start_date) dbr_start,ldbr_pay_ref,rac_end_date
FROM admin_years aye,
revenue_Accounts rac,
dl_hra_debit_breakdowns ldbr
WHERE ldbr.ldbr_dlb_batch_id = p_batch_id
AND ldbr.ldbr_dl_load_status = 'V'
AND ldbr.ldbr_pay_ref        = rac.rac_pay_ref
AND rac.rac_aun_code         = aye.aye_aun_code
GROUP BY ldbr.ldbr_pay_ref,rac_end_date;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_DEBIT_BREAKDOWNS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_rac_start_date DATE;
l_rac_accno      NUMBER;
l_answer         VARCHAR2(1);
l_answer2        VARCHAR2(1);
i                INTEGER := 0;
l_pro_refno      NUMBER;
l_an_tab         VARCHAR2(1);
l_par_refno      parties.par_refno%TYPE;
l_exists         VARCHAR2(1);
l_sre_start          DATE;
l_rac_accno2         NUMBER;
--
BEGIN
--
fsc_utils.proc_start('s_hra_debit_breakdowns.dataload_delete');
fsc_utils.debug_message( 's_hra_debit_breakdowns.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
-- Get the answer to the 'Create Element Question'
-- and 'End existing Charges'
--
l_answer  := s_dl_batches.get_answer(p_batch_id, 1);
l_answer2 := s_dl_batches.get_answer(p_batch_id, 2);
--
FOR p1 IN c1 LOOP
--
BEGIN
cs := p1.ldbr_dl_seqno;
i := i +1;
l_id := p1.rec_rowid;
--
-- Get Revenue Account number
l_rac_accno := s_revenue_accounts2.get_rac_accno_from_pay_ref
                                           ( p1.ldbr_pay_ref );
 --
  DELETE FROM debit_breakdowns
   WHERE dbr_rac_accno   = l_rac_accno
     AND dbr_ele_code    = p1.ldbr_ele_code
     AND dbr_start_date  = p1.ldbr_start_date;
 --
 -- If the question to 'End the DBR's is set to 'Y' reinstate 
 -- the one we ended on create.
 --
      IF l_answer2 = 'Y'
        THEN
         UPDATE debit_breakdowns
            SET dbr_end_date  = null
          WHERE dbr_rac_accno = l_rac_accno
            AND dbr_ele_code  = p1.ldbr_ele_code
            AND dbr_att_code  = nvl(p1.ldbr_att_code, dbr_att_code)
            AND dbr_status    = 'A'
            AND dbr_end_date  = p1.ldbr_start_date-1;
 --
      END IF;
 --
  -- Check to see if it is a SP Element
  --
  l_exists    := null;
  l_par_refno := null;
  --
  OPEN c_sp_ele(p1.ldbr_ele_code, nvl(p1.ldbr_att_code, 'NUL'));
   FETCH c_sp_ele into l_exists;
  CLOSE c_sp_ele;
  --
  -- If it is a SP Element then get the PAR_REFNO
  -- if its been supplied, if not use the main tenant
  --
  IF l_exists is not null
   THEN
    --
    -- Has the Person Alt Ref been supplied? If so get it.
    --
     IF p1.ldbr_par_alt_ref is not null
      THEN 
       OPEN c_par_refno(p1.ldbr_par_alt_ref);
        FETCH c_par_refno into l_par_refno;
       CLOSE c_par_refno;
     ELSE
      OPEN c_get_par_refno(l_rac_accno);
       FETCH c_get_par_refno into l_par_refno;
      CLOSE c_get_par_refno;
     END IF;
  --
  -- So its a SP Element, we therefore need to
  -- delete from sp_service_usages
  --
   DELETE FROM sp_service_usages
   WHERE  spsu_rac_accno       = l_rac_accno
     AND  spsu_par_refno       = l_par_refno
     AND  spsu_spsv_ele_code   = p1.ldbr_ele_code
     AND  spsu_spsv_att_code   = nvl(p1.ldbr_att_code, 'NUL')
     AND  spsu_start_date      = p1.ldbr_start_date;
  --
  END IF;
--
IF NVL(l_answer,'N') = 'Y' THEN
--
   l_pro_refno := null;
  --
  IF (p1.ldbr_pro_refno != '~NCT~' )
  THEN
    l_pro_refno := s_properties.get_refno_for_propref
                                  (p1.ldbr_pro_refno);
--
    DELETE FROM property_elements
    WHERE  pel_pro_refno  = l_pro_refno
    AND    pel_ele_code   = p1.ldbr_ele_code
    AND    pel_start_date = p1.ldbr_start_date
    AND    pel_source     = 'LDBR';
--
  END IF;
--
END IF;
--
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
EXCEPTION
   WHEN OTHERS THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
COMMIT;
--
-- now the loop to calculate the summary_rents
--
i := 0;
--
FOR r_account IN c_account(p_batch_id) LOOP
  BEGIN
  --
  l_sre_start := r_account.dbr_start;
  IF r_account.aye_start > r_account.dbr_start
  THEN
    l_sre_start := r_account.aye_start;
  END IF;
  -- 
  -- Don't need to create a Summary Rent for periods after the account end date
     IF l_sre_start < nvl(r_account.rac_end_date,l_sre_start+1) 
     THEN 
  --
  -- Get Revenue Account number
       l_rac_accno2 := s_revenue_accounts2.get_rac_accno_from_pay_ref
                     ( r_account.ldbr_pay_ref );
  --
       s_sum_rent.p_create_summary_rent(l_sre_start, l_rac_accno2);
  --
     END IF;
  -- keep a count of the rows processed and commit after every 1000
  --
i := i+1; IF MOD(i,100)=0 THEN COMMIT; END IF;
 EXCEPTION
   WHEN OTHERS THEN
 NULL;
END;
END LOOP;
COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PROPERTY_ELEMENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DEBIT_BREAKDOWNS');
--
fsc_utils.proc_end;
commit;
--
   EXCEPTION
      WHEN OTHERS THEN
      set_record_status_flag(l_id,'C');
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_delete;
--
--
END s_dl_hra_debit_breakdowns;
/
