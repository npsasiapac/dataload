CREATE OR REPLACE PACKAGE BODY s_dl_mad_notepads
AS
-- ***********************************************************************
--  DESCRIPTION:

--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.16.     VS   17-OCT-2009  Initial Version
--      2.0  5.16.     VS   30-NOV-2009  Fix for Defect Id 2710. Subsidy
--                                       application reference not required
--                                       for SAS Subsidy Review notepads.
--      3.0  5.16.     VS   22-JUL-2010  Fix for Defect Id 5458. Created By
--                                       and Date not maintained. Trigger NOP_BR_IU
--                                       needs to be disabled/enabled. Also perform
--                                       replace on NOP_TEXT to replace chr(10) which
--                                       would have been performed by NOP_BR_IU trigger
--      4.0  5.16.     VS   26-JUL-2010  Fix for Defect Id 5560. DELETE process not
--                                       deleteing records.
--      5.0  6.11      AJ   05-MAR-2015  Amended Product Area from HEM to MAD new MAD Guide produced
--      5.1  6.11      PJD  02-JUN-2015  Removed code which enabled/disabled
--                                       triggers and replaced this with
--                                       usual method of post insert updates.
--      5.2  6.13      AJ   11-MAY-2016  LNOP_TYPE LNOP_LEGACY_REF and LNOP_TEXT nullable set to Y
--                                       as mandatory item check moved to PKB instead of on Load
--      5.3  6.13      AJ   12-MAY-2016  added Organisation Notepads using par_refno (PAR2) and
--                                       short name and organisation type combination (ORG) also
--                                       (PAR2) can be used for Parties and Companies
--      5.4  6.15      MJK  23-JUN-2017  Reformatted and land titles added
--      5.5  6.15      MJK  23-JUN-2017  Fix to allow LTL type
--      5.6  6.17      PN   09-NOV-2018  Fix to notepad timestamp functionality
-- ***********************************************************************
--
--  declare package variables AND constants
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_mad_notepads
    SET    lnop_dl_load_status = p_status
    WHERE  ROWID = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_mad_notepads');
    RAISE;
  END set_record_status_flag;
  --
  -- ************************************************************************************
  --
  PROCEDURE dataload_create
    (p_batch_id    IN VARCHAR2
    ,p_date        IN DATE
    )
  AS
  CURSOR c1
  IS
    SELECT ROWID rec_rowid
    ,      lnop_dlb_batch_id
    ,      lnop_dl_seqno
    ,      lnop_dl_load_status
    ,      lnop_type
    ,      lnop_legacy_ref
    ,      lnop_secondary_ref
    ,      lnop_secondary_date
    ,      NVL(lnop_created_by,'DATALOAD') lnop_created_by
    ,      NVL(lnop_created_date,SYSDATE)  lnop_created_date
    ,      NVL(lnop_current_ind,'Y')       lnop_current_ind
    ,      NVL(lnop_highlight_ind,'N')     lnop_highlight_ind
    ,      REPLACE(lnop_text,CHR(10),' ')  lnop_text
    ,      lnop_ntt_code
    ,      lnop_application_type
    ,      lnop_modified_by
    ,      lnop_modified_date
    FROM   dl_mad_notepads
    WHERE  lnop_dlb_batch_id   = p_batch_id
    AND    lnop_dl_load_status = 'V';
  CURSOR get_aun_refno(p_aun_code VARCHAR2)
  IS
    SELECT aun_reusable_refno
    FROM   admin_units
    WHERE  aun_code = p_aun_code;
  CURSOR get_app_refno(p_app_legacy_ref VARCHAR2)
  IS
    SELECT app_reusable_refno
    FROM   applications
    WHERE  app_legacy_ref = p_app_legacy_ref;
  CURSOR get_par_refno(p_par_per_alt_ref VARCHAR2)
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_per_alt_ref = p_par_per_alt_ref;
  CURSOR get_pro_refno(p_pro_propref VARCHAR2)
  IS
    SELECT pro_reusable_refno
    FROM   properties
    WHERE  pro_propref = p_pro_propref;
  CURSOR get_tcy_refno(p_tcy_alt_ref VARCHAR2)
  IS
    SELECT tcy_reusable_refno
    FROM   tenancies
    WHERE  tcy_alt_ref = p_tcy_alt_ref;
  CURSOR get_rac_refno(p_rac_pay_ref VARCHAR2)
  IS
    SELECT rac_reusable_refno
    FROM   revenue_accounts
    WHERE  rac_pay_ref = p_rac_pay_ref;
  CURSOR get_arr_refno
    (p_rac_pay_ref     VARCHAR2
    ,p_arr_ara_code    VARCHAR2
    ,p_arr_start_date  DATE
    )
  IS
    SELECT a.aca_reusable_refno
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  r.rac_pay_ref = p_rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    a.aca_ara_code = p_arr_ara_code
    AND    TRUNC(a.aca_created_date) = p_arr_start_date;
  CURSOR get_aca_refno
    (p_rac_pay_ref       VARCHAR2
    ,p_aca_ara_code      VARCHAR2
    ,p_aca_created_date  DATE
    )
  IS
    SELECT a.aca_reusable_refno
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  r.rac_pay_ref = p_rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    a.aca_ara_code = p_aca_ara_code
    AND    TRUNC(a.aca_created_date) = p_aca_created_date;
  CURSOR get_ins_refno(p_ins_legacy_ref VARCHAR2)
  IS
    SELECT ins_reusable_refno
    FROM   inspections
    WHERE  ins_legacy_refno = p_ins_legacy_ref;
  CURSOR get_wor_refno(p_wor_legacy_ref VARCHAR2)
  IS
    SELECT wor_reusable_refno
    FROM   works_orders
    WHERE  wor_legacy_ref = p_wor_legacy_ref;
  CURSOR get_srq_refno(p_srq_legacy_refno VARCHAR2)
  IS
    SELECT srq_reusable_refno
    FROM   service_requests
    WHERE  srq_legacy_refno = p_srq_legacy_refno;
  CURSOR get_ban_refno
    (p_ban_reference VARCHAR2)
  IS
    SELECT ban_reusable_refno
    FROM   business_actions
    WHERE  ban_reference = p_ban_reference;
  CURSOR get_peg_refno
    (p_peg_code VARCHAR2)
  IS
    SELECT peg_reusable_refno
    FROM   people_groups
    WHERE  peg_code = p_peg_code;
  CURSOR get_ppo_refno
    (p_ppo_pro_propref VARCHAR2
    ,p_ppo_room_number VARCHAR2
    )
  IS
    SELECT ppo.ppo_reusable_refno
    FROM   placement_property_rooms ppo
    ,      properties               pro
    WHERE pro.pro_propref = p_ppo_pro_propref
    AND   ppo.ppo_pro_refno = pro.pro_refno
    AND   ppo.ppo_room_number = p_ppo_room_number;
  CURSOR get_ref_refno
    (p_ref_alternate_ref VARCHAR2)
  IS
    SELECT ref_reusable_refno
    FROM   referrals
    WHERE  ref_alternate_ref = p_ref_alternate_ref;
  CURSOR get_suap_refno
    (p_suap_reference VARCHAR2)
  IS
    SELECT suap_reusable_refno
    FROM   subsidy_applications
    WHERE  suap_reference = p_suap_reference;
  CURSOR get_surv_refno
    (p_surv_refno          VARCHAR2
    ,p_surv_suap_reference VARCHAR2
    )
  IS
    SELECT surv_reusable_refno
    FROM   subsidy_reviews
    WHERE  surv_refno = p_surv_refno
    AND    NVL(surv_suap_reference,9999999) = NVL(p_surv_suap_reference, 9999999);
  CURSOR c_get_created_date
    (p_resusable_refno INTEGER
    ,p_nop_type VARCHAR2
    ,p_date DATE
    ) 
  IS
    SELECT MAX(nop_created_date) + (1/24/60/60)
    FROM   notepads
    WHERE  nop_reusable_refno = p_resusable_refno
    AND    nop_type = p_nop_type
    AND    nop_created_date = p_date;
  CURSOR get_par2_refno
    (p_par_refno VARCHAR2)
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_refno = p_par_refno;
  CURSOR get_org_refno
    (p_org_short_name   VARCHAR2
    ,p_org_frv_oty_code VARCHAR2
    ) 
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_org_short_name = p_org_short_name
    AND    par_org_frv_oty_code = p_org_frv_oty_code;
  CURSOR get_ltl_refno(p_ltl_legacy_ref VARCHAR2)
  IS
    SELECT ltl_reusable_refno
    FROM   land_titles
    WHERE  ltl_reference = p_ltl_legacy_ref;
  cb                      VARCHAR2(30);
  cd                      DATE;
  cp                      VARCHAR2(30) := 'CREATE';
  ct                      VARCHAR2(30) := 'DL_MAD_NOTEPADS';
  cs                      INTEGER;
  ce                      VARCHAR2(200);
  l_id                    ROWID;
  l_an_tab                VARCHAR2(1);
  l_reusable_refno        INTEGER;
  l_created_date          DATE;
  i                       INTEGER := 0;
  l_nop_type              VARCHAR2(5);
  BEGIN
    fsc_utils.proc_start('s_dl_mad_notepads.dataload_create');
    fsc_utils.debug_message('s_dl_mad_notepads.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1 
    LOOP
      BEGIN
        cs := p1.lnop_dl_seqno;
        l_id := p1.rec_rowid;
        l_nop_type := NULL;
        SAVEPOINT SP1;
        --
        -- get the reusable_refno
        --
        l_reusable_refno := NULL;
        IF p1.lnop_type = 'AUN' 
        THEN
          OPEN get_aun_refno(p1.lnop_legacy_ref);
          FETCH get_aun_refno INTO l_reusable_refno;
          CLOSE get_aun_refno;
        ELSIF p1.lnop_type = 'APP' 
        THEN
          OPEN get_app_refno(p1.lnop_legacy_ref);
          FETCH get_app_refno INTO l_reusable_refno;
          CLOSE get_app_refno;
        ELSIF p1.lnop_type = 'PAR' 
        THEN
          OPEN get_par_refno(p1.lnop_legacy_ref);
          FETCH get_par_refno INTO l_reusable_refno;
          CLOSE get_par_refno;
        ELSIF p1.lnop_type = 'PRO' 
        THEN
          OPEN get_pro_refno(p1.lnop_legacy_ref);
          FETCH get_pro_refno INTO l_reusable_refno;
          CLOSE get_pro_refno;
        ELSIF p1.lnop_type = 'TCY' 
        THEN
          OPEN get_tcy_refno(p1.lnop_legacy_ref);
          FETCH get_tcy_refno INTO l_reusable_refno;
          CLOSE get_tcy_refno;
        ELSIF p1.lnop_type = 'RAC' 
        THEN
          OPEN get_rac_refno(p1.lnop_legacy_ref);
          FETCH get_rac_refno INTO l_reusable_refno;
          CLOSE get_rac_refno;
        ELSIF p1.lnop_type = 'ARR' 
        THEN
          OPEN get_arr_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref, p1.lnop_secondary_date);
          FETCH get_arr_refno INTO l_reusable_refno;
          CLOSE get_arr_refno;
        ELSIF p1.lnop_type = 'ACA' 
        THEN
          OPEN get_aca_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref, p1.lnop_secondary_date);
          FETCH get_aca_refno INTO l_reusable_refno;
          CLOSE get_aca_refno;
        ELSIF p1.lnop_type = 'INS'
        THEN
          OPEN get_ins_refno(p1.lnop_legacy_ref);
          FETCH get_ins_refno INTO l_reusable_refno;
          CLOSE get_ins_refno;
        ELSIF p1.lnop_type = 'WOR' 
        THEN
          OPEN get_wor_refno(p1.lnop_legacy_ref);
          FETCH get_wor_refno INTO l_reusable_refno;
          CLOSE get_wor_refno;
        ELSIF p1.lnop_type = 'SRQ'
        THEN
          OPEN get_srq_refno(p1.lnop_legacy_ref);
          FETCH get_srq_refno INTO l_reusable_refno;
          CLOSE get_srq_refno;
        ELSIF p1.lnop_type = 'BAN'
        THEN
          OPEN get_ban_refno(p1.lnop_legacy_ref);
          FETCH get_ban_refno INTO l_reusable_refno;
          CLOSE get_ban_refno;
        ELSIF p1.lnop_type = 'PEG' 
        THEN
          OPEN get_peg_refno(p1.lnop_legacy_ref);
          FETCH get_peg_refno INTO l_reusable_refno;
          CLOSE get_peg_refno;
        ELSIF p1.lnop_type = 'PPO' 
        THEN
          OPEN get_ppo_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref);
          FETCH get_ppo_refno INTO l_reusable_refno;
          CLOSE get_ppo_refno;
        ELSIF p1.lnop_type = 'REF' 
        THEN
          OPEN get_ref_refno(p1.lnop_legacy_ref);
          FETCH get_ref_refno INTO l_reusable_refno;
          CLOSE get_ref_refno;
        ELSIF p1.lnop_type = 'SUAP' 
        THEN
          OPEN get_suap_refno(p1.lnop_legacy_ref);
          FETCH get_suap_refno INTO l_reusable_refno;
          CLOSE get_suap_refno;
        ELSIF p1.lnop_type = 'SURV' 
        THEN
          OPEN get_surv_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref);
          FETCH get_surv_refno INTO l_reusable_refno;
          CLOSE get_surv_refno;
        ELSIF p1.lnop_type = 'PAR2' 
        THEN
          OPEN get_par2_refno(p1.lnop_legacy_ref);
          FETCH get_par2_refno INTO l_reusable_refno;
          CLOSE get_par2_refno;
        ELSIF p1.lnop_type = 'ORG'
        THEN
          OPEN get_org_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref);
          FETCH get_org_refno INTO l_reusable_refno;
          CLOSE get_org_refno;
        ELSIF p1.lnop_type = 'LTL' 
        THEN
          OPEN get_ltl_refno(p1.lnop_legacy_ref);
          FETCH get_ltl_refno INTO l_reusable_refno;
          CLOSE get_ltl_refno;
        END IF;     
        l_nop_type := NULL;      
        IF p1.lnop_type IN ('PAR2','ORG') 
        THEN
          l_nop_type := 'PAR';
        ELSE
          l_nop_type := p1.lnop_type;
        END IF;      
        l_created_date := NULL;
        OPEN c_get_created_date(l_reusable_refno,p1.lnop_type,NVL(p1.lnop_created_date,TRUNC(SYSDATE)));
        FETCH c_get_created_date INTO l_created_date;
        CLOSE c_get_created_date;      
        IF l_created_date IS NULL 
        THEN
          l_created_date := NVL(p1.lnop_created_date, TRUNC(sysdate));
        END IF;
        INSERT INTO notepads
        (nop_reusable_refno 
        ,nop_type 
        ,nop_created_date 
        ,nop_created_by 
        ,nop_current_ind 
        ,nop_highlight_ind 
        ,nop_text 
        ,nop_ntt_code 
        ,nop_application_type
        )
        VALUES
        (l_reusable_refno 
        ,l_nop_type 
        ,l_created_date 
        ,NVL(p1.lnop_created_by,'DATALOAD') 
        ,p1.lnop_current_ind 
        ,p1.lnop_highlight_ind 
        ,p1.lnop_text 
        ,p1.lnop_ntt_code 
        ,p1.lnop_application_type
        );
        UPDATE notepads
        SET    nop_created_by = NVL(p1.lnop_created_by,'DATALOAD')
        WHERE  nop_type = p1.lnop_type
        AND    nop_reusable_refno = l_reusable_refno
        AND    nop_created_date = l_created_date;      
        -- Set Record Status Flag and Process count
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        s_dl_utils.set_record_status_flag(ct,cb,cs,'C');      
        -- keep a count of the rows processed AND COMMIT after every 5000      
        i := i + 1;
        IF MOD(i,5000) = 0 
        THEN
          COMMIT;
        END IF;      
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;    
    END LOOP;
    COMMIT;
    --
    -- Section to analyze the table(s) populated by this dataload
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('NOTEPADS');
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_create;
  --
  -- ************************************************************************************
  --
  PROCEDURE dataload_validate
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )
  AS
  CURSOR c1
  IS
    SELECT ROWID rec_rowid
    ,      lnop_dlb_batch_id
    ,      lnop_dl_seqno
    ,      lnop_dl_load_status
    ,      lnop_type
    ,      lnop_legacy_ref
    ,      lnop_secondary_ref
    ,      lnop_secondary_date
    ,      NVL(lnop_created_by,'DATALOAD')  lnop_created_by
    ,      NVL(lnop_created_date,SYSDATE)   lnop_created_date
    ,      NVL(lnop_current_ind,'Y')        lnop_current_ind
    ,      NVL(lnop_highlight_ind,'N')      lnop_highlight_ind
    ,      REPLACE(lnop_text,chr(10),' ')   lnop_text
    ,      lnop_ntt_code
    ,      lnop_application_type
    ,      lnop_modified_by
    ,      lnop_modified_date
    FROM dl_mad_notepads
    WHERE lnop_dlb_batch_id = p_batch_id
    AND lnop_dl_load_status IN ('L','F','O');
  CURSOR chk_aun_code(p_aun_code  VARCHAR2)
  IS
    SELECT 'X'
    FROM   admin_units
    WHERE  aun_code = p_aun_code;
  CURSOR chk_app_ref(p_app_ref VARCHAR2)
  IS
    SELECT 'X'
    FROM   applications
    WHERE  app_legacy_ref = p_app_ref;
  CURSOR chk_par_per_alt_ref(p_par_alt_ref VARCHAR2)
  IS
    SELECT 'X'
    FROM   parties
    WHERE  par_per_alt_ref = p_par_alt_ref;
  CURSOR chk_pro_refno(p_pro_propref VARCHAR2)
  IS
    SELECT 'X'
    FROM   properties
    WHERE  pro_propref = p_pro_propref;
  CURSOR chk_tcy_refno(p_tcy_ref VARCHAR2)
  IS
    SELECT 'X'
    FROM   tenancies
    WHERE  tcy_alt_ref = p_tcy_ref;
  CURSOR chk_rac_accno(p_pay_ref VARCHAR2)
  IS
    SELECT 'X'
    FROM   revenue_accounts
    WHERE  rac_pay_ref = p_pay_ref;
  CURSOR chk_arr_refno
    (p_rac_pay_ref      VARCHAR2
    ,p_arr_ara_code     VARCHAR2
    ,p_arr_start_date   DATE
    )
  IS
    SELECT 'X'
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  r.rac_pay_ref = p_rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    a.aca_ara_code = p_arr_ara_code
    AND    TRUNC(a.aca_created_date) = p_arr_start_date;
  CURSOR chk_aca_refno
    (p_rac_pay_ref              VARCHAR2
    ,p_aca_ara_code             VARCHAR2
    ,p_aca_created_date         DATE
    )
  IS
    SELECT 'X'
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  r.rac_pay_ref = p_rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    a.aca_ara_code = p_aca_ara_code
    AND    TRUNC(a.aca_created_date) = p_aca_created_date;
  CURSOR chk_ins_refno(p_ins_legacy_ref VARCHAR2)
  IS
    SELECT 'X'
    FROM   inspections
    WHERE  ins_legacy_refno = p_ins_legacy_ref;
  CURSOR chk_wor_refno(p_legacy_ref VARCHAR2)
  IS
    SELECT 'X'
    FROM works_orders
     WHERE wor_legacy_ref = p_legacy_ref;
  CURSOR chk_srq_refno(p_srq_legacy_refno VARCHAR2)
  IS
    SELECT 'X'
    FROM service_requests
    WHERE srq_legacy_refno = p_srq_legacy_refno;
  CURSOR chk_ban_refno(p_ban_reference VARCHAR2)
  IS
    SELECT 'X'
    FROM business_actions
    WHERE ban_reference = p_ban_reference;
  CURSOR chk_peg_refno(p_peg_code VARCHAR2)
  IS
    SELECT 'X'
    FROM people_groups
    WHERE peg_code = p_peg_code;
  CURSOR chk_ppo_refno
    (p_ppo_pro_propref VARCHAR2
    ,p_ppo_room_number VARCHAR2
    )
  IS
    SELECT 'X'
    FROM   placement_property_rooms ppo
    ,      properties               pro
    WHERE  pro.pro_propref     = p_ppo_pro_propref
    AND    ppo.ppo_pro_refno   = pro.pro_refno
    AND    ppo.ppo_room_number = p_ppo_room_number;
  CURSOR chk_ref_refno(p_ref_alternate_ref VARCHAR2)
  IS
    SELECT 'X'
    FROM   referrals
    WHERE  ref_alternate_ref = p_ref_alternate_ref;
  CURSOR chk_suap_refno(p_suap_reference VARCHAR2)
  IS
    SELECT 'X'
    FROM   subsidy_applications
    WHERE  suap_reference = p_suap_reference;
  CURSOR chk_surv_refno
    (p_surv_refno          VARCHAR2
    ,p_surv_suap_reference VARCHAR2
    )
  IS
    SELECT 'X'
    FROM   subsidy_reviews
    WHERE  surv_refno = p_surv_refno
    AND    NVL(surv_suap_reference,9999999) = NVL(p_surv_suap_reference, 9999999);
  CURSOR chk_ntt_code(p_ntt_code VARCHAR2)
  IS
    SELECT 'X'
    FROM   note_types
    WHERE  ntt_code = p_ntt_code;
  CURSOR chk_par_refno(p_par_refno VARCHAR2)
  IS
    SELECT 'X'
    FROM   parties
    WHERE  par_refno = p_par_refno;
  CURSOR chk_org_refno
    (p_org_short_name   VARCHAR2
    ,p_org_frv_oty_code VARCHAR2
    ) 
  IS
    SELECT 'X'
    FROM   parties
    WHERE  par_org_short_name = p_org_short_name
    AND    par_org_frv_oty_code = p_org_frv_oty_code;
  CURSOR chk_org_count
    (p_org_short_name   VARCHAR2
    ,p_org_frv_oty_code VARCHAR2
    ) 
  IS
    SELECT count(*)
    FROM   parties
    WHERE  par_org_short_name = p_org_short_name
    AND    par_org_frv_oty_code = p_org_frv_oty_code;
  CURSOR chk_ltl_ref(p_ltl_legacy_ref VARCHAR2)
  IS
    SELECT 'X'
    FROM   land_titles
    WHERE  ltl_reference = p_ltl_legacy_ref;
  cb              VARCHAR2(30);
  cd              DATE;
  cp              VARCHAR2(30) := 'VALIDATE';
  ct              VARCHAR2(30) := 'DL_MAD_NOTEPADS';
  cs              INTEGER;
  ce              VARCHAR2(200);
  l_id            ROWID;
  l_exists        VARCHAR2(1);
  l_pro_refno     NUMBER(10);
  l_errors        VARCHAR2(10);
  l_error_ind     VARCHAR2(10);
  l_ntt_exists    VARCHAR2(1);
  l_org_count     INTEGER;
  i               INTEGER :=0;
  BEGIN
    fsc_utils.proc_start   ('s_dl_mad_notepads.dataload_validate');
    fsc_utils.debug_message('s_dl_mad_notepads.dataload_validate',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.lnop_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        l_exists := NULL;
        -- ************************************************************************************
        -- Check the lnop_text has been supplied
        -- ************************************************************************************
        IF p1.lnop_text IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',894);
        END IF;
        -- ************************************************************************************
        -- Check the notepad type has been supplied and is valid
        -- ************************************************************************************
        IF p1.lnop_type IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',182);
        END IF;
        IF p1.lnop_type NOT IN ('AUN','APP','PAR','PAR2','PRO','TCY','RAC','ARR','ACA','INS','WOR','SRQ','BAN','PEG','PPO','REF','SUAP','SURV','ORG','LTL') 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',920);
        END IF;
        -- ************************************************************************************
        -- Check the lnop_legacy_ref has been supplied
        -- ************************************************************************************
        IF p1.lnop_legacy_ref IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',510);
        END IF;
        -- ************************************************************************************
        -- IF aun code supplied does it exist on admin units
        -- ************************************************************************************
        IF p1.lnop_type = 'AUN'
        THEN
          OPEN chk_aun_code(p1.lnop_legacy_ref);
          FETCH chk_aun_code INTO l_exists;
          CLOSE chk_aun_code;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',725);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF app_refno supplied does it exist on applications
        -- ************************************************************************************
        IF p1.lnop_type = 'APP'
        THEN
          OPEN chk_app_ref(p1.lnop_legacy_ref);
          FETCH chk_app_ref INTO l_exists;
          CLOSE chk_app_ref;
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',216);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF a party alt reference supplied does it exist on parties
        -- ************************************************************************************
        IF p1.lnop_type = 'PAR'
        THEN
          OPEN chk_par_per_alt_ref(p1.lnop_legacy_ref);
          FETCH chk_par_per_alt_ref INTO l_exists;
          CLOSE chk_par_per_alt_ref;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF pro_propref supplied does it exist on properties
        -- ************************************************************************************
        IF p1.lnop_type = 'PRO'
        THEN
          OPEN chk_pro_refno(p1.lnop_legacy_ref);
          FETCH chk_pro_refno INTO l_exists;
          CLOSE chk_pro_refno;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF tcy alt ref supplied does it exist on tenancies
        -- ************************************************************************************
        IF p1.lnop_type = 'TCY'
        THEN
          OPEN chk_tcy_refno(p1.lnop_legacy_ref);
          FETCH chk_tcy_refno INTO l_exists;
          CLOSE chk_tcy_refno;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',080);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF pay_ref supplied does it exist on revenue accounts
        -- ************************************************************************************
        IF p1.lnop_type = 'RAC' 
        THEN
          OPEN chk_rac_accno(p1.lnop_legacy_ref);
          FETCH chk_rac_accno INTO l_exists;
          CLOSE chk_rac_accno;
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',117);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF Account Arrears Arrangements supplied does it exist on Account Arrears Arrangements
        -- ************************************************************************************
        IF p1.lnop_type = 'ARR'
        THEN
          OPEN chk_arr_refno(p1.lnop_legacy_ref,p1.lnop_secondary_ref,p1.lnop_secondary_date);
          FETCH chk_arr_refno into l_exists;
          CLOSE chk_arr_refno;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',645);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF Account Arrears Actions supplied does it exist on Account Arrears Actions
        -- ************************************************************************************
        IF p1.lnop_type = 'ACA'
        THEN
          OPEN chk_aca_refno(p1.lnop_legacy_ref,p1.lnop_secondary_ref,p1.lnop_secondary_date);
          FETCH chk_aca_refno into l_exists;
          CLOSE chk_aca_refno;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',941);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF ins_legacy_ref supplied does it exist on inspections
        -- ************************************************************************************
        IF p1.lnop_type = 'INS'
        THEN
          OPEN chk_ins_refno(p1.lnop_legacy_ref);
          FETCH chk_ins_refno INTO l_exists;
          CLOSE chk_ins_refno;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',646);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF wor_legacy_ref supplied does it exist on works orders
        -- ************************************************************************************
        IF p1.lnop_type = 'WOR'
        THEN
          OPEN chk_wor_refno(p1.lnop_legacy_ref);
          FETCH chk_wor_refno INTO l_exists;
          CLOSE chk_wor_refno;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',938);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF srq_legacy_refno supplied does it exist on service request
        -- ************************************************************************************
        IF p1.lnop_type = 'SRQ'
        THEN
          OPEN chk_srq_refno(p1.lnop_legacy_ref);
          FETCH chk_srq_refno into l_exists;
          CLOSE chk_srq_refno;
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',693);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF ban_reference supplied does it exist on business Actions
        -- ************************************************************************************
        IF p1.lnop_type = 'BAN' 
        THEN
          OPEN chk_ban_refno(p1.lnop_legacy_ref);
          FETCH chk_ban_refno into l_exists;
          CLOSE chk_ban_refno;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',571);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF peg_code supplied does it exist on people groups
        -- ************************************************************************************
        IF p1.lnop_type = 'PEG' 
        THEN
          OPEN chk_peg_refno(p1.lnop_legacy_ref);
          FETCH chk_peg_refno into l_exists;
          CLOSE chk_peg_refno;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',647);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF placement property rooms exist
        -- ************************************************************************************
        IF p1.lnop_type = 'PPO'
        THEN
          OPEN chk_ppo_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref);
          FETCH chk_ppo_refno into l_exists;
          CLOSE chk_ppo_refno;
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',648);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF ref_alternate_ref supplied does it exist on referrals
        -- ************************************************************************************
        IF p1.lnop_type = 'REF'
        THEN
          OPEN chk_ref_refno(p1.lnop_legacy_ref);
          FETCH chk_ref_refno into l_exists;
          CLOSE chk_ref_refno;
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',649);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF suap_reference supplied does it exist on subsidy applications
        -- ************************************************************************************
        IF p1.lnop_type = 'SUAP'
        THEN
          OPEN chk_suap_refno(p1.lnop_legacy_ref);
          FETCH chk_suap_refno into l_exists;
          CLOSE chk_suap_refno; 
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',032);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF subsidy_reviews supplied does it exist on subsidy reviews
        -- ************************************************************************************
        IF p1.lnop_type = 'SURV'
        THEN
          OPEN chk_surv_refno(p1.lnop_legacy_ref,p1.lnop_secondary_ref);
          FETCH chk_surv_refno into l_exists;
          CLOSE chk_surv_refno;
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',033);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF Parties2 (par_refno) supplied does it exist on parties
        -- ************************************************************************************
        IF p1.lnop_type = 'PAR2'
        THEN
          OPEN chk_par_refno(p1.lnop_legacy_ref);
          FETCH chk_par_refno into l_exists;
          CLOSE chk_par_refno;
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF Organisation supplied does it exist on parties
        -- ************************************************************************************
        l_org_count := NULL;
        IF p1.lnop_type = 'ORG'
        THEN
          OPEN chk_org_refno(p1.lnop_legacy_ref,p1.lnop_secondary_ref);
          FETCH chk_org_refno into l_exists;
          CLOSE chk_org_refno;
          IF l_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);
          END IF;
          OPEN chk_org_count(p1.lnop_legacy_ref,p1.lnop_secondary_ref);
          FETCH chk_org_count into l_org_count;
          CLOSE chk_org_count;
          IF l_org_count > 1
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',895);
          END IF;
        END IF;
        -- ************************************************************************************
        -- IF a ltl reference supplied does it exist on land_titles
        -- ************************************************************************************
        IF p1.lnop_type = 'LTL'
        THEN
          OPEN chk_ltl_ref(p1.lnop_legacy_ref);
          FETCH chk_ltl_ref INTO l_exists;
          CLOSE chk_ltl_ref;
          IF l_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',459);
          END IF;
        END IF;
        -- ************************************************************************************
        -- Now Chek the Y/N columns
        -- Current Indicator
        -- ************************************************************************************
        IF p1.lnop_current_ind NOT IN ('Y','N')
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',922);
        END IF;
        -- ************************************************************************************
        -- Highlighted Indicator
        -- ************************************************************************************
        IF p1.lnop_highlight_ind NOT IN ('Y','N') 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',923);
        END IF;
        -- ************************************************************************************
        -- Now validate the Notepad Type Code
        -- ************************************************************************************
        IF p1.lnop_ntt_code IS NOT NULL 
        THEN
          l_ntt_exists := NULL;
          OPEN chk_ntt_code(p1.lnop_ntt_code);
          FETCH chk_ntt_code INTO l_ntt_exists;
          CLOSE chk_ntt_code;
          IF l_ntt_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',921);
          END IF;
        END IF;
        -- ************************************************************************************
        -- Check Application Type is Valid
        -- ************************************************************************************
        IF p1.lnop_application_type IS NOT NULL
        THEN
          IF p1.lnop_application_type NOT IN ('A','H')
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',650);
          END IF;
        END IF;
        -- ************************************************************************************
        -- Now UPDATE the record count AND error code
        -- ************************************************************************************
        IF l_errors = 'F' 
        THEN
          l_error_ind := 'Y';
        ELSE
          l_error_ind := 'N';
        END IF;
        s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
        set_record_status_flag(l_id,l_errors);
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
        set_record_status_flag(l_id,'O');
      END;
    END LOOP;
    COMMIT;
    fsc_utils.proc_END;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  END dataload_validate;
  --
  -- ************************************************************************************
  --
  PROCEDURE dataload_delete 
    (p_batch_id        IN VARCHAR2
    ,p_date            IN DATE
    ) 
  IS
  CURSOR c1
  IS
    SELECT ROWID rec_rowid
    ,      lnop_dlb_batch_id
    ,      lnop_dl_seqno
    ,      lnop_dl_load_status
    ,      lnop_type
    ,      lnop_legacy_ref
    ,      lnop_secondary_ref
    ,      lnop_secondary_date
    ,      lnop_created_by
    ,      lnop_created_date
    ,      NVL(lnop_current_ind,'Y')       lnop_current_ind
    ,      NVL(lnop_highlight_ind,'N')     lnop_highlight_ind
    ,      REPLACE(lnop_text,chr(10),' ')  lnop_text
    ,      lnop_ntt_code
    ,      lnop_application_type
    ,      lnop_modified_by
    ,      lnop_modified_date
    FROM dl_mad_notepads
    WHERE lnop_dlb_batch_id = p_batch_id
    AND lnop_dl_load_status = 'C';
  CURSOR get_aun_refno(p_aun_code VARCHAR2)
  IS
    SELECT aun_reusable_refno
    FROM   admin_units
    WHERE  aun_code = p_aun_code;
  CURSOR get_app_refno(p_app_legacy_ref VARCHAR2)
  IS
    SELECT app_reusable_refno
    FROM   applications
    WHERE  app_legacy_ref = p_app_legacy_ref;
  CURSOR get_par_refno(p_par_per_alt_ref VARCHAR2)
  IS
    SELECT par_reusable_refno
    FROM parties
    WHERE par_per_alt_ref = p_par_per_alt_ref;
  CURSOR get_pro_refno(p_pro_propref VARCHAR2)
  IS
    SELECT pro_reusable_refno
    FROM properties
    WHERE pro_propref = p_pro_propref;
  CURSOR get_tcy_refno(p_tcy_alt_ref VARCHAR2)
  IS
    SELECT tcy_reusable_refno
    FROM tenancies
    WHERE tcy_alt_ref = p_tcy_alt_ref;
  CURSOR get_rac_refno(p_rac_pay_ref VARCHAR2)
  IS
    SELECT rac_reusable_refno
    FROM revenue_accounts
    WHERE rac_pay_ref = p_rac_pay_ref;
  CURSOR get_arr_refno
    (p_rac_pay_ref      VARCHAR2
    ,p_arr_ara_code     VARCHAR2
    ,p_arr_start_date   DATE
    )
  IS
    SELECT a.aca_reusable_refno
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  r.rac_pay_ref = p_rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    a.aca_ara_code = p_arr_ara_code
    AND    TRUNC(a.aca_created_date) = p_arr_start_date;
  CURSOR get_aca_refno
    (p_rac_pay_ref       VARCHAR2
    ,p_aca_ara_code      VARCHAR2
    ,p_aca_created_date  DATE
    )
  IS
    SELECT a.aca_reusable_refno
    FROM   account_arrears_actions a
    ,      revenue_accounts        r
    WHERE  r.rac_pay_ref = p_rac_pay_ref
    AND    a.aca_rac_accno = r.rac_accno
    AND    a.aca_ara_code = p_aca_ara_code
    AND    TRUNC(a.aca_created_date) = p_aca_created_date;
  CURSOR get_ins_refno(p_ins_legacy_ref VARCHAR2)
  IS
    SELECT ins_reusable_refno
    FROM   inspections
    WHERE  ins_legacy_refno = p_ins_legacy_ref;
  CURSOR get_wor_refno(p_wor_legacy_ref VARCHAR2)
  IS
    SELECT wor_reusable_refno
    FROM   works_orders
    WHERE  wor_legacy_ref = p_wor_legacy_ref;
  CURSOR get_srq_refno(p_srq_legacy_refno VARCHAR2)
  IS
    SELECT srq_reusable_refno
    FROM   service_requests
    WHERE  srq_legacy_refno = p_srq_legacy_refno;
  CURSOR get_ban_refno(p_ban_reference VARCHAR2)
  IS
    SELECT ban_reusable_refno
    FROM   business_actions
    WHERE  ban_reference = p_ban_reference;
  CURSOR get_peg_refno(p_peg_code VARCHAR2)
  IS
    SELECT peg_reusable_refno
    FROM   people_groups
    WHERE  peg_code = p_peg_code;
  CURSOR get_ppo_refno
    (p_ppo_pro_propref VARCHAR2
    ,p_ppo_room_number VARCHAR2
    )
  IS
    SELECT ppo.ppo_reusable_refno
    FROM   placement_property_rooms ppo
    ,      properties               pro
    WHERE  pro.pro_propref = p_ppo_pro_propref
    AND    ppo.ppo_pro_refno = pro.pro_refno
    AND    ppo.ppo_room_number = p_ppo_room_number;
  CURSOR get_ref_refno(p_ref_alternate_ref VARCHAR2)
  IS
    SELECT ref_reusable_refno
    FROM   referrals
    WHERE  ref_alternate_ref = p_ref_alternate_ref;
  CURSOR get_suap_refno(p_suap_reference VARCHAR2)
  IS
    SELECT suap_reusable_refno
    FROM   subsidy_applications
    WHERE  suap_reference = p_suap_reference;
  CURSOR get_surv_refno
    (p_surv_refno          VARCHAR2
    ,p_surv_suap_reference VARCHAR2
    )
  IS
    SELECT surv_reusable_refno
    FROM   subsidy_reviews
    WHERE  surv_refno = p_surv_refno
    AND    NVL(surv_suap_reference,9999999) = NVL(p_surv_suap_reference, 9999999);
  CURSOR c_get_created_date
    (p_resusable_refno INTEGER
    ,p_nop_type VARCHAR2
    ,p_date DATE
    ) 
  IS
    SELECT MAX(nop_created_date) + (1/24/60/60)
    FROM   notepads
    WHERE  nop_reusable_refno = p_resusable_refno
    AND    nop_type = p_nop_type
    AND    nop_created_date = p_date;
  CURSOR get_par2_refno(p_par_refno VARCHAR2)
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_refno = p_par_refno;
  CURSOR get_org_refno
    (p_org_short_name   VARCHAR2
    ,p_org_frv_oty_code VARCHAR2
    ) 
  IS
    SELECT par_reusable_refno
    FROM   parties
    WHERE  par_org_short_name = p_org_short_name
    AND   par_org_frv_oty_code = p_org_frv_oty_code;
  CURSOR get_ltl_refno(p_ltl_legacy_ref VARCHAR2)
  IS
    SELECT ltl_reusable_refno
    FROM   land_titles
    WHERE  ltl_reference = p_ltl_legacy_ref;
  cb                      VARCHAR2(30);
  cd                      DATE;
  cp                      VARCHAR2(30) := 'DELETE';
  ct                      VARCHAR2(30) := 'DL_MAD_NOTEPADS';
  cs                      INTEGER;
  ce                      VARCHAR2(200);
  l_an_tab                VARCHAR2(1);
  l_id                    ROWID;
  l_reusable_refno        NUMBER(20);
  i                       INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_mad_notepads.dataload_delete');
    fsc_utils.debug_message('s_dl_mad_notepads.dataload_delete',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.lnop_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        --
        -- get the reusable_refno
        --
        l_reusable_refno := NULL;
        IF p1.lnop_type = 'AUN' 
        THEN
          OPEN get_aun_refno(p1.lnop_legacy_ref);
          FETCH get_aun_refno INTO l_reusable_refno;
          CLOSE get_aun_refno;
        ELSIF p1.lnop_type = 'APP'
        THEN
          OPEN get_app_refno(p1.lnop_legacy_ref);
          FETCH get_app_refno INTO l_reusable_refno;
          CLOSE get_app_refno;
        ELSIF p1.lnop_type = 'PAR'
        THEN
          OPEN get_par_refno(p1.lnop_legacy_ref);
          FETCH get_par_refno INTO l_reusable_refno;
          CLOSE get_par_refno;
        ELSIF p1.lnop_type = 'PRO'
        THEN
          OPEN get_pro_refno(p1.lnop_legacy_ref);
          FETCH get_pro_refno INTO l_reusable_refno;
          CLOSE get_pro_refno;
        ELSIF p1.lnop_type = 'TCY'
        THEN
          OPEN get_tcy_refno(p1.lnop_legacy_ref);
          FETCH get_tcy_refno INTO l_reusable_refno;
          CLOSE get_tcy_refno;
        ELSIF p1.lnop_type = 'RAC' 
        THEN
          OPEN get_rac_refno(p1.lnop_legacy_ref);
          FETCH get_rac_refno INTO l_reusable_refno;
          CLOSE get_rac_refno;
        ELSIF p1.lnop_type = 'ARR'
        THEN
          OPEN get_arr_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref, p1.lnop_secondary_date);
          FETCH get_arr_refno INTO l_reusable_refno;
          CLOSE get_arr_refno;
        ELSIF p1.lnop_type = 'ACA' 
        THEN
          OPEN get_aca_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref, p1.lnop_secondary_date);
          FETCH get_aca_refno INTO l_reusable_refno;
          CLOSE get_aca_refno;
        ELSIF p1.lnop_type = 'INS'
        THEN
          OPEN get_ins_refno(p1.lnop_legacy_ref);
          FETCH get_ins_refno INTO l_reusable_refno;
          CLOSE get_ins_refno;
        ELSIF p1.lnop_type = 'WOR'
        THEN
          OPEN get_wor_refno(p1.lnop_legacy_ref);
          FETCH get_wor_refno INTO l_reusable_refno;
          CLOSE get_wor_refno;
        ELSIF p1.lnop_type = 'SRQ'
        THEN
          OPEN get_srq_refno(p1.lnop_legacy_ref);
          FETCH get_srq_refno INTO l_reusable_refno;
          CLOSE get_srq_refno;
        ELSIF p1.lnop_type = 'BAN'
        THEN
          OPEN get_ban_refno(p1.lnop_legacy_ref);
          FETCH get_ban_refno INTO l_reusable_refno;
          CLOSE get_ban_refno;
        ELSIF p1.lnop_type = 'PEG'
        THEN
          OPEN get_peg_refno(p1.lnop_legacy_ref);
          FETCH get_peg_refno INTO l_reusable_refno;
          CLOSE get_peg_refno;
        ELSIF p1.lnop_type = 'PPO'
        THEN
          OPEN get_ppo_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref);
          FETCH get_ppo_refno INTO l_reusable_refno;
          CLOSE get_ppo_refno;
        ELSIF p1.lnop_type = 'REF' 
        THEN
          OPEN get_ref_refno(p1.lnop_legacy_ref);
          FETCH get_ref_refno INTO l_reusable_refno;
          CLOSE get_ref_refno;
        ELSIF p1.lnop_type = 'SUAP' 
        THEN
          OPEN get_suap_refno(p1.lnop_legacy_ref);
          FETCH get_suap_refno INTO l_reusable_refno;
          CLOSE get_suap_refno;
        ELSIF p1.lnop_type = 'SURV'
        THEN
          OPEN get_surv_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref);
          FETCH get_surv_refno INTO l_reusable_refno;
          CLOSE get_surv_refno;
        ELSIF p1.lnop_type = 'PAR2'
        THEN
          OPEN get_par2_refno(p1.lnop_legacy_ref);
          FETCH get_par2_refno INTO l_reusable_refno;
          CLOSE get_par2_refno;
        ELSIF p1.lnop_type = 'ORG'
        THEN
          OPEN get_org_refno(p1.lnop_legacy_ref, p1.lnop_secondary_ref);
          FETCH get_org_refno INTO l_reusable_refno;
          CLOSE get_org_refno;
        ELSIF p1.lnop_type = 'LTL'
        THEN
          OPEN get_ltl_refno(p1.lnop_legacy_ref);
          FETCH get_ltl_refno INTO l_reusable_refno;
          CLOSE get_ltl_refno;
        END IF;
        --
        -- Delete record from notepds
        --
        DELETE
        FROM  notepads
        WHERE nop_reusable_refno = l_reusable_refno
        AND   nop_text = p1.lnop_text
        AND   nop_created_date = NVL(p1.lnop_created_date,nop_created_date)
        AND   nop_created_by = NVL(p1.lnop_created_by,nop_created_by)
        AND   nop_highlight_ind = p1.lnop_highlight_ind
        AND   nop_current_ind = p1.lnop_current_ind;
        --
        -- keep a count of the rows processed and commit after every 1000
        --
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'V');
        i := i + 1;
        IF MOD(i,1000) = 0 
        THEN
          COMMIT;
        END IF;
      EXCEPTION
      WHEN OTHERS 
      THEN
        ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
        set_record_status_flag(l_id,'C');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
    END LOOP;
    COMMIT;
    --
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('NOTEPADS');
    fsc_utils.proc_end;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_mad_notepads;
/

show errors

