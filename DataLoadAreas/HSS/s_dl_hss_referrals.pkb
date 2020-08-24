CREATE OR REPLACE PACKAGE BODY s_dl_hss_referrals
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VER    DB Ver   WHO  WHEN         WHY
--  1.0    6.18     AJ   08-FEB-2019  Created For new SAHT loader
--                                    Support Services Referrals
--  1.1    6.18     AJ   11-FEB-2019  Further updates delete and create completed
--  1.2    6.18     AJ   15-FEB-2019  Further updates 
--  1.3    6.18     AJ   17-FEB-2019  Further updates 
--  1.4    6.18     AJ   18-FEB-2019  Further updates during testing
--  1.5    6.18     AJ   22-MAR-2019  Further updates during testing
--  1.6    6.18     AJ   25-MAR-2019  Further updates during testing
--
-- ***********************************************************************
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_hss_referrals
    SET lref_dl_load_status = p_status
    WHERE rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_hss_referrals');
    RAISE;
  END set_record_status_flag;
-- ***********************************************************************
--
 PROCEDURE dataload_create
   (p_batch_id          IN VARCHAR2
   ,p_date              IN DATE
   )
 AS
 CURSOR c1(cp_batch_id VARCHAR2)
 IS 
 SELECT rowid rec_rowid
 ,lref_dlb_batch_id
 ,lref_dl_seqno
 ,lref_alternate_ref
 ,lref_type 
 ,lref_clnt_par_per_forename
 ,lref_clnt_par_per_surname
 ,lref_par_per_date_of_birth
 ,lref_par_per_alt_ref
 ,lref_par_refno
 ,lref_sco_code
 ,NVL(lref_status_date,TRUNC(SYSDATE)) lref_status_date
 ,lref_received_date
 ,NVL(lref_created_by,'DATALOAD') lref_created_by
 ,lref_created_date
 ,lref_csvc_code
 ,lref_to_supr_code
 ,lref_den_to_supr_code
 ,lref_aun_code
 ,lref_comments
 ,lref_reference
 ,lref_reusable_refno
 ,lref_client_par_refno  -- found updated by validate
 FROM   dl_hss_referrals
 WHERE  lref_dlb_batch_id = p_batch_id
 AND    lref_dl_load_status = 'V'
 ORDER BY lref_dl_seqno;
 -- ******************************
 CURSOR c2
 IS
 SELECT ref_refno_seq.nextval
 FROM dual;
 --
 CURSOR c3
 IS
 SELECT reusable_refno_seq.nextval
 FROM dual;
 --
 CURSOR c4
 (cp_lref_type              dl_hss_referrals.lref_type%TYPE
 ,cp_lref_client_par_refno  dl_hss_referrals.lref_client_par_refno%TYPE
 ,cp_lref_sco_code          dl_hss_referrals.lref_sco_code%TYPE
 ,cp_lref_status_date       dl_hss_referrals.lref_status_date%TYPE
 ,cp_lref_received_date     dl_hss_referrals.lref_received_date%TYPE
 ,cp_lref_csvc_code         dl_hss_referrals.lref_csvc_code%TYPE
 ,cp_lref_to_supr_code      dl_hss_referrals.lref_to_supr_code%TYPE
 ,cp_lref_alternate_ref     dl_hss_referrals.lref_alternate_ref%TYPE
 ,cp_lref_den_to_supr_code  dl_hss_referrals.lref_den_to_supr_code%TYPE
 ,cp_lref_aun_code          dl_hss_referrals.lref_aun_code%TYPE
 )
 IS
 SELECT 'X'
 FROM   referrals
 WHERE  ref_type = cp_lref_type
   AND  ref_client_par_refno = cp_lref_client_par_refno
   AND  ref_sco_code = cp_lref_sco_code
   AND  ref_status_date = cp_lref_status_date
   AND  ref_received_date = cp_lref_received_date
   AND  nvl(ref_csvc_code,'X') = nvl(cp_lref_csvc_code,'X')
   AND  nvl(ref_to_supr_code,'X') = nvl(cp_lref_to_supr_code,'X')
   AND  ref_alternate_ref = cp_lref_alternate_ref
   AND  nvl(ref_den_to_supr_code,'X') = nvl(cp_lref_den_to_supr_code,'X')
   AND  nvl(ref_aun_code,'X') = nvl(cp_lref_aun_code,'X');
 r4 c4%ROWTYPE;
 -- **********************************
 cb          VARCHAR2(30);
 cd          DATE;
 cp          VARCHAR2(30) := 'CREATE';
 ct          VARCHAR2(30) := 'DL_HSS_REFERRALS';
 cs          INTEGER;
 ce          VARCHAR2(200);
 i           INTEGER := 0;
 l_id        ROWID;
--
 l_an_tab     VARCHAR2(1);
 l_reusable_refno   referrals.ref_reusable_refno%TYPE;
 l_ref_reference    referrals.ref_reference%TYPE;
 l_ref_chk          VARCHAR2(1);
  --
  -- *****************************************
 BEGIN
  fsc_utils.proc_start('s_dl_hss_referrals.dataload_create');
  fsc_utils.debug_message( 's_dl_hss_referrals.dataload_create',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR r1 in c1(p_batch_id) 
   LOOP
   BEGIN
    cs := r1.lref_dl_seqno;
    l_id := r1.rec_rowid;
    r4 := NULL;
    l_reusable_refno := NULL;
    l_ref_reference  := NULL;
    l_ref_chk        := NULL;
 --
 -- get ref_refence from sequence
 --
    OPEN c2;
    FETCH c2 INTO l_ref_reference;
    CLOSE c2;
 --
 -- get ref_reusable_refno from sequence
 --
    OPEN c3;
    FETCH c3 INTO l_reusable_refno;
    CLOSE c3;
 --
 -- check again to prevent duplicates
 --
    OPEN c4(r1.lref_type
           ,r1.lref_client_par_refno
           ,r1.lref_sco_code
           ,r1.lref_status_date
           ,r1.lref_received_date
           ,r1.lref_csvc_code
           ,r1.lref_to_supr_code
           ,r1.lref_alternate_ref
           ,r1.lref_den_to_supr_code
           ,r1.lref_aun_code);
    FETCH c4 INTO l_ref_chk;
    CLOSE c4;
--
    IF l_ref_chk IS NULL
     THEN

      INSERT INTO referrals
      (
      ref_reference,
      ref_type,
      ref_client_par_refno,
      ref_sco_code,
      ref_status_date,
      ref_received_date,
      ref_reusable_refno,
      ref_csvc_code,
      ref_to_supr_code,
      ref_alternate_ref,
      ref_comments,
      ref_den_to_supr_code,
      ref_aun_code,
      ref_exr_code,
      ref_exr_code2
      )
      VALUES
      (
      l_ref_reference,
      r1.lref_type,
      r1.lref_client_par_refno,
      r1.lref_sco_code,
      r1.lref_status_date,
      r1.lref_received_date,
      l_reusable_refno,
      r1.lref_csvc_code,
      r1.lref_to_supr_code,
      r1.lref_alternate_ref,
      r1.lref_comments,
      r1.lref_den_to_supr_code,
      r1.lref_aun_code,
      CASE r1.lref_sco_code WHEN 'CLO' THEN 'CARLREFER' END,  
      CASE r1.lref_sco_code WHEN 'CLO' THEN 'REFMADE' END
      );
 --
 -- Now set created by and date as required
 --
      IF r1.lref_created_date IS NOT NULL
       THEN
        UPDATE referrals
           SET ref_created_by = r1.lref_created_by
              ,ref_created_date = r1.lref_created_date
         WHERE ref_reference = l_ref_reference;
      ELSE
        UPDATE referrals
           SET ref_created_by = r1.lref_created_by
         WHERE ref_reference = l_ref_reference;
      END IF;
 --
 --  Update the data load record with the reusable refno 
 --  and ref_reference used needed for checking and delete
 --
      UPDATE dl_hss_referrals
         SET lref_reference = l_ref_reference
            ,lref_reusable_refno = l_reusable_refno
       WHERE ROWID = r1.rec_rowid;
 --
    END IF; -- end of insert
 --
 -- Update Record Status and Record Count
 --
    s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
    set_record_status_flag(l_id,'C');
--
-- *****************************************
-- keep a count of the rows processed and commit after every 1000
--
    i := i + 1; 
    IF MOD(i,1000) = 0 
     THEN 
      COMMIT; 
    END IF;
 --
    EXCEPTION
     WHEN OTHERS 
     THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
   END;
  END LOOP;
 --
 -- Section to analyse the tables populated with this data load
 --
  l_an_tab := s_dl_hem_utils.dl_comp_stats('REFERRALS');
  l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HSS_REFERRALS');
 --
  fsc_utils.proc_end;
  COMMIT;
  EXCEPTION
   WHEN OTHERS 
    THEN
     s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
 --
 -- ***********************************************************************
 --
 PROCEDURE dataload_validate
    (p_batch_id  IN VARCHAR2
    ,p_date      IN date)
 AS
 CURSOR c1 
 IS
 SELECT rowid rec_rowid
 ,lref_dlb_batch_id
 ,lref_dl_seqno
 ,lref_alternate_ref         -- M
 ,lref_type                  -- M 
 ,lref_clnt_par_per_forename -- M if
 ,lref_clnt_par_per_surname  -- M if
 ,lref_par_per_date_of_birth -- M if
 ,lref_par_per_alt_ref       -- M if
 ,lref_par_refno             -- M if
 ,lref_sco_code              -- M 
 ,lref_status_date
 ,lref_received_date         -- M
 ,lref_created_by
 ,lref_created_date
 ,lref_csvc_code
 ,lref_to_supr_code
 ,lref_den_to_supr_code
 ,lref_aun_code
 ,lref_comments
 ,lref_reference
 ,lref_reusable_refno
 ,lref_client_par_refno
 FROM   dl_hss_referrals
 WHERE  lref_dlb_batch_id = p_batch_id
 AND    lref_dl_load_status IN ('L','F','O')
 ORDER BY lref_dl_seqno;
 --
  CURSOR c2
    (cp_ref_alt_ref  dl_hss_referrals.lref_alternate_ref%TYPE
    )
  IS
    SELECT ref_alternate_ref
    FROM   referrals
    WHERE  ref_alternate_ref = cp_ref_alt_ref;
  r2 c2%ROWTYPE;
 --
  CURSOR c3
    (cp_ref_alt_ref  dl_hss_referrals.lref_alternate_ref%TYPE
    )
  IS
    SELECT count(*)
    FROM   dl_hss_referrals
    WHERE  lref_alternate_ref = cp_ref_alt_ref;
 --
  CURSOR c4
    (cp_par_alt_ref  dl_hss_referrals.lref_par_per_alt_ref%TYPE
    )
  IS
    SELECT count(*)
    FROM   parties
    WHERE  par_per_alt_ref = cp_par_alt_ref;
 --
  CURSOR c5
    (cp_par_alt_ref  dl_hss_referrals.lref_par_per_alt_ref%TYPE
    )
  IS
    SELECT par_refno, par_type
    FROM   parties
    WHERE  par_per_alt_ref = cp_par_alt_ref;
 --
  CURSOR c6
    (cp_par_refno dl_hss_referrals.lref_par_refno%TYPE
    )
  IS
    SELECT par_refno, par_type
    FROM   parties
    WHERE  par_refno = cp_par_refno;
 --
  CURSOR c7
    (cp_forename dl_hss_referrals.lref_clnt_par_per_forename%TYPE
    ,cp_surname  dl_hss_referrals.lref_clnt_par_per_surname%TYPE
    ,cp_dob      dl_hss_referrals.lref_par_per_date_of_birth%TYPE
    )
  IS
    SELECT count(*)
    FROM   parties
    WHERE  nvl(par_per_forename,'~') = nvl(cp_forename, nvl(par_per_forename,'~'))
    AND    nvl(par_per_surname,'~') = nvl(cp_surname,nvl(par_per_surname,'~')) 
    AND    nvl(par_per_date_of_birth,'01-JAN-1900') =  nvl(cp_dob,nvl(par_per_date_of_birth,'01-JAN-1900'))
    AND    par_type NOT IN ('ORG','COM');
 --
  CURSOR c8
    (cp_forename dl_hss_referrals.lref_clnt_par_per_forename%TYPE
    ,cp_surname  dl_hss_referrals.lref_clnt_par_per_surname%TYPE
    ,cp_dob      dl_hss_referrals.lref_par_per_date_of_birth%TYPE
    )
  IS
    SELECT par_refno
    FROM   parties
    WHERE  nvl(par_per_forename,'~') = nvl(cp_forename, nvl(par_per_forename,'~'))
    AND    nvl(par_per_surname,'~') = nvl(cp_surname,nvl(par_per_surname,'~')) 
    AND    nvl(par_per_date_of_birth,'01-JAN-1900') =  nvl(cp_dob,nvl(par_per_date_of_birth,'01-JAN-1900'))
    AND    par_type NOT IN ('ORG','COM');
 --
  CURSOR c9
    (cp_csvc_code dl_hss_referrals.lref_csvc_code%TYPE
    )
  IS
    SELECT 'X'
    FROM   client_services
    WHERE  csvc_code = cp_csvc_code
    AND    csvc_current_ind = 'Y';
 --
  CURSOR c10
    (cp_csvc_code support_provider_services.sups_csvc_code%TYPE
    ,cp_supr_code support_provider_services.sups_supr_code%TYPE
    )
  IS
    SELECT  'X'
    FROM   support_provider_services
    WHERE  sups_csvc_code = cp_csvc_code
    AND    sups_supr_code = cp_supr_code
    AND    sups_current_ind = 'Y';
 --
  CURSOR c11
    (cp_supr_code support_providers.supr_code%TYPE
    )
  IS
    SELECT  'X'
    FROM   support_providers
    WHERE  supr_code = cp_supr_code
    AND    supr_current_ind = 'Y';
 --
  CURSOR c12 (cp_aun_code admin_units.aun_code_mlang%TYPE
              )
  IS
    SELECT 'X'
    FROM admin_units, admin_unit_types
    WHERE aun_code = cp_aun_code
    AND auy_code = aun_auy_code 
    AND auy_type = 'HOU'
    AND aun_auy_code = (SELECT s_parameter_values.get_param('HSS_AUN_TYPE','SYSTEM','HSS') FROM dual)
    AND aun_current_ind = 'Y';
 --
  CURSOR c14 ( cp_ref_client_refno referrals.ref_client_par_refno%TYPE
              ,cp_ref_csvc_code    referrals.ref_csvc_code%TYPE
              ,cp_ref_to_supr_code referrals.ref_to_supr_code%TYPE
              )
  IS
    SELECT 'X'
    FROM referrals
    WHERE ref_client_par_refno = cp_ref_client_refno
    AND ref_csvc_code = cp_ref_csvc_code
    AND ref_sco_code IN ('RAI','ACC','SOF')
    AND ref_to_supr_code = cp_ref_to_supr_code;
 --
  CURSOR c15 ( cp_ref_client_refno referrals.ref_client_par_refno%TYPE
              ,cp_ref_csvc_code    referrals.ref_csvc_code%TYPE
              ,cp_ref_to_supr_code referrals.ref_to_supr_code%TYPE
              )
  IS
    SELECT 'X'
    FROM referrals
    WHERE ref_client_par_refno = cp_ref_client_refno
    AND ref_csvc_code = cp_ref_csvc_code
    AND ref_sco_code NOT IN ('CLO','SRF','RFR','SAC')
    AND ref_to_supr_code != cp_ref_to_supr_code;    
 --
  CURSOR c16 ( cp_forename         dl_hss_referrals.lref_clnt_par_per_forename%TYPE
              ,cp_surname          dl_hss_referrals.lref_clnt_par_per_surname%TYPE
              ,cp_dob              dl_hss_referrals.lref_par_per_date_of_birth%TYPE
              ,cp_alt_ref          dl_hss_referrals.lref_par_per_alt_ref%TYPE
              ,cp_par_refno        dl_hss_referrals.lref_par_refno%TYPE
              ,cp_ref_csvc_code    dl_hss_referrals.lref_csvc_code%TYPE
              ,cp_ref_to_supr_code dl_hss_referrals.lref_to_supr_code%TYPE
              ,cp_batch_id         dl_hss_referrals.lref_dlb_batch_id%TYPE
              )
  IS
    SELECT count(*)
    FROM dl_hss_referrals
    WHERE nvl(lref_clnt_par_per_forename,'~') = nvl(cp_forename,nvl(lref_clnt_par_per_forename,'~'))
    AND nvl(lref_clnt_par_per_surname,'~')  = nvl(cp_surname,nvl(lref_clnt_par_per_surname,'~')) 
    AND nvl(lref_par_per_date_of_birth,'01-JAN-1900') = nvl(cp_dob,nvl(lref_par_per_date_of_birth,'01-JAN-1900'))
    AND nvl(lref_par_per_alt_ref,'~') = nvl(cp_alt_ref,nvl(lref_par_per_alt_ref,'~'))
    AND nvl(lref_par_refno,0) = nvl(cp_par_refno,nvl(lref_par_refno,0))
    AND nvl(lref_csvc_code,'~') = nvl(cp_ref_csvc_code,nvl(lref_csvc_code,'~'))
    AND lref_sco_code IN ('RAI','ACC','SOF')
    AND nvl(lref_to_supr_code,'~') = nvl(cp_ref_to_supr_code,nvl(lref_to_supr_code,'~'))
    AND lref_dlb_batch_id = cp_batch_id;
 --
 -- *******
 --  
 cb          VARCHAR2(30);
 cd          DATE;
 cp          VARCHAR2(30) := 'VALIDATE';
 ct          VARCHAR2(30) := 'DL_HSS_REFERRALS';
 cs          INTEGER;
 ce          VARCHAR2(200);
 i           INTEGER := 0;
 l_id        ROWID;
 l_errors    VARCHAR2(1);
 l_error_ind VARCHAR2(1);
 l_an_tab    VARCHAR2(1);
 l_count_dup_alt_ref  INTEGER := 0;
 l_count_par_alt_ref  INTEGER := 0;
 l_par_refno          NUMBER(8);
 l_count_par_refno    INTEGER := 0;
 l_csvc_exists        VARCHAR2(1);
 l_csvc_supr_exists   VARCHAR2(1);
 l_supr_exists        VARCHAR2(1);
 l_aun_exists         VARCHAR2(1);
 l_referral_exists    VARCHAR2(1);
 l_ref_other_exists   VARCHAR2(1);
 l_count_dup          INTEGER := 0;
 l_null_forename      VARCHAR2(1);
 l_null_surname       VARCHAR2(1);
 l_null_dob           DATE;
 l_null_alt_ref       VARCHAR2(1);
 l_null_par_refno     NUMBER(8);
 l_null_ref_csvc_code VARCHAR2(1);
 l_null_ref_to_supr_code  VARCHAR2(1);
 l_null_batch_id      VARCHAR2(1);
 l_par_type           VARCHAR2(5);
 --
 -- *****************************************
 BEGIN
  fsc_utils.proc_start('s_dl_hss_referrals.dataload_validate');
  fsc_utils.debug_message('s_dl_hss_referrals.dataload_validate',3 );
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR r1 in c1 
   LOOP
   BEGIN
    cs := r1.lref_dl_seqno;
    l_id := r1.rec_rowid;
    l_errors := 'V';
    l_error_ind := 'N';
    r2 := NULL;
    l_count_dup_alt_ref := 0;
    l_count_par_alt_ref := 0;
    l_par_refno         := NULL;
    l_count_par_refno   := 0;
    l_csvc_exists       := NULL;
    l_csvc_supr_exists  := NULL;
    l_supr_exists       := NULL;
    l_aun_exists        := NULL;
    l_referral_exists   := NULL;
    l_ref_other_exists  := NULL;
    l_count_dup         := 0;
    l_null_forename     := NULL;
    l_null_surname      := NULL;
    l_null_dob          := NULL;
    l_null_alt_ref      := NULL;
    l_null_par_refno    := NULL;
    l_null_ref_csvc_code  := NULL;
    l_null_ref_to_supr_code := NULL;
    l_null_batch_id     := NULL;
    l_par_type          := NULL;
  --
  -- Mandatory fields supplied
  --
  -- Alternative Reference
  --
    IF r1.lref_alternate_ref IS NULL
     THEN
     -- 'Alternative Referral Reference must be supplied'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',812);
    END IF;
    IF r1.lref_alternate_ref IS NOT NULL
     THEN
     --
      OPEN c2(r1.lref_alternate_ref);
      FETCH c2 INTO r2;
      IF c2%FOUND
       THEN
       -- 'The Referral Alternative Reference already exits in the Referrals table'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',813);
      END IF;
      CLOSE c2;
      --
      OPEN c3(r1.lref_alternate_ref);
      FETCH c3 INTO l_count_dup_alt_ref;
      CLOSE c3;
      --
      IF (l_count_dup_alt_ref > 1)
       THEN	
       -- 'The Referral Alternative Reference is duplicated within the batch'
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',814);
      END IF;
      --
    END IF;
  --
  --
  -- Referral Type
  --
    IF (r1.lref_type IS NULL)
     THEN
     -- 'The Referral Type must be supplied'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',815);
    END IF;
    IF (r1.lref_type IS NOT NULL AND r1.lref_type != 'Y')
     THEN
     -- 'The Referral Type must be set to Y'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',816);
    END IF;
    --
  --
  -- Client Party Reference
  --
    IF ( r1.lref_clnt_par_per_forename IS NULL
     AND r1.lref_clnt_par_per_surname IS NULL
     AND r1.lref_par_per_date_of_birth IS NULL
     AND r1.lref_par_per_alt_ref IS NULL
     AND r1.lref_par_refno IS NULL)
     THEN
     -- 'You must supply at least 1 of the Client Referral fields'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',817);
    END IF;
    --
    -- find client with par_per_alt_ref 
    --
    IF ( r1.lref_par_per_alt_ref IS NOT NULL
     AND r1.lref_par_refno IS NULL)
     THEN
     --
      OPEN c4(r1.lref_par_per_alt_ref);
      FETCH c4 INTO l_count_par_alt_ref;
      CLOSE c4;
      --
      IF (l_count_par_alt_ref = 0)
       THEN
       -- 'No Party record found for the Party Alternative Reference supplied'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',818);
      END IF;
      --
      IF (l_count_par_alt_ref > 1)
       THEN
       -- 'More than one Party record found for the Party Alternative Reference supplied'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',819);
      END IF;
      --
      IF (l_count_par_alt_ref = 1)
       THEN
        OPEN c5(r1.lref_par_per_alt_ref);
        FETCH c5 INTO l_par_refno,l_par_type;
        CLOSE c5;
	    --
      END IF;
    END IF;   
    --
    -- find client with par_refno 
    --
    IF ( r1.lref_par_per_alt_ref IS NULL
     AND r1.lref_par_refno IS NOT NULL)
     THEN
     --
--      IF c6%ISOPEN THEN CLOSE c16; END IF;
      OPEN c6(r1.lref_par_refno);
      FETCH c6 INTO l_par_refno,l_par_type;
      CLOSE c6;
      --
      IF (l_par_refno IS NULL)
       THEN
       -- 'No Party record found for the Party Reference supplied'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',820);
      END IF;
      --
    END IF;   
    --
    -- Check Party Type when using par_refno and par_per_alt_ref directly
    --
    IF l_par_type IN('ORG','COM')
     THEN
     -- 'A Referral can only be for a Person'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',859);
    END IF;
    --
    -- find client with forename Surname and DOB
    --
    IF ( r1.lref_par_per_alt_ref IS NULL
     AND r1.lref_par_refno IS NULL
     AND ( r1.lref_clnt_par_per_forename IS NOT NULL
        OR r1.lref_clnt_par_per_surname  IS NOT NULL
        OR r1.lref_par_per_date_of_birth IS NOT NULL )
        )
     THEN
     --
      OPEN c7(r1.lref_clnt_par_per_forename
             ,r1.lref_clnt_par_per_surname
             ,r1.lref_par_per_date_of_birth);
      FETCH c7 INTO l_count_par_refno;
      CLOSE c7;
      --
      IF (l_count_par_refno = 0)
       THEN
       -- 'No Party record found for Forename Surname DOB supplied'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',821);
      END IF;
      --
      IF (l_count_par_refno > 1)
       THEN
       -- 'More than one Party record found for Forename Surname DOB supplied'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',822);
      END IF;
      --
      IF (l_count_par_refno = 1)
       THEN
        OPEN c8(r1.lref_clnt_par_per_forename
               ,r1.lref_clnt_par_per_surname
               ,r1.lref_par_per_date_of_birth);
        FETCH c8 INTO l_par_refno;
        CLOSE c8;
        --
      END IF;
    END IF;   
    --
  --
  -- Referral Status Code
  --
    IF (nvl(r1.lref_sco_code,'X') NOT IN ('RAI','CLO'))
     THEN
     -- 'The Referral Status Code must be Raised (RAI)'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',823);
    END IF;
  --
  -- Referral Status Application and Created Date Check
  --
    IF (r1.lref_status_date IS NULL)
     THEN
      r1.lref_status_date := TRUNC(SYSDATE);
    END IF;
    --
    IF (r1.lref_created_date IS NULL)
     THEN
      r1.lref_created_date := TRUNC(SYSDATE);
    END IF;
    --	
    IF (r1.lref_status_date < r1.lref_created_date)
     THEN
      -- 'The Referral Status Date cannot be earlier that the Created Date'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',824);
    END IF;
    --	
    IF (r1.lref_received_date IS NULL)
     THEN
      -- 'The Application Referral Received Date must be supplied'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',825);
    END IF;
    --
    IF (r1.lref_received_date IS NULL)
     THEN
        r1.lref_received_date := TRUNC(SYSDATE);
    END IF;
    --	
    IF (r1.lref_status_date < r1.lref_received_date)
     THEN
      -- 'The Referral Status Date cannot be earlier than the Application Referral Received Date'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',826);
    END IF;
  --
  -- CSVC Code Check
  --
    IF (r1.lref_csvc_code IS NULL)
       THEN
      -- 'The Client Services Code the Referral is for must be supplied'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',827);
    END IF;
    --
    IF (r1.lref_csvc_code IS NOT NULL)
      THEN
       OPEN c9(r1.lref_csvc_code);
       FETCH c9 INTO l_csvc_exists;
       CLOSE c9;
       --
       IF (l_csvc_exists IS NULL)
        THEN
        -- 'A Current Client Services Code does not exist for the code supplied'
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',828);
       END IF;
    END IF;
  --
  -- CSVC and Support Provider Services check
  --
    IF ( r1.lref_csvc_code IS NOT NULL 
     AND r1.lref_to_supr_code IS NOT NULL )
      THEN
       OPEN c10(r1.lref_csvc_code
               ,r1.lref_to_supr_code);
       FETCH c10 INTO l_csvc_supr_exists;
       CLOSE c10;
       --
       IF (l_csvc_supr_exists IS NULL)
        THEN
        -- 'A Combination of Client Services Code and Support Provider Code does not exist'
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',829);
       END IF;
    END IF;
  --
  -- Support Provider check
  --
    IF ( r1.lref_to_supr_code IS NOT NULL )
      THEN
       OPEN c11(r1.lref_to_supr_code);
       FETCH c11 INTO l_supr_exists;
       CLOSE c11;
       --
       IF (l_supr_exists IS NULL)
        THEN
        -- 'A Client Services Referred onto Code does not exist in the support providers table'
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',830);
       END IF;
    END IF;
  --
  -- Admin Unit Check
  --
    IF ( r1.lref_aun_code IS NOT NULL )
      THEN
       OPEN c12(r1.lref_aun_code);
       FETCH c12 INTO l_aun_exists;
       CLOSE c12;
       --
       IF (l_aun_exists IS NULL)
        THEN
        -- 'The Admin Unit Supplied A Client Services Referred onto Code does not exist in the support providers table'
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',831);
       END IF;
    END IF;
  --
  -- Check client hasn't already been referred to providers for this service
  --
    IF ( l_par_refno IS NOT NULL 
     AND r1.lref_csvc_code IS NOT NULL 
     AND r1.lref_to_supr_code IS NOT NULL )
      THEN
      --
       OPEN c14( l_par_refno
                ,r1.lref_csvc_code
                ,r1.lref_to_supr_code);
       FETCH c14 INTO l_referral_exists;
       CLOSE c14;
       --
       IF (l_referral_exists IS NOT NULL)
        THEN
        -- 'This client has already been referred to this support provider for this service'
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',852);
       END IF;
       --
       OPEN c15( l_par_refno
                ,r1.lref_csvc_code
                ,r1.lref_to_supr_code);
       FETCH c15 INTO l_ref_other_exists;
       CLOSE c15;
       --
       IF (l_ref_other_exists IS NOT NULL)
        THEN
        -- 'This client has been referred to another support provider for this service'
         l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',853);
       END IF;
       --
    END IF;
  --
  -- Check for duplicate records in the batch against support provider and need
  -- 1) If par_refno supplied just use that
  -- 
    IF ( r1.lref_par_refno IS NOT NULL )
      THEN
      --
      OPEN c16( l_null_forename
               ,l_null_surname
               ,l_null_dob
               ,l_null_alt_ref
               ,r1.lref_par_refno
               ,r1.lref_csvc_code
               ,r1.lref_to_supr_code
               ,r1.lref_dlb_batch_id );
      FETCH c16 INTO l_count_dup;
      CLOSE c16;
      --
      IF (l_count_dup > 1)
       THEN
       -- 'From the par_refno duplicate records for support provider and service found in the batch'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',855);
      END IF;
      --
    END IF;
  --
  -- 2) If par_per_alt_ref supplied just use that
  --
    IF ( r1.lref_par_per_alt_ref IS NOT NULL )
      THEN
      --
      OPEN c16( l_null_forename
               ,l_null_surname
               ,l_null_dob
               ,r1.lref_par_per_alt_ref
               ,l_null_par_refno
               ,r1.lref_csvc_code
               ,r1.lref_to_supr_code
               ,r1.lref_dlb_batch_id );
      FETCH c16 INTO l_count_dup;
      CLOSE c16;
      --
      IF (l_count_dup > 1)
       THEN
       -- 'From the Party Alt Ref duplicate records for support provider and service found in the batch'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',856);
      END IF;
      --
    END IF;
  --
  -- 3) If par_per_alt_ref or par_refno not supplied use name and dob
  --
    IF ( r1.lref_par_per_alt_ref IS NULL AND r1.lref_par_refno IS NULL )
      THEN
      --
      OPEN c16( r1.lref_clnt_par_per_forename
               ,r1.lref_clnt_par_per_surname
               ,r1.lref_par_per_date_of_birth
               ,l_null_alt_ref
               ,l_null_par_refno
               ,r1.lref_csvc_code
               ,r1.lref_to_supr_code
               ,r1.lref_dlb_batch_id );
      FETCH c16 INTO l_count_dup;
      CLOSE c16;
      --
      IF (l_count_dup > 1)
       THEN
       -- 'From the Party Name and DOB duplicate records for support provider and service found in the batch'
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',857);
      END IF;
      --
    END IF;
  --
  -- Check the Referred to Third Party Support Provider codes do not clash
  --
    IF (r1.lref_to_supr_code = r1.lref_den_to_supr_code)
       THEN
      -- 'The Referred to and third party Support Providers cannot be the same'
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',858);
    END IF;
 --
 -- ************ update data load table *****
 -- 
 -- Record Client Party (par_refno) found needed for create
 --
    IF (l_par_refno IS NOT NULL)
     THEN
      UPDATE dl_hss_referrals
      SET    lref_client_par_refno = l_par_refno
      WHERE  ROWID = r1.rec_rowid;
    END IF;
 --
 -- *****************************************
 -- Now UPDATE the record count AND error code
 --
    IF l_errors = 'F' 
     THEN
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
 --
 -- Section to analyse the tables populated with this data load
 --
  l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HSS_REFERRALS');
--
  COMMIT;
  fsc_utils.proc_end;
  EXCEPTION
   WHEN OTHERS 
    THEN
     s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
 END dataload_validate;
--
-- ***********************************************************************
--
 PROCEDURE dataload_delete 
    (p_batch_id  IN VARCHAR2
    ,p_date      IN DATE) 
 AS
 --
 CURSOR c1 
 IS
 SELECT rowid rec_rowid
 ,lref_dlb_batch_id
 ,lref_dl_seqno
 ,lref_type
 ,lref_reference
 ,lref_reusable_refno
 ,lref_client_par_refno
 FROM   dl_hss_referrals
 WHERE  lref_dlb_batch_id = p_batch_id
 AND    lref_dl_load_status = 'C';
 --
 cb          VARCHAR2(30);
 cd          DATE;
 cp          VARCHAR2(30) := 'DELETE';
 ct          VARCHAR2(30) := 'DL_HSS_REFERRALS';
 cs          INTEGER;
 ce          VARCHAR2(200);
 i           INTEGER := 0;
 l_id        ROWID;
 l_an_tab    VARCHAR2(1);
 --
 -- *****************************************
 BEGIN
  fsc_utils.proc_start('s_dl_hss_referrals.dataload_delete');
  fsc_utils.debug_message('s_dl_hss_referrals.dataload_delete',3 );
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR r1 in c1 
   LOOP
   BEGIN
    cs := r1.lref_dl_seqno;
    l_id := r1.rec_rowid;
    i := i + 1;
 --
 -- Delete the record created in referrals but only if status and client are the same
 -- as when first created
 --
    DELETE 
    FROM   referrals
    WHERE  ref_reference = r1.lref_reference
    AND    ref_reusable_refno = r1.lref_reusable_refno
    AND    ref_type = r1.lref_type
    AND    ref_client_par_refno = r1.lref_client_par_refno;
 --
 --  Now remove the saved data from the data load table
 --  for that record
 --
    UPDATE dl_hss_referrals
    SET    lref_reference = NULL
    ,      lref_reusable_refno = NULL
    WHERE  ROWID = r1.rec_rowid;
 --
 -- *****************************************
 -- Update record status and record count
 --
    s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
    set_record_status_flag(l_id,'V');
    IF MOD(i,5000) = 0 
     THEN 
      COMMIT; 
    END IF;
    EXCEPTION
     WHEN OTHERS 
      THEN
       ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
       set_record_status_flag(l_id,'C');
       s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
   END;
  END LOOP;
 --
 -- Section to analyse the tables populated with this data load
 --
  l_an_tab := s_dl_hem_utils.dl_comp_stats('REFERRALS');
  l_an_tab := s_dl_hem_utils.dl_comp_stats('DL_HSS_REFERRALS');
 --
  fsc_utils.proc_end;
  COMMIT;
  EXCEPTION
   WHEN OTHERS 
    THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
 END dataload_delete;
 --
END s_dl_hss_referrals;
/
        
show errors
--commit;


