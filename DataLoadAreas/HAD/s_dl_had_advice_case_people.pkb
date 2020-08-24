CREATE OR REPLACE PACKAGE BODY HOU.s_dl_had_advice_case_people
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO   WHEN         WHY
--  1.0     5.15.0    VS    16-JAN-2009  Initial Creation.
--                          
--  2.0     5.15.0    VS    17-APR-2009  Validation to avoid trigger failure
--                                       for "HOU.ACPE_BR_IU" on CREATE.
--                          
--  3.0     5.15.0    VS    22-MAY-2009  Performance issues with the CREATE
--                                       Processes due to the indexes. DROP
--                                       index not needed and re-create once
--                                       CREATE process has finished.
--                          
--  4.0     5.15.0    VS    09-OCT-2009  Commented out CREATE and DROP INDEX
--                                       for acp_perf_1. Defect Id 2393
--                          
--  5.0     5.15.0    VS    11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                       ACPE_BR_I in CREATE Process. Added
--                                       validation to check if par type is
--                                       HOU/HOUP as a result of diabling trigger
--                          
--  6.0     5.15.0    VS    17-FEB-2010  Add TO_CHAR to cursors to make sure
--                                       indexes are used correctly
--                                       Changed commit 500000 to 50000
--                          
--  7.0     6.14      MJK   14-NOV-2017  Added LACPE_HEAD_HHOLD_IND and
--                                       LACPE_HHOLD_GROUP_NO.
--                                       Reformatted to introduce correct
--                                       indentation
--  8.0     6.14      Umesh 08-MAY-2018  Modified for Validate and Create process errors
--  9.0     6.17      PN    14-JAN-2019  Fix to dataload_create (acpe_head_hhold_ind fix)
-- ***********************************************************************
--
PROCEDURE set_record_status_flag
  (p_rowid  IN ROWID
  ,p_status IN VARCHAR2
  )
AS
BEGIN
  UPDATE dl_had_advice_case_people
  SET    lacpe_dl_load_status = p_status
  WHERE  rowid = p_rowid;
EXCEPTION
WHEN OTHERS
THEN
  dbms_output.put_line('Error updating status of dl_had_advice_case_people');
  RAISE;
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create
  (p_batch_id          IN VARCHAR2
  ,p_date              IN DATE
  )
AS
CURSOR c1
IS
  SELECT ROWID                            rec_rowid
  ,      lacpe_dlb_batch_id
  ,      lacpe_dl_seqno
  ,      lacpe_dl_load_status
  ,      lacpe_acas_alternate_ref
  ,      lacpe_par_per_alt_ref
  ,      lacpe_client_ind
  ,      lacpe_joint_client_ind
  ,      lacpe_start_date
  ,      nvl(lacpe_created_by,'DATALOAD') lacpe_created_by
  ,      nvl(lacpe_created_date,SYSDATE)  lacpe_created_date
  ,      lacpe_hrv_frl_code
  ,      lacpe_end_date
  ,      lacpe_comment
  ,      lacpe_head_hhold_ind
  ,      lacpe_hhold_group_no
  FROM   dl_had_advice_case_people
  WHERE  lacpe_dlb_batch_id = p_batch_id
  AND    lacpe_dl_load_status = 'V';
CURSOR get_acas_reference
  (cp_acas_alt_reference VARCHAR2)
IS
  SELECT acas_reference
  FROM   advice_cases
  WHERE  acas_alternate_reference = TO_CHAR(cp_acas_alt_reference);
CURSOR get_par_refno
  (cp_par_per_alt_ref VARCHAR2)
IS
  SELECT par_refno
  FROM   parties
  -- WHERE  par_refno = cp_par_per_alt_ref; -- Ref# 8.0
  WHERE  par_per_alt_ref = cp_par_per_alt_ref;  -- Ref# 8.0
cb                VARCHAR2(30);
cd                DATE;
cp                VARCHAR2(30) := 'CREATE';
ct                VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_PEOPLE';
cs                INTEGER;
ce                VARCHAR2(200);
l_id              ROWID;
l_an_tab          VARCHAR2(1);
i                 INTEGER := 0;
l_exists          VARCHAR2(1);
l_acas_reference  NUMBER(10);
l_par_refno       NUMBER(10);
BEGIN
  execute immediate 'alter trigger ACPE_BR_I disable';
  fsc_utils.proc_start('s_dl_had_advice_case_people.dataload_create');
  fsc_utils.debug_message('s_dl_had_advice_case_people.dataload_create',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR r1 in c1
  LOOP
    BEGIN
      cs   := r1.lacpe_dl_seqno;
      l_id := r1.rec_rowid;
      SAVEPOINT SP1;
      l_acas_reference := NULL;
      OPEN get_acas_reference(r1.lacpe_acas_alternate_ref);
      FETCH get_acas_reference INTO l_acas_reference;
      CLOSE get_acas_reference;
      l_par_refno := NULL;
      OPEN get_par_refno(r1.lacpe_par_per_alt_ref);
      FETCH get_par_refno INTO l_par_refno;
      CLOSE get_par_refno;
      --
      -- Insert into ADVICE_CASE_PEOPLE
      --
      INSERT /* +APPEND */ INTO advice_case_people
      (acpe_acas_reference
      ,acpe_par_refno
      ,acpe_client_ind
      ,acpe_joint_client_ind
      ,acpe_start_date
      ,acpe_created_by
      ,acpe_created_date
      ,acpe_hrv_frl_code
      ,acpe_end_date
      ,acpe_comments
      ,acpe_head_hhold_ind
      ,acpe_hhold_group_no
      )
      VALUES
      (l_acas_reference
      ,l_par_refno
      ,r1.lacpe_client_ind
      ,r1.lacpe_joint_client_ind
      ,r1.lacpe_start_date
      ,r1.lacpe_created_by
      ,r1.lacpe_created_date
      ,r1.lacpe_hrv_frl_code
      ,r1.lacpe_end_date
      ,r1.lacpe_comment
      ,NVL(r1.lacpe_head_hhold_ind, 'N')
      ,r1.lacpe_hhold_group_no
      );
      i := i+1;
      IF MOD(i,50000) = 0
      THEN
        COMMIT;
      END IF;
      --
      -- Now UPDATE the record status and process count
      --
      s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'C');
    EXCEPTION
    WHEN OTHERS
    THEN
      ROLLBACK TO SP1;
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    END;
  END LOOP;
  COMMIT;
  execute immediate 'alter trigger ACPE_BR_I enable';
  fsc_utils.proc_end;
EXCEPTION
WHEN OTHERS
THEN
  s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
  RAISE;
END dataload_create;
--
-- ***********************************************************************
--
PROCEDURE dataload_validate
  (p_batch_id          IN VARCHAR2
  ,p_date              IN DATE
  )
AS
CURSOR c1
IS
  SELECT ROWID                            rec_rowid
  ,      lacpe_dlb_batch_id
  ,      lacpe_dl_seqno
  ,      lacpe_dl_load_status
  ,      lacpe_acas_alternate_ref
  ,      lacpe_par_per_alt_ref
  ,      lacpe_client_ind
  ,      lacpe_joint_client_ind
  ,      lacpe_start_date
  ,      nvl(lacpe_created_by,'DATALOAD') lacpe_created_by
  ,      nvl(lacpe_created_date,SYSDATE)  lacpe_created_date
  ,      lacpe_hrv_frl_code
  ,      lacpe_end_date
  ,      lacpe_comment
  ,      lacpe_head_hhold_ind
  ,      lacpe_hhold_group_no
  FROM   dl_had_advice_case_people
  WHERE  lacpe_dlb_batch_id = p_batch_id
  AND    lacpe_dl_load_status in ('L','F','O');
CURSOR chk_acas_exists
  (cp_alternate_reference VARCHAR2)
IS
  SELECT acas_reference
  FROM   advice_cases
  WHERE  acas_alternate_reference = TO_CHAR(cp_alternate_reference);
CURSOR chk_par_exists
  (cp_par_per_alt_ref VARCHAR2)
IS
  SELECT par_refno
  FROM   parties
--  WHERE  par_refno = cp_par_per_alt_ref; --Ref#  8.0
  WHERE  par_per_alt_ref = cp_par_per_alt_ref; --Ref#  8.0
CURSOR chk_acas_acpe_exists
  (cp_acas_reference NUMBER
  ,cp_par_refno       NUMBER
  )
IS
  SELECT 'X'
  FROM   advice_case_people
  WHERE  acpe_acas_reference = cp_acas_reference
  AND    acpe_par_refno = cp_par_refno;
CURSOR chk_only_one_head
  (cp_batch_id                dl_had_advice_case_people.lacpe_dlb_batch_id%TYPE
  ,cp_acpe_acas_alternate_ref dl_had_advice_case_people.lacpe_acas_alternate_ref%TYPE
  ,cp_acpe_acas_reference     advice_case_people.acpe_acas_reference%TYPE
  ,cp_acpe_hhold_group_no     dl_had_advice_case_people.lacpe_hhold_group_no%TYPE
  )
IS
  SELECT COUNT(*)
  FROM   (SELECT 'X'
          FROM   dl_had_advice_case_people
          WHERE  lacpe_dlb_batch_id = cp_batch_id
          AND    lacpe_acas_alternate_ref = cp_acpe_acas_alternate_ref
          AND    lacpe_hhold_group_no = cp_acpe_hhold_group_no
          AND    lacpe_head_hhold_ind = 'Y'
          UNION ALL
          SELECT 'X'
          FROM   advice_case_people
          WHERE  acpe_acas_reference = cp_acpe_acas_reference
          AND    acpe_hhold_group_no = cp_acpe_hhold_group_no
          AND    acpe_head_hhold_ind = 'Y'
         );
cb                    VARCHAR2(30);
cd                    DATE;
cp                    VARCHAR2(30) := 'VALIDATE';
ct                    VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_PEOPLE';
cs                    INTEGER;
ce                    VARCHAR2(200);
l_id                  ROWID;
l_exists              VARCHAR2(1);
l_acas_reference      NUMBER(10);
l_par_refno           NUMBER(10);
l_acas_acpe_exists    VARCHAR2(1);
l_head_count          INTEGER;
l_errors              VARCHAR2(10);
l_error_ind           VARCHAR2(10);
i                     INTEGER :=0;
BEGIN
  fsc_utils.proc_start('s_dl_had_advice_case_people.dataload_validate');
  fsc_utils.debug_message('s_dl_had_advice_case_people.dataload_validate',3);
  cb := p_batch_id;
  cd := p_DATE;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR r1 IN c1
  LOOP
    BEGIN
      cs := r1.lacpe_dl_seqno;
      l_id := r1.rec_rowid;
      l_errors := 'V';
      l_error_ind := 'N';
      --
      -- Check Advice Case Alt Reference LACPE_ACAS_ALTERNATE_REF has been supplied
      -- and exists on advice_cases. Get advice case status code for use further on.
      --
      IF r1.lacpe_acas_alternate_ref IS NULL
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
      ELSE
        l_acas_reference := NULL;
        OPEN chk_acas_exists(r1.lacpe_acas_alternate_ref);
        FETCH chk_acas_exists INTO l_acas_reference;
        CLOSE chk_acas_exists;
        IF l_acas_reference IS NULL
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',144);
        END IF;
      END IF;
      --
      -- Check Advice Case Person LACPE_PAR_PER_ALT_REF is supplied and valid
      --
      IF r1.lacpe_par_per_alt_ref IS NULL
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',147);
      ELSE
        l_par_refno := NULL;
        OPEN chk_par_exists (r1.lacpe_par_per_alt_ref);
        FETCH chk_par_exists INTO l_par_refno;
        CLOSE chk_par_exists;
        IF (l_par_refno IS NULL)
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',148);
        END IF;
      END IF;
      --
      -- Check Client Indicator LACPE_CLIENT_IND is supplied and valid
      --
      IF r1.lacpe_client_ind IS NULL
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',149);
      ELSIF (r1.lacpe_client_ind NOT IN ('Y','N'))
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',150);
      END IF;
      --
      -- Check Joint Client Indicator LACPE_JOINT_CLIENT_IND is supplied and valid
      --
      IF r1.lacpe_joint_client_ind IS NULL
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',151);
      ELSIF r1.lacpe_joint_client_ind NOT IN ('Y','N')
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',152);
      END IF;
      --
      -- Overcome trigger failure "HOU.ACPE_BR_IU". Advice Case Person cannot be both a client
      -- and a joint client
      --
      IF r1.lacpe_client_ind IS NOT NULL
      AND r1.lacpe_joint_client_ind IS NOT NULL
      THEN
        IF r1.lacpe_client_ind = 'Y'
        AND r1.lacpe_joint_client_ind = 'Y'
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',269);
        END IF;
      END IF;
      --
      -- Check Person Start date LACPE_START_DATE has been supplied
      --
      IF r1.lacpe_start_date IS NULL
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',153);
      END IF;
      --
      -- Check Person End date LACPE_END_DATE is valid if supplied
      --
      IF r1.lacpe_end_date IS NOT NULL
      THEN
        IF r1.lacpe_end_date < r1.lacpe_start_date
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',154);
        END IF;
      END IF;
      --
      -- The combination of Advice Case and party reference must not already exist on
      -- Advice Case People Table
      --
      IF l_acas_reference IS NOT NULL
      AND l_par_refno IS NOT NULL
      THEN
        l_acas_acpe_exists := NULL;
        OPEN chk_acas_acpe_exists (l_acas_reference, l_par_refno);
        FETCH chk_acas_acpe_exists INTO l_acas_acpe_exists;
        CLOSE chk_acas_acpe_exists;
        IF l_acas_acpe_exists IS NOT NULL
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',155);
        END IF;
      END IF;
      --
      -- Relation Code
      --
      IF r1.lacpe_hrv_frl_code IS NOT NULL
      THEN
        IF NOT s_dl_hem_utils.exists_frv('RELATION',r1.lacpe_hrv_frl_code,'Y')
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',156);
        END IF;
      END IF;
      --
      -- Check that household ind and group are correctly entered
      --
      IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'N'
      THEN
        --start Ref# 8.0
		--IF r1.lacpe_head_hhold_ind IS NULL
        --OR r1.lacpe_hhold_group_no IS NULL
        IF r1.lacpe_head_hhold_ind IS NOT NULL
        OR r1.lacpe_hhold_group_no IS NOT NULL
		--end Ref# 8.0
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',342);
        END IF;
      ELSE
        --start Ref# 8.0
	    --IF r1.lacpe_head_hhold_ind IS NOT NULL
        --OR r1.lacpe_hhold_group_no IS NOT NULL
        IF r1.lacpe_head_hhold_ind IS NULL
        OR r1.lacpe_hhold_group_no IS NULL
		--end Ref# 8.0
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',343);
        END IF;
        --
        -- Check that, within this batch and within the existing advice_case_people, there is one, and only one, head for this advice case/group
        --
        OPEN chk_only_one_head(p_batch_id,r1.lacpe_acas_alternate_ref,NVL(l_acas_reference,-1),r1.lacpe_hhold_group_no);
        FETCH chk_only_one_head INTO l_head_count;
        IF l_head_count < 1
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',349);
        ELSIF l_head_count > 1
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',350);
        END IF;
        CLOSE chk_only_one_head;
      END IF;
      --
      -- Defect 2897 Fix . This check was part of the ACPE_BR_I trigger. Because
      -- we are going to disbale it in the CREATE this check has been added as a validation check
      -- only restrict party type of FRBP where the SHARE_PARTIES parameter is not set
      --
      IF l_par_refno IS NOT NULL
      THEN
        IF NVL(fsc_utils.get_sys_param('SHARE_PARTIES'),'N') = 'N'
        THEN
          --
          -- Advice Case Person must be a HOU Person or BOTH
          --
          IF s_parties.get_par_type(l_par_refno) NOT IN ('HOUP','BOTP')
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',326);
          END IF;
        END IF;
      END IF;
      --
      -- Relation Code
      --
      IF r1.lacpe_hrv_frl_code IS NOT NULL
      THEN
        IF NOT s_dl_hem_utils.exists_frv('RELATION',r1.lacpe_hrv_frl_code,'Y')
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',156);
        END IF;
      END IF;
      --
      -- Now UPDATE the record status and process count
      --
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
  fsc_utils.proc_END;
  COMMIT;
EXCEPTION
WHEN OTHERS
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
END dataload_validate;
--
-- ***********************************************************************
--
PROCEDURE dataload_delete
  (p_batch_id       IN VARCHAR2
  ,p_date           IN DATE
  )
IS
CURSOR c1
IS
  SELECT ROWID                       rec_rowid
  ,      lacpe_dlb_batch_id
  ,      lacpe_dl_seqno
  ,      lacpe_dl_load_status
  ,      lacpe_acas_alternate_ref
  ,      lacpe_par_per_alt_ref
  FROM   dl_had_advice_case_people
  WHERE  lacpe_dlb_batch_id = p_batch_id
  AND    lacpe_dl_load_status = 'C';
CURSOR get_acas_reference
  (cp_acas_alt_reference VARCHAR2)
IS
  SELECT acas_reference
  FROM   advice_cases
  WHERE  acas_alternate_reference = TO_CHAR(cp_acas_alt_reference);
CURSOR get_par_refno
  (cp_par_per_alt_ref VARCHAR2)
IS
  SELECT par_refno
  FROM   parties
  WHERE  par_refno = cp_par_per_alt_ref;
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'DELETE';
ct               VARCHAR2(30) := 'DL_HAD_ADVICE_CASE_PEOPLE';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
l_exists         VARCHAR2(1);
l_acas_reference NUMBER(10);
l_par_refno      NUMBER(10);
i                INTEGER :=0;
l_an_tab         VARCHAR2(1);
BEGIN
  fsc_utils.proc_start('s_dl_had_advice_case_people.dataload_delete');
  fsc_utils.debug_message('s_dl_had_advice_case_people.dataload_delete',3 );
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR r1 in c1
  LOOP
    BEGIN
      cs := r1.lacpe_dl_seqno;
      l_id := r1.rec_rowid;
      i := i + 1;
      l_acas_reference := NULL;
      OPEN get_acas_reference(r1.lacpe_acas_alternate_ref);
      FETCH get_acas_reference INTO l_acas_reference;
      CLOSE get_acas_reference;
      l_par_refno := NULL;
      OPEN get_par_refno(r1.lacpe_par_per_alt_ref);
      FETCH get_par_refno INTO l_par_refno;
      CLOSE get_par_refno;
      DELETE
      FROM   advice_case_people
      WHERE  acpe_acas_reference = l_acas_reference
      AND    acpe_par_refno = l_par_refno;
      --
      -- Now UPDATE the record status and process count
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
  fsc_utils.proc_end;
  COMMIT;
EXCEPTION
WHEN OTHERS
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
END dataload_delete;
END s_dl_had_advice_case_people;
/
