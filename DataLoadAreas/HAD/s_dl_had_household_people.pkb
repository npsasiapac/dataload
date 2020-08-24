CREATE OR REPLACE PACKAGE BODY s_dl_had_household_people
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    IR   22-SEP-2009  Initial Creation.
--
--  2.0     5.15.0    VS   17-FEB-2010  Add TO_CHAR to cursors to make sure
--                                      indexes are used correctly
--  2.1     6.14      MJK  14-NOV-2017  Added 'LACPE_HEAD_HHOLD_IND'and 'LACPE_HHOLD_GROUP_NO'
--                                      This was added to change control note on the 7th Dec(AJ)
--
--                                      Changed commit 500000 to 50000
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
  PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
  AS
  BEGIN
    UPDATE dl_had_household_people
    SET    lhop_dl_load_status = p_status
    WHERE  rowid                = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_had_household_people');
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
    SELECT ROWID           rec_rowid
    ,      lhop_dlb_batch_id
    ,      lhop_dl_seqno
    ,      lhop_dl_load_status
    ,      lhop_acas_alternate_ref
    ,      lhop_acho_alternate_ref
    ,      lhop_par_per_alt_ref
    ,      lhop_start_date
    ,      lhop_end_date
    ,      lhop_hrv_frl_code
    ,      lhop_refno
    ,      lhop_hrv_hpsr_code
    ,      lhop_hrv_hper_code
    ,      lhop_head_hhold_ind
    ,      lhop_hhold_group_no
    FROM   dl_had_household_people
    WHERE  lhop_dlb_batch_id = p_batch_id
    AND    lhop_dl_load_status = 'V';
  CURSOR get_hou_refno  
    (p_acas_reference    VARCHAR2
    ,p_acho_reference    VARCHAR2
    ) 
  IS
    SELECT lhou_refno
    FROM   dl_had_households
    WHERE  lhou_acas_alternate_ref = p_acas_reference
    AND    lhou_acho_alternate_ref = p_acho_reference
    AND    lhou_dl_load_status = 'C';
  cb          VARCHAR2(30);      
  cd          DATE;      
  cp          VARCHAR2(30) := 'CREATE';      
  ct          VARCHAR2(30) := 'DL_HAD_HOUSEHOLD_PEOPLE';      
  cs          INTEGER;      
  ce          VARCHAR2(200);      
  l_id        ROWID;      
  l_an_tab    VARCHAR2(1);      
  i           INTEGER := 0;      
  l_exists    VARCHAR2(1);      
  l_hou_refno NUMBER(10);      
BEGIN
    fsc_utils.proc_start('s_dl_had_household_people.dataload_create');
    fsc_utils.debug_message('s_dl_had_household_people.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1 
    LOOP
      BEGIN
        cs   := p1.lhop_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        l_hou_refno := NULL;
        OPEN get_hou_refno(p1.lhop_acas_alternate_ref, p1.lhop_acho_alternate_ref);
        FETCH get_hou_refno into l_hou_refno;
        CLOSE get_hou_refno;
        INSERT /* +APPEND */ INTO household_persons
        (hop_refno
        ,hop_hou_refno
        ,hop_par_refno
        ,hop_start_date
        ,hop_end_date
        ,hop_hrv_rel_code
        ,hop_hrv_hpsr_code
        ,hop_hrv_hper_code
        ,hop_head_hhold_ind
        ,hop_hhold_group_no
        )
        VALUES 
        (p1.lhop_refno
        ,l_hou_refno
        ,p1.lhop_par_per_alt_ref
        ,p1.lhop_start_date
        ,p1.lhop_end_date
        ,p1.lhop_hrv_frl_code
        ,p1.lhop_hrv_hpsr_code
        ,p1.lhop_hrv_hper_code                                                                                                    
        ,p1.lhop_head_hhold_ind
        ,p1.lhop_hhold_group_no
        );
        i := i + 1; 
        IF MOD(i,50000) = 0 
        THEN 
          COMMIT; 
        END IF;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOUSEHOLD_PERSONS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOUSEHOLDS');
    fsc_utils.proc_END;
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
    SELECT ROWID             rec_rowid
    ,      lhop_dlb_batch_id
    ,      lhop_dl_seqno
    ,      lhop_dl_load_status
    ,      lhop_acas_alternate_ref
    ,      lhop_acho_alternate_ref
    ,      lhop_par_per_alt_ref
    ,      lhop_start_date
    ,      lhop_end_date
    ,      lhop_hrv_frl_code
    ,      lhop_refno
    ,      lhop_hrv_hpsr_code
    ,      lhop_hrv_hper_code
    ,      lhop_head_hhold_ind
    ,      lhop_hhold_group_no
    FROM   dl_had_household_people
    WHERE  lhop_dlb_batch_id = p_batch_id
    AND    lhop_dl_load_status in ('L','F','O');
  CURSOR chk_par_exists(p_par_refno VARCHAR2) 
  IS
    SELECT 'X'
    FROM   parties
    WHERE  par_refno = p_par_refno;
  CURSOR chk_acas_exists(p_alternate_reference VARCHAR2) 
  IS
    SELECT 'X'
    FROM   advice_cases
    WHERE  acas_alternate_reference = TO_CHAR(p_alternate_reference);
  CURSOR chk_hou_exists
    (p_acas_reference VARCHAR2
    ,p_acho_reference VARCHAR2 
    )
  IS
    SELECT 'X'
    FROM   dl_had_households
    WHERE  lhou_acas_alternate_ref = p_acas_reference
    AND    lhou_acho_alternate_ref = p_acho_reference
    AND    lhou_dl_load_status = 'C';
  CURSOR chk_only_one_head
    (cp_batch_id            dl_had_household_people.lhop_dlb_batch_id%TYPE
    ,cp_acas_reference      dl_had_household_people.lhop_acas_alternate_ref%TYPE
    ,cp_acho_reference      dl_had_household_people.lhop_acho_alternate_ref%TYPE
    ,cp_hhold_group_no      dl_had_household_people.lhop_hhold_group_no%TYPE
    )
  IS
    SELECT COUNT(*)
    FROM   (SELECT 'X'
            FROM   dl_had_household_people 
            WHERE  lhop_dlb_batch_id = cp_batch_id
            AND    lhop_acas_alternate_ref = cp_acas_reference
            AND    lhop_acho_alternate_ref = cp_acho_reference
            AND    lhop_hhold_group_no = cp_hhold_group_no
            AND    lhop_head_hhold_ind = 'Y'
            UNION ALL
            SELECT 'X'
            FROM   household_persons
            WHERE  hop_hou_refno = 
                     (SELECT lhou_refno
                      FROM   dl_had_households
                      WHERE  lhou_acas_alternate_ref = cp_acas_reference
                      AND    lhou_acho_alternate_ref = cp_acho_reference
                      AND    lhou_dl_load_status = 'C'
                     )
            AND    hop_hhold_group_no = cp_hhold_group_no
            AND    hop_head_hhold_ind = 'Y'                
           );
  cb             VARCHAR2(30);
  cd             DATE;
  cp             VARCHAR2(30) := 'VALIDATE';
  ct             VARCHAR2(30) := 'DL_HAD_HOUSEHOLD_PEOPLE';
  cs             INTEGER;
  ce             VARCHAR2(200);
  l_id           ROWID;
  l_exists       VARCHAR2(1);
  l_acas_exists  VARCHAR2(1);
  l_hou_exists   VARCHAR2(1);
  l_par_exists   VARCHAR2(1);
  l_errors       VARCHAR2(10);
  l_error_ind    VARCHAR2(10);
  i              INTEGER := 0;
  l_head_count   INTEGER := 0;
  BEGIN
    fsc_utils.proc_start('s_dl_had_household_people.dataload_validate');
    fsc_utils.debug_message('s_dl_had_household_people.dataload_validate',3);
    cb := p_batch_id;
    cd := p_DATE;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs := p1.lhop_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        IF p1.lhop_acas_alternate_ref IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
        ELSE
          l_acas_exists := NULL;
          OPEN chk_acas_exists(p1.lhop_acas_alternate_ref);
          FETCH chk_acas_exists INTO l_acas_exists;
          CLOSE chk_acas_exists;
          IF l_acas_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',144);
          END IF;            
        END IF;
        l_par_exists := NULL;
        OPEN chk_par_exists (p1.LHOP_PAR_PER_ALT_REF);
        FETCH chk_par_exists INTO l_par_exists;
        CLOSE chk_par_exists;
        IF l_par_exists IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',315);
        END IF;
        l_hou_exists := NULL;
        OPEN chk_hou_exists (p1.LHOP_ACAS_ALTERNATE_REF, p1.LHOP_ACHO_ALTERNATE_REF);
        FETCH chk_hou_exists INTO l_hou_exists;
        CLOSE chk_hou_exists;
        IF l_hou_exists IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',316);
        END IF;
        IF NVL(p1.lhop_end_date,SYSDATE + 1) < p1.lhop_start_date
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',003);
        END IF;        
        --
        -- Check that household ind and group are correctly entered
        --
        IF fsc_utils.get_sys_param('HOUSEHOLD_GROUPINGS_REQD') = 'N'
        THEN
          IF p1.lhop_head_hhold_ind IS NULL
          OR p1.lhop_hhold_group_no IS NULL
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',342);
          END IF;
        ELSE
          IF p1.lhop_head_hhold_ind IS NOT NULL
          OR p1.lhop_hhold_group_no IS NOT NULL
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',343);
          END IF;
          --
          -- Check that, within this batch and within the existing household_people, there is one, and only one, head for this household/group
          -- 
          OPEN chk_only_one_head(p_batch_id,p1.lhop_acas_alternate_ref,p1.lhop_acho_alternate_ref,p1.lhop_hhold_group_no);
          FETCH chk_only_one_head INTO l_head_count;
          IF l_head_count < 1
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',357);
          ELSIF l_head_count > 1
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',358);        
          END IF;
          CLOSE chk_only_one_head;
        END IF;        
        -- 
        -- Check relationship code exists 
        --
        IF p1.lhop_hrv_frl_code IS NOT NULL
        THEN
           IF NOT s_dl_hem_utils.exists_frv('RELATION',p1.lhop_hrv_frl_code,'Y')
           THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',156);
           END IF;
        END IF;
        --
        -- Check household start reason code exists 
        --
        IF p1.lhop_hrv_hpsr_code IS NOT NULL
        THEN
          IF NOT s_dl_hem_utils.exists_frv('HLD_START',p1.lhop_hrv_hpsr_code,'Y')
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',847);
          END IF;
        END IF;
        --
        -- Check household end reason code exists 
        --
        IF p1.lhop_hrv_hper_code IS NOT NULL
        THEN
          IF NOT s_dl_hem_utils.exists_frv('HLD_END',p1.lhop_hrv_hper_code,'Y')
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',848);
          END IF;
        END IF;
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
  CURSOR c1 is
    SELECT ROWID       rec_rowid
    ,      lhop_dlb_batch_id
    ,      lhop_dl_seqno
    ,      lhop_dl_load_status
    ,      lhop_acas_alternate_ref
    ,      lhop_acho_alternate_ref
    ,      lhop_par_per_alt_ref
    ,      lhop_start_date
    ,      lhop_end_date
    ,      lhop_hrv_frl_code
    ,      lhop_refno
    FROM   dl_had_household_people
    WHERE  lhop_dlb_batch_id   = p_batch_id
    AND    lhop_dl_load_status = 'C';
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'DELETE';
  ct       VARCHAR2(30) := 'DL_HAD_HOUSEHOLD_PEOPLE';
  cs       INTEGER;
  ce       VARCHAR2(200);
  l_id     ROWID;
  l_exists VARCHAR2(1);
  i        INTEGER :=0;
  l_an_tab VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_had_household_people.dataload_delete');
    fsc_utils.debug_message('s_dl_had_household_people.dataload_delete',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1 
    LOOP
      BEGIN
        cs   := p1.lhop_dl_seqno;
        l_id := p1.rec_rowid;
        i    := i + 1;
        DELETE 
        FROM   household_persons
        WHERE  hop_refno = p1.lhop_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOUSEHOLD_PERSONS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_had_household_people;
/

