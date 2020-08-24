CREATE OR REPLACE PACKAGE BODY s_dl_had_households
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    IR   24-SEP-2009  Initial Creation.
--
--
--  3.0     5.15.0    VS   17-FEB-2010  Add TO_CHAR to cursors to make sure
--                                      indexes are used correctly
--  3.1     6.14      MJK  06-DEC-2017  Noted changes done by Martin added DL_HAD_HOUSEHOLDS
--                                      update on table and has been reformatted (AJ 7th Dec)
--
--                                      Changed commit 500000 to 50000
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
  PROCEDURE set_record_status_flag
    (p_rowid  IN ROWID
    ,p_status IN VARCHAR2
    )
  AS
  BEGIN
    UPDATE dl_had_households
    SET    lhou_dl_load_status = p_status
    WHERE  rowid = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_had_households');
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
      SELECT ROWID                     rec_rowid
      ,      lhou_dlb_batch_id
      ,      lhou_dl_seqno
      ,      lhou_dl_load_status
      ,      lhou_acas_alternate_ref
      ,      lhou_acho_alternate_ref
      ,      hou_refno_seq.NEXTVAL     lhou_refno
      FROM   dl_had_households
      WHERE  lhou_dlb_batch_id   = p_batch_id
      AND    lhou_dl_load_status = 'V';
  cb                VARCHAR2(30);
  cd                DATE;
  cp                VARCHAR2(30) := 'CREATE';
  ct                VARCHAR2(30) := 'DL_HAD_HOUSEHOLDS';
  cs                INTEGER;
  ce                VARCHAR2(200);
  l_id              ROWID;
  l_an_tab          VARCHAR2(1);
  i                 INTEGER := 0;
  l_exists          VARCHAR2(1);
  l_hou_refno       NUMBER(10);
  BEGIN
    fsc_utils.proc_start('s_dl_had_households.dataload_create');
    fsc_utils.debug_message('s_dl_had_households.dataload_create',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1 
    LOOP
      BEGIN
        cs   := p1.lhou_dl_seqno;
        l_id := p1.rec_rowid;
        SAVEPOINT SP1;
        INSERT /* +APPEND */ INTO   households
        (hou_refno
        )
        VALUES 
        (p1.lhou_refno
        );
        UPDATE dl_had_households SET lhou_refno = p1.lhou_refno WHERE ROWID = p1.rec_rowid;
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
    SELECT ROWID                    rec_rowid
    ,      lhou_dlb_batch_id
    ,      lhou_dl_seqno
    ,      lhou_dl_load_status
    ,      lhou_acas_alternate_ref
    ,      lhou_acho_alternate_ref
    ,      lhou_refno
    FROM   dl_had_households
    WHERE  lhou_dlb_batch_id = p_batch_id
    AND lhou_dl_load_status in ('L','F','O');
  CURSOR chk_acas_exists(p_alternate_reference VARCHAR2) 
  IS
    SELECT 'X'
    FROM   advice_cases
    WHERE  acas_alternate_reference = TO_CHAR(p_alternate_reference);
  cb              VARCHAR2(30);
  cd              DATE;
  cp              VARCHAR2(30) := 'VALIDATE';
  ct              VARCHAR2(30) := 'DL_HAD_HOUSEHOLDS';
  cs              INTEGER;
  ce              VARCHAR2(200);
  l_id            ROWID;
  l_exists        VARCHAR2(1);
  l_acas_exists   VARCHAR2(1);
  l_errors        VARCHAR2(10);
  l_error_ind     VARCHAR2(10);
  i               INTEGER :=0;
  BEGIN
    fsc_utils.proc_start('s_dl_had_householdS.dataload_validate');
    fsc_utils.debug_message('s_dl_had_households.dataload_validate',3);
    cb := p_batch_id;
    cd := p_DATE;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs   := p1.lhou_dl_seqno;
        l_id := p1.rec_rowid;
        --
        -- Check Advice Case Alt Reference LHOP_ACAS_ALTERNATE_REF has been supplied 
        -- and exists on advice_cases.
        --  
        IF p1.lhou_acas_alternate_ref IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',113);
        ELSE
          l_acas_exists := NULL;
          OPEN chk_acas_exists(p1.lhou_acas_alternate_ref);
          FETCH chk_acas_exists INTO l_acas_exists;
          CLOSE chk_acas_exists;
          IF l_acas_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',144);
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
    ,p_date           IN date
    ) 
  IS
  CURSOR c1 
  IS
    SELECT ROWID                        rec_rowid
    ,      lhou_dlb_batch_id
    ,      lhou_dl_seqno
    ,      lhou_dl_load_status
    ,      lhou_acas_alternate_ref
    ,      lhou_acho_alternate_ref
    ,      lhou_refno
    FROM   dl_had_households
    WHERE  lhou_dlb_batch_id = p_batch_id
    AND    lhou_dl_load_status = 'C';
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'DELETE';
  ct       VARCHAR2(30) := 'DL_HAD_HOUSEHOLDS';
  cs       INTEGER;
  ce       VARCHAR2(200);
  l_id     ROWID;
  l_exists VARCHAR2(1);
  i        INTEGER :=0;
  l_an_tab VARCHAR2(1);
  BEGIN
    fsc_utils.proc_start('s_dl_had_households.dataload_delete');
    fsc_utils.debug_message('s_dl_had_households.dataload_delete',3);
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1 
    LOOP
      BEGIN
        cs := p1.lhou_dl_seqno;
        l_id := p1.rec_rowid;
        i := i + 1;
        DELETE 
        FROM   households
        WHERE  hou_refno = p1.lhou_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('HOUSEHOLDS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_had_households;
/

