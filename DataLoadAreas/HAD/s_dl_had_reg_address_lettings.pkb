CREATE OR REPLACE PACKAGE BODY HOU.s_dl_had_reg_address_lettings
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   08-FEB-2009  Initial Creation.
--
--  2.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      REAL_BR_I in CREATE Process
--
--  3.0     5.15.0    VS   17-DEC-2009  Defect 3526 Fix. Insert into interested
--                                      party usages table. Also add TO_CHAR to
--                                      make use of indexes correctly.
--
--                                      Changed commit 500000 to 50000
--
--  4.0     5.15.0    MT   22-FEB-2010  Added IF/END IF processing to NOT insert
--                                      IPU when type or ipp refno is null...otherwise
--                                      insert fails as ipp refno is mandatory.
--                                      NSW do not have any data where same ipp_refno is
--                                      has more than one RLET_REFNO, hence the update
--                                      on USAGE to set end dates to stop overlaps is
--                                      not needed.  The update statement is also not
--                                      quite right, because uniqueness is actually 
--                                      desigend for ipprefno AND RLET refno.  This
--                                      code being commented out, but has not been corrected
--                                      as not needed for NSW.
--
--  5.0     5.15.0    VS   21-SEP-2010  Defect 5955 Fix. Remove validation for SCO code 
--                                      = VIS then visit_datetime must be supplied (HD2217)
--  6.0     6.6       MK   09-AUG-2013  Altered to use legacy reference in dl_had_registered_addresses table
--  6.1     6.8       PJD  29-AUG-2013  Removed IPP fields 
--  6.2     6.9       MM   02-DEC-2013  Removed reference to real_ipp_refno
--  6.2     6.13      AJ   25-FEB-2016  1)amended create and validate to allow lrega_ins_rega_refno found to use
--                                      same conditions as the validate and also extra check to make sure only
--                                      1 is found for the combination supplied
--                                      2) added check to make sure rega_refno found exists on registered_addresses
--                                      and the dates supplied are within the start and end dates of the it.
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
    UPDATE dl_had_reg_address_lettings
    SET    lreal_dl_load_status = p_status
    WHERE  rowid                = p_rowid;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error updating status of dl_had_reg_address_lettings');
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
    SELECT ROWID rec_rowid
    ,      lreal_dlb_batch_id
    ,      lreal_dl_seqno
    ,      lreal_dl_load_status
    ,      lreal_rega_legacy_ref
    ,      lreal_rega_adre_code
    ,      lreal_rega_start_date
    ,      lreal_reference
    ,      lreal_acas_alternate_ref
    ,      lreal_sco_code
    ,      lreal_status_date
    ,      NVL(lreal_created_by,'DATALOAD') lreal_created_by
    ,      NVL(lreal_created_date,SYSDATE)  lreal_created_date
    ,      lreal_comments
    ,      lreal_start_date
    ,      lreal_end_date
    ,      lreal_proposed_end_date
    ,      lreal_visit_datetime
    ,      lreal_acho_legacy_ref
    ,      lreal_refno
    FROM   dl_had_reg_address_lettings
    WHERE  lreal_dlb_batch_id   = p_batch_id
    AND    lreal_dl_load_status = 'V';
  CURSOR get_acas_reference(p_acas_alt_reference VARCHAR2)
  IS
    SELECT acas_reference
    FROM   advice_cases
    WHERE  acas_alternate_reference = TO_CHAR(p_acas_alt_reference);
  CURSOR get_acho_reference(p_acho_reference VARCHAR2)
  IS
    SELECT acho_reference
    FROM advice_case_housing_options
    WHERE acho_alternative_reference = TO_CHAR(p_acho_reference);
  --
  CURSOR get_rega_refno
    (p_rega_legacy_ref VARCHAR2)
  IS
    SELECT lrega_ins_rega_refno  
    FROM   dl_had_registered_addresses
    WHERE  lrega_legacy_ref = p_rega_legacy_ref
    AND    lrega_dl_load_status = 'C';
  --
  CURSOR get_rega_refno2
    (p_rega_legacy_ref VARCHAR2
    ,p_rega_adre_code  VARCHAR2
    )
  IS
    SELECT DISTINCT lrega_ins_rega_refno
    FROM   dl_had_registered_addresses
    WHERE  lrega_legacy_ref = p_rega_legacy_ref
    AND    lrega_adre_code  = p_rega_adre_code
    AND    lrega_dl_load_status = 'C';
  --
  --
  -- Constants for process_summary
  --
  cb                   VARCHAR2(30);
  cd                   DATE;
  cp                   VARCHAR2(30) := 'CREATE';
  ct                   VARCHAR2(30) := 'DL_HAD_REG_ADDRESS_LETTINGS';
  cs                   INTEGER;
  ce                   VARCHAR2(200);
  l_id                 ROWID;
  l_an_tab             VARCHAR2(1);
  --
  -- Other variables
  --
  i                 INTEGER := 0;
  l_exists          VARCHAR2(1);
  l_rega_refno      NUMBER(10);
  l_acas_reference  NUMBER(10);
  l_acho_reference  NUMBER(10);
 
  BEGIN
      execute immediate 'alter trigger REAL_BR_I disable';
      fsc_utils.proc_start('s_dl_had_reg_address_lettings.dataload_create');
      fsc_utils.debug_message('s_dl_had_reg_address_lettings.dataload_create',3);
      cb := p_batch_id;
      cd := p_date;
      s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
      FOR p1 in c1 
      LOOP
        BEGIN
          cs   := p1.lreal_dl_seqno;
          l_id := p1.rec_rowid;
          SAVEPOINT SP1;
          --
          -- Get acas_reference
          --
          l_acas_reference := NULL;
          IF (p1.lreal_acas_alternate_ref IS NOT NULL) 
          THEN
            OPEN get_acas_reference(p1.lreal_acas_alternate_ref);
            FETCH get_acas_reference INTO l_acas_reference;
            CLOSE get_acas_reference;
          END IF;
          --
          -- Get acho_reference
          --
          l_acho_reference := NULL;
          IF (p1.lreal_acho_legacy_ref IS NOT NULL) 
          THEN
            OPEN get_acho_reference(p1.lreal_acho_legacy_ref);
            FETCH get_acho_reference INTO l_acho_reference;
            CLOSE get_acho_reference;
          END IF;
          --
          -- Get rega_refno
          --		  
		  -- Altered to use same conditions as check in validate section
		  -- by adding in address register code (lreal_rega_adre_code) which
		  -- is a mandatory field in the registered address data loader AJ 25feb2016
          --
          l_rega_refno := NULL;
          OPEN get_rega_refno2(p1.lreal_rega_legacy_ref, p1.lreal_rega_adre_code);
          FETCH get_rega_refno2 INTO l_rega_refno;
          CLOSE get_rega_refno2;
          --
          -- Insert into REGISTERED_ADDRESS_LETTINGS
          --
          INSERT /* +APPEND */ INTO registered_address_lettings
          (real_refno
          ,real_reference
          ,real_rega_refno
          ,real_acas_reference
          ,real_sco_code
          ,real_status_date
          ,real_reuseable_refno
          ,real_created_by
          ,real_created_date
          ,real_comments
          ,real_start_date
          ,real_end_date
          ,real_proposed_end_date
          ,real_visit_datetime
          ,real_acho_reference
          --,real_ipp_refno Removed as column no longer used
          )
          VALUES 
          (p1.lreal_refno
          ,p1.lreal_reference
          ,l_rega_refno
          ,l_acas_reference
          ,p1.lreal_sco_code
          ,p1.lreal_status_date
          ,reusable_refno_seq.NEXTVAL
          ,p1.lreal_created_by
          ,p1.lreal_created_date
          ,p1.lreal_comments
          ,p1.lreal_start_date
          ,p1.lreal_end_date
          ,p1.lreal_proposed_end_date
          ,p1.lreal_visit_datetime
          ,l_acho_reference
          --,l_ipp_refno Removed as column no longer used
          );
          --
          -- Now UPDATE the record status and process count
          --
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
    --
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('REGISTERED_ADDRESS_LETTINGS');
    execute immediate 'alter trigger REAL_BR_I enable';
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
  --
  PROCEDURE dataload_validate
    (p_batch_id          IN VARCHAR2
    ,p_date              IN DATE
    )
  AS
  CURSOR c1
  IS
    SELECT ROWID rec_rowid
    ,      lreal_dlb_batch_id
    ,      lreal_dl_seqno
    ,      lreal_dl_load_status
    ,      lreal_rega_legacy_ref
    ,      lreal_rega_adre_code
    ,      lreal_rega_start_date
    ,      lreal_reference
    ,      lreal_acas_alternate_ref
    ,      lreal_sco_code
    ,      lreal_status_date
    ,      NVL(lreal_created_by,'DATALOAD') lreal_created_by
    ,      NVL(lreal_created_date,SYSDATE)  lreal_created_date
    ,      lreal_comments
    ,      lreal_start_date
    ,      lreal_end_date
    ,      lreal_proposed_end_date
    ,      lreal_visit_datetime
    ,      lreal_acho_legacy_ref
    ,      lreal_refno
    FROM   dl_had_reg_address_lettings
    WHERE  lreal_dlb_batch_id = p_batch_id
    AND    lreal_dl_load_status in ('L','F','O');
  CURSOR chk_acas_exists
    (p_alternate_reference VARCHAR2)
  IS
    SELECT acas_reference
    FROM   advice_cases
    WHERE  acas_alternate_reference = TO_CHAR(p_alternate_reference);
  CURSOR chk_acho_exists
    (p_acho_reference VARCHAR2)
  IS
    SELECT acho_reference
    FROM   advice_case_housing_options
    WHERE  acho_alternative_reference = TO_CHAR(p_acho_reference);
  CURSOR chk_real_ref_exists
    (p_real_reference VARCHAR2)
  IS
    SELECT 'X'
    FROM   registered_address_lettings
    WHERE  real_reference = p_real_reference;
  CURSOR chk_sco_exists
    (p_sco_code VARCHAR2)
  IS
    SELECT 'X'
    FROM   status_codes
    WHERE  sco_code = p_sco_code; 
  --
  CURSOR chk_rega_exists
    (p_rega_legacy_ref VARCHAR2
    ,p_rega_adre_code  VARCHAR2
    )
  IS
    SELECT 'X'
    FROM   dl_had_registered_addresses
    WHERE  lrega_legacy_ref = p_rega_legacy_ref
    AND    lrega_adre_code  = p_rega_adre_code
    AND    lrega_dl_load_status = 'C';
  --
  CURSOR chk_rega_count
    (p_rega_legacy_ref VARCHAR2
    ,p_rega_adre_code  VARCHAR2
    )
  IS
    SELECT COUNT(DISTINCT lrega_ins_rega_refno)
    FROM   dl_had_registered_addresses
    WHERE  lrega_legacy_ref = p_rega_legacy_ref
    AND    lrega_adre_code  = p_rega_adre_code
    AND    lrega_dl_load_status = 'C';
  --
  CURSOR chk_rega_refno
    (p_rega_legacy_ref VARCHAR2
    ,p_rega_adre_code  VARCHAR2
    )
  IS
    SELECT DISTINCT lrega_ins_rega_refno
    FROM   dl_had_registered_addresses
    WHERE  lrega_legacy_ref = p_rega_legacy_ref
    AND    lrega_adre_code  = p_rega_adre_code
    AND    lrega_dl_load_status = 'C';
  --
  CURSOR chk_rega_refno2(p_rega_refno NUMBER)
  IS
    SELECT rega_start_date, rega_end_date
    FROM   registered_addresses
    WHERE  rega_refno = p_rega_refno;
  --



  --
  -- Constants for process_summary
  --
  cb       VARCHAR2(30);
  cd       DATE;
  cp       VARCHAR2(30) := 'VALIDATE';
  ct       VARCHAR2(30) := 'DL_HAD_REG_ADDRESS_LETTINGS';
  cs       INTEGER;
  ce       VARCHAR2(200);
  l_id     ROWID;
  --
  -- Other variables
  --
  l_exists          VARCHAR2(1);
  l_real_ref_exists VARCHAR2(1);
  l_sco_exists      VARCHAR2(1);
  l_acas_reference  NUMBER(10);
  l_acho_reference  NUMBER(10);
  l_rega_exists     VARCHAR2(1);
  l_errors          VARCHAR2(10);
  l_error_ind       VARCHAR2(10);
  i                 INTEGER :=0;
  l_rega_count      INTEGER :=0;
  l_rega_refno      NUMBER(10);
  lrega_start_date  DATE;
  lrega_end_date    DATE;
  BEGIN
    fsc_utils.proc_start('s_dl_had_reg_address_lettings.dataload_validate');
    fsc_utils.debug_message('s_dl_had_reg_address_lettings.dataload_validate',3);
    cb := p_batch_id;
    cd := p_DATE;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 IN c1 
    LOOP
      BEGIN
        cs   := p1.lreal_dl_seqno;
        l_id := p1.rec_rowid;
        l_errors := 'V';
        l_error_ind := 'N';
        --
        -- Check the Registered Address Legacy reference LREAL_REGA_LEGACY_REF is supplied and valid
        --
        IF p1.lreal_rega_legacy_ref IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',212);
        END IF;
        --
        -- Check the Address register code LREAL_REGA_ADRE_CODE is supplied and valid
        --
        IF (p1.lreal_rega_adre_code IS NULL) 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',213);
        END IF;
        --
        -- Check the Registered Address Lettings reference REAL_REFERENCE is
        -- supplied and does not already exists on registered_address_lettings table
        --
        IF (p1.lreal_reference IS NULL) 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',214);
        ELSE
          l_real_ref_exists := NULL;
          OPEN chk_real_ref_exists(p1.lreal_reference);
          FETCH chk_real_ref_exists INTO l_real_ref_exists;
          CLOSE chk_real_ref_exists;
          IF l_real_ref_exists IS NOT NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',215);
          END IF;
        END IF;
        --
        -- Check the Status code LREAL_SCO_CODE is supplied and valid
        --
        IF p1.lreal_sco_code IS NULL 
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',047);
        ELSE
          l_sco_exists := NULL;
          OPEN chk_sco_exists(p1.lreal_sco_code);
          FETCH chk_sco_exists INTO l_sco_exists;
          CLOSE chk_sco_exists;
          IF l_sco_exists IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',014);
          END IF;
          IF p1.lreal_sco_code = 'ACC' 
          AND p1.lreal_start_date IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',216);
          END IF;
        END IF;
        --
        -- Check Advice Case Alt Reference LREAL_ACAS_ALTERNATE_REF has been supplied and exists on advice_cases.
        --
        IF p1.lreal_acas_alternate_ref IS NOT NULL
        THEN
          l_acas_reference := NULL;
          OPEN chk_acas_exists(p1.lreal_acas_alternate_ref);
          FETCH chk_acas_exists INTO l_acas_reference;
          CLOSE chk_acas_exists;
          IF l_acas_reference IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',144);
          END IF;
        END IF;
        --
        -- Check Housing Options Reference LACHH_ACHO_LEGACY_REF has been supplied and is valid
        --
        IF p1.lreal_acho_legacy_ref IS NOT NULL 
        THEN
          l_acho_reference := NULL;
          OPEN chk_acho_exists(p1.lreal_acho_legacy_ref);
          FETCH chk_acho_exists INTO l_acho_reference;
          CLOSE chk_acho_exists;
          IF l_acho_reference IS NULL 
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',185);
          END IF;
        END IF;
        --
        -- Check that either the Advice Case Reference or the Housing Option Reference has been supplied
        --
        IF p1.lreal_acas_alternate_ref IS NULL
        AND p1.lreal_acho_legacy_ref IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',186);
        END IF;
        --
        -- Check that not both the Advice Case Reference or the Housing Option Reference has been supplied
        --
        IF p1.lreal_acas_alternate_ref IS NOT NULL
        AND p1.lreal_acho_legacy_ref    IS NOT NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',218);
        END IF;
        --
        -- Check If supplied, the End Date LREAL_END_DATE must not be before the Start Date LREAL_START_DATE
        --
        IF p1.lreal_end_date IS NOT NULL
        AND p1.lreal_end_date <= p1.lreal_start_date
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',003);
        END IF;
        --
        -- Check If supplied, the proposed end Date LREAL_PROPOSED_END_DATE must not 
        -- be before the Start Date LREAL_START_DATE
        --
        IF p1.lreal_proposed_end_date IS NOT NULL
        AND p1.lreal_proposed_end_date <= p1.lreal_start_date
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',219);
        END IF;
        --
        -- Check If supplied, the proposed end Date LREAL_PROPOSED_END_DATE must not 
        -- be before the Visit Date/Time LREAL_VISIT_DATETIME
        --
        IF p1.lreal_proposed_end_date IS NOT NULL
        AND p1.lreal_visit_datetime IS NOT NULL
        AND p1.lreal_proposed_end_date <= p1.lreal_visit_datetime
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',220);
        END IF;
        --
        -- There must be a record on the Registered Address table for the
        -- Legacy Reference LREAL_REGA_LEGACY_REF and Address Register Code
        -- LREAL_REGA_ADRE_CODE supplied.
        -- Added further check to make sure that thos combination has no more than 1
        -- lrega_ins_rega_refno against it AJ 25Feb2016
        -- 
        IF p1.lreal_rega_legacy_ref IS NOT NULL
        AND p1.lreal_rega_adre_code IS NOT NULL
        THEN
          l_rega_exists := NULL;
          lrega_start_date := NULL;
          lrega_end_date := NULL;
          OPEN chk_rega_exists (p1.lreal_rega_legacy_ref, p1.lreal_rega_adre_code);
          FETCH chk_rega_exists INTO l_rega_exists;
          CLOSE chk_rega_exists;
          IF l_rega_exists IS NULL
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',266);
          END IF;
        --
          l_rega_count :=0;
          OPEN chk_rega_count (p1.lreal_rega_legacy_ref, p1.lreal_rega_adre_code);
          FETCH chk_rega_count INTO l_rega_count;
          CLOSE chk_rega_count;
          IF (l_rega_count > 1)
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',852);
          END IF;
          IF (l_rega_count = 0)
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',853);
          END IF;
        --
          IF (l_rega_count = 1)
          THEN
        --
            l_rega_refno := NULL;
            OPEN chk_rega_refno(p1.lreal_rega_legacy_ref, p1.lreal_rega_adre_code);
            FETCH chk_rega_refno INTO l_rega_refno;
            CLOSE chk_rega_refno;
          --
            OPEN chk_rega_refno2(l_rega_refno);
            FETCH chk_rega_refno2 INTO lrega_start_date, lrega_end_date;
            CLOSE chk_rega_refno2;
          --
            IF (lrega_start_date IS NULL)
             THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',854);
            END IF;
          --
            IF (p1.lreal_start_date < lrega_start_date)
             THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',855);
            END IF;
          --
           IF p1.lreal_end_date IS NOT NULL
           AND p1.lreal_end_date <= lrega_start_date
            THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',856);
           END IF;		  
          --		  
           IF p1.lreal_end_date IS NOT NULL
           AND p1.lreal_end_date > NVL(lrega_end_date, TO_DATE('01-JAN-2099','DD-MON-YYYY'))
            THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',857);
           END IF;
          --
           IF p1.lreal_proposed_end_date IS NOT NULL
           AND p1.lreal_proposed_end_date <= lrega_start_date
            THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',858);
           END IF;
          --
           IF p1.lreal_proposed_end_date IS NOT NULL
           AND p1.lreal_proposed_end_date  > NVL(lrega_end_date, TO_DATE('01-JAN-2099','DD-MON-YYYY'))
            THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',859);
           END IF;
          --
		  END IF;
        END IF;
        --
        -- All reference values supplied are valid
        --
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
    ,p_date           IN date
    ) 
  IS
  CURSOR c1 
  IS
    SELECT ROWID rec_rowid
    ,      lreal_dlb_batch_id
    ,      lreal_dl_seqno
    ,      lreal_dl_load_status 
    ,      lreal_start_date
    ,      lreal_refno
    FROM   dl_had_reg_address_lettings
    WHERE  lreal_dlb_batch_id = p_batch_id
    AND    lreal_dl_load_status = 'C';
   
  --
  -- Constants FOR process_summary
  --
  cb         VARCHAR2(30);
  cd         DATE;
  cp         VARCHAR2(30) := 'DELETE';
  ct         VARCHAR2(30) := 'DL_HAD_REG_ADDRESS_LETTINGS';
  cs         INTEGER;
  ce         VARCHAR2(200);
  l_id       ROWID;
  l_an_tab   VARCHAR2(1);
  --
  -- Other variables
  --
  l_exists          VARCHAR2(1);
  i                 INTEGER :=0;
  BEGIN
    fsc_utils.proc_start('s_dl_had_reg_address_lettings.dataload_delete');
    fsc_utils.debug_message('s_dl_had_reg_address_lettings.dataload_delete',3 );
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
    FOR p1 in c1 
    LOOP
      BEGIN
        cs := p1.lreal_dl_seqno;
        l_id := p1.rec_rowid;
        i := i + 1;
        --
        -- Delete from interested_party_usages
        --
        DELETE FROM interested_party_usages
        WHERE ipus_real_refno = p1.lreal_refno;
        --
        -- Delete from registered_address_lettings table
        --
        DELETE FROM registered_address_lettings
        WHERE real_refno = p1.lreal_refno;
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
    --
    -- Section to anayze the table(s) populated by this dataload
    --
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('REGISTERED_ADDRESS_LETTINGS');
    fsc_utils.proc_end;
    COMMIT;
  EXCEPTION
  WHEN OTHERS 
  THEN
    s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
    RAISE;
  END dataload_delete;
END s_dl_had_reg_address_lettings;
/
