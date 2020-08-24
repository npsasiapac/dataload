CREATE OR REPLACE PACKAGE BODY s_dl_hem_person_also_known_as
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       WHY
--      1.0  6.18     JT   23/01/2019 Initial Creation for SAHT
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
  UPDATE dl_hem_person_also_known_as
  SET lpaka_dl_load_status = p_status
  WHERE rowid = p_rowid;
--
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_person_also_known_as');
     RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--  Package Specific Functions
--
--  determine_par_refno is used to try to match an existing person record using 
--  potential data sources in this dataload.
PROCEDURE determine_par_refno
    ( p_per_alt_ref          IN VARCHAR2   := NULL
    , p_per_forename         IN VARCHAR2   := NULL
    , p_per_surname          IN VARCHAR2   := NULL
    , p_det_par_refno        OUT INTEGER   
    , p_e1                   OUT VARCHAR2  )
AS
  l_cnt                 INTEGER := 0;
  l_det_par_refno       INTEGER := NULL;
  l_combo               VARCHAR2(5)        ;
  l_exception           VARCHAR2(5);
BEGIN
      IF (p_per_alt_ref IS NULL AND (p_per_forename IS NOT NULL AND p_per_surname IS NOT NULL))
        THEN       -- only per_forename and per_surname provided
          SELECT par_refno INTO l_det_par_refno FROM parties
          WHERE par_per_forename = p_per_forename
          AND par_per_surname = p_per_surname;
          l_combo := 'FS';
      ELSIF (p_per_alt_ref IS NOT NULL AND p_per_forename IS NOT NULL AND p_per_surname IS NOT NULL)
        THEN      -- per_alt_ref, per_forename and per_surname provided
          SELECT par_refno INTO l_det_par_refno FROM parties
          WHERE par_per_alt_ref = p_per_alt_ref
          AND par_per_forename = p_per_forename
          AND par_per_surname = p_per_surname;
          l_combo := 'AFS';
      ELSIF (p_per_alt_ref IS NOT NULL AND (p_per_forename IS NULL AND p_per_surname IS NULL))
        THEN      -- only per_alt_ref provided
          SELECT par_refno INTO l_det_par_refno FROM parties
          WHERE par_per_alt_ref = p_per_alt_ref;
          l_combo := 'A';
      ELSE
        -- Not enough information provided
        l_exception := 'NDP';
        l_combo:=NULL;
      END IF;
      p_det_par_refno := l_det_par_refno;
--    
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      p_det_par_refno := NULL;
      l_exception := 'NDF';
      p_e1 := l_exception|| ':' ||l_combo;
    WHEN TOO_MANY_ROWS THEN
      --dbms_output.put_line('TMR');
      p_det_par_refno := NULL;
      l_exception := 'TMR';
      p_e1 := l_exception|| ':' ||l_combo;
    WHEN OTHERS THEN
      p_det_par_refno := NULL;
      l_exception := 'OTH';
      --dbms_output.put_line(SQLERRM);
      p_e1 := l_exception|| ':' ||l_combo;
      RAISE;
--
END determine_par_refno;

--
PROCEDURE dataload_create
   (p_batch_id          IN VARCHAR2
   ,p_date              IN DATE    )
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid,
lpaka_dlb_batch_id,
lpaka_dl_seqno,
lpaka_dl_load_status,
lpaka_refno,
lpaka_par_refno,
lpaka_per_alt_ref,
lpaka_per_forename,
lpaka_per_surname,
lpaka_frv_akar_code,
lpaka_start_date,
lpaka_created_date,
lpaka_created_by,
lpaka_forename,
lpaka_surname,
lpaka_end_date,
lpaka_comments
FROM  dl_hem_person_also_known_as
WHERE lpaka_dlb_batch_id   = p_batch_id
AND   lpaka_dl_load_status = 'V';
--
-- *****************************
--
CURSOR c_next_paka_refno IS
SELECT paka_refno_seq.nextval
FROM dual;
-- *****************************
--
CURSOR c_chk_for_pk (p_paka_refno INTEGER) IS
SELECT 'X'
FROM person_also_known_as
WHERE paka_refno = p_paka_refno;
-- *******************
--
CURSOR c_chk_for_par (p_par_refno INTEGER) IS
SELECT 'X'
FROM parties
WHERE par_refno = p_par_refno;
-- *****************************
--
CURSOR c_chk_for_dup (p_par_refno INTEGER, p_forename VARCHAR2, p_surname VARCHAR2, p_start_date DATE) IS
SELECT count(*)
FROM person_also_known_as
WHERE paka_par_refno = p_par_refno
AND coalesce(paka_forename,'NUL') = coalesce(p_forename,'NUL')
AND coalesce(paka_surname,'NUL') = coalesce(p_surname,'NUL')
AND paka_start_date = coalesce(p_start_date,trunc(sysdate));
-- *******************
--
CURSOR c_chk_akar_code (p_akar_code VARCHAR2) IS
SELECT 'X'
FROM first_ref_values
WHERE frv_frd_domain = 'ALSOKNOWNASREASON'
AND frv_code = p_akar_code;
-- *******************
--
CURSOR c2 (p_paka_refno INTEGER)
IS
SELECT NULL
FROM   person_also_known_as
WHERE  paka_refno = p_paka_refno
--ORDER  BY paka_created_date DESC
FOR    UPDATE OF paka_created_date;
-- *****************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_PERSON_ALSO_KNOWN_AS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
l_an_tab         VARCHAR2(1);
i                INTEGER := 0;
l_paka_refno	 person_also_known_as.paka_refno%TYPE;
l_created_date   person_also_known_as.paka_created_date%TYPE;
l_created_by     person_also_known_as.paka_created_by%TYPE;
p2               c2%ROWTYPE;
l_chk_for_pk	 VARCHAR2(1);
l_chk_for_dup    INTEGER;
l_chk_akar   VARCHAR2(1);
l_chk_for_par	   VARCHAR2(1);
l_det_par_refno  INTEGER;
l_par_lkup_err   VARCHAR2(10);
--
-- *****************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_person_also_known_as.dataload_create');
fsc_utils.debug_message( 's_dl_hem_person_also_known_as.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lpaka_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'C';
  l_error_ind := 'N';
  l_paka_refno    := NULL;
  l_created_date  := NULL;
  l_created_by    := NULL;
  p2              := NULL;
  l_chk_for_dup   := NULL;
  l_chk_for_pk    := NULL;
  l_chk_akar      := NULL;
  l_chk_for_par   := NULL;

--
  SAVEPOINT SP1;
--
-- Get paka_refno
  OPEN c_next_paka_refno;
  FETCH c_next_paka_refno INTO l_paka_refno;
  CLOSE c_next_paka_refno;
--
-- Check make sure record will not violate PK
-- (PK) PAKA_PK => PAKA_REFNO (UK)
--
  OPEN  c_chk_for_pk(l_paka_refno);
  FETCH c_chk_for_pk INTO l_chk_for_pk;
  CLOSE c_chk_for_pk;
--
--
 IF (p1.lpaka_forename IS NULL AND p1.lpaka_surname IS NULL)
  THEN
    l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',669);
--  'At least one of Also Known As FORENAME or SURNAME is required.'
 END IF;
-- 
--
  OPEN  c_chk_for_dup(p1.lpaka_par_refno, p1.lpaka_forename, p1.lpaka_surname, p1.lpaka_start_date);
  FETCH c_chk_for_dup INTO l_chk_for_dup;
  CLOSE c_chk_for_dup;
--
-- Raise an error if duplicate found after previously passing validation
--
  IF (l_chk_for_pk IS NOT NULL)
   THEN
    l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',670);
--  'The paka_refno already exists.'
  END IF;
  IF (l_chk_for_dup > 0)
   THEN
    l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',671);
--  'This combination of Also Known As information already exists for this person.'
  END IF;
--
  OPEN c_chk_akar_code(p1.lpaka_frv_akar_code);
  FETCH c_chk_akar_code INTO l_chk_akar;
  CLOSE c_chk_akar_code;
--
-- Raise an Error if the AKAR code is not in the reference domain .
--
  IF (l_chk_akar IS NULL)
   THEN
    l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',672);
--  'The Reason Code given does not exist in the Reference Domain ALSOKNOWNASREASON.'
  END IF;
--
-- Perform the same lookup check for the par_refno here as we do at validation 
-- as it is possible for data consistency to change between validate and create
--
-- As part of the dataload a user can supply a combination of person details.
-- Either Party Refno used irrespective of other provided data
-- OR combination of ALT_REF, FORENAME and SURNAME, ALT_REF and FORENAME and SURNAME.
--
  IF (p1.lpaka_par_refno  IS NOT NULL)
   THEN
    OPEN c_chk_for_par(p1.lpaka_par_refno);
    FETCH c_chk_for_par INTO l_chk_for_par;
    CLOSE c_chk_for_par;
    IF (l_chk_for_par IS NULL)
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',583);
  --    'The Person does not exist.'
      ELSE
        l_det_par_refno := p1.lpaka_par_refno;
    END IF;
--
  ELSE
    determine_par_refno( p1.lpaka_per_alt_ref
                        , p1.lpaka_per_forename
                        , p1.lpaka_per_surname
                        , l_det_par_refno
                        , l_par_lkup_err);
--
    IF (l_det_par_refno IS NULL)
      THEN
        --'Unable to associate to party'
        IF (substr(l_par_lkup_err,1,3) = 'TMR' )
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',674);
            -- 'Multiple People match with the details provided.'
        ELSIF (substr(l_par_lkup_err,1,3) = 'NDF')
          THEN
            IF substr(l_par_lkup_err,5) = 'A' 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',243);
                -- hdl 243 'Alternative person reference does not exist on PEOPLE'
            ELSE
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',675);
              -- 'No People match using the details provided.'
            END IF;
        ELSIF (l_par_lkup_err = 'NDP:')
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',676);
            -- 'No enough information provided to attempt a person lookup'.
        END IF;
    END IF;

  
  END IF;
--
-- End of Create process validation.
--
-- l_errors will no longer be 'C' if any of the above checks failed.
  IF (l_errors = 'C')
   THEN
--
-- Check and set defaults fields
--
   IF (p1.lpaka_created_date IS NULL)
    THEN
     l_created_date := SYSDATE;
   ELSE
     l_created_date := p1.lpaka_created_date;
   END IF;
--
   IF (p1.lpaka_created_by IS NULL)
    THEN
     l_created_by := 'DATALOAD';
   ELSE
     l_created_by := p1.lpaka_created_by;
   END IF;
--
--
   INSERT INTO person_also_known_as
          (PAKA_REFNO,
		   PAKA_PAR_REFNO,
		   PAKA_FRV_AKAR_CODE,
		   PAKA_START_DATE,
		   PAKA_CREATED_DATE,
		   PAKA_CREATED_BY,
		   PAKA_FORENAME,
		   PAKA_SURNAME,
		   PAKA_END_DATE,
		   PAKA_COMMENTS)
    VALUES
          (l_paka_refno,
           l_det_par_refno,
           p1.lpaka_frv_akar_code,
           p1.lpaka_start_date,
           l_created_date,
           l_created_by,
           p1.lpaka_forename,
           p1.lpaka_surname,
           p1.lpaka_end_date,
           p1.lpaka_comments);

--
-- We need to capture the paka_refno back into the dataload batch record
--
   UPDATE dl_hem_person_also_known_as
   SET lpaka_refno = l_paka_refno
   WHERE rowid = l_id;
--   
--
-- Now update record with created date as trigger PAKA_BR_I sets it to sysdate
-- only needed if creation date supplied otherwise sysdate is fine   
--
   IF (p1.lpaka_created_date IS NOT NULL)
    THEN
     OPEN  c2 (l_paka_refno);
     FETCH c2 INTO p2;
       IF    c2%FOUND
        THEN
         UPDATE person_also_known_as
         SET    paka_created_date = l_created_date
               ,paka_created_by = l_created_by
         WHERE  CURRENT OF c2;
       END IF;
     CLOSE c2;
   END IF;
--
  END IF;
--
-- *****************************
-- keep a count of the rows processed and commit after every 1000
--
-- Now UPDATE the record count and error code
--
  IF l_errors = 'F' THEN
    l_error_ind := 'Y';
  ELSE
    l_error_ind := 'N';
  END IF;
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
    ROLLBACK TO SP1;
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    set_record_status_flag(l_id,'O');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  END;
--
 END LOOP;
--
-- *****************************
-- Section to analyse the tables populated with this data load
--
  l_an_tab := s_dl_hem_utils.dl_comp_stats('PERSON_ALSO_KNOWN_AS');
--
fsc_utils.proc_end;
COMMIT;
--
 EXCEPTION
  WHEN OTHERS THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
--
END dataload_create;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid,
lpaka_dlb_batch_id,
lpaka_dl_seqno,
lpaka_dl_load_status,
lpaka_refno,
lpaka_par_refno,
lpaka_per_alt_ref,
lpaka_per_forename,
lpaka_per_surname,
lpaka_frv_akar_code,
lpaka_start_date,
lpaka_created_date,
lpaka_created_by,
lpaka_forename,
lpaka_surname,
lpaka_end_date,
lpaka_comments
FROM  dl_hem_person_also_known_as
WHERE lpaka_dlb_batch_id   = p_batch_id
AND   lpaka_dl_load_status IN ('L','F','O');
--
-- *****************************
--
CURSOR c_chk_for_dup (p_par_refno INTEGER, p_forename VARCHAR2, p_surname VARCHAR2, p_start_date DATE) IS
SELECT count(*)
FROM person_also_known_as
WHERE paka_par_refno = p_par_refno
AND coalesce(paka_forename,'NUL') = coalesce(p_forename,'NUL')
AND coalesce(paka_surname,'NUL') = coalesce(p_surname,'NUL')
AND paka_start_date = coalesce(p_start_date,trunc(sysdate));

-- *******************
--
CURSOR c_chk_for_par (p_par_refno INTEGER) IS
SELECT 'X'
FROM parties
WHERE par_refno = p_par_refno;
-- *****************************
--
CURSOR c_chk_dup_dl (     p_dlb_batch_id  VARCHAR2
                        , p_par_refno     INTEGER
                        , p_forename      VARCHAR2 
                        , p_surname       VARCHAR2
                        , p_start_date    DATE
                        , p_per_alt_ref   VARCHAR2
                        , p_per_forename  VARCHAR2
                        , p_per_surname   VARCHAR2)
IS
SELECT count(*)
FROM dl_hem_person_also_known_as
WHERE lpaka_dlb_batch_id = p_dlb_batch_id
  AND (   (p_par_refno IS NOT NULL AND lpaka_par_refno = p_par_refno) 
       OR (p_par_refno IS NULL AND (    coalesce(lpaka_per_alt_ref,'NUL') = coalesce(p_per_alt_ref,'NUL')
                                    AND coalesce(lpaka_per_forename,'NUL') = coalesce(p_per_forename,'NUL')
                                    AND coalesce(lpaka_per_surname,'NUL') = coalesce(p_per_surname,'NUL')
                                   )
          )
      )
  AND coalesce(lpaka_forename,'NUL') = coalesce(p_forename,'NUL')
  AND coalesce(lpaka_surname,'NUL') = coalesce(p_surname,'NUL')
  AND coalesce(lpaka_start_date,trunc(sysdate)) = coalesce(p_start_date,trunc(sysdate));
-- *******************
--
CURSOR c_chk_akar_code (p_akar_code VARCHAR2) IS
SELECT 'X'
FROM first_ref_values
WHERE frv_frd_domain = 'ALSOKNOWNASREASON'
AND frv_code = p_akar_code;
-- *****************************
--
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HEM_PERSON_ALSO_KNOWN_AS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER := 0;
l_chk_for_par	   VARCHAR2(1);
l_chk_for_dup    INTEGER;
l_chk_akar       VARCHAR2(1);
l_chk_for_fn_sn  VARCHAR2(1);
l_chk_dup_dl     INTEGER;
l_det_par_refno  INTEGER;
l_par_lkup_err   VARCHAR2(10);
--
-- *****************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_person_also_known_as.dataload_validate');
fsc_utils.debug_message( 's_dl_hem_person_also_known_as.dataload_validate',3 );
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
  cs := p1.lpaka_dl_seqno;
  l_id := p1.rec_rowid;
--
  l_errors := 'V';
  l_error_ind := 'N';
  l_chk_for_dup := NULL;
  l_chk_for_par := NULL;
  l_chk_akar := NULL; --0;
  l_chk_for_fn_sn := NULL;
  l_chk_dup_dl := NULL; --0;
  l_det_par_refno  := NULL;
  l_par_lkup_err   := NULL;
--
-- Mandatory field check and content
--
-- Start Date
--
  IF (p1.lpaka_start_date IS NULL)
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',181);
  --  'Start Date must be supplied'
  END IF;
--
-- As part of the dataload a user can supply a combination of person details.
-- Either Party Refno used irrespective of other provided data
-- OR combination of ALT_REF, FORENAME and SURNAME, ALT_REF and FORENAME and SURNAME.
--
  IF (p1.lpaka_par_refno  IS NOT NULL)
   THEN
    OPEN c_chk_for_par(p1.lpaka_par_refno);
    FETCH c_chk_for_par INTO l_chk_for_par;
    CLOSE c_chk_for_par;
    IF (l_chk_for_par IS NULL)
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',583);
  --    'The Person does not exist.'
    END IF;
--
  ELSE
    determine_par_refno( p1.lpaka_per_alt_ref
                        , p1.lpaka_per_forename
                        , p1.lpaka_per_surname
                        , l_det_par_refno
                        , l_par_lkup_err);
--
    IF (l_det_par_refno IS NULL)
      THEN
        --'Unable to associate to party'
        IF (substr(l_par_lkup_err,1,3) = 'TMR' )
          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',674);
            -- 'Multiple People match with the details provided.'
        ELSIF (substr(l_par_lkup_err,1,3) = 'NDF')
          THEN
            IF substr(l_par_lkup_err,5) = 'A' 
              THEN
                l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',243);
                -- hdl 243 'Alternative person reference does not exist on PEOPLE'
            ELSE
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',675);
              -- 'No People match using the details provided.'
            END IF;
        ELSIF (l_par_lkup_err = 'NDP:')
          THEN
            l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',676);
            -- 'No enough information provided to attempt a person lookup'.
        END IF;
    END IF;

  
  END IF;
--
-- Duplicate Combination
--
  OPEN  c_chk_for_dup(coalesce(p1.lpaka_par_refno,l_det_par_refno), p1.lpaka_forename, p1.lpaka_surname, p1.lpaka_start_date);
  FETCH c_chk_for_dup INTO l_chk_for_dup;
  CLOSE c_chk_for_dup;
   IF (l_chk_for_dup > 0)
    THEN
     l_errors:= s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',671);
--   'This combination of Also Known As information already exists for this person.'
   END IF;
--
--
-- Other Fields and combinations
--
-- Also Known As Reason
--
  IF (p1.lpaka_frv_akar_code IS NULL)
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',531);
--  'The Also Known As Reason must be supplied.'
   ELSE
    OPEN c_chk_akar_code(p1.lpaka_frv_akar_code);
    FETCH c_chk_akar_code INTO l_chk_akar;
    CLOSE c_chk_akar_code;
    IF (l_chk_akar IS NULL)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',672);
    --	  'The Reason Code given does not exist in the Reference Domain ALSOKNOWNASREASON.'
    END IF;
  END IF;
--
-- Forename and/or Surname
--
 IF (p1.lpaka_forename IS NULL AND p1.lpaka_surname IS NULL)
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',669);
--  'At least one of FORENAME or SURNAME is required.'
 END IF;
--
-- Check data load batch for duplicates
--
    OPEN  c_chk_dup_dl( p1.lpaka_dlb_batch_id
                           ,p1.lpaka_par_refno
                           , p1.lpaka_forename
                           ,p1.lpaka_surname
                           , p1.lpaka_start_date
                           , p1.lpaka_per_alt_ref
                           , p1.lpaka_per_forename
                           , p1.lpaka_per_surname);
    FETCH c_chk_dup_dl INTO l_chk_dup_dl;
    CLOSE c_chk_dup_dl;
--
    IF (l_chk_dup_dl > 1)
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',673);
-- 'Duplicate records exist in the data load batch for the Person, Forename, Surname and Start Date.'
    END IF;
--
-- Check the Created date if supplied
--
  IF (nvl(p1.lpaka_created_date,TRUNC(SYSDATE)) > TRUNC(SYSDATE))
   THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',531);
-- 'The Created date cannot be later than today (truncated sysdate)'
  END IF;
--
-- *****************************************
-- Now UPDATE the record count and error code
--
  IF l_errors = 'F' THEN
    l_error_ind := 'Y';
  ELSE
    l_error_ind := 'N';
  END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
  set_record_status_flag(l_id,l_errors);
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
--
fsc_utils.proc_end;
commit;
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
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid,
lpaka_dlb_batch_id,
lpaka_dl_seqno,
lpaka_dl_load_status,
lpaka_refno,
lpaka_par_refno,
lpaka_per_alt_ref,
lpaka_per_forename,
lpaka_per_surname,
lpaka_frv_akar_code,
lpaka_start_date,
lpaka_created_date,
lpaka_created_by,
lpaka_forename,
lpaka_surname,
lpaka_end_date,
lpaka_comments
FROM  dl_hem_person_also_known_as
WHERE lpaka_dlb_batch_id   = p_batch_id
AND   lpaka_dl_load_status = 'C';
--
-- ********************************
i INTEGER := 0;
l_an_tab  VARCHAR2(1);
--
-- *******************************
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_PERSON_ALSO_KNOWN_AS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_person_also_known_as.dataload_delete');
fsc_utils.debug_message( 's_dl_hem_person_also_known_as.dataload_delete',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs := p1.lpaka_dl_seqno;
  i  := i +1;
  l_id := p1.rec_rowid;
--
-- delete record created just needs to match PK
-- (PK) PAKA_PK => PAKA_REFNO
--
  DELETE FROM person_also_known_as
   WHERE paka_refno = p1.lpaka_refno;
--
-- Remove the paka_refno from the dataload batch record
-- [Right approach?]
--
  UPDATE dl_hem_person_also_known_as
  SET lpaka_refno = NULL
  WHERE rowid = l_id;
--
--
-- ***********************************
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
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
--
-- ***********************************
-- Section to analyse the tables populated with this dataload
--
l_an_tab := s_dl_hem_utils.dl_comp_stats('PERSON_ALSO_KNOWN_AS');
fsc_utils.proc_end;
COMMIT;
--
  EXCEPTION
   WHEN OTHERS THEN
   s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
   RAISE;
--
END dataload_delete;
--
--
END s_dl_hem_person_also_known_as;
/

show errors
