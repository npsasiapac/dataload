CREATE OR REPLACE PACKAGE BODY s_dl_hem_user_obj_admin_units
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VER     DB Ver  WHO  WHEN         WHY
--  1.0     6.9.0   AJ   25-OCT-2013  Initial Creation for Alberta
--
--
--
--  declare package variables AND constants
--
-- **************************************************************************************************
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
    UPDATE dl_hem_user_obj_admin_units
       SET luoa_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_user_obj_admin_units');
         RAISE;
--
END set_record_status_flag;
--
-- **************************************************************************************************
--
PROCEDURE dataload_create(p_batch_id    IN VARCHAR2,
                          p_date        IN DATE)
AS
--
CURSOR c1(p_batch_id  VARCHAR2) 
IS
SELECT rowid rec_rowid,
       luoa_dlb_batch_id,
       luoa_dl_seqno,
       luoa_dl_load_status,
       luoa_usr_username,
       luoa_aun_code,
       luoa_obj_name,
       luoa_access_level,
       luoa_start_date,
       luoa_end_date,
       luoa_comments
  FROM dl_hem_user_obj_admin_units
 WHERE luoa_dlb_batch_id   = p_batch_id
   AND luoa_dl_load_status = 'V';
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb             VARCHAR2(30);
cd             DATE;
cp             VARCHAR2(30) := 'CREATE';
ct             VARCHAR2(30) := 'DL_HEM_USER_OBJ_ADMIN_UNITS';
cs             INTEGER;
ce             VARCHAR2(200);
ci             INTEGER;
l_id           ROWID;
--
i              INTEGER := 0;
l_an_tab       VARCHAR2(1);
--
-- Other variables
--
-- NONE
--
-- ***********************************************************************
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hem_user_obj_admin_units.dataload_create');
  fsc_utils.debug_message('s_dl_hem_user_obj_admin_units.dataload_create',3);
--
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  cb := p_batch_id;
  cd := p_date;
--
  FOR p1 in c1(p_batch_id) LOOP
--
    BEGIN
--
      cs   := p1.luoa_dl_seqno;
      l_id := p1.rec_rowid;
--
      SAVEPOINT SP1;
--
-- ***********************************************************************
--
-- Insert into relevant table
--
      INSERT INTO USER_OBJECT_ADMIN_UNITS(uoa_usr_username,
                                          uoa_aun_code,
                                          uoa_obj_name,
                                          uoa_access_level,
                                          uoa_start_date,
                                          uoa_end_date,
                                          uoa_comments			
                                          )
                                 VALUES  (p1.luoa_usr_username,
                                          p1.luoa_aun_code,
                                          p1.luoa_obj_name,
                                          p1.luoa_access_level,
                                          p1.luoa_start_date,
                                          p1.luoa_end_date,
                                          p1.luoa_comments
                                         );
--
-- ***********************************************************************
--
-- keep a count of the rows processed and commit after every 1000
--
      i := i+1; 
--
      IF MOD(i,1000)=0 THEN 
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
-- ***********************************************************************
--
-- Section to analyse the table(s) populated by this data load
--
--
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('USER_OBJECT_ADMIN_UNITS');
--
  fsc_utils.proc_end;
  COMMIT;
--
  EXCEPTION
    WHEN OTHERS THEN
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_create;
--
-- **************************************************************************************************
--
PROCEDURE dataload_validate(p_batch_id  IN VARCHAR2,
                            p_date      IN DATE)
AS
--
CURSOR c1(p_batch_id  VARCHAR2) 
IS
SELECT rowid rec_rowid,
       luoa_dlb_batch_id,
       luoa_dl_seqno,
       luoa_dl_load_status,
       luoa_usr_username,
       luoa_aun_code,
       luoa_obj_name,
       luoa_access_level,
       luoa_start_date,
       luoa_end_date,
       luoa_comments
  FROM dl_hem_user_obj_admin_units
 WHERE luoa_dlb_batch_id   = p_batch_id
   AND luoa_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
CURSOR chk_aun_exists(p_aun_code VARCHAR2) 
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
-- ***********************************************************************
--
CURSOR c_get_aun_cid(p_aun_code VARCHAR2) 
IS
SELECT aun_current_ind
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
-- ***********************************************************************
--
CURSOR chk_usr_exists(p_usr_username VARCHAR2) 
IS
SELECT 'X'
  FROM users
 WHERE usr_username = p_usr_username;
--
-- ***********************************************************************
--
CURSOR chk_obj_exists(p_obj_name VARCHAR2) 
IS
SELECT 'X'
  FROM objects
 WHERE obj_name = p_obj_name
   AND obj_name IN ('PARTIES',
                    'ORGANISATIONS',
                    'INTERESTED_PARTIES');
--
-- ***********************************************************************
--
CURSOR chk_uoa_dup1(p_username      VARCHAR2,
                     p_aun_code    VARCHAR2,
                     p_obj_name    VARCHAR2,
                     p_end_date    DATE)   
IS
SELECT 'X'
  FROM user_object_admin_units
 WHERE uoa_usr_username = p_username
   AND uoa_aun_code     = p_aun_code
   AND uoa_obj_name     = p_obj_name
   AND p_end_date BETWEEN uoa_start_date AND NVL(uoa_end_date,p_end_date+1);
--
-- ***********************************************************************
--
CURSOR chk_uoa_dup(p_username    VARCHAR2,
                    p_aun_code    VARCHAR2,
                    p_obj_name    VARCHAR2,
                    p_start_date  DATE)   
IS
SELECT 'X'
  FROM user_object_admin_units
 WHERE uoa_usr_username = p_username
   AND uoa_aun_code     = p_aun_code
   AND uoa_obj_name     = p_obj_name
   AND p_start_date BETWEEN uoa_start_date AND NVL(uoa_end_date,p_start_date+1);
--
-- ***********************************************************************
--
CURSOR chk_uoa_dup2(p_username    VARCHAR2,
                    p_aun_code    VARCHAR2,
                    p_obj_name    VARCHAR2,
                    p_start_date  DATE)   
IS
SELECT 'X'
  FROM user_object_admin_units
 WHERE uoa_usr_username = p_username
   AND uoa_aun_code     = p_aun_code
   AND uoa_obj_name     = p_obj_name
   AND uoa_start_date   >= p_start_date;
--
-- ***********************************************************************
--
CURSOR chk_pv_exists(p_pv_name VARCHAR2) 
IS
SELECT 'X'
  FROM parameter_values
 WHERE pva_pdu_pdf_name       = p_pv_name
   AND pva_pdu_pdf_param_type = 'SYSTEM'
   AND pva_char_value         = 'Y';
--
-- ***********************************************************************
--
-- Constants FOR summary reporting
--
cb                 VARCHAR2(30);
cd                 DATE;
cp                 VARCHAR2(30) := 'VALIDATE';
ct                 VARCHAR2(30) := 'DL_HEM_USER_OBJ_ADMIN_UNITS';
cs                 INTEGER;
ce                 VARCHAR2(200);
--
-- variables for error reporting
--
l_errors           VARCHAR2(1);
l_error_ind        VARCHAR2(1);
i                  INTEGER :=0;
l_id               ROWID;
--
-- other variables
--
--
l_aun_exists       VARCHAR2(1);
l_usr_exists       VARCHAR2(1);
l_uoa_1_exists     VARCHAR2(1);
l_uoa_2_exists     VARCHAR2(1);
l_obj_exists       VARCHAR2(1);
l_uoa_exists       VARCHAR2(1);
l_pv_exists        VARCHAR2(1);
l_pv_name          VARCHAR2(30);
l_aun_cid          VARCHAR2(1);
--
--
-- ***********************************************************************
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hem_user_obj_admin_units.dataload_validate');
  fsc_utils.debug_message('s_dl_hem_user_obj_admin_units.dataload_validate',3 );
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  cb := p_batch_id;
  cd := p_date;
--
  FOR p1 in c1(p_batch_id) LOOP
--  
    BEGIN
--
      cs   := p1.luoa_dl_seqno;
      l_id := p1.rec_rowid;
--
      l_errors    := 'V';
      l_error_ind := 'N';
--
      l_aun_exists       := NULL;
      l_usr_exists       := NULL;
      l_obj_exists       := NULL;
      l_uoa_exists       := NULL;
      l_uoa_1_exists     := NULL;
      l_uoa_2_exists     := NULL;
      l_pv_exists        := NULL;
      l_pv_name          := NULL;
      l_aun_cid          := NULL;
--
--
-- ***********************************************************************
--
-- Check that the Admin Unit Code has been supplied does not exceed the max
-- length of 20 Char and exists in admin_units table 
--
      IF (p1.luoa_aun_code IS NULL) THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',189);
--
      ELSIF (p1.luoa_aun_code IS NOT NULL
	          AND LENGTH(p1.luoa_aun_code) > 20) THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',614);
--
      ELSE
--
        OPEN chk_aun_exists(p1.luoa_aun_code);
       FETCH chk_aun_exists INTO l_aun_exists;
          IF chk_aun_exists%NOTFOUND THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',614);
          END IF;
       CLOSE chk_aun_exists;
--
      END IF;
--
      IF (l_aun_exists IS NOT NULL) THEN
--
        OPEN c_get_aun_cid(p1.luoa_aun_code);
       FETCH c_get_aun_cid INTO l_aun_cid;
          IF (l_aun_cid !='Y')          THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',674);
          END IF;
       CLOSE c_get_aun_cid;	  
--
      END IF;
--
-- ***********************************************************************
--
-- Check the Start Date has been supplied
--
      IF (p1.luoa_start_date IS NULL) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',42);
--
      END IF;
--
-- ***********************************************************************
--
-- Check the END DATE field 6 on the data load file if supplied is NOT
-- BEFORE the START DATE field 5 on the data load Table
--
      IF (p1.luoa_end_date IS NOT NULL) THEN
--
       IF (p1.luoa_end_date < NVL(p1.luoa_start_date, TRUNC(SYSDATE))) THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',653);
       END IF;
--
      END IF;
--
-- ***********************************************************************
--
-- Check that the OBJECT NAME Field 3 on the dataload file has been supplied
-- and is one of the three valid types
--
      IF (p1.luoa_obj_name IS NULL) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',665);
--
      ELSIF (p1.luoa_obj_name NOT IN ('PARTIES',
                                      'ORGANISATIONS',
                                      'INTERESTED_PARTIES')) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',666);
--
      END IF;
--
-- ***********************************************************************
--
-- Check that the ACCESS LEVEL Field 4 on the dataload file has been supplied
-- and is one of the three valid types OF W(Update) R(Read All) V(Read some)
--
      IF (p1.luoa_access_level IS NULL) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',667);
--
      ELSIF (p1.luoa_access_level NOT IN ('W',
                                          'R',
                                          'V')) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',668);
--
      END IF;
--
-- ***********************************************************************
--
-- Check that the USERNAME has been supplied and exists in users table 
--
      IF (p1.luoa_usr_username IS NULL) THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',669);
--
      ELSE
--
        OPEN chk_usr_exists(p1.luoa_usr_username);
       FETCH chk_usr_exists INTO l_usr_exists;
          IF chk_usr_exists%NOTFOUND THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',670);
          END IF;
       CLOSE chk_usr_exists;
--
      END IF;
--
-- ***********************************************************************
-- Check that the OBJECT Exists in OBJECTS Table if supplied correctly 
--
      IF (p1.luoa_obj_name IN ('PARTIES',
                               'ORGANISATIONS',
                               'INTERESTED_PARTIES')) THEN
--
        OPEN chk_obj_exists(p1.luoa_obj_name);
       FETCH chk_obj_exists INTO l_obj_exists;
          IF chk_obj_exists%NOTFOUND THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',671);
          END IF;
       CLOSE chk_obj_exists;
--
      END IF;
--
-- ***********************************************************************
-- Check that the Admin Unit Security for the record type has been switched on
-- in the Parameter Values Table PARTIES (AU_SEC_FOR_PAR) ORGANISATIONS
-- (AU_SEC_FOR_ORG) and INTERESTED_PARTIES (AU_SEC_FOR_IPP)
--
--
      IF (p1.luoa_obj_name IN ('PARTIES',
                               'ORGANISATIONS',
                               'INTERESTED_PARTIES')) THEN
--
--
        IF (p1.luoa_obj_name = 'PARTIES')   THEN
--
        l_pv_name := 'AU_SEC_FOR_PAR';
--
        ELSIF (p1.luoa_obj_name = 'ORGANISATIONS')   THEN
--
        l_pv_name := 'AU_SEC_FOR_ORG';
--
        ELSIF (p1.luoa_obj_name = 'INTERESTED_PARTIES')   THEN
--
        l_pv_name := 'AU_SEC_FOR_IPP';
--
        END IF; 
--
          OPEN chk_pv_exists(l_pv_name);
         FETCH chk_pv_exists INTO l_pv_exists;
            IF chk_pv_exists%NOTFOUND THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',673);
            END IF;
         CLOSE chk_pv_exists;
--
      END IF;
--
-- ***********************************************************************
--
-- Check that the combination supplied does not already exist in the 
-- user_object_admin_units table to prevent overlaps
--
--
      IF (    l_usr_exists IS NOT NULL
          AND l_aun_exists IS NOT NULL
          AND l_obj_exists IS NOT NULL) THEN
--
--
-- Check for overlaps
--
-- Check start date for all records if supplied
--
        IF (p1.luoa_start_date IS NOT NULL)   THEN
--
          l_uoa_exists    := NULL;
--
          OPEN chk_uoa_dup(p1.luoa_usr_username
		                  ,p1.luoa_aun_code
                          ,p1.luoa_obj_name
                          ,p1.luoa_start_date);
         FETCH chk_uoa_dup INTO l_uoa_exists;
            IF    chk_uoa_dup%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
         CLOSE chk_uoa_dup;
--
       END IF;
--
-- Check End Date when supplied
--
        IF (p1.luoa_end_date IS NOT NULL)   THEN
--
          l_uoa_1_exists    := NULL;
--
          OPEN chk_uoa_dup1(p1.luoa_usr_username
		                   ,p1.luoa_aun_code
                           ,p1.luoa_obj_name
                           ,p1.luoa_end_date);
         FETCH chk_uoa_dup1 INTO l_uoa_1_exists;
            IF    chk_uoa_dup1%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
         CLOSE chk_uoa_dup1;
--
        END IF;
--
-- Check a record does not exist with a greater start date if loau_end_date
-- has not been supplied so trying to create an open record
--
        IF (p1.luoa_end_date IS NULL)   THEN
--
          l_uoa_2_exists    := NULL;
--
          OPEN chk_uoa_dup2(p1.luoa_usr_username
		                   ,p1.luoa_aun_code
                           ,p1.luoa_obj_name
                           ,p1.luoa_start_date);
         FETCH chk_uoa_dup2 INTO l_uoa_2_exists;
          IF    chk_uoa_dup2%found THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',677);
          END IF;
         CLOSE chk_uoa_dup2;
--
        END IF;
--
      END IF;
--
-- ***********************************************************************
--
-- Now UPDATE the record count AND error code
--
      IF l_errors = 'F' THEN
       l_error_ind := 'Y';
      ELSE
       l_error_ind := 'N';
      END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
      i := i+1; 
--
      IF (MOD(i,1000) = 0) THEN 
       COMMIT; 
      END IF;
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
  END LOOP; -- FOR LOOP
--
  fsc_utils.proc_END;
  COMMIT;
--
  EXCEPTION
       WHEN OTHERS THEN
          s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- **************************************************************************************************
--
--
PROCEDURE dataload_delete(p_batch_id  IN VARCHAR2,
                          p_date      IN DATE)
AS
--
CURSOR c1(p_batch_id  VARCHAR2) 
IS
SELECT rowid rec_rowid,
       luoa_dlb_batch_id,
       luoa_dl_seqno,
       luoa_dl_load_status,
       luoa_usr_username,
       luoa_aun_code,
       luoa_obj_name,
       luoa_access_level,
       luoa_start_date,
       luoa_end_date,
       luoa_comments
  FROM dl_hem_user_obj_admin_units
 WHERE luoa_dlb_batch_id   = p_batch_id
   AND luoa_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb             VARCHAR2(30);
cd             DATE;
cp             VARCHAR2(30) := 'DELETE';
ct             VARCHAR2(30) := 'DL_HEM_USER_OBJ_ADMIN_UNITS';
cs             INTEGER;
ce             VARCHAR2(200);
l_id           ROWID;
--
i              INTEGER := 0;
l_an_tab       VARCHAR2(1);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_user_obj_admin_units.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_user_obj_admin_units.dataload_delete',3);
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    cb := p_batch_id;
    cd := p_date;
--
    FOR p1 in c1(p_batch_id) LOOP
--
      BEGIN
--
        cs   := p1.luoa_dl_seqno;
        l_id := p1.rec_rowid;
--
        SAVEPOINT SP1;
-- *********************************************************************** 
--
-- Deletion of record INDEXE/CONSTRAINT "UOA PK" used to identify the record
-- that has been inserted.  The 4 Fields are uoa_usr_username, uoa_aun_code,
-- uoa_obj_name and uoa_start_date 
--
--
        DELETE 
          FROM user_object_admin_units
         WHERE uoa_usr_username = p1.luoa_usr_username
           AND uoa_aun_code     = p1.luoa_aun_code
           AND uoa_obj_name     = p1.luoa_obj_name
           AND uoa_start_date   = p1.luoa_start_date;
--
--
-- *********************************************************************** 
--
-- keep a count of the rows processed and commit after every 1000
--
        i := i+1; 
--
        IF MOD(i,1000)=0 THEN 
         COMMIT; 
        END IF;
--
        s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
        set_record_status_flag(l_id,'V');
--
        EXCEPTION
             WHEN OTHERS THEN
                ROLLBACK TO SP1;
                ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
                set_record_status_flag(l_id,'C');
                s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
--
    END LOOP;
--
    COMMIT;
--
-- ***********************************************************************
--
-- Section to analyse the table(s) populated by this data load
--
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('USER_OBJECT_ADMIN_UNITS');
--
    fsc_utils.proc_end;
    COMMIT;
--
    EXCEPTION
       WHEN OTHERS THEN
          s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
          RAISE;
--
END dataload_delete;
--
--
END s_dl_hem_user_obj_admin_units;
--
/