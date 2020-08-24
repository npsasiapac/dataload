CREATE OR REPLACE PACKAGE BODY s_dl_hem_object_admin_units
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
    UPDATE dl_hem_object_admin_units
       SET loau_dl_load_status = p_status
     WHERE rowid               = p_rowid;
--
    EXCEPTION
         WHEN OTHERS THEN
            dbms_output.put_line('Error updating status of dl_hem_object_admin_units');
         RAISE;
--
END set_record_status_flag;
--
-- **************************************************************************************************
--
PROCEDURE dataload_create(p_batch_id    IN VARCHAR2,
                          p_date        IN DATE)
AS

CURSOR c1(p_batch_id  VARCHAR2) 
IS
SELECT rowid rec_rowid,
       loau_dlb_batch_id,
       loau_dl_seqno,
       loau_dl_load_status,
       loau_aun_code,
       loau_start_date,
       loau_end_date,
       loau_rec_type,
       loau_obj_ref,
       loau_comments,
       loau_del_oau_refno
  FROM dl_hem_object_admin_units
 WHERE loau_dlb_batch_id   = p_batch_id
   AND loau_dl_load_status = 'V';
--
-- ***********************************************************************
--
CURSOR c_get_par_alt_refno(p_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_per_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_org_full_refno(p_org_full_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_org_name = p_org_full_ref;
--
-- ***********************************************************************
--
CURSOR c_get_org_short_refno(p_org_short_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_org_short_name = p_org_short_ref;
--
-- ***********************************************************************
--
CURSOR c_get_ipp_short_refno(p_ipp_short_ref VARCHAR2) 
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_short_ref;
--
-- ***********************************************************************
--
CURSOR c_get_par_refno(p_par_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_ref;
--
-- ***********************************************************************
--
CURSOR c_get_par_oau_refno(p_aun_code    VARCHAR2,
                           p_start_date  DATE,
                           p_par_refno   NUMBER)   
IS
SELECT oau_refno
  FROM object_admin_units
 WHERE oau_aun_code      = p_aun_code
   AND oau_start_date    = p_start_date
   AND oau_par_refno     = p_par_refno
   AND oau_ipp_refno IS NULL;
--
-- ***********************************************************************
--
CURSOR c_get_ipp_oau_refno(p_aun_code    VARCHAR2,
                           p_start_date  DATE,
                           p_ipp_refno   NUMBER)   
IS
SELECT oau_refno
  FROM object_admin_units
 WHERE oau_aun_code      = p_aun_code
   AND oau_start_date    = p_start_date
   AND oau_ipp_refno     = p_ipp_refno
   AND oau_par_refno IS NULL;
--
-- ***********************************************************************
--
--
-- Constants for process_summary
--
cb             VARCHAR2(30);
cd             DATE;
cp             VARCHAR2(30) := 'CREATE';
ct             VARCHAR2(30) := 'DL_HEM_OBJECT_ADMIN_UNITS';
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
l_rec_type     VARCHAR2(10);
l_par_refno    NUMBER(8);
l_ipp_refno    NUMBER(10);
l_oau_refno    NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hem_object_admin_units.dataload_create');
  fsc_utils.debug_message('s_dl_hem_object_admin_units.dataload_create',3);
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
      cs   := p1.loau_dl_seqno;
      l_id := p1.rec_rowid;
--
      SAVEPOINT SP1;
--
-- ***********************************************************************
--
-- get the par_refno and ipp_refno from parties and interested_parties tables
--
      l_par_refno := NULL;
      l_ipp_refno := NULL;
      l_rec_type  := NULL;
      l_oau_refno := NULL;
--
      l_rec_type  := p1.loau_rec_type;
--
	  IF  (l_rec_type = 'PAR_ALT') THEN
--
        OPEN c_get_par_alt_refno(p1.loau_obj_ref);
       FETCH c_get_par_alt_refno into l_par_refno;
       CLOSE c_get_par_alt_refno;
--
      ELSIF (l_rec_type = 'ORG_FULL') THEN
--
        OPEN c_get_org_full_refno(p1.loau_obj_ref);
       FETCH c_get_org_full_refno into l_par_refno;
       CLOSE c_get_org_full_refno;
--
      ELSIF (l_rec_type = 'ORG_SHORT') THEN
--
        OPEN c_get_org_short_refno(p1.loau_obj_ref);
       FETCH c_get_org_short_refno into l_par_refno;
       CLOSE c_get_org_short_refno;
--
      ELSIF (l_rec_type = 'IPP_SHORT') THEN
--
        OPEN c_get_ipp_short_refno(p1.loau_obj_ref);
       FETCH c_get_ipp_short_refno into l_ipp_refno;
       CLOSE c_get_ipp_short_refno;
--
      ELSIF (l_rec_type = 'PAR_REFNO') THEN
-- 
        OPEN c_get_par_refno(p1.loau_obj_ref);
       FETCH c_get_par_refno into l_par_refno;
       CLOSE c_get_par_refno;
--
      END IF;
--
-- ***********************************************************************
--
-- Insert into relevant table
--
      INSERT INTO OBJECT_ADMIN_UNITS(oau_aun_code,
                                     oau_start_date,
                                     oau_par_refno,
                                     oau_ipp_refno,
                                     oau_end_date,
                                     oau_comments
                                    )
                            VALUES  (p1.loau_aun_code,
                                     p1.loau_start_date,
                                     l_par_refno,
                                     l_ipp_refno,
                                     p1.loau_end_date,
                                     p1.loau_comments
                                    );
--
-- ***********************************************************************
--
-- Get oau_refno and store it in loau_del_oau_refno to use for delete
-- not checked if field is NULL to allow for create delete create delete
-- without deleting batch
--
      IF (l_rec_type = 'IPP_SHORT') THEN
--
        OPEN c_get_ipp_oau_refno(p1.loau_aun_code, p1.loau_start_date, l_ipp_refno);
       FETCH c_get_ipp_oau_refno INTO l_oau_refno;
       CLOSE c_get_ipp_oau_refno;
--
      ELSE
--
        OPEN c_get_par_oau_refno(p1.loau_aun_code, p1.loau_start_date, l_par_refno);
       FETCH c_get_par_oau_refno INTO l_oau_refno;
       CLOSE c_get_par_oau_refno;
--
      END IF;
--
      UPDATE dl_hem_object_admin_units
            SET loau_del_oau_refno        = l_oau_refno
          WHERE loau_dlb_batch_id         = p1.loau_dlb_batch_id
            AND loau_dl_seqno             = p1.loau_dl_seqno
            AND loau_aun_code             = p1.loau_aun_code
            AND loau_start_date           = p1.loau_start_date
            AND loau_rec_type             = p1.loau_rec_type
            AND loau_obj_ref              = p1.loau_obj_ref;
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
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('OBJECT_ADMIN_UNITS');
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
       loau_dlb_batch_id,
       loau_dl_seqno,
       loau_dl_load_status,
       loau_aun_code,
       loau_start_date,
       loau_end_date,
       loau_rec_type,
       loau_obj_ref,
       loau_comments,
       loau_del_oau_refno
  FROM dl_hem_object_admin_units
 WHERE loau_dlb_batch_id   = p_batch_id
   AND loau_dl_load_status IN ('L','F','O');
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
CURSOR chk_par_alt_exists(p_par_per_alt_ref VARCHAR2) 
IS
SELECT 'X'
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
-- ***********************************************************************
--
CURSOR chk_par_alt_pt(p_par_per_alt_ref VARCHAR2) 
IS
SELECT 'X'
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref
   AND par_type = 'HOUP';
--
-- ***********************************************************************
--
CURSOR chk_org_full_exists(p_par_org_name VARCHAR2) 
IS
SELECT 'X'
  FROM parties
 WHERE par_org_name = p_par_org_name;
--
-- ***********************************************************************
--
CURSOR chk_org_full_pt(p_par_org_name VARCHAR2) 
IS
SELECT 'X'
  FROM parties
 WHERE par_org_name = p_par_org_name
   AND par_type = 'ORG';
--
-- ***********************************************************************
--
CURSOR chk_org_short_exists(p_par_org_short_name VARCHAR2) 
IS
SELECT 'X'
  FROM parties
 WHERE par_org_short_name = p_par_org_short_name;
--
-- ***********************************************************************
--
CURSOR chk_org_short_pt(p_par_org_short_name VARCHAR2) 
IS
SELECT 'X'
  FROM parties
 WHERE par_org_short_name = p_par_org_short_name
   AND par_type = 'ORG';
--
-- ***********************************************************************
--
CURSOR chk_par_refno_exists(p_par_refno VARCHAR2) 
IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_par_refno;
--
-- ***********************************************************************
--
CURSOR chk_par_refno_pt(p_par_refno VARCHAR2) 
IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_par_refno
   AND par_type IN ('ORG','HOUP');
--
-- ***********************************************************************
--
CURSOR chk_ipp_short_exists(p_ipp_shortname VARCHAR2) 
IS
SELECT 'X'
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname;
--
-- ***********************************************************************
--
CURSOR c_get_par_alt_refno(p_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_per_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_org_full_refno(p_org_full_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_org_name = p_org_full_ref;
--
-- ***********************************************************************
--
CURSOR c_get_org_short_refno(p_org_short_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_org_short_name = p_org_short_ref;
--
-- ***********************************************************************
--
CURSOR c_get_ipp_short_refno(p_ipp_short_ref VARCHAR2) 
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_short_ref;
--
-- ***********************************************************************
--
CURSOR c_get_par_refno(p_par_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_ref;
--
-- ***********************************************************************
--
CURSOR chk_oau_dup1_par(p_aun_code    VARCHAR2,
                        p_end_date    DATE,
                        p_par_refno   NUMBER)   
IS
SELECT 'X'
  FROM object_admin_units
 WHERE oau_aun_code      = p_aun_code
   AND oau_par_refno     = p_par_refno
   AND p_end_date BETWEEN oau_start_date AND NVL(oau_end_date,p_end_date+1);
--
-- ***********************************************************************
--
CURSOR chk_oau_dup_par(p_aun_code    VARCHAR2,
                       p_start_date  DATE,
                       p_par_refno   NUMBER)   
IS
SELECT 'X'
  FROM object_admin_units
 WHERE oau_aun_code      = p_aun_code
   AND oau_par_refno     = p_par_refno
   AND p_start_date BETWEEN oau_start_date AND NVL(oau_end_date,p_start_date+1);
--
-- ***********************************************************************
--
CURSOR chk_oau_dup2_ref(p_aun_code    VARCHAR2,
                        p_start_date  DATE,
                        p_par_refno   NUMBER)   
IS
SELECT 'X'
  FROM object_admin_units
 WHERE oau_aun_code      = p_aun_code
   AND oau_par_refno     = p_par_refno
   AND oau_start_date   >= p_start_date;
--
-- ***********************************************************************
--
CURSOR chk_oau_dup1_ipp(p_aun_code    VARCHAR2,
                        p_end_date    DATE,
                        p_ipp_refno   NUMBER)   
IS
SELECT 'X'
  FROM object_admin_units
 WHERE oau_aun_code      = p_aun_code
   AND oau_ipp_refno     = p_ipp_refno
   AND p_end_date BETWEEN oau_start_date AND NVL(oau_end_date,p_end_date+1);
--
-- ***********************************************************************
--
CURSOR chk_oau_dup_ipp(p_aun_code    VARCHAR2,
                       p_start_date  DATE,
                       p_ipp_refno   NUMBER)   
IS
SELECT 'X'
  FROM object_admin_units
 WHERE oau_aun_code      = p_aun_code
   AND oau_ipp_refno     = p_ipp_refno
   AND p_start_date BETWEEN oau_start_date AND NVL(oau_end_date,p_start_date+1);
--
-- ***********************************************************************
--
CURSOR chk_oau_dup2_ipp(p_aun_code    VARCHAR2,
                        p_start_date  DATE,
                        p_ipp_refno   NUMBER)   
IS
SELECT 'X'
  FROM object_admin_units
 WHERE oau_aun_code      = p_aun_code
   AND oau_ipp_refno     = p_ipp_refno
   AND oau_start_date   >= p_start_date;
--
-- ***********************************************************************
--
-- Constants FOR summary reporting
--
cb                 VARCHAR2(30);
cd                 DATE;
cp                 VARCHAR2(30) := 'VALIDATE';
ct                 VARCHAR2(30) := 'DL_HEM_OBJECT_ADMIN_UNITS';
cs                 INTEGER;
ce                 VARCHAR2(200);
--
-- other variables
--
--
l_aun_exists       VARCHAR2(1);
l_aun_cid          VARCHAR2(1);
l_par_alt_exists   VARCHAR2(1);
l_org_full_exists  VARCHAR2(1);
l_org_short_exists VARCHAR2(1);
l_ipp_short_exists VARCHAR2(1);
l_par_refno_exists VARCHAR2(1);
l_par_alt_pt       VARCHAR2(1);
l_org_full_pt      VARCHAR2(1);
l_org_short_pt     VARCHAR2(1);
l_par_refno_pt     VARCHAR2(1);
l_oau_pa_exists    VARCHAR2(1);
l_oau_pa1_exists   VARCHAR2(1);
l_oau_pa2_exists   VARCHAR2(1);
l_oau_of_exists    VARCHAR2(1);
l_oau_of1_exists   VARCHAR2(1);
l_oau_of2_exists   VARCHAR2(1);
l_oau_os_exists    VARCHAR2(1);
l_oau_os1_exists   VARCHAR2(1);
l_oau_os2_exists   VARCHAR2(1);
l_oau_is_exists    VARCHAR2(1);
l_oau_is1_exists   VARCHAR2(1);
l_oau_is2_exists   VARCHAR2(1);
l_oau_pr_exists    VARCHAR2(1);
l_oau_pr1_exists   VARCHAR2(1);
l_oau_pr2_exists   VARCHAR2(1);
l_pa_par_refno     NUMBER(8);
l_of_par_refno     NUMBER(8);
l_os_par_refno     NUMBER(8);
l_is_ipp_refno     NUMBER(10);
l_pr_par_refno     NUMBER(8);
l_errors           VARCHAR2(1);
l_error_ind        VARCHAR2(1);
i                  INTEGER :=0;
l_id               ROWID;
--
BEGIN
--
  fsc_utils.proc_start('s_dl_hem_object_admin_units.dataload_validate');
  fsc_utils.debug_message('s_dl_hem_object_admin_units.dataload_validate',3 );
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
      cs   := p1.loau_dl_seqno;
      l_id := p1.rec_rowid;
--
      l_errors    := 'V';
      l_error_ind := 'N';
--
      l_aun_exists       := NULL;
      l_aun_cid          := NULL;
      l_par_alt_exists   := NULL;
      l_org_full_exists  := NULL;
      l_org_short_exists := NULL;
      l_ipp_short_exists := NULL;
      l_par_refno_exists := NULL;
      l_par_alt_pt       := NULL;
      l_org_full_pt      := NULL;
      l_org_short_pt     := NULL;
      l_par_refno_pt     := NULL;
      l_oau_pa_exists    := NULL;
      l_oau_pa1_exists   := NULL;
      l_oau_pa2_exists   := NULL;
      l_oau_of_exists    := NULL;
      l_oau_of1_exists   := NULL;
      l_oau_of2_exists   := NULL;
      l_oau_os_exists    := NULL;
      l_oau_os1_exists   := NULL;
      l_oau_os2_exists   := NULL;
      l_oau_is_exists    := NULL;
      l_oau_is1_exists   := NULL;
      l_oau_is2_exists   := NULL;
      l_oau_pr_exists    := NULL;
      l_oau_pr1_exists   := NULL;
      l_oau_pr2_exists   := NULL;
      l_pa_par_refno     := NULL;
      l_of_par_refno     := NULL;
      l_os_par_refno     := NULL;
      l_is_ipp_refno     := NULL;
      l_pr_par_refno     := NULL;
--
-- ***********************************************************************
--
-- Check that the Admin Unit Code has been supplied and exists in 
-- admin_units table and that it is current indicator is to to Y 
--
      IF (p1.loau_aun_code IS NULL) THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',189);
      ELSE
--
        OPEN chk_aun_exists(p1.loau_aun_code);
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
        OPEN c_get_aun_cid(p1.loau_aun_code);
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
      IF (p1.loau_start_date IS NULL) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',42);
--
      END IF;
--
-- ***********************************************************************
--
-- Check the End Date if supplied is NOT before the Start Date supplied
--
      IF (p1.loau_end_date IS NOT NULL) THEN
--
--       IF (p1.loau_end_date < NVL(p1.loau_start_date, TRUNC(SYSDATE))) THEN
--
       IF (p1.loau_end_date < NVL(p1.loau_start_date, p1.loau_end_date+1)) THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',653);
       END IF;
--
      END IF;
--
-- ***********************************************************************
--
-- Check that the record type has been supplied and is valid
--
      IF (p1.loau_rec_type IS NULL) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',654);
--
      ELSIF (p1.loau_rec_type NOT IN ('PAR_ALT','ORG_FULL','ORG_SHORT',
	                                  'IPP_SHORT','PAR_REFNO')) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',655);
--
      END IF;
--
-- ***********************************************************************
--
-- Check that the Object Reference has been supplied
--
      IF (p1.loau_obj_ref  IS NULL) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',656);
      END IF;
--
-- ***********************************************************************
--
-- Both the Record Type and Object Reference must be supplied for a valid
-- check to be performed
--
      IF (    p1.loau_obj_ref  IS NOT NULL
	      AND p1.loau_rec_type IS NOT NULL) THEN
--
-- **********
--
-- Check that the Object Reference is valid for the record type of 'PAR_ALT'
-- this should be the Party Alternative Reference (par_per_alt_ref)in the
-- Parties table.  The party type must be 'HOUP' for each record
--
        IF (p1.loau_rec_type = 'PAR_ALT') THEN
--
          OPEN chk_par_alt_exists(p1.loau_obj_ref);
         FETCH chk_par_alt_exists INTO l_par_alt_exists;
         CLOSE chk_par_alt_exists;
--
          IF (l_par_alt_exists IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',657);
--
          ELSE
--
            OPEN chk_par_alt_pt(p1.loau_obj_ref);
           FETCH chk_par_alt_pt INTO l_par_alt_pt;
           CLOSE chk_par_alt_pt;
--
              IF (l_par_alt_pt IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',658);
--
              END IF;
--
          END IF;
--
        END IF;
--
-- **********
--
-- Check that the Object Reference is valid for the record type of 'ORG_FULL'
-- this should be the Organisation Full Name (par_org_name)in the
-- Parties table.  The party type must be 'ORG' for each record
--
        IF (p1.loau_rec_type = 'ORG_FULL') THEN
--
          OPEN chk_org_full_exists(p1.loau_obj_ref);
         FETCH chk_org_full_exists INTO l_org_full_exists;
         CLOSE chk_org_full_exists;
--
          IF (l_org_full_exists IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',659);
--
          ELSE
--
            OPEN chk_org_full_pt(p1.loau_obj_ref);
           FETCH chk_org_full_pt INTO l_org_full_pt;
           CLOSE chk_org_full_pt;
--
              IF (l_org_full_pt IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',660);
--
              END IF;
--
          END IF;
--
        END IF;
--
-- **********
--
-- Check that the Object Reference is valid for the record type of 'ORG_SHORT'
-- this should be the Organisation Short Name (par_org_short_name)in the
-- Parties table.  The party type must be 'ORG' for each record
--
        IF (p1.loau_rec_type = 'ORG_SHORT') THEN
--
          OPEN chk_org_short_exists(p1.loau_obj_ref);
         FETCH chk_org_short_exists INTO l_org_short_exists;
         CLOSE chk_org_short_exists;
--
          IF (l_org_short_exists IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',661);
--
          ELSE
--
            OPEN chk_org_short_pt(p1.loau_obj_ref);
           FETCH chk_org_short_pt INTO l_org_short_pt;
           CLOSE chk_org_short_pt;
--
              IF (l_org_short_pt IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',662);
--
              END IF;
--
          END IF;
--
        END IF;
--
-- **********
--
-- Check that the Object Reference is valid for the record type of 'PAR_REFNO'
-- this should be the Internal Reference Number of the Party or Organisation
-- (par_refno)in the Parties table.  The party type must be 'ORG' or 'HOUP'
-- for each record
--
        IF (p1.loau_rec_type = 'PAR_REFNO') THEN
--
          OPEN chk_par_refno_exists(p1.loau_obj_ref);
         FETCH chk_par_refno_exists INTO l_par_refno_exists;
         CLOSE chk_par_refno_exists;
--
          IF (l_par_refno_exists IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',663);
--
          ELSE
--
            OPEN chk_par_refno_pt(p1.loau_obj_ref);
           FETCH chk_par_refno_pt INTO l_par_refno_pt;
           CLOSE chk_par_refno_pt;
--
              IF (l_par_refno_pt IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',664);
--
              END IF;
--
          END IF;
--
        END IF;
--
-- **********
--
-- Check that the Object Reference is valid for the record type of 'IPP_SHORT'
-- this should be the Interested Parties Short Name (ipp_shortname)in the
-- Interested Parties table.
--
        IF (p1.loau_rec_type = 'IPP_SHORT') THEN
--
          OPEN chk_ipp_short_exists(p1.loau_obj_ref);
         FETCH chk_ipp_short_exists INTO l_ipp_short_exists;
         CLOSE chk_ipp_short_exists;
--
          IF (l_ipp_short_exists IS NULL) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',675);
--
          END IF;
--
        END IF;
--
-- **********
--
      END IF;  -- End of Record Type and Object Reference
--
-- ***********************************************************************
--
-- Check that the combination supplied does not already exist in the 
-- object_admin_units table to prevent overlaps for PAR_ALT
--
      IF (p1.loau_rec_type = 'PAR_ALT') THEN
--
-- Get par_refno when reference and type are OK
--
        IF (    l_par_alt_exists IS NOT NULL
            AND l_par_alt_pt     IS NOT NULL) THEN
--
          OPEN c_get_par_alt_refno(p1.loau_obj_ref);
         FETCH c_get_par_alt_refno into l_pa_par_refno;
         CLOSE c_get_par_alt_refno;
--
-- Check for overlaps
--
-- Check start date for all records if supplied
--
          IF (p1.loau_start_date IS NOT NULL)   THEN
--
          l_oau_pa_exists    := NULL;
--
            OPEN chk_oau_dup_par(p1.loau_aun_code
		                        ,p1.loau_start_date
                                ,l_pa_par_refno);
           FETCH chk_oau_dup_par INTO l_oau_pa_exists;
            IF    chk_oau_dup_par%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup_par;
--
          END IF;
--
-- Check End Date when supplied
--
          IF (p1.loau_end_date IS NOT NULL)   THEN
--
          l_oau_pa1_exists    := NULL;
--
            OPEN chk_oau_dup1_par(p1.loau_aun_code
                                 ,p1.loau_end_date
                                 ,l_pa_par_refno);
           FETCH chk_oau_dup1_par INTO l_oau_pa1_exists;
            IF    chk_oau_dup1_par%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup1_par;
--
          END IF;
--
-- Check a record does not exist with a greater start date if loau_end_date
-- has not been supplied so trying to create an open record
--
          IF (p1.loau_end_date IS NULL)   THEN
--
            l_oau_pa2_exists    := NULL;
--
            OPEN chk_oau_dup2_ref(p1.loau_aun_code
		                         ,p1.loau_start_date
                                 ,l_pa_par_refno);
           FETCH chk_oau_dup2_ref INTO l_oau_pa2_exists;
            IF    chk_oau_dup2_ref%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',677);
            END IF;
           CLOSE chk_oau_dup2_ref;
--
          END IF;
--
        END IF;
--
      END IF;
--
-- ******************
--
-- Check that the combination supplied does not already exist in the 
-- object_admin_units table to prevent overlaps for ORG_FULL
--
      IF (p1.loau_rec_type = 'ORG_FULL') THEN
--
-- Get par_refno when reference and type are OK
--
        IF (    l_org_full_exists IS NOT NULL
            AND l_org_full_pt     IS NOT NULL) THEN
--
          OPEN c_get_org_full_refno(p1.loau_obj_ref);
         FETCH c_get_org_full_refno into l_of_par_refno;
         CLOSE c_get_org_full_refno;
--
-- Check for overlaps
--
-- Check start date for all records if supplied
--
          IF (p1.loau_start_date IS NOT NULL)   THEN
--
          l_oau_of_exists    := NULL;
--
            OPEN chk_oau_dup_par(p1.loau_aun_code
		                        ,p1.loau_start_date
                                ,l_of_par_refno);
           FETCH chk_oau_dup_par INTO l_oau_of_exists;
            IF    chk_oau_dup_par%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup_par;
--
          END IF;
--
--
-- Check End Date when supplied
--
--
          IF (p1.loau_end_date IS NOT NULL)   THEN
--
          l_oau_of1_exists    := NULL;
--
            OPEN chk_oau_dup1_par(p1.loau_aun_code
                                 ,p1.loau_end_date
                                 ,l_of_par_refno);
           FETCH chk_oau_dup1_par INTO l_oau_of1_exists;
            IF    chk_oau_dup1_par%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup1_par;
--
          END IF;
--
--
-- Check a record does not exist with a greater start date if loau_end_date
-- has not been supplied so trying to create an open record
--
          IF (p1.loau_end_date IS NULL)   THEN
--
            l_oau_of2_exists    := NULL;
--
            OPEN chk_oau_dup2_ref(p1.loau_aun_code
		                         ,p1.loau_start_date
                                 ,l_of_par_refno);
           FETCH chk_oau_dup2_ref INTO l_oau_of2_exists;
            IF    chk_oau_dup2_ref%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',677);
            END IF;
           CLOSE chk_oau_dup2_ref;
--
          END IF;
--
        END IF;
--
      END IF;
--
-- ******************
--
-- Check that the combination supplied does not already exist in the 
-- object_admin_units table to prevent overlaps for ORG_SHORT
--
      IF (p1.loau_rec_type = 'ORG_SHORT') THEN
--
-- Get par_refno when reference and type are OK
--
        IF (    l_org_short_exists IS NOT NULL
            AND l_org_short_pt     IS NOT NULL) THEN
--
          OPEN c_get_org_short_refno(p1.loau_obj_ref);
         FETCH c_get_org_short_refno into l_os_par_refno;
         CLOSE c_get_org_short_refno;
--
-- Check for overlaps
--
--
-- Check start date for all records if supplied
--
          IF (p1.loau_start_date IS NOT NULL)   THEN
--
          l_oau_os_exists    := NULL;
--
            OPEN chk_oau_dup_par(p1.loau_aun_code
		                        ,p1.loau_start_date
                                ,l_os_par_refno);
           FETCH chk_oau_dup_par INTO l_oau_os_exists;
            IF    chk_oau_dup_par%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup_par;
--
          END IF;
--
-- Check End Date when supplied
--
          IF (p1.loau_end_date IS NOT NULL)   THEN
--
          l_oau_os1_exists    := NULL;
--
            OPEN chk_oau_dup1_par(p1.loau_aun_code
                                 ,p1.loau_end_date
                                 ,l_os_par_refno);
           FETCH chk_oau_dup1_par INTO l_oau_os1_exists;
            IF    chk_oau_dup1_par%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup1_par;
--
          END IF;
--
--
-- Check a record does not exist with a greater start date if loau_end_date
-- has not been supplied so trying to create an open record
--
          IF (p1.loau_end_date IS NULL)   THEN
--
            l_oau_os2_exists    := NULL;
--
            OPEN chk_oau_dup2_ref(p1.loau_aun_code
		                          ,p1.loau_start_date
                                  ,l_os_par_refno);
           FETCH chk_oau_dup2_ref INTO l_oau_os2_exists;
            IF    chk_oau_dup2_ref%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',677);
            END IF;
           CLOSE chk_oau_dup2_ref;
--
          END IF;
--
        END IF;
--
      END IF;
--
-- ******************
--
-- Check that the combination supplied does not already exist in the 
-- object_admin_units table to prevent overlaps for PAR_REFNO
--
      IF (p1.loau_rec_type = 'PAR_REFNO') THEN
--
-- Get par_refno when reference and type are OK
--
        IF (    l_par_refno_exists IS NOT NULL
            AND l_par_refno_pt     IS NOT NULL) THEN
--
          OPEN c_get_par_refno(p1.loau_obj_ref);
         FETCH c_get_par_refno into l_pr_par_refno;
         CLOSE c_get_par_refno;
--
-- Check for overlaps
--
-- Check start date for all records if supplied
--
          IF (p1.loau_start_date IS NOT NULL)   THEN
--
          l_oau_pr_exists    := NULL;
--
            OPEN chk_oau_dup_par(p1.loau_aun_code
		                        ,p1.loau_start_date
                                ,l_pr_par_refno);
           FETCH chk_oau_dup_par INTO l_oau_pr_exists;
            IF    chk_oau_dup_par%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup_par;
--
          END IF;
--
-- Check End Date when supplied
--
          IF (p1.loau_end_date IS NOT NULL)   THEN
--
          l_oau_pr1_exists    := NULL;
--
            OPEN chk_oau_dup1_par(p1.loau_aun_code
                                 ,p1.loau_end_date
                                 ,l_pr_par_refno);
           FETCH chk_oau_dup1_par INTO l_oau_pr1_exists;
            IF    chk_oau_dup1_par%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup1_par;
--
          END IF;
--
-- Check a record does not exist with a greater start date if loau_end_date
-- has not been supplied so trying to create an open record
--
          IF (p1.loau_end_date IS NULL)   THEN
--
            l_oau_pr2_exists    := NULL;
--
            OPEN chk_oau_dup2_ref(p1.loau_aun_code
		                          ,p1.loau_start_date
                                  ,l_pr_par_refno);
           FETCH chk_oau_dup2_ref INTO l_oau_pr2_exists;
            IF    chk_oau_dup2_ref%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',677);
            END IF;
           CLOSE chk_oau_dup2_ref;
--
          END IF;
--
        END IF;
--
      END IF;
--
-- ******************
--
-- Check that the combination supplied does not already exist in the 
-- object_admin_units table to prevent overlaps for IPP_SHORT
--
      IF (p1.loau_rec_type = 'IPP_SHORT') THEN
--
-- Get par_refno when reference and type are OK
--
        IF (    l_ipp_short_exists IS NOT NULL) THEN
--
          OPEN c_get_ipp_short_refno(p1.loau_obj_ref);
         FETCH c_get_ipp_short_refno into l_is_ipp_refno;
         CLOSE c_get_ipp_short_refno;
--
-- Check for overlaps
--
-- Check start date for all records if supplied
--
          IF (p1.loau_start_date IS NOT NULL)   THEN
--
          l_oau_is_exists    := NULL;
--
            OPEN chk_oau_dup_ipp(p1.loau_aun_code
		                        ,p1.loau_start_date
                                ,l_is_ipp_refno);
           FETCH chk_oau_dup_ipp INTO l_oau_is_exists;
            IF    chk_oau_dup_ipp%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup_ipp;
--
          END IF;
--
-- Check End Date when supplied
--
          IF (p1.loau_end_date IS NOT NULL)   THEN
--
          l_oau_is1_exists    := NULL;
--
            OPEN chk_oau_dup1_ipp(p1.loau_aun_code
                                 ,p1.loau_end_date
                                 ,l_is_ipp_refno);
           FETCH chk_oau_dup1_ipp INTO l_oau_is1_exists;
            IF    chk_oau_dup1_ipp%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',676);
            END IF;
           CLOSE chk_oau_dup1_ipp;
--
          END IF;
--
-- Check a record does not exist with a greater start date if loau_end_date
-- has not been supplied so trying to create an open record
--
          IF (p1.loau_end_date IS NULL)   THEN
--
            l_oau_is2_exists    := NULL;
--
            OPEN chk_oau_dup2_ipp(p1.loau_aun_code
		                          ,p1.loau_start_date
                                  ,l_is_ipp_refno);
           FETCH chk_oau_dup2_ipp INTO l_oau_is2_exists;
            IF    chk_oau_dup2_ipp%found THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',677);
            END IF;
           CLOSE chk_oau_dup2_ipp;
--
          END IF;
--
        END IF;
--
      END IF; --end of overlap check
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
PROCEDURE dataload_delete(p_batch_id  IN VARCHAR2,
                          p_date      IN DATE)
AS
CURSOR c1(p_batch_id  VARCHAR2) 
IS
SELECT rowid rec_rowid,
       loau_dlb_batch_id,
       loau_dl_seqno,
       loau_dl_load_status,
       loau_aun_code,
       loau_start_date,
       loau_end_date,
       loau_rec_type,
       loau_obj_ref,
       loau_comments,
	   loau_del_oau_refno
  FROM dl_hem_object_admin_units
 WHERE loau_dlb_batch_id   = p_batch_id
   AND loau_dl_load_status = 'C';
--
-- ***********************************************************************
--
CURSOR c_get_par_alt_refno(p_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_per_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_org_full_refno(p_org_full_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_org_name = p_org_full_ref;
--
-- ***********************************************************************
--
CURSOR c_get_org_short_refno(p_org_short_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_org_short_name = p_org_short_ref;
--
-- ***********************************************************************
--
CURSOR c_get_ipp_short_refno(p_ipp_short_ref VARCHAR2) 
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_short_ref;
--
-- ***********************************************************************
--
CURSOR c_get_par_refno(p_par_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb             VARCHAR2(30);
cd             DATE;
cp             VARCHAR2(30) := 'DELETE';
ct             VARCHAR2(30) := 'DL_HEM_OBJECT_ADMIN_UNITS';
cs             INTEGER;
ce             VARCHAR2(200);
l_id           ROWID;
--
i              INTEGER := 0;
l_an_tab       VARCHAR2(1);
--
l_par_refno    NUMBER(8);
l_ipp_refno    NUMBER(10);
l_rec_type     VARCHAR2(10);
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_object_admin_units.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_object_admin_units.dataload_delete',3);
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
        cs   := p1.loau_dl_seqno;
        l_id := p1.rec_rowid;
--
        SAVEPOINT SP1;
-- *********************************************************************** 
--
-- get the par_refno and ipp_refno from parties and interested_parties tables
--
        l_par_refno := NULL;
        l_ipp_refno := NULL;
        l_rec_type  := NULL;
--
        l_rec_type  := p1.loau_rec_type;
--
	    IF  (l_rec_type = 'PAR_ALT') THEN
--
          OPEN c_get_par_alt_refno(p1.loau_obj_ref);
         FETCH c_get_par_alt_refno into l_par_refno;
         CLOSE c_get_par_alt_refno;
--
        ELSIF (l_rec_type = 'ORG_FULL') THEN
--
          OPEN c_get_org_full_refno(p1.loau_obj_ref);
         FETCH c_get_org_full_refno into l_par_refno;
         CLOSE c_get_org_full_refno;
--
        ELSIF (l_rec_type = 'ORG_SHORT') THEN
--
          OPEN c_get_org_short_refno(p1.loau_obj_ref);
         FETCH c_get_org_short_refno into l_par_refno;
         CLOSE c_get_org_short_refno;
--
        ELSIF (l_rec_type = 'IPP_SHORT') THEN
--
          OPEN c_get_ipp_short_refno(p1.loau_obj_ref);
         FETCH c_get_ipp_short_refno into l_ipp_refno;
         CLOSE c_get_ipp_short_refno;
--
        ELSIF (l_rec_type = 'PAR_REFNO') THEN
-- 
          OPEN c_get_par_refno(p1.loau_obj_ref);
         FETCH c_get_par_refno into l_par_refno;
         CLOSE c_get_par_refno;
--
        END IF;
--
-- ***********************************************************************
--
        IF (l_rec_type = 'IPP_SHORT') THEN
--
          DELETE 
            FROM object_admin_units
           WHERE oau_refno      = p1.loau_del_oau_refno
             AND oau_aun_code   = p1.loau_aun_code
             AND oau_start_date = p1.loau_start_date
             AND oau_ipp_refno  = l_ipp_refno
             AND oau_par_refno IS NULL;
--
        ELSE
--
          DELETE 
            FROM object_admin_units
           WHERE oau_refno      = p1.loau_del_oau_refno
             AND oau_aun_code   = p1.loau_aun_code
             AND oau_start_date = p1.loau_start_date
             AND oau_par_refno  = l_par_refno
             AND oau_ipp_refno IS NULL;
--
        END IF;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('OBJECT_ADMIN_UNITS');
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
END s_dl_hem_object_admin_units;
--
/