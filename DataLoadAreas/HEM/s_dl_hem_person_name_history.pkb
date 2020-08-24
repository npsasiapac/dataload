--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_person_name_history
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.3.0     VS   17-FEB-2011  Initial Creation.
--
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_hem_person_name_history
     SET lpnh_dl_load_status = p_status
   WHERE rowid               = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_person_name_history');
          RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPNH_DLB_BATCH_ID,
       LPNH_DL_SEQNO,
       LPNH_DL_LOAD_STATUS,
       LPNH_PAR_TYPE,
       LPNH_PAR_PER_ALT_REF,
       LPNH_START_DATE,
       LPNH_END_DATE,
       LPNH_FRV_FPH_CODE,
       LPNH_USERNAME,
       LPNH_SURNAME,
       LPNH_FORENAME,
       LPNH_INITIALS,
       LPNH_TITLE,
       LPNH_OTHER_NAMES,
       LPNH_SURNAME_PREFIX,
       LPNH_SURNAME_MLANG,
       LPNH_FORENAME_MLANG,
       LPNH_INITIALS_MLANG,
       LPNH_OTHER_NAMES_MLANG
  FROM dl_hem_person_name_history
 WHERE lpnh_dlb_batch_id   = p_batch_id
   AND lpnh_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_get_par(p_par_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_per_alt_ref;
--
-- ***********************************************************************
--
CURSOR c_get_prf(p_par_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_NAME_HISTORY';
cs                   INTEGER;
ce	             VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                          INTEGER := 0;
l_exists                   VARCHAR2(1);
l_par_refno                parties.par_refno%type;
--
--
-- ***********************************************************************
--
BEGIN
--
--   execute immediate 'alter trigger CNS_BR_I disable';
--    execute immediate 'alter trigger CNS_BR_IU disable';
--
    fsc_utils.proc_start('s_dl_hem_person_name_history.dataload_create');
    fsc_utils.debug_message('s_dl_hem_person_name_history.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lpnh_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          l_par_refno := NULL;
--
          IF (p1.lpnh_par_type = 'PAR') THEN
--
            OPEN c_get_par(p1.lpnh_par_per_alt_ref);
           FETCH c_get_par INTO l_par_refno;
           CLOSE c_get_par;
--
          ELSE
--
              OPEN c_get_prf(p1.lpnh_par_per_alt_ref);
             FETCH c_get_prf INTO l_par_refno;
             CLOSE c_get_prf;
--
          END IF;
--
-- Insert into consents
--
          INSERT /* +APPEND */ INTO PERSON_NAME_HISTORY(PNH_PAR_REFNO,
                                                        PNH_START_DATE,
                                                        PNH_END_DATE,
                                                        PNH_FRV_FPH_CODE,
                                                        PNH_USERNAME,
                                                        PNH_SURNAME,
                                                        PNH_FORENAME,
                                                        PNH_INITIALS,
                                                        PNH_TITLE,
                                                        PNH_OTHER_NAMES,
                                                        PNH_SURNAME_PREFIX,
                                                        PNH_SURNAME_MLANG,
                                                        PNH_FORENAME_MLANG,
                                                        PNH_INITIALS_MLANG,
                                                        PNH_OTHER_NAMES_MLANG
                                                       )
--
                                                 VALUES(l_par_refno,
                                                        p1.LPNH_START_DATE,
                                                        p1.LPNH_END_DATE,
                                                        p1.LPNH_FRV_FPH_CODE,
                                                        p1.LPNH_USERNAME,
                                                        p1.LPNH_SURNAME,
                                                        p1.LPNH_FORENAME,
                                                        p1.LPNH_INITIALS,
                                                        p1.LPNH_TITLE,
                                                        p1.LPNH_OTHER_NAMES,
                                                        p1.LPNH_SURNAME_PREFIX,
                                                        p1.LPNH_SURNAME_MLANG,
                                                        p1.LPNH_FORENAME_MLANG,
                                                        p1.LPNH_INITIALS_MLANG,
                                                        p1.LPNH_OTHER_NAMES_MLANG
                                                       );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,500000)=0 THEN 
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
    COMMIT;
--
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONSENTS');
--
--    execute immediate 'alter trigger CNS_BR_I enable';
--    execute immediate 'alter trigger CNS_BR_IU enable';
--
    fsc_utils.proc_END;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_create;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_validate(p_batch_id IN VARCHAR2,
                            p_date     IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPNH_DLB_BATCH_ID,
       LPNH_DL_SEQNO,
       LPNH_DL_LOAD_STATUS,
       LPNH_PAR_TYPE,
       LPNH_PAR_PER_ALT_REF,
       LPNH_START_DATE,
       LPNH_END_DATE,
       LPNH_FRV_FPH_CODE,
       LPNH_USERNAME,
       LPNH_SURNAME,
       LPNH_FORENAME,
       LPNH_INITIALS,
       LPNH_TITLE,
       LPNH_OTHER_NAMES,
       LPNH_SURNAME_PREFIX,
       LPNH_SURNAME_MLANG,
       LPNH_FORENAME_MLANG,
       LPNH_INITIALS_MLANG,
       LPNH_OTHER_NAMES_MLANG
  FROM dl_hem_person_name_history
 WHERE lpnh_dlb_batch_id   = p_batch_id
   AND lpnh_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_chk_par(p_par_per_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_per_alt_ref;
--
--
-- ***********************************************************************
--
CURSOR c_chk_prf(p_par_per_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_NAME_HISTORY';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists                   VARCHAR2(1);
l_errors                   VARCHAR2(10);
l_error_ind                VARCHAR2(10);
i                          INTEGER :=0;
l_par_refno                parties.par_refno%type;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_person_name_history.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_person_name_history.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_DATE;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs   := p1.lpnh_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';

--
-- Validation checks required
--
-- Check the Person exists
--
          l_par_refno := NULL;
--
--
-- ***********************************************************************
--
-- Check party reference type is valid
--
          IF (p1.lpnh_par_type NOT IN ('PAR','PRF')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',455);
          END IF;
--
-- ***********************************************************************
--
-- If the Person Exists 
--
--
--
          IF (p1.lpnh_par_type = 'PAR') THEN
--
            OPEN c_chk_par(p1.lpnh_par_per_alt_ref);
           FETCH c_chk_par INTO l_par_refno;
--
           IF (c_chk_par%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',331);
           END IF;
--
           CLOSE c_chk_par;
--
          ELSE
--
              OPEN c_chk_prf(p1.lpnh_par_per_alt_ref);
             FETCH c_chk_prf INTO l_par_refno;
--
             IF (c_chk_prf%NOTFOUND) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',331);
             END IF;
--
             CLOSE c_chk_prf;
--
          END IF;
--
-- ***********************************************************************
--
-- Check the end date is not before the Start Date
--
          IF (p1.lpnh_end_date < p1.lpnh_start_date) THEN   
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',003);
          END IF;
--
-- ***********************************************************************
--
-- Check the Reference Values
--
-- Reason
--
          IF (NOT s_dl_hem_utils.exists_frv('FNREAS',p1.lpnh_frv_fph_code,'N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',456);
          END IF;
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
         IF (l_errors = 'F') THEN
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
         i := i+1; 
--
         IF MOD(i,1000)=0 THEN 
          COMMIT; 
         END IF;
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
    fsc_utils.proc_END;
--
    COMMIT;
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
PROCEDURE dataload_delete(p_batch_id  IN VARCHAR2,
                          p_date      IN date) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPNH_DLB_BATCH_ID,
       LPNH_DL_SEQNO,
       LPNH_DL_LOAD_STATUS,
       LPNH_PAR_TYPE,
       LPNH_PAR_PER_ALT_REF,
       LPNH_START_DATE
  FROM dl_hem_person_name_history
 WHERE lpnh_dlb_batch_id   = p_batch_id
   AND lpnh_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_get_par(p_par_per_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_per_alt_ref;
--
--
-- ***********************************************************************
--
CURSOR c_get_prf(p_par_per_alt_ref  VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_NAME_HISTORY';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
l_par_refno      NUMBER(10);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_person_name_history.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_person_name_history.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lpnh_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
          l_par_refno := NULL;
--
          IF (p1.lpnh_par_type = 'PAR') THEN
--
            OPEN c_get_par(p1.lpnh_par_per_alt_ref);
           FETCH c_get_par INTO l_par_refno;
           CLOSE c_get_par;
--
          ELSE
--
              OPEN c_get_prf(p1.lpnh_par_per_alt_ref);
             FETCH c_get_prf INTO l_par_refno;
             CLOSE c_get_prf;
--
          END IF;
--
--
-- Delete from consents
--
          DELETE 
            FROM person_name_history
           WHERE pnh_par_refno  = l_par_refno
             AND pnh_start_date = p1.lpnh_start_date;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          IF mod(i,5000) = 0 THEN 
           commit; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
                  set_record_status_flag(l_id,'C');
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PERSON_NAME_HISTORY');
--
    fsc_utils.proc_end;
--
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_delete;
--
END s_dl_hem_person_name_history;
/
