--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_consents
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.7.0     VS   18-MAR-2013  Initial Creation.
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
  UPDATE dl_hem_consents
     SET lcns_dl_load_status = p_status
   WHERE rowid               = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_consents');
          RAISE;
--
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create(p_batch_id IN VARCHAR2,
                          p_date     IN DATE)
AS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       LCNS_DLB_BATCH_ID,
       LCNS_DL_SEQNO,
       LCNS_DL_LOAD_STATUS,
       LCNS_PAR_REFERENCE_TYPE,
       LCNS_PAR_REFERENCE,
       LCNS_GRANTED_DATE,
       LCNS_START_DATE,
       LCNS_PAR_REFERENCE_GRANTED,
       LCNS_HRV_CTYP_CODE,
       LCNS_HRV_CSO_CODE,
       NVL(LCNS_CREATED_BY, 'DATALOAD') LCNS_CREATED_BY,
       NVL(LCNS_CREATED_DATE, SYSDATE)  LCNS_CREATED_DATE,
       LCNS_HRV_CER_CODE,
       LCNS_END_DATE,
       LCNS_ALTERNATIVE_REFERENCE,
       LCNS_REVIEW_DATE,
       LCNS_COMMENTS,
       LCNS_REFNO
  FROM dl_hem_consents
 WHERE lcns_dlb_batch_id   = p_batch_id
   AND lcns_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_get_par(p_par_refno VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_refno;
--
-- ***********************************************************************
--
CURSOR c_get_prf(p_par_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_CONSENTS';
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
i                          INTEGER := 0;
l_exists                   VARCHAR2(1);
l_par_refno                parties.par_refno%type;
l_par_refno_granted        parties.par_refno%type;
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_consents.dataload_create');
    fsc_utils.debug_message('s_dl_hem_consents.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lcns_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
          l_par_refno          := NULL;
          l_par_refno_granted  := NULL;
--
          IF (p1.LCNS_PAR_REFERENCE_TYPE = 'PAR') THEN
--
            OPEN c_get_par(p1.lcns_par_reference);
           FETCH c_get_par INTO l_par_refno;
           CLOSE c_get_par;
--
            OPEN c_get_par(p1.lcns_par_reference_granted);
           FETCH c_get_par INTO l_par_refno_granted;
           CLOSE c_get_par;
--
          ELSIF (p1.LCNS_PAR_REFERENCE_TYPE = 'PRF') THEN
--
            OPEN c_get_prf(p1.lcns_par_reference);
           FETCH c_get_prf INTO l_par_refno;
           CLOSE c_get_prf;
--
            OPEN c_get_prf(p1.lcns_par_reference_granted);
           FETCH c_get_prf INTO l_par_refno_granted;
           CLOSE c_get_prf;
--
          END IF;
--
-- Insert into consents
--
          INSERT /* +APPEND */ into  consents(CNS_REFNO,
                                              CNS_GRANTED_DATE,
                                              CNS_START_DATE,
                                              CNS_PAR_REFNO,
                                              CNS_PAR_REFNO_GRANTED_BY,
                                              CNS_HRV_CTYP_CODE,
                                              CNS_HRV_CSO_CODE,
                                              CNS_HRV_CER_CODE,
                                              CNS_END_DATE,
                                              CNS_ALTERNATIVE_REFERENCE,
                                              CNS_REVIEW_DATE,
                                              CNS_COMMENTS     
                                             )
--
                                      VALUES(p1.lcns_refno,
                                             p1.lcns_granted_date,
                                             p1.lcns_start_date,
                                             l_par_refno,
                                             l_par_refno_granted,
                                             p1.lcns_hrv_ctyp_code,
                                             p1.lcns_hrv_cso_code,
                                             p1.lcns_hrv_cer_code,
                                             p1.lcns_end_date,
                                             p1.lcns_alternative_reference,
                                             p1.lcns_review_date,
                                             p1.lcns_comments
                                            );
--
-- Maintain CREATED BY/DATE
--
          UPDATE consents
             SET cns_created_by   = p1.lcns_created_by,
                 cns_created_date = p1.lcns_created_date
           WHERE cns_refno = p1.lcns_refno;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,500000) = 0 THEN 
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
       LCNS_DLB_BATCH_ID,
       LCNS_DL_SEQNO,
       LCNS_DL_LOAD_STATUS,
       LCNS_PAR_REFERENCE_TYPE,
       LCNS_PAR_REFERENCE,
       LCNS_GRANTED_DATE,
       LCNS_START_DATE,
       LCNS_PAR_REFERENCE_GRANTED,
       LCNS_HRV_CTYP_CODE,
       LCNS_HRV_CSO_CODE,
       LCNS_HRV_CER_CODE,
       LCNS_END_DATE,
       LCNS_ALTERNATIVE_REFERENCE,
       LCNS_REVIEW_DATE,
       LCNS_COMMENTS,
       LCNS_REFNO
  FROM dl_hem_consents
 WHERE lcns_dlb_batch_id    = p_batch_id
   AND lcns_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
-- ***********************************************************************
--
CURSOR c_chk_par(p_par_refno VARCHAR2) 
IS
SELECT par_refno,
       par_per_date_of_birth,
       par_per_forename
  FROM parties
 WHERE par_refno = p_par_refno;
--
-- ***********************************************************************
--
CURSOR c_chk_prf(p_par_alt_ref VARCHAR2) 
IS
SELECT par_refno,
       par_per_date_of_birth,
       par_per_forename
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_CONSENTS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists                     VARCHAR2(1);
l_pro_refno                  NUMBER(10);
l_errors                     VARCHAR2(10);
l_error_ind                  VARCHAR2(10);
i                            INTEGER :=0;
--
l_par_refno                  parties.par_refno%type;
l_par_date_of_birth          parties.par_per_date_of_birth%type;
l_par_forename               parties.par_per_forename%type;
--
l_par_refno_granted          parties.par_refno%type;
l_par_date_of_birth_granted  parties.par_per_date_of_birth%type;
l_par_forename_granted       parties.par_per_forename%type;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_consents.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_consents.dataload_validate',3);
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
          cs   := p1.lcns_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';

--
-- Validation checks required
--

--
-- ***********************************************************************
--
-- Check Person Reference Value Type has been supplied and is valid
--
          IF (p1.LCNS_PAR_REFERENCE_TYPE IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',580);
--
          ELSIF (p1.LCNS_PAR_REFERENCE_TYPE NOT IN ('PAR','PRF')) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',581);
--
          END IF;
--
-- ***********************************************************************
--
-- Check the Consent Person Reference has been supplied and is valid
--
-- If the Person Exists and the Consent Type is ICS then Date of Birth and 
-- Forename must be held against existing person

          l_par_refno          := NULL;
          l_par_date_of_birth  := NULL;
          l_par_forename       := NULL;
--
--
          IF (p1.LCNS_PAR_REFERENCE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',582);
          ELSE
--
             IF (p1.LCNS_PAR_REFERENCE_TYPE = 'PAR') THEN
--
               OPEN c_chk_par(p1.lcns_par_reference);
              FETCH c_chk_par INTO l_par_refno, l_par_date_of_birth, l_par_forename;
              CLOSE c_chk_par;
--
              IF (l_par_refno IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',583);
              END IF;
--
             ELSIF (p1.LCNS_PAR_REFERENCE_TYPE = 'PRF') THEN
--
                  OPEN c_chk_prf(p1.lcns_par_reference);
                 FETCH c_chk_prf INTO l_par_refno, l_par_date_of_birth, l_par_forename;
                 CLOSE c_chk_prf;
--
                 IF (l_par_refno IS NULL) THEN
                  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',583);
                 END IF;
--
             END IF; 
--
          END IF; --(p1.LCNS_PAR_REFERENCE IS NULL)
--
-- ***********************************************************************
--
-- Check Granted By Date has been supplied
--
          IF (p1.LCNS_GRANTED_DATE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',011);
          END IF;
--
-- ***********************************************************************
--
-- Check Start Date has been supplied
--
          IF (p1.LCNS_START_DATE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',042);
          END IF;
--
-- ***********************************************************************
--
-- Check the Consent Granted By Person Reference has been supplied and is valid
--
          l_par_refno_granted         := NULL;
          l_par_date_of_birth_granted := NULL;
          l_par_forename_granted      := NULL;
--
--
          IF (p1.LCNS_PAR_REFERENCE_GRANTED IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',584);
          ELSE
--
             IF (p1.LCNS_PAR_REFERENCE_TYPE = 'PAR') THEN
--
               OPEN c_chk_par(p1.lcns_par_reference_granted);
              FETCH c_chk_par INTO l_par_refno_granted, l_par_date_of_birth_granted, l_par_forename_granted;
              CLOSE c_chk_par;
--
              IF (l_par_refno_granted IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',002);
              END IF;
--
             ELSIF (p1.LCNS_PAR_REFERENCE_TYPE = 'PRF') THEN
--
                  OPEN c_chk_prf(p1.lcns_par_reference_granted);
                 FETCH c_chk_prf INTO l_par_refno_granted, l_par_date_of_birth_granted, l_par_forename_granted;
                 CLOSE c_chk_prf;
--
                 IF (l_par_refno_granted IS NULL) THEN
                  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',002);
                 END IF;
--
             END IF; 
--
          END IF; --(p1.LCNS_PAR_REFERENCE_GRANTED IS NULL)
--
-- ***********************************************************************
--
-- Check Consent Type has been supplied and is valid
--
          IF (p1.LCNS_HRV_CTYP_CODE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',585);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('CONSENTTYPE',p1.lcns_hrv_ctyp_code,'N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',008);
--
          END IF;
--
-- ***********************************************************************
--
-- Consent Source has been supplied and is valid
--
          IF (p1.LCNS_HRV_CSO_CODE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',586);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('CONSENTSOURCE',p1.lcns_hrv_cso_code,'N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',009);
--
          END IF;
--
-- ***********************************************************************
--
          IF (p1.LCNS_HRV_CTYP_CODE = 'ICS') THEN
--
           IF (   l_par_date_of_birth IS NULL
               OR l_par_forename      IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',001);
--
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- End Reason is valid if supplied
--
          IF (p1.LCNS_HRV_CER_CODE IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('CONSENTENDRSN',p1.lcns_hrv_cer_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',010);
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check the end date is not before the Granted Date
--
          IF (NVL(p1.lcns_end_date, p1.lcns_granted_date) < p1.lcns_granted_date) THEN 
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',004);
          END IF;
--
-- ***********************************************************************
--
-- Check the end date is not before the Start Date
--
          IF (nvl(p1.lcns_end_date, p1.lcns_start_date) < p1.lcns_start_date) THEN   
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',003);
          END IF;
--
-- ***********************************************************************
--
-- Check End Reason is supplied if end date is supplied
--
          IF (    p1.lcns_end_date IS NOT NULL
              AND p1.lcns_hrv_cer_code IS NULL) THEN 
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',005);
--
          END IF;
--
-- ***********************************************************************
--
-- Check the end date is supplied if the end reason is supplied
--
          IF (    p1.lcns_end_date IS NULL
              AND p1.lcns_hrv_cer_code IS NOT NULL) THEN 
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',006);
--
          END IF;
--
-- ***********************************************************************
--
-- Check the review date is in the future
--
          IF (NVL(p1.lcns_review_date, TRUNC(sysdate)) < TRUNC(sysdate)) THEN 
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',007);
          END IF;
--
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
PROCEDURE dataload_delete(p_batch_id IN VARCHAR2,
                          p_date     IN date)
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       lcns_dlb_batch_id,
       lcns_dl_seqno,
       lcns_dl_load_status,
       lcns_refno
  FROM dl_hem_consents
 WHERE lcns_dlb_batch_id   = p_batch_id
   AND lcns_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb               VARCHAR2(30);
cd               DATE;
cp               VARCHAR2(30) := 'DELETE';
ct               VARCHAR2(30) := 'DL_HEM_CONSENTS';
cs               INTEGER;
ce               VARCHAR2(200);
l_id             ROWID;
l_an_tab         VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_consents.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_consents.dataload_delete',3 );
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
          cs   := p1.lcns_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from consents
--
          DELETE 
		    FROM consents
           WHERE cns_refno = p1.lcns_refno;
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
           COMMIT; 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONSENTS');
--
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
END s_dl_hem_consents;
/