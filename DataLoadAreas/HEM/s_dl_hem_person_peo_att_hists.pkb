--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_person_peo_att_hists
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.3.0     VS   07-MAR-2011  Initial Creation.
--  1.1     6.12      AJ   06-JUN-2016  HD2 error numbers changed and put on
--                                      standard version as numbers numbers
--                                      on old bespoke have been used for
--                                      other data loaders
--                                      HNZ Consents (was 455 now 910)
--                                      HNZ Name Change History (was 456 now 911)
--                                      HNZ Person Attributes (was 457-464 now 912-919)
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
  UPDATE dl_hem_person_peo_att_hists
     SET lppah_dl_load_status = p_status
   WHERE rowid                = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_person_peo_att_hists');
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
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPPAH_DLB_BATCH_ID,
       LPPAH_DL_SEQNO,
       LPPAH_DL_LOAD_STATUS,
       LPPAH_PAR_TYPE,
       LPPAH_PAR_PER_ALT_REF,
       LPPAH_PEAT_CODE,
       LPPAH_ORIGINAL_CREATED_BY,
       LPPAH_ORIGINAL_CREATED_DATE,
       NVL(LPPAH_CREATED_BY,'DATALOAD') LPPAH_CREATED_BY,
       NVL(LPPAH_CREATED_DATE,SYSDATE)  LPPAH_CREATED_DATE,
       LPPAH_PAAV_CODE,
       LPPAH_DATE_VALUE,
       LPPAH_NUMERIC_VALUE,
       LPPAH_TEXT_VALUE,
       LPPAH_COMMENTS,
       LPPAH_YES_NO_VALUE
  FROM dl_hem_person_peo_att_hists
 WHERE lppah_dlb_batch_id   = p_batch_id
   AND lppah_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_PEO_ATT_HISTS';
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
   execute immediate 'alter trigger PPAH_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_person_peo_att_hists.dataload_create');
    fsc_utils.debug_message('s_dl_hem_person_peo_att_hists.dataload_create',3);
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
          cs   := p1.lppah_dl_seqno;
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
          IF (p1.lppah_par_type = 'PAR') THEN
--
            OPEN c_get_par(p1.lppah_par_per_alt_ref);
           FETCH c_get_par INTO l_par_refno;
           CLOSE c_get_par;
--
          ELSE
--
              OPEN c_get_prf(p1.lppah_par_per_alt_ref);
             FETCH c_get_prf INTO l_par_refno;
             CLOSE c_get_prf;
--
          END IF;
--
-- Insert into person_people_attributes
--
          INSERT /* +APPEND */ INTO PERSON_PEOPLE_ATTRIB_HISTS(PPAH_PAR_REFNO,
                                                               PPAH_PEAT_CODE,
                                                               PPAH_ORIGINAL_CREATED_BY,
                                                               PPAH_ORIGINAL_CREATED_DATE,
                                                               PPAH_CREATED_BY,
                                                               PPAH_CREATED_DATE,
                                                               PPAH_PAAV_CODE,
                                                               PPAH_DATE_VALUE,
                                                               PPAH_NUMERIC_VALUE,
                                                               PPAH_TEXT_VALUE,
                                                               PPAH_COMMENTS,
                                                               PPAH_YES_NO_VALUE
                                                              )
--
                                                        VALUES(l_par_refno,
                                                               p1.LPPAH_PEAT_CODE,
                                                               p1.LPPAH_ORIGINAL_CREATED_BY,
                                                               p1.LPPAH_ORIGINAL_CREATED_DATE,
                                                               p1.LPPAH_CREATED_BY,
                                                               p1.LPPAH_CREATED_DATE,
                                                               p1.LPPAH_PAAV_CODE,
                                                               p1.LPPAH_DATE_VALUE,
                                                               p1.LPPAH_NUMERIC_VALUE,
                                                               p1.LPPAH_TEXT_VALUE,
                                                               p1.LPPAH_COMMENTS,
                                                               p1.LPPAH_YES_NO_VALUE
                                                              );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
          i := i+1; 
--
          IF MOD(i,50000)=0 THEN 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PERSON_PEOPLE_ATTRIB_HISTS');
--
    execute immediate 'alter trigger PPAH_BR_I enable';
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
       LPPAH_DLB_BATCH_ID,
       LPPAH_DL_SEQNO,
       LPPAH_DL_LOAD_STATUS,
       LPPAH_PAR_TYPE,
       LPPAH_PAR_PER_ALT_REF,
       LPPAH_PEAT_CODE,
       LPPAH_ORIGINAL_CREATED_BY,
       LPPAH_ORIGINAL_CREATED_DATE,
       NVL(LPPAH_CREATED_BY,'DATALOAD') LPPAH_CREATED_BY,
       NVL(LPPAH_CREATED_DATE,SYSDATE)  LPPAH_CREATED_DATE,
       LPPAH_PAAV_CODE,
       LPPAH_DATE_VALUE,
       LPPAH_NUMERIC_VALUE,
       LPPAH_TEXT_VALUE,
       LPPAH_COMMENTS,
       LPPAH_YES_NO_VALUE
  FROM dl_hem_person_peo_att_hists
 WHERE lppah_dlb_batch_id   = p_batch_id
   AND lppah_dl_load_status IN ('L','F','O');
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
--
-- ***********************************************************************
--
CURSOR c_chk_peat(p_peat_code  VARCHAR2)
IS
SELECT *
  FROM people_attributes
 WHERE peat_code = p_peat_code;
--
--
-- ***********************************************************************
--
CURSOR c_chk_paav(p_paav_code  VARCHAR2,
                  p_peat_code  VARCHAR2)
IS
SELECT 'X'
  FROM people_attrib_allowed_vals
 WHERE paav_code      = p_paav_code
   AND paav_peat_code = p_peat_code;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_PEO_ATT_HISTS';
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
l_peat_rec                 people_attributes%ROWTYPE;
l_paav_exists              VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_person_peo_att_hists.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_person_peo_att_hists.dataload_validate',3);
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
          cs   := p1.lppah_dl_seqno;
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
          IF (p1.lppah_par_type NOT IN ('PAR','PRF')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',910);
          END IF;
--
-- ***********************************************************************
--
-- If the Person Exists 
--
--
--
          IF (p1.lppah_par_type = 'PAR') THEN
--
            OPEN c_chk_par(p1.lppah_par_per_alt_ref);
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
              OPEN c_chk_prf(p1.lppah_par_per_alt_ref);
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
-- Check Person Attribute Code is valid
--
           OPEN c_chk_peat(p1.lppah_peat_code);
          FETCH c_chk_peat INTO l_peat_rec;
--
          IF (c_chk_peat%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',913);
          END IF;
--
          CLOSE c_chk_peat;
--
-- ***********************************************************************
--
-- Check People Allowed Vals Code is valid for peat_code, if supplied
--
          IF (    p1.lppah_paav_code   IS NOT NULL
              AND l_peat_rec.peat_code IS NOT NULL) THEN
--
            OPEN c_chk_paav(p1.lppah_paav_code,p1.lppah_peat_code);
           FETCH c_chk_paav INTO l_paav_exists;
--
           IF (c_chk_paav%NOTFOUND) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',914);
           END IF;
--
           CLOSE c_chk_paav;
--
          END IF;  
--
-- ***********************************************************************
--
-- Check Yes/No Value is valid
--
          IF (p1.lppah_yes_no_value NOT IN ('Y', 'N')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',919);
          END IF;
--
-- ***********************************************************************
--
-- Check the Reference Values
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
PROCEDURE dataload_delete(p_batch_id  IN VARCHAR2,
                          p_date      IN date) IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LPPAH_DLB_BATCH_ID,
       LPPAH_DL_SEQNO,
       LPPAH_DL_LOAD_STATUS,
       LPPAH_PAR_TYPE,
       LPPAH_PAR_PER_ALT_REF,
       LPPAH_PEAT_CODE,
       LPPAH_ORIGINAL_CREATED_BY,
       LPPAH_ORIGINAL_CREATED_DATE,
       LPPAH_CREATED_BY,
       LPPAH_CREATED_DATE,
       LPPAH_PAAV_CODE,
       LPPAH_DATE_VALUE,
       LPPAH_NUMERIC_VALUE,
       LPPAH_TEXT_VALUE,
       LPPAH_COMMENTS,
       LPPAH_YES_NO_VALUE
  FROM dl_hem_person_peo_att_hists
 WHERE lppah_dlb_batch_id   = p_batch_id
   AND lppah_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
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
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_PEO_ATT_HISTS';
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
l_exists             VARCHAR2(1);
i                    INTEGER :=0;
l_par_refno          parties.par_refno%type;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_person_peo_att_hists.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_person_peo_att_hists.dataload_delete',3 );
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
          cs   := p1.lppah_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
          l_par_refno := NULL;
--
          IF (p1.lppah_par_type = 'PAR') THEN
--
            OPEN c_get_par(p1.lppah_par_per_alt_ref);
           FETCH c_get_par INTO l_par_refno;
           CLOSE c_get_par;
--
          ELSE
--
              OPEN c_get_prf(p1.lppah_par_per_alt_ref);
             FETCH c_get_prf INTO l_par_refno;
             CLOSE c_get_prf;
--
          END IF;
--
--
-- Delete from person_people_attributes
--
          DELETE 
            FROM person_people_attrib_hists
           WHERE PPAH_PAR_REFNO                         = l_par_refno
             AND PPAH_PEAT_CODE                         = p1.LPPAH_PEAT_CODE
             AND PPAH_ORIGINAL_CREATED_BY               = p1.LPPAH_ORIGINAL_CREATED_BY
             AND PPAH_ORIGINAL_CREATED_DATE             = p1.LPPAH_ORIGINAL_CREATED_DATE
             AND PPAH_CREATED_BY                        = NVL(p1.LPPAH_CREATED_BY,PPAH_CREATED_BY)
             AND PPAH_CREATED_DATE                      = NVL(p1.LPPAH_CREATED_DATE,PPAH_CREATED_DATE)
             AND NVL(PPAH_PAAV_CODE,     '~XYZ~')       = NVL(p1.LPPAH_PAAV_CODE,     '~XYZ~')
             AND NVL(PPAH_DATE_VALUE,    '01-JAN-2099') = NVL(p1.LPPAH_DATE_VALUE,    '01-JAN-2099')
             AND NVL(PPAH_NUMERIC_VALUE, 1234567)       = NVL(p1.LPPAH_NUMERIC_VALUE, 1234567)
             AND NVL(PPAH_TEXT_VALUE,    '~XYZ~')       = NVL(p1.LPPAH_TEXT_VALUE,    '~XYZ~')
             AND NVL(PPAH_COMMENTS,      '~XYZ~')       = NVL(p1.LPPAH_COMMENTS,      '~XYZ~')
             AND NVL(PPAH_YES_NO_VALUE,  '~XYZ~')       = NVL(p1.LPPAH_YES_NO_VALUE,  '~XYZ~');
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PERSON_PEOPLE_ATTRIB_HISTS');
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
END s_dl_hem_person_peo_att_hists;
/
