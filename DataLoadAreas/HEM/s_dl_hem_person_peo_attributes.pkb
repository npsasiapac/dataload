--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_person_peo_attributes
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
  UPDATE dl_hem_person_peo_attributes
     SET lpepa_dl_load_status = p_status
   WHERE rowid                = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_person_peo_attributes');
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
       lpepa_dlb_batch_id,
       lpepa_dl_seqno,
       lpepa_dl_load_status,
       lpepa_par_type,
       lpepa_class_code,
       lpepa_par_per_alt_ref,
       NVL(lpepa_created_date,SYSDATE)  lpepa_created_date,
       NVL(lpepa_created_by,'DATALOAD') lpepa_created_by,
       lpepa_peat_code,
       lpepa_paav_code,
       lpepa_yes_no_value,
       lpepa_date_value,
       lpepa_numeric_value,
       lpepa_text_value,
       lpepa_comments,
       lpepa_modified_date,
       lpepa_modified_by,
       lpepa_refno
  FROM dl_hem_person_peo_attributes
 WHERE lpepa_dlb_batch_id   = p_batch_id
   AND lpepa_dl_load_status = 'V';
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
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_PEO_ATTRIBUTES';
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
   execute immediate 'alter trigger PEPA_BR_I disable';
   execute immediate 'alter trigger PEPA_BR_IU disable';
--
    fsc_utils.proc_start('s_dl_hem_person_peo_attributes.dataload_create');
    fsc_utils.debug_message('s_dl_hem_person_peo_attributes.dataload_create',3);
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
          cs   := p1.lpepa_dl_seqno;
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
          IF (p1.lpepa_par_type = 'PAR') THEN
--
            OPEN c_get_par(p1.lpepa_par_per_alt_ref);
           FETCH c_get_par INTO l_par_refno;
           CLOSE c_get_par;
--
          ELSE
--
              OPEN c_get_prf(p1.lpepa_par_per_alt_ref);
             FETCH c_get_prf INTO l_par_refno;
             CLOSE c_get_prf;
--
          END IF;
--
-- Insert into person_people_attributes
--
          INSERT /* +APPEND */ INTO PERSON_PEOPLE_ATTRIBUTES(PEPA_REFNO,
                                                             PEPA_CLASS_CODE,
                                                             PEPA_PAR_REFNO,
                                                             PEPA_CREATED_DATE,
                                                             PEPA_CREATED_BY,
                                                             PEPA_PEAT_CODE,
                                                             PEPA_PAAV_CODE,
                                                             PEPA_YES_NO_VALUE,
                                                             PEPA_DATE_VALUE,
                                                             PEPA_NUMERIC_VALUE,
                                                             PEPA_TEXT_VALUE,
                                                             PEPA_COMMENTS,
                                                             PEPA_MODIFIED_DATE,
                                                             PEPA_MODIFIED_BY
                                                            )
--
                                                      VALUES(p1.LPEPA_REFNO,
                                                             p1.LPEPA_CLASS_CODE,
                                                             l_par_refno,
                                                             p1.LPEPA_CREATED_DATE,
                                                             p1.LPEPA_CREATED_BY,
                                                             p1.LPEPA_PEAT_CODE,
                                                             p1.LPEPA_PAAV_CODE,
                                                             p1.LPEPA_YES_NO_VALUE,
                                                             p1.LPEPA_DATE_VALUE,
                                                             p1.LPEPA_NUMERIC_VALUE,
                                                             p1.LPEPA_TEXT_VALUE,
                                                             p1.LPEPA_COMMENTS,
                                                             p1.LPEPA_MODIFIED_DATE,
                                                             p1.LPEPA_MODIFIED_BY
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PERSON_PEOPLE_ATTRIBUTES');
--
    execute immediate 'alter trigger PEPA_BR_I enable';
    execute immediate 'alter trigger PEPA_BR_IU enable';
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
       lpepa_dlb_batch_id,
       lpepa_dl_seqno,
       lpepa_dl_load_status,
       lpepa_par_type,
       lpepa_class_code,
       lpepa_par_per_alt_ref,
       NVL(lpepa_created_date,SYSDATE)  lpepa_created_date,
       NVL(lpepa_created_by,'DATALOAD') lpepa_created_by,
       lpepa_peat_code,
       lpepa_paav_code,
       LPEPA_YES_NO_VALUE,
       lpepa_date_value,
       lpepa_numeric_value,
       lpepa_text_value,
       lpepa_comments,
       lpepa_modified_date,
       lpepa_modified_by,
       lpepa_refno
  FROM dl_hem_person_peo_attributes
 WHERE lpepa_dlb_batch_id    = p_batch_id
   AND lpepa_dl_load_status IN ('L','F','O');
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
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_PEO_ATTRIBUTES';
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
    fsc_utils.proc_start('s_dl_hem_person_peo_attributes.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_person_peo_attributes.dataload_validate',3);
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
          cs   := p1.lpepa_dl_seqno;
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
          IF (p1.lpepa_par_type NOT IN ('PAR','PRF')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',910);
          END IF;
--
-- ***********************************************************************
--
-- If the Person Exists 
--
--
--
          IF (p1.lpepa_par_type = 'PAR') THEN
--
            OPEN c_chk_par(p1.lpepa_par_per_alt_ref);
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
              OPEN c_chk_prf(p1.lpepa_par_per_alt_ref);
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
-- Check Class Code is valid
--
          IF (p1.lpepa_class_code NOT IN ('TEXT', 'YESNO', 'CODED', 'NUMERIC', 'DATE')) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',912);
          END IF;
--
-- ***********************************************************************
--
-- Check Person Attribute Code is valid
--
           OPEN c_chk_peat(p1.lpepa_peat_code);
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
          IF (    p1.lpepa_paav_code   IS NOT NULL
              AND l_peat_rec.peat_code IS NOT NULL) THEN
--
            OPEN c_chk_paav(p1.lpepa_paav_code,p1.lpepa_peat_code);
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
-- 1: If the associated NUMERIC PEOPLE ATTRIBUTE has a value specified for Max Numeric
--    Value then the Numeric Value must be less than or equal to this
--
-- 2: If the associated NUMERIC PEOPLE ATTRIBUTE has a value specified for Min Numeric
--    Value then the Numeric Value must be greater than or equal to this
--
--
          IF (    p1.lpepa_class_code     = 'NUMERIC' 
              AND p1.lpepa_numeric_value IS NOT NULL) THEN

           IF (    l_peat_rec.peat_max_numeric_value IS NOT NULL 
               AND p1.lpepa_numeric_value > l_peat_rec.peat_max_numeric_value) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',915);
--
           END IF;

           IF (    l_peat_rec.peat_min_numeric_value IS NOT NULL
               AND p1.lpepa_numeric_value < l_peat_rec.peat_min_numeric_value) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',916);
--
           END IF;
--
          END IF;
--
--
-- ***********************************************************************
--
-- 1: If the associated DATE PEOPLE ATTRIBUE has a value specified for Max
--    Date then Date Value be earlier than or equal to this--
--
-- The Date value must be earlier than or equal to the Max Date {1}
--
-- 2: If the associated DATE PEOPLE ATTRIBUE has a value specified for Min
--    Date then Date Value be later than or equal to this
--
-- The Date value must be later than or equal to the Min Date {1}
--

--
          IF (    p1.lpepa_class_code  = 'DATE' 
              AND p1.lpepa_date_value IS NOT NULL) THEN
--
           IF (l_peat_rec.peat_max_date IS NOT NULL 
               AND p1.lpepa_date_value > l_peat_rec.peat_max_date) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',917);
--
           END IF;
--
           IF (    l_peat_rec.peat_min_date IS NOT NULL 
               AND p1.lpepa_date_value < l_peat_rec.peat_min_date) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',918);
-- 
           END IF;
--
          END IF;
--
-- ***********************************************************************
--
-- Check Yes/No Value is valid
--
          IF (p1.lpepa_yes_no_value NOT IN ('Y', 'N')) THEN
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
       lpepa_dlb_batch_id,
       lpepa_dl_seqno,
       lpepa_dl_load_status,
       lpepa_par_type,
       lpepa_class_code,
       lpepa_par_per_alt_ref,
       lpepa_created_date,
       lpepa_created_by,
       lpepa_peat_code,
       lpepa_paav_code,
       lpepa_yes_no_value,
       lpepa_date_value,
       lpepa_numeric_value,
       lpepa_text_value,
       lpepa_comments,
       lpepa_modified_date,
       lpepa_modified_by,
       lpepa_refno
  FROM dl_hem_person_peo_attributes
 WHERE lpepa_dlb_batch_id   = p_batch_id
   AND lpepa_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HEM_PERSON_PEO_ATTRIBUTES';
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
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_person_peo_attributes.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_person_peo_attributes.dataload_delete',3 );
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
          cs   := p1.lpepa_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
--
-- Delete from person_people_attributes
--
          DELETE 
            FROM person_people_attributes
           WHERE pepa_refno = p1.lpepa_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('PERSON_PEOPLE_HISTORY');
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
END s_dl_hem_person_peo_attributes;
/
