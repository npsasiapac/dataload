CREATE OR REPLACE PACKAGE BODY s_dl_hra_subsidy_letters
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    KH   23-FEB-2009  Initial Creation.
--
--  1.1     5.15.0    VS   05-MAY-2009  Disable trigger SULE_BR_IU on
--                                      CREATE process to stop the supplied
--                                      created_by and created_date from 
--                                      being over written.
--
--  1.2     5.15.0    VS   18-MAY-2009  Use the supplied Subsidy Review Legacy
--                                      Reference to populate Primary/Foreign
--                                      key fields.
--
--                                      Added additional validation checks for
--                                      mandatory fields.
--  1.3     6.4.0     PH   30-JUN-2011  Removed enable/disable of triggers now
--                                      run update after inserted.
--                                      Added cursor for subsidy_applications
--                                      to use legacy ref
--  1.4     6.5.0     PH   24-FEB-2012  Legacy Ref now held in subsidy
--                                      reviews, removed call to dl table
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
  UPDATE dl_hra_subsidy_letters
  SET    lsule_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_subsidy_letters');
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
SELECT rowid REC_ROWID,
       LSULE_DLB_BATCH_ID,
       LSULE_DL_SEQNO,
       LSULE_DL_LOAD_STATUS,
       LSULE_SURV_LEGACY_REF,
       LSULE_SEQ,
       LSULE_HSLT_CODE,
       LSULE_SCO_CODE,
       NVL(LSULE_CREATED_DATE, SYSDATE)  LSULE_CREATED_DATE,
       NVL(LSULE_CREATED_BY, 'DATALOAD') LSULE_CREATED_BY,
       LSULE_COMMENTS,
       LSULE_PRINTED_DATE,
       LSULE_CANCELLED_DATE,
       LSULE_CANCELLED_BY,
       LSULE_MODIFIED_DATE,
       LSULE_MODIFIED_BY,
       LSULE_HLCR_CODE 
  FROM dl_hra_subsidy_letters
 WHERE lsule_dlb_batch_id    = p_batch_id
   AND lsule_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_surv_refno(p_surv_legacy_ref VARCHAR2) 
IS
SELECT surv_refno
  FROM subsidy_reviews
 WHERE surv_legacy_ref = p_surv_legacy_ref ;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_SUBSIDY_LETTERS';
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
l_surv_refno               NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_letters.dataload_create');
    fsc_utils.debug_message('s_dl_hra_subsidy_letters.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lsule_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
          l_surv_refno := NULL;
--
          OPEN get_surv_refno(p1.lsule_surv_legacy_ref);
          FETCH get_surv_refno INTO l_surv_refno;
          CLOSE get_surv_refno;
--
-- Insert into relevent table
--
          INSERT /* +APPEND */ INTO subsidy_letters(SULE_SURV_REFNO,
                                           SULE_SEQ,
                                           SULE_HSLT_CODE,
                                           SULE_SCO_CODE,
                                           SULE_CREATED_DATE,
                                           SULE_CREATED_BY,
                                           SULE_COMMENTS,
                                           SULE_PRINTED_DATE,
                                           SULE_CANCELLED_DATE,
                                           SULE_CANCELLED_BY,
                                           SULE_MODIFIED_DATE,
                                           SULE_MODIFIED_BY,
                                           SULE_HLCR_CODE
                                          )
--
                                    VALUES(l_surv_refno,
                                           p1.lsule_seq,
                                           p1.lsule_hslt_code,
                                           p1.lsule_sco_code,
                                           p1.lsule_created_date,
                                           p1.lsule_created_by,
                                           p1.lsule_comments,
                                           p1.lsule_printed_date,
                                           p1.lsule_cancelled_date,
                                           p1.lsule_cancelled_by,
                                           p1.lsule_modified_date,
                                           p1.lsule_modified_by,
                                           p1.lsule_hlcr_code
                                           );
--
-- Now update the record to set the correct created by and created date
-- to ovecome the trigger
--
         UPDATE   subsidy_letters
            SET   sule_created_date = p1.lsule_created_date
                , sule_created_by   = p1.lsule_created_by
          WHERE   sule_surv_refno   = l_surv_refno
            AND   sule_seq          = p1.lsule_seq;
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_LETTERS');
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
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid REC_ROWID,
       LSULE_DLB_BATCH_ID,
       LSULE_DL_SEQNO,
       LSULE_DL_LOAD_STATUS,
       LSULE_SURV_LEGACY_REF,
       LSULE_SEQ,
       LSULE_HSLT_CODE,
       LSULE_SCO_CODE,
       LSULE_CREATED_DATE,
       LSULE_CREATED_BY,
       LSULE_COMMENTS,
       LSULE_PRINTED_DATE,
       LSULE_CANCELLED_DATE,
       LSULE_CANCELLED_BY,
       LSULE_MODIFIED_DATE,
       LSULE_MODIFIED_BY,
       LSULE_HLCR_CODE
  FROM dl_hra_subsidy_letters
 WHERE lsule_dlb_batch_id    = p_batch_id
   AND lsule_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR chk_surv_exists(p_surv_legacy_ref VARCHAR2) 
IS
SELECT surv_refno
  FROM subsidy_reviews
 WHERE surv_legacy_ref = p_surv_legacy_ref;
--
-- ***********************************************************************
--
CURSOR chk_sule_exists(p_surv_refno      NUMBER,
                       p_sule_seq        NUMBER) 
IS
SELECT 'X'
  FROM subsidy_letters
 WHERE sule_surv_refno = p_surv_refno
   AND sule_seq        = p_sule_seq;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_LETTERS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         	VARCHAR2(1);
l_surv_exists           VARCHAR2(1);
l_sule_exists           VARCHAR2(1);
l_surv_refno            NUMBER(10);
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_letters.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_subsidy_letters.dataload_validate',3);
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
          cs   := p1.lsule_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
--
-- ***********************************************************************
--
-- Validation checks required
--
--
-- Check Subsidy Review Legacy Reference has been supplied and doesn't already
-- exist
--
--
          IF (p1.lsule_surv_legacy_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',274);
--
          ELSE
--
             l_surv_refno := NULL;
--
              OPEN chk_surv_exists (p1.lsule_surv_legacy_ref);
             FETCH chk_surv_exists INTO l_surv_refno;
             CLOSE chk_surv_exists;
--
             IF (l_surv_refno IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',72);
             END IF;
--
          END IF;
--
--
-- ********************************************
--
-- Check Letter Sequence Number has been supplied
-- 
         IF (p1.lsule_seq IS NULL) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',277);
         END IF;
--
--
-- ********************************************
--
-- Check Letter Status Code has been supplied
-- 
         IF (p1.lsule_sco_code IS NULL) THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',248);
--
         ELSIF (p1.lsule_sco_code NOT IN ('PRT','RAI','CAN','SEN')) THEN
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',14);
--
         END IF;
--
--
-- ********************************************
--
-- Check if any of the cancelled fields have been populated then a cancellation
-- reason code must be supplied
-- 

         IF ((    p1.lsule_sco_code        = 'CAN'
              OR  p1.lsule_cancelled_date IS NOT NULL
              OR  p1.lsule_cancelled_by   IS NOT NULL)
              AND p1.lsule_hlcr_code IS NULL) THEN
--
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',282);
--
         END IF;
--
--
-- ********************************************
--
-- Check Subsidy Letter doesn't already exist for supplied Subsidy
-- Review Legacy ref and Sequence
-- 
--  
          IF (    l_surv_refno  IS NOT NULL
              AND p1.lsule_seq  IS NOT NULL) THEN
--
           l_sule_exists := NULL;
--
            OPEN chk_sule_exists (l_surv_refno,p1.lsule_seq);
           FETCH chk_sule_exists INTO l_sule_exists;
           CLOSE chk_sule_exists;
--
           IF (l_sule_exists IS NOT NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',281);
           END IF;
--
          END IF;
--
--
-- ********************************************
--
-- If Supplied check Printed Date isn't earlier than the created date
-- 
         IF (p1.lsule_printed_date IS NOT NULL) THEN
--
          IF (p1.lsule_printed_date < p1.lsule_created_date) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',280);
          END IF;
--
         END IF;
--
--
-- ********************************************
--
-- Subsidy Letter Type Code SULE_HSLT_CODE has been supplied and is valid
--
          IF (p1.lsule_hslt_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',278);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('SUBLETTYPE',p1.lsule_hslt_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',279);
--
          END IF;
--
--
-- ********************************************
--
-- All reference values supplied are valid
-- 
-- Subsidy Letter Cancellation Reason
--
          IF (p1.lsule_hlcr_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('SUB_LET_CANCEL_RSN',p1.lsule_hlcr_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',244);
           END IF;
--
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
PROCEDURE dataload_delete(p_batch_id       IN VARCHAR2,
                          p_date           IN date) IS
--
CURSOR c1 is
SELECT rowid REC_ROWID,
       LSULE_DLB_BATCH_ID,
       LSULE_DL_SEQNO,
       LSULE_DL_LOAD_STATUS,
       LSULE_SURV_LEGACY_REF,
       LSULE_SEQ
  FROM dl_hra_subsidy_letters
 WHERE lsule_dlb_batch_id   = p_batch_id
   AND lsule_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_surv_refno(p_surv_legacy_ref VARCHAR2) 
IS
SELECT surv_refno
  FROM subsidy_reviews
 WHERE surv_legacy_ref = p_surv_legacy_ref ;
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_LETTERS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
i                INTEGER :=0;
l_tcy_refno      NUMBER(10);
l_par_refno      NUMBER(8);
l_surv_refno     NUMBER(10);
l_suap_refno     NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_letters.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_subsidy_letters.dataload_delete',3 );
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
          cs   := p1.lsule_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
          l_surv_refno := NULL;
--
          OPEN get_surv_refno(p1.lsule_surv_legacy_ref);
          FETCH get_surv_refno INTO l_surv_refno;
          CLOSE get_surv_refno;
--
-- Delete from table
--
          DELETE 
            FROM subsidy_letters
           WHERE sule_surv_refno = l_surv_refno
             AND sule_seq        = p1.lsule_seq;
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
          IF MOD(i,5000) = 0 THEN 
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_LETTERS');
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
END s_dl_hra_subsidy_letters;
/