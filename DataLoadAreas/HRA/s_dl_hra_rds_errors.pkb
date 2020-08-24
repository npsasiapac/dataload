--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_errors
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    IR   31-JAN-2009  Initial Creation.
--
--  2.0     5.15.0    VS   11-JUN-2009  Changed DL_HRA_RDS_TRANSMISSION_FILES
--                                      to DL_HRA_RDS_TRANS_FILES.
--
--  3.0     5.15.0    VS   12-FEB-2010  Fix for Defect 3257. 
--                                      RDS Errors in V4 are linked to wrong 
--                                      RDS Transmission File in V5.
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_hra_rds_errors
  SET    lrerr_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_rds_errors');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR  c1 IS
SELECT  rowid rec_rowid
      , lrerr_dlb_batch_id
      , lrerr_dl_seqno
      , lrerr_dl_load_status
      , lrerr_rdtf_alt_ref
      , lrerr_source_tran_ref
      , lrerr_trans_code
      , lrerr_edx_error_code
      , lrerr_timestamp
      , lrerr_crn
      , lrerr_ext_ref_id
      , lrerr_data_length
      , lrerr_transaction_data
      , lrerr_refno
FROM    dl_hra_rds_errors
WHERE   lrerr_dlb_batch_id    = p_batch_id
AND     lrerr_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_rdtf_refno(p_dtf_refno  VARCHAR2) 
IS
SELECT lrdtf_refno
  FROM dl_hra_rds_trans_files 
 WHERE lrdtf_alt_ref = p_dtf_refno
   AND lrdtf_type    = 'I';
--
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_ERRORS';
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
l_rdtf_refno               NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_errors.dataload_create');
fsc_utils.debug_message('s_dl_hra_rds_errors.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1 LOOP
--
    BEGIN
--
   cs   := p1.lrerr_dl_seqno;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
OPEN c_rdtf_refno(p1.lrerr_rdtf_alt_ref);
FETCH c_rdtf_refno into l_rdtf_refno;
CLOSE c_rdtf_refno;
--
-- Insert into RDS Errors table
--
        INSERT /* +APPEND */ into  rds_errors
              ( rerr_refno
              , rerr_rdtf_refno 
              , rerr_source_trans_ref
              , rerr_trans_code
              , rerr_edx_error_code
              , rerr_timestamp
              , rerr_crn
              , rerr_ext_ref_id
              , rerr_data_length
              , rerr_transaction_data
              )
        VALUES
              ( p1.lrerr_refno
              , l_rdtf_refno
              , p1.lrerr_source_tran_ref
              , p1.lrerr_trans_code
              , p1.lrerr_edx_error_code
              , p1.lrerr_timestamp
              , p1.lrerr_crn
              , p1.lrerr_ext_ref_id
              , p1.lrerr_data_length
              , p1.lrerr_transaction_data
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
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_ERRORS');
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
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR  c1 IS
SELECT  rowid rec_rowid
      , lrerr_dlb_batch_id
      , lrerr_dl_seqno
      , lrerr_dl_load_status
      , lrerr_rdtf_alt_ref
      , lrerr_source_tran_ref
      , lrerr_trans_code
      , lrerr_edx_error_code
      , lrerr_timestamp
      , lrerr_crn
      , lrerr_ext_ref_id
      , lrerr_data_length
      , lrerr_transaction_data
      , lrerr_refno
FROM    dl_hra_rds_errors
WHERE   lrerr_dlb_batch_id    = p_batch_id
AND     lrerr_dl_load_status  in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_rdtf_refno(p_dtf_refno  VARCHAR2)
IS
SELECT 1
  FROM dl_hra_rds_trans_files, 
       rds_transmission_files
 WHERE lrdtf_alt_ref = p_dtf_refno
   AND lrdtf_refno   = rdtf_refno
   AND lrdtf_type    = 'I';
--
CURSOR c_ref(p_ref   VARCHAR2 ) IS
SELECT 'X'
FROM   rds_errors
WHERE  rerr_refno = p_ref;
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_ERRORS';
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
l_rdtf_refno               NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_errors.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_rds_errors.dataload_validate',3);
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
   cs   := p1.lrerr_dl_seqno;
   l_id := p1.rec_rowid;
--
   l_errors := 'V';
   l_error_ind := 'N';
--
-- Validation checks required
--
-- Check a Transmission File Exists
--
   OPEN c_rdtf_refno(p1.lrerr_rdtf_alt_ref);
   FETCH c_rdtf_refno into l_exists;
    IF c_rdtf_refno%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',227);
    END IF;
   CLOSE c_rdtf_refno;
--
-- Check RDS Error reference is unique
--
   OPEN c_ref(p1.lrerr_refno);
   FETCH c_ref into l_exists;
    IF c_ref%FOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',228);
    END IF;
   CLOSE c_ref;
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
PROCEDURE dataload_delete
         (p_batch_id       IN VARCHAR2
         ,p_date           IN date) IS
--
CURSOR c1 is
SELECT  rowid rec_rowid
      , lrerr_dlb_batch_id
      , lrerr_dl_seqno
      , lrerr_dl_load_status
      , lrerr_refno
FROM    dl_hra_rds_errors
WHERE   lrerr_dlb_batch_id    = p_batch_id
AND     lrerr_dl_load_status  = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
CURSOR  c_rdtf_refno(p_dtf_refno  VARCHAR2) is
SELECT  lrdtf_refno
FROM    dl_hra_rds_trans_files
WHERE   lrdtf_alt_ref = p_dtf_refno;
--
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_ERRORS';
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
l_rdtf_refno     NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_errors.dataload_delete');
fsc_utils.debug_message('s_dl_hra_rds_errors.dataload_delete',3 );
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
   cs   := p1.lrerr_dl_seqno;
   l_id := p1.rec_rowid;
   i    := i +1;
--
-- Delete from RDS Errors table
--
   DELETE FROM rds_errors
   WHERE  rerr_refno  = p1.lrerr_refno
   ;
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
IF mod(i,5000) = 0 THEN commit; END IF;
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_ERRORS');
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
END s_dl_hra_rds_errors;
/

