--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_trans_files
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    IR   30-JAN-2009  Initial Creation.
--  2.0     5.15.0    IR   07-Sep-2009  Changed transmission files load
--                                      for sending/receiving agency
--  3.0     6.9.0     MM   06-JAN-2014  Added in WDH as Sending Agency Value
--                                      as needed for WA DoH
--
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
  UPDATE dl_hra_rds_trans_files
  SET    lrdtf_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_rds_trans_files');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
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
      , lrdtf_dlb_batch_id
      , lrdtf_dl_seqno
      , lrdtf_dl_load_status
      , lrdtf_alt_ref
      , lrdtf_hrv_rpag_code
      , lrdtf_type
      , nvl(lrdtf_created_date, sysdate)   lrdtf_created_date
      , nvl(lrdtf_created_by, 'DATALOAD')  lrdtf_created_by
      , lrdtf_sco_code
      , lrdtf_file_number
      , lrdtf_timestamp
      , lrdtf_sending_agency
      , lrdtf_receiving_agency
      , lrdtf_processed_datetime
      , lrdtf_transaction_count
      , lrdtf_rec_authd_datetime
      , lrdtf_rec_authd_by
      , lrdtf_refno
FROM    dl_hra_rds_trans_files
WHERE   lrdtf_dlb_batch_id    = p_batch_id
AND     lrdtf_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
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
ct                   VARCHAR2(30) := 'DL_HRA_RDS_TRANS_FILES';
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
--
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_trans_files.dataload_create');
fsc_utils.debug_message('s_dl_hra_rds_trans_files.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1 LOOP
--
    BEGIN
--
   cs   := p1.lrdtf_dl_seqno;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
--
-- Insert into RDS Transmission Files table
--
        INSERT /* +APPEND */ into  rds_transmission_files
              ( rdtf_refno
              , rdtf_type
              , rdtf_created_date
              , rdtf_created_by
              , rdtf_sco_code
              , rdtf_file_number
              , rdtf_timestamp
              , rdtf_sending_agency
              , rdtf_receiving_agency
              , rdtf_processed_datetime
              , rdtf_transaction_count
              , rdtf_rec_authd_datetime
              , rdtf_rec_authd_by
              , rdtf_hrv_rpag_code
              )
        VALUES
              ( p1.lrdtf_refno
              , p1.lrdtf_type
              , p1.lrdtf_created_date
              , p1.lrdtf_created_by
              , p1.lrdtf_sco_code
              , p1.lrdtf_file_number
              , p1.lrdtf_timestamp
              , p1.lrdtf_sending_agency
              , p1.lrdtf_receiving_agency
              , p1.lrdtf_processed_datetime
              , p1.lrdtf_transaction_count
              , p1.lrdtf_rec_authd_datetime
              , p1.lrdtf_rec_authd_by
              , p1.lrdtf_hrv_rpag_code
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
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_TRANSMISSION_FILES');
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
      , lrdtf_dlb_batch_id
      , lrdtf_dl_seqno
      , lrdtf_dl_load_status
      , lrdtf_alt_ref
      , lrdtf_hrv_rpag_code
      , lrdtf_type
      , nvl(lrdtf_created_date, sysdate)   lrdtf_created_date
      , nvl(lrdtf_created_by, 'DATALOAD')  lrdtf_created_by
      , lrdtf_sco_code
      , lrdtf_file_number
      , lrdtf_timestamp
      , lrdtf_sending_agency
      , lrdtf_receiving_agency
      , lrdtf_processed_datetime
      , lrdtf_transaction_count
      , lrdtf_rec_authd_datetime
      , lrdtf_rec_authd_by
      , lrdtf_refno
FROM    dl_hra_rds_trans_files
WHERE   lrdtf_dlb_batch_id    = p_batch_id
AND     lrdtf_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_pay_agency(p_rpag_code   VARCHAR2 ) IS
SELECT 'X'
FROM   first_ref_values
WHERE  frv_frd_domain = 'RDS_PAY_AGENCY'
AND    frv_code       = p_rpag_code;
--
CURSOR c_ref(p_ref   VARCHAR2 ) IS
SELECT 'X'
FROM   rds_transmission_files
WHERE  rdtf_refno = p_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_TRANS_FILES';
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
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_trans_files.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_rds_trans_files.dataload_validate',3);
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
   cs   := p1.lrdtf_dl_seqno;
   l_id := p1.rec_rowid;
--
   l_errors := 'V';
   l_error_ind := 'N';
--
-- Validation checks required
--
--
-- Check transmission file type is either 'I' or 'O'
--
   IF p1.lrdtf_type not in ('I','O')
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',224);
   END IF;
--
-- Check Payment Agency Exists
--
   OPEN c_pay_agency(p1.lrdtf_hrv_rpag_code);
   FETCH c_pay_agency into l_exists;
    IF c_pay_agency%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',225);
    END IF;
   CLOSE c_pay_agency;
--
-- Check Transmission File reference is unique
--
   OPEN c_ref(p1.lrdtf_refno);
   FETCH c_ref into l_exists;
    IF c_ref%FOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',226);
    END IF;
   CLOSE c_ref;
--
-- Checking sending agency code
--
     IF p1.lrdtf_sending_agency NOT IN ('CLK','DSS','NDH','WDH')   --addedin WDH
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',313);
     END IF;
--
-- Checking receiving agency code
--
     IF p1.lrdtf_sending_agency NOT IN ('CLK','DSS','NDH','WDH')   --addedin WDH
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',314);
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
PROCEDURE dataload_delete
         (p_batch_id       IN VARCHAR2
         ,p_date           IN date) IS
--
CURSOR c1 is
SELECT  rowid rec_rowid
      , lrdtf_dlb_batch_id
      , lrdtf_dl_seqno
      , lrdtf_dl_load_status
      , lrdtf_refno
FROM    dl_hra_rds_trans_files
WHERE   lrdtf_dlb_batch_id    = p_batch_id
AND     lrdtf_dl_load_status  = 'C';
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
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_TRANS_FILES';
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
fsc_utils.proc_start('s_dl_hra_rds_trans_files.dataload_delete');
fsc_utils.debug_message('s_dl_hra_rds_trans_files.dataload_delete',3 );
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
   cs   := p1.lrdtf_dl_seqno;
   l_id := p1.rec_rowid;
   i    := i +1;
--
-- Delete from rds_transmission_files table
--
   DELETE FROM rds_transmission_files
   WHERE  rdtf_refno  = p1.lrdtf_refno
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_TRANSMISSION_FILES');
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
END s_dl_hra_rds_trans_files;
/

