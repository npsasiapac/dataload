--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_pyi530
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    KH   01-FEB-2009  Initial Creation.
--
--  2.0     5.15.0    VS   11-JUN-2009  Changed DL_HRA_RDS_TRANSMISSION_FILES
--                                      to DL_HRA_RDS_TRANS_FILES.
--
--  3.0     5.15.0    VS   23-FEB-2010  Defect Id 3611 Fix. Should be linked to 
--                                      Incoming Transmission File.
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
  UPDATE dl_hra_rds_pyi530
  SET    lp530_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_rds_pyi530');
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
      , lp530_dlb_batch_id
      , lp530_dl_seqno
      , lp530_dl_load_status
      , lp530_refno
      , lp530_rdtf_alt_ref
      , lp530_hrv_rbeg_code
      , lp530_timestamp
      , lp530_deduction_action_type
      , lp530_environment_id
      , lp530_costing_date
      , lp530_service_type
      , lp530_unit_count
      , lp530_unit_cost
      , lp530_total_cost
FROM    dl_hra_rds_pyi530
WHERE   lp530_dlb_batch_id    = p_batch_id
AND     lp530_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Get RDS Transmission File Reference Number
--
CURSOR c_get_rdtf_ref( p_rdtf_ref    VARCHAR2)
IS
SELECT rdtf_refno
  FROM rds_transmission_files, 
       dl_hra_rds_trans_files
 WHERE lrdtf_refno   = rdtf_refno
   AND lrdtf_alt_ref = p_rdtf_ref
   AND lrdtf_type    = 'I';
--
-- ***********************************************************************
--
--CURSOR  c_rdtf_refno(p_dtf_refno  VARCHAR2) is
--SELECT  lrdtf_refno
--FROM    dl_hra_rds_trans_files
--WHERE   lrdtf_alt_ref = p_dtf_refno;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_PYI530';
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
i                    INTEGER := 0;
l_exists             VARCHAR2(1);
l_rdtf_ref           NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_pyi530.dataload_create');
fsc_utils.debug_message('s_dl_hra_rds_pyi530.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1 LOOP
--
    BEGIN
--
   cs   := p1.lp530_dl_seqno;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
 OPEN c_get_rdtf_ref(p1.lp530_rdtf_alt_ref);
FETCH c_get_rdtf_ref into l_rdtf_ref;
CLOSE c_get_rdtf_ref;
--
-- Insert into RDS PYI530 table
--
        INSERT /* +APPEND */ into  rds_pyi530s
              ( p530_refno       
              , p530_rdtf_refno
              , p530_timestamp
              , p530_deduction_action_type
              , p530_environment_id
              , p530_benefit_group
              , p530_costing_date
              , p530_service_type
              , p530_unit_count
              , p530_unit_cost
              , p530_total_cost
              )
        VALUES
              ( p1.lp530_refno       
              , l_rdtf_ref
              , p1.lp530_timestamp
              , p1.lp530_deduction_action_type
              , p1.lp530_environment_id
              , p1.lp530_hrv_rbeg_code
              , p1.lp530_costing_date
              , p1.lp530_service_type
              , p1.lp530_unit_count
              , p1.lp530_unit_cost
              , p1.lp530_total_cost
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
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_PYI530S');
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
      , lp530_dlb_batch_id
      , lp530_dl_seqno
      , lp530_dl_load_status
      , lp530_refno
      , lp530_rdtf_alt_ref
      , lp530_hrv_rbeg_code
      , lp530_timestamp
      , lp530_deduction_action_type
      , lp530_environment_id
      , lp530_costing_date
      , lp530_service_type
      , lp530_unit_count
      , lp530_unit_cost
      , lp530_total_cost
FROM    dl_hra_rds_pyi530
WHERE   lp530_dlb_batch_id    = p_batch_id
AND     lp530_dl_load_status  in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
-- Check RDS Transmission File Reference Number Exists
--
CURSOR c_rdtf_ref( p_rdtf_ref    VARCHAR2 )
IS
SELECT 'X'
  FROM rds_transmission_files, 
       dl_hra_rds_trans_files
 WHERE lrdtf_refno   = rdtf_refno
   AND lrdtf_alt_ref = p_rdtf_ref
   AND lrdtf_type    = 'I';
--
-- ***********************************************************************
--
-- Check Reference does not exist
--
CURSOR c_p530_ref( p_p530_ref    VARCHAR2 ) IS
SELECT 'X'
FROM   rds_pyi530s
WHERE  p530_refno = p_p530_ref;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_PYI530';
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
l_rdtf_ref                 NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_pyi530.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_rds_pyi530.dataload_validate',3);
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
   cs   := p1.lp530_dl_seqno;
   l_id := p1.rec_rowid;
--
   l_errors := 'V';
   l_error_ind := 'N';
--
-- Validation checks required
--
-- Check PYI530 reference is unique
--
   OPEN c_p530_ref(p1.lp530_refno);
   FETCH c_p530_ref into l_exists;
    IF c_p530_ref%FOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',229);
    END IF;
   CLOSE c_p530_ref;
--
-- Check Transmission File Reference exists
--
   OPEN  c_rdtf_ref(p1.lp530_rdtf_alt_ref);
   FETCH c_rdtf_ref into l_exists;
    IF c_rdtf_ref%NOTFOUND
     THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',231);
    END IF;
   CLOSE c_rdtf_ref;
--
/*
-- Check Benefit Group Code Exists
--
   IF p1.lp530_hrv_rbeg_code is NOT NULL
    THEN
     IF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lp530_hrv_rbeg_code,'Y'))
      THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',238);
     END IF;
   END IF;
*/
--
-- Check Deduction Action Type Code (Not required as default to 'CST')
--
/*
  IF p1.lp530_deduction_action_type is NOT NULL
   THEN
    IF p1.lp530_deduction_action_type NOT IN ('INI','IVI','ITI','ITA','ITB'
                                             ,'ENI','EVI','ETI','ETD','ESI','ERI')
     THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',239);
     END IF;
   END IF;
*/
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
      , lp530_dlb_batch_id
      , lp530_dl_seqno
      , lp530_dl_load_status
      , lp530_rdtf_alt_ref
      , lp530_refno
FROM    dl_hra_rds_pyi530
WHERE   lp530_dlb_batch_id    = p_batch_id
AND     lp530_dl_load_status  = 'C';
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_RDS_PYI530';
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
l_rdtf_refno               NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_pyi530.dataload_delete');
fsc_utils.debug_message('s_dl_hra_rds_pyi530.dataload_delete',3 );
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
   cs   := p1.lp530_dl_seqno;
   l_id := p1.rec_rowid;
   i    := i +1;
--
-- Delete from RDS PYI530 table
--
-- May need additional where clauses?  Need to check
--
   DELETE FROM rds_pyi530s
   WHERE  p530_refno     = p1.lp530_refno
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_PYI530S');
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
END s_dl_hra_rds_pyi530;
/

