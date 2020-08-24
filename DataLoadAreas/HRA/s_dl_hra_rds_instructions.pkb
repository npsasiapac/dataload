--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_instructions
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    PH   03-FEB-2009  Initial Creation. 
--  1.1     6.13      AJ   16-JUN-2016  Updated after initial re-creation by MOK
--                                      after compile issue raised by DB
--                                      1) comp stats in delete missing table
--                                         name of RDS_INSTRUCTIONS
--                                      2) c1 cursor lrdin_raud_start_date missing
--                                      3) l_raud_refno declare table name wrong validate
--
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
  UPDATE dl_hra_rds_instructions
  SET    lrdin_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_rds_instructions');
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
      , lrdin_dlb_batch_id
      , lrdin_dl_seqno
      , lrdin_dl_load_status
      , lrdin_refno
      , lrdin_rdsa_ha_reference
      , lrdin_hrv_dedt_code
      , lrdin_hrv_rbeg_code
      , lrdin_raud_start_date
      , lrdin_effective_date
      , lrdin_instruction_amount
      , nvl(lrdin_created_by, 'DATALOAD')   lrdin_created_by
      , nvl(lrdin_created_date, sysdate)    lrdin_created_date
      , lrdin_end_date
FROM    dl_hra_rds_instructions
WHERE   lrdin_dlb_batch_id    = p_batch_id
AND     lrdin_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_get_raud_refno( p_ha_reference  VARCHAR2
                        , p_dedt_code     VARCHAR2
                        , p_rbeg_code     VARCHAR2
                        , p_start_date    DATE     ) IS
SELECT  raud_refno
FROM    rds_authorised_deductions
      , rds_authorities
WHERE   rdsa_refno         = raud_rdsa_refno
AND     rdsa_ha_reference  = p_ha_reference
AND     raud_hrv_dedt_code = p_dedt_code
AND     raud_hrv_rbeg_code = p_rbeg_code
AND     raud_start_date    = p_start_date;
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
ct                   VARCHAR2(30) := 'DL_HRA_RDS_INSTRUCTIONS';
cs                   INTEGER;
ce	                 VARCHAR2(200);
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
l_raud_refno               rds_authorised_deductions.raud_refno%type;
--
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_instructions.dataload_create');
fsc_utils.debug_message('s_dl_hra_rds_instructions.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1 LOOP
--
    BEGIN
--
   cs   := p1.lrdin_dl_seqno;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
   l_raud_refno := NULL;
--
   OPEN c_get_raud_refno( p1.lrdin_rdsa_ha_reference
                        , p1.lrdin_hrv_dedt_code
                        , p1.lrdin_hrv_rbeg_code
                        , p1.lrdin_raud_start_date);
    FETCH c_get_raud_refno INTO l_raud_refno ;
   CLOSE c_get_raud_refno;
--
-- Insert int0 relevent table
--
        INSERT into  rds_instructions
              ( rdin_refno
              , rdin_raud_refno
              , rdin_effective_date
              , rdin_hrv_rbeg_code
              , rdin_instruction_amount
              , rdin_created_by
              , rdin_created_date
              , rdin_end_date
              )
        VALUES
              ( p1.lrdin_refno
              , l_raud_refno
              , p1.lrdin_effective_date
              , p1.lrdin_hrv_rbeg_code
              , p1.lrdin_instruction_amount
              , p1.lrdin_created_by
              , p1.lrdin_created_date
              , p1.lrdin_end_date
              );
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
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
COMMIT;
--
-- ***********************************************************************
--
-- Section to anayze the table(s) populated by this dataload
--
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_INSTRUCTIONS');
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
CURSOR c1 is
SELECT  rowid rec_rowid
      , lrdin_dlb_batch_id
      , lrdin_dl_seqno
      , lrdin_dl_load_status
      , lrdin_refno
      , lrdin_rdsa_ha_reference
      , lrdin_hrv_dedt_code
      , lrdin_hrv_rbeg_code
      , lrdin_raud_start_date
      , lrdin_effective_date
      , lrdin_instruction_amount
      , lrdin_created_by
      , lrdin_created_date
      , lrdin_end_date
FROM    dl_hra_rds_instructions
WHERE   lrdin_dlb_batch_id    = p_batch_id
AND     lrdin_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_check_rdsa( p_ha_reference  VARCHAR2) IS
SELECT  'X'
FROM    rds_authorities
WHERE   rdsa_ha_reference  = p_ha_reference;
--
CURSOR  c_get_raud( p_ha_reference  VARCHAR2
                  , p_dedt_code     VARCHAR2
                  , p_rbeg_code     VARCHAR2
                  , p_start_date    DATE     ) IS
SELECT  raud_refno
FROM    rds_authorised_deductions
      , rds_authorities
WHERE   rdsa_refno         = raud_rdsa_refno
AND     rdsa_ha_reference  = p_ha_reference
AND     raud_hrv_dedt_code = p_dedt_code
AND     raud_hrv_rbeg_code = p_rbeg_code
AND     raud_start_date    = p_start_date;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_RDS_INSTRUCTIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists                   VARCHAR2(1);
l_pro_refno                NUMBER(10);
l_errors                   VARCHAR2(10);
l_error_ind                VARCHAR2(10);
i                          INTEGER :=0;
l_raud_refno               rds_authorised_deductions.raud_refno%type;
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_instructions.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_rds_instructions.dataload_validate',3);
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
   cs   := p1.lrdin_dl_seqno;
   l_id := p1.rec_rowid;
--
   l_errors := 'V';
   l_error_ind := 'N';

--
   l_raud_refno  := NULL;
--
-- Validation checks required
--
--
-- Check the Authority Ref exists
--
   OPEN c_check_rdsa(p1.lrdin_rdsa_ha_reference);
    FETCH c_check_rdsa INTO l_exists;
      IF c_check_rdsa%NOTFOUND
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',99);
      END IF;
   CLOSE c_check_rdsa;
--
-- Check Authorised Deductions Exist
--
   OPEN c_get_raud( p1.lrdin_rdsa_ha_reference, p1.lrdin_hrv_dedt_code
                  , p1.lrdin_hrv_rbeg_code    , p1.lrdin_raud_start_date);
    FETCH c_get_raud INTO l_raud_refno;
      IF c_get_raud%NOTFOUND
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',105);
      END IF;
   CLOSE c_get_raud;
--
-- End Date 
--
   IF p1.lrdin_end_date is NOT NULL
    THEN
     IF p1.lrdin_end_date < nvl(p1.lrdin_effective_date, p1.lrdin_end_date)
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',3);
     END IF;
   END IF;
--
-- Reference Values
--
-- Deduction Type
--
   IF (NOT s_dl_hem_utils.exists_frv('RDS_DED_TYPE',p1.lrdin_hrv_dedt_code,'N'))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',103);
   END IF;
--
-- Benefit Group -- Mandatory as used in a link
--
   IF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lrdin_hrv_rbeg_code,'N'))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',104);
   END IF;
--
-- Other Mandatory Fields
--
-- Effective Date
--
   IF p1.lrdin_effective_date IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',165);
   END IF;
--
-- Instruction Amount
--
   IF p1.lrdin_instruction_amount IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',166);
   END IF;
--
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
PROCEDURE dataload_delete
            (p_batch_id       IN VARCHAR2
            ,p_date           IN date) IS
--
CURSOR c1 is
SELECT  rowid rec_rowid
      , lrdin_dlb_batch_id
      , lrdin_dl_seqno
      , lrdin_dl_load_status
      , lrdin_refno
FROM    dl_hra_rds_instructions
WHERE   lrdin_dlb_batch_id    = p_batch_id
AND     lrdin_dl_load_status  = 'C';
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
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HRA_RDS_INSTRUCTIONS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
i                INTEGER :=0;
l_an_tab         VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_instructions.dataload_delete');
fsc_utils.debug_message('s_dl_hra_rds_instructions.dataload_delete',3 );
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
   cs   := p1.lrdin_dl_seqno;
   l_id := p1.rec_rowid;
   i    := i +1;
--
-- Delete from table
--
   DELETE FROM rds_instructions
   WHERE  rdin_refno  = p1.lrdin_refno
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_INSTRUCTIONS');
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
END s_dl_hra_rds_instructions;
/

