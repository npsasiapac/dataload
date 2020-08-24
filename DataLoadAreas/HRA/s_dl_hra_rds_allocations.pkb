--
CREATE OR REPLACE PACKAGE BODY s_dl_hra_rds_allocations
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    PH   03-FEB-2008  Initial Creation.
--
--  2.0     5.15.0    VS   16-APR-2009  Changed raud_start_date to
--                                              rdsa_start_date
--
--  3.0     5.15.0    VS   28-OCT-2009  Changed rdsa_start_date to
--                                      raud_start_date in c_get_rdin
--                                      CREATE/VALIDATE processes
--                                      (Defect ID: 2462)
--  4.0     5.15.0    Matt 08-SEP-2010  MQC 5902 Add Order By refno to 
--                                      create and validate
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
  UPDATE dl_hra_rds_allocations
  SET    lrdal_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_rds_allocations');
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
      , lrdal_dlb_batch_id
      , lrdal_dl_seqno
      , lrdal_dl_load_status
      , lrdal_refno
      , lrdal_rdsa_ha_reference
      , lrdal_raud_start_date
      , lrdal_hrv_dedt_code
      , lrdal_hrv_rbeg_code
      , lrdal_rdin_effective_date
      , lrdal_effective_date
      , lrdal_allocated_amount
      , lrdal_deduction_action_type
      , nvl(lrdal_created_by, 'DATALOAD')  lrdal_created_by
      , nvl(lrdal_created_date, sysdate)   lrdal_created_date
FROM    dl_hra_rds_allocations
WHERE   lrdal_dlb_batch_id    = p_batch_id
AND     lrdal_dl_load_status  = 'V'
ORDER BY lrdal_refno ;
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_get_rdin( p_ha_reference    VARCHAR2
                  , p_dedt_code       VARCHAR2
                  , p_rbeg_code       VARCHAR2
                  , p_start_date      DATE
                  , p_effect_date     DATE     ) IS
SELECT  rdin_refno
FROM    rds_instructions
      , rds_authorised_deductions
      , rds_authorities
WHERE   rdsa_refno          = raud_rdsa_refno
AND     rdin_raud_refno     = raud_refno
AND     rdin_effective_date = p_effect_date
AND     rdsa_ha_reference   = p_ha_reference
AND     raud_hrv_dedt_code  = p_dedt_code
AND     raud_hrv_rbeg_code  = p_rbeg_code
AND     raud_start_date     = p_start_date;
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
ct                   VARCHAR2(30) := 'DL_HRA_RDS_ALLOCATIONS';
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
l_rdin_refno               rds_instructions.rdin_refno%type;
--
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_allocations.dataload_create');
fsc_utils.debug_message('s_dl_hra_rds_allocations.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
  FOR p1 in c1 LOOP
--
    BEGIN
--
   cs   := p1.lrdal_dl_seqno;
   l_id := p1.rec_rowid;
--
   SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
   l_rdin_refno := NULL;
--
   OPEN c_get_rdin( p1.lrdal_rdsa_ha_reference   , p1.lrdal_hrv_dedt_code
                  , p1.lrdal_hrv_rbeg_code       , p1.lrdal_raud_start_date
                  , p1.lrdal_rdin_effective_date );
    FETCH c_get_rdin INTO l_rdin_refno;
   CLOSE c_get_rdin;
--
-- Insert int relevent table
--
        INSERT /* +APPEND */ into  rds_allocations
              ( rdal_refno
              , rdal_rdin_refno
              , rdal_effective_date
              , rdal_allocated_amount
              , rdal_deduction_action_type
              , rdal_created_by
              , rdal_created_date
              )
        VALUES
              ( p1.lrdal_refno
              , l_rdin_refno
              , p1.lrdal_effective_date
              , p1.lrdal_allocated_amount
              , p1.lrdal_deduction_action_type
              , p1.lrdal_created_by
              , p1.lrdal_created_date
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
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_ALLOCATIONS');
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
      , lrdal_dlb_batch_id
      , lrdal_dl_seqno
      , lrdal_dl_load_status
      , lrdal_refno
      , lrdal_rdsa_ha_reference
      , lrdal_raud_start_date
      , lrdal_hrv_dedt_code
      , lrdal_hrv_rbeg_code
      , lrdal_rdin_effective_date
      , lrdal_effective_date
      , lrdal_allocated_amount
      , lrdal_deduction_action_type
      , nvl(lrdal_created_by, 'DATALOAD')  lrdal_created_by
      , nvl(lrdal_created_date, sysdate)   lrdal_created_date
FROM    dl_hra_rds_allocations
WHERE   lrdal_dlb_batch_id    = p_batch_id
AND     lrdal_dl_load_status in ('L','F','O')
ORDER BY lrdal_refno ;
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_get_rdin( p_ha_reference    VARCHAR2
                  , p_dedt_code       VARCHAR2
                  , p_rbeg_code       VARCHAR2
                  , p_start_date      DATE
                  , p_effect_date     DATE     ) IS
SELECT  'X'
FROM    rds_instructions
      , rds_authorised_deductions
      , rds_authorities
WHERE   rdsa_refno          = raud_rdsa_refno
AND     rdin_raud_refno     = raud_refno
AND     rdin_effective_date = p_effect_date
AND     rdsa_ha_reference   = p_ha_reference
AND     raud_hrv_dedt_code  = p_dedt_code
AND     raud_hrv_rbeg_code  = p_rbeg_code
AND     raud_start_date     = p_start_date;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_RDS_ALLOCATIONS';
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
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_allocations.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_rds_allocations.dataload_validate',3);
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
   cs   := p1.lrdal_dl_seqno;
   l_id := p1.rec_rowid;
--
   l_errors := 'V';
   l_error_ind := 'N';
--
--
-- Validation checks required
--
-- Check record exists on RDS Instructions
--
   OPEN c_get_rdin( p1.lrdal_rdsa_ha_reference   , p1.lrdal_hrv_dedt_code
                  , p1.lrdal_hrv_rbeg_code       , p1.lrdal_raud_start_date
                  , p1.lrdal_rdin_effective_date );
    FETCH c_get_rdin INTO l_exists;
      IF c_get_rdin%NOTFOUND
       THEN
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',167);
      END IF;
   CLOSE c_get_rdin;
--
-- Check mandatory Fields (except those above)
--
-- Effective Date
--
   IF p1.lrdal_effective_date IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',165);
   END IF;
--
-- Allocated Amount
--
   IF p1.lrdal_allocated_amount IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',168);
   END IF;
--
-- Deduction Action Type
--
   IF p1.lrdal_deduction_action_type IS NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',169);
   END IF;
--
-- Reference Values
--
-- Deduction Type -- Mandatory as used in a link
--
   IF (NOT s_dl_hem_utils.exists_frv('RDS_DED_TYPE',p1.lrdal_hrv_dedt_code,'N'))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',103);
   END IF;
--
-- Benefit Group -- Mandatory as used in a link
--
   IF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_GRP',p1.lrdal_hrv_rbeg_code,'N'))
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',104);
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
PROCEDURE dataload_delete
            (p_batch_id       IN VARCHAR2
            ,p_date           IN date) IS
--
CURSOR c1 is
SELECT  rowid rec_rowid
      , lrdal_dlb_batch_id
      , lrdal_dl_seqno
      , lrdal_dl_load_status
      , lrdal_refno
FROM    dl_hra_rds_allocations
WHERE   lrdal_dlb_batch_id    = p_batch_id
AND     lrdal_dl_load_status  = 'C';
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
ct       VARCHAR2(30) := 'DL_HRA_RDS_ALLOCATIONS';
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
l_an_tab             VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_rds_allocations.dataload_delete');
fsc_utils.debug_message('s_dl_hra_rds_allocations.dataload_delete',3 );
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
   cs   := p1.lrdal_dl_seqno;
   l_id := p1.rec_rowid;
   i    := i +1;
--
-- Delete from table
--
   DELETE FROM rds_allocations
   WHERE  rdal_refno  = p1.lrdal_refno
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
l_an_tab:=s_dl_hem_utils.dl_comp_stats('RDS_ALLOCATIONS');
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
END s_dl_hra_rds_allocations;
/

