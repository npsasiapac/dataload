--
CREATE OR REPLACE PACKAGE BODY s_dl_had_adv_case_hsg_optn_his
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   05-FEB-2009  Initial Creation.
--
--  2.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      ACHH_BR_I in CREATE Process
--
--  3.0     5.15.0    VS   17-FEB-2010  Add TO_CHAR to cursors to make sure
--                                      indexes are used correctly
--
--                                      Changed commit 500000 to 50000
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
  UPDATE dl_had_adv_case_hsg_optn_his
  SET    lachh_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_had_adv_case_hsg_optn_his');
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
SELECT ROWID REC_ROWID,
       LACHH_DLB_BATCH_ID,
       LACHH_DL_SEQNO,
       LACHH_DL_LOAD_STATUS,
       LACHH_ACHO_LEGACY_REF,
       LACHH_HOAU_AUN_CODE,
       LACHH_HODS_HRV_DEST_CODE,
       LACHH_STATUS_DATE,
       NVL(LACHH_CREATED_BY, 'DATALOAD') LACHH_CREATED_BY,
       NVL(LACHH_CREATED_DATE, SYSDATE) LACHH_CREATED_DATE,
       LACHH_REFNO
  FROM dl_had_adv_case_hsg_optn_his
 WHERE lachh_dlb_batch_id   = p_batch_id
   AND lachh_dl_load_status = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR get_acho_reference(p_acho_reference VARCHAR2)
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = TO_CHAR(p_acho_reference);
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HAD_ADV_CASE_HSG_OPTN_HIS';
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
i                 INTEGER := 0;
l_exists          VARCHAR2(1);
l_acho_reference  NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger ACHH_BR_I disable';
--
    fsc_utils.proc_start('s_dl_had_adv_case_hsg_optn_his.dataload_create');
    fsc_utils.debug_message('s_dl_had_adv_case_hsg_optn_his.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lachh_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
--
-- Get acho_reference
--
          l_acho_reference := NULL;
--
           OPEN get_acho_reference(p1.lachh_acho_legacy_ref);
          FETCH get_acho_reference INTO l_acho_reference;
          CLOSE get_acho_reference;
--
--
-- Insert into relevent table
--
--
-- Insert into ADVICE_CASE_HOUSING_OPTION_HIS
--
--
          INSERT /* +APPEND */ INTO advice_case_housing_option_his(ACHH_REFNO,
                                                     ACHH_ACHO_REFERENCE,
                                                     ACHH_HOAU_AUN_CODE,
                                                     ACHH_HODS_HRV_DEST_CODE,
                                                     ACHH_STATUS_DATE,
                                                     ACHH_CREATED_BY,
                                                     ACHH_CREATED_DATE
                                                    )
--
                                             VALUES (p1.lachh_refno,
                                                     l_acho_reference,
                                                     p1.LACHH_HOAU_AUN_CODE,
                                                     p1.LACHH_HODS_HRV_DEST_CODE,
                                                     p1.LACHH_STATUS_DATE,
                                                     p1.LACHH_CREATED_BY,
                                                     p1.LACHH_CREATED_DATE
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_HOUSING_OPTION_HIS');
--
   execute immediate 'alter trigger ACHH_BR_I enable';
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
SELECT ROWID REC_ROWID,
       LACHH_DLB_BATCH_ID,
       LACHH_DL_SEQNO,
       LACHH_DL_LOAD_STATUS,
       LACHH_ACHO_LEGACY_REF,
       LACHH_HOAU_AUN_CODE,
       LACHH_HODS_HRV_DEST_CODE,
       LACHH_STATUS_DATE,
       NVL(LACHH_CREATED_BY, 'DATALOAD') LACHH_CREATED_BY,
       NVL(LACHH_CREATED_DATE, SYSDATE) LACHH_CREATED_DATE
  FROM dl_had_adv_case_hsg_optn_his
 WHERE lachh_dlb_batch_id    = p_batch_id
   AND lachh_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR chk_acho_exists(p_acho_reference VARCHAR2) 
IS
SELECT acho_reference
  FROM advice_case_housing_options
 WHERE acho_alternative_reference = TO_CHAR(p_acho_reference);
--
--
-- ***********************************************************************
--
CURSOR chk_aun_exists(p_aun_code VARCHAR2)
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HAD_ADV_CASE_HSG_OPTN_HIS';
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
l_acho_reference       	NUMBER(10);
l_achh_aun_exists       VARCHAR2(1);
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_had_adv_case_hsg_optn_his.dataload_validate');
    fsc_utils.debug_message('s_dl_had_adv_case_hsg_optn_his.dataload_validate',3);
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
          cs   := p1.lachh_dl_seqno;
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
-- Check Housing Options Reference LACHH_ACHO_LEGACY_REF has been supplied
-- and valid
--
--  
          IF (p1.lachh_acho_legacy_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',157);
          ELSE
--
             l_acho_reference := NULL;
--
              OPEN chk_acho_exists(p1.lachh_acho_legacy_ref);
             FETCH chk_acho_exists INTO l_acho_reference;
             CLOSE chk_acho_exists;
--
             IF (l_acho_reference IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',185);
             END IF;
--
          END IF;
--
-- ***********
--
-- Check Case Admin Unit LACHH_HOAU_AUN_CODE is valid if supplied
-- 
--
          IF (p1.lachh_hoau_aun_code IS NOT NULL) THEN
--
           l_achh_aun_exists := NULL;
--
            OPEN chk_aun_exists (p1.lachh_hoau_aun_code);
           FETCH chk_aun_exists INTO l_achh_aun_exists;
           CLOSE chk_aun_exists;
--
           IF (l_achh_aun_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',127);
           END IF;
--
          END IF; 
--
-- ***********
--
-- Check Delivery Status LACHH_HODS_HRV_DEST_CODE is supplied and valid
--
          IF (p1.LACHH_HODS_HRV_DEST_CODE IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',160);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('DELIVERYSTATUS',p1.lachh_hods_hrv_dest_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',161);
--
          END IF;
--
-- ***********
--
-- Check Status date LACHH_STATUS_DATE has been supplied
--
-- 
         IF (p1.lachh_status_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',188);
         END IF;
--
--
-- ***********************************************************************
--
-- All reference values supplied are valid
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
SELECT ROWID REC_ROWID,
       LACHH_DLB_BATCH_ID,
       LACHH_DL_SEQNO,
       LACHH_DL_LOAD_STATUS,
       LACHH_ACHO_LEGACY_REF,
       LACHH_REFNO
  FROM dl_had_adv_case_hsg_optn_his
 WHERE lachh_dlb_batch_id   = p_batch_id
   AND lachh_dl_load_status = 'C';
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
cb         VARCHAR2(30);
cd         DATE;
cp         VARCHAR2(30) := 'DELETE';
ct         VARCHAR2(30) := 'DL_HAD_ADV_CASE_HSG_OPTN_HIS';
cs         INTEGER;
ce         VARCHAR2(200);
l_id       ROWID;
l_an_tab   VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_had_adv_case_hsg_optn_his.dataload_delete');
    fsc_utils.debug_message('s_dl_had_adv_case_hsg_optn_his.dataload_delete',3 );
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
          cs   := p1.lachh_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
--
-- Delete from advice_case_housing_option_his table
--
          DELETE 
            FROM advice_case_housing_option_his
           WHERE achh_refno = p1.lachh_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADVICE_CASE_HOUSING_OPTION_HIS');
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
END s_dl_had_adv_case_hsg_optn_his;
/