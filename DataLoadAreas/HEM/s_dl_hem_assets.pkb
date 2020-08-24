--
CREATE OR REPLACE PACKAGE BODY s_dl_hem_assets
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.5.0     VS   04-JUL-2011  Initial Creation.
--
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
  UPDATE dl_hem_assets
     SET lasse_dl_load_status = p_status
   WHERE rowid                = p_rowid;
  --
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hem_assets');
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
CURSOR c1 IS
SELECT rowid rec_rowid,
       LASSE_DLB_BATCH_ID,
       LASSE_DL_SEQNO,
       LASSE_DL_LOAD_STATUS,
       LASSE_INH_LEGACY_REF,
       LASSE_ASCO_CODE,
       LASSE_AMOUNT,
       LASSE_PERCENTAGE_OWNED,
       NVL(LASSE_CREATED_BY,'DATALOAD') LASSE_CREATED_BY,
       NVL(LASSE_CREATED_DATE,SYSDATE)  LASSE_CREATED_DATE,
       LASSE_ASSESSMENT_DATE,
       LASSE_COMMENTS,
       LASSE_MODIFIED_BY,
       LASSE_MODIFIED_DATE,
       LASSE_ANNUAL_INCOME,
       LASSE_REFNO
  FROM dl_hem_assets
 WHERE lasse_dlb_batch_id    = p_batch_id
   AND lasse_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR c_get_inh(p_inh_legacy_ref NUMBER) 
IS
SELECT linh_refno
  FROM dl_hem_income_headers,
       income_headers
 WHERE linh_legacy_ref     = p_inh_legacy_ref
   AND linh_dl_load_status = 'C'
   AND inh_refno           = linh_refno;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HEM_ASSETS';
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
l_par_refno          parties.par_refno%type;
l_inh_refno          NUMBER(10);
l_inh_indr_refno     NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger ASSE_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hem_assets.dataload_create');
    fsc_utils.debug_message('s_dl_hem_assets.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lasse_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
-- Open any cursors
--
--
          l_inh_refno      := NULL;
--
-- Get the income_header_refno
--
           OPEN c_get_inh(p1.LASSE_INH_LEGACY_REF);
          FETCH c_get_inh INTO l_inh_refno;
          CLOSE c_get_inh;
--
--
-- Insert into Assets table
--
--
          INSERT /* +APPEND*/ into  assets(asse_refno,
                                           asse_asco_code,
                                           asse_amount,
                                           asse_percentage_owned,
                                           asse_created_by,
                                           asse_created_date,
                                           asse_indr_refno,
                                           asse_inh_refno,
                                           asse_assessment_date,
                                           asse_comments,
                                           asse_modified_by,
                                           asse_modified_date,
                                           asse_annual_income
                                          )
--
                                   VALUES (p1.lasse_refno,
                                           p1.lasse_asco_code,
                                           p1.lasse_amount,
                                           p1.lasse_percentage_owned,
                                           p1.lasse_created_by,
                                           p1.lasse_created_date,
                                           NULL,
                                           l_inh_refno,
                                           p1.lasse_assessment_date,
                                           p1.lasse_comments,
                                           p1.lasse_modified_by,
                                           p1.lasse_modified_date,
                                           p1.lasse_annual_income
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ASSETS');
--
    fsc_utils.proc_END;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
            RAISE;
--
    execute immediate 'alter trigger ASSE_BR_I enable';
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
SELECT rowid rec_rowid,
       LASSE_DLB_BATCH_ID,
       LASSE_DL_SEQNO,
       LASSE_DL_LOAD_STATUS,
       LASSE_INH_LEGACY_REF,
       LASSE_ASCO_CODE,
       LASSE_AMOUNT,
       LASSE_PERCENTAGE_OWNED,
       NVL(LASSE_CREATED_BY,'DATALOAD') LASSE_CREATED_BY,
       NVL(LASSE_CREATED_DATE,SYSDATE)  LASSE_CREATED_DATE,
       LASSE_ASSESSMENT_DATE,
       LASSE_COMMENTS,
       LASSE_MODIFIED_BY,
       LASSE_MODIFIED_DATE,
       LASSE_ANNUAL_INCOME,
       LASSE_REFNO
  FROM dl_hem_assets
 WHERE lasse_dlb_batch_id    = p_batch_id
   AND lasse_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR c_chk_asco(p_asco_code VARCHAR2) 
IS
SELECT asco_code,
       asco_asset_value_reqd_ind
  FROM asset_codes
 WHERE asco_code = p_asco_code;
--
-- ***********************************************************************
--
CURSOR c_chk_inh_exists(p_inh_legacy_ref NUMBER) 
IS
SELECT inh_refno
  FROM dl_hem_income_headers,
       income_headers
 WHERE linh_dl_load_status = 'C'
   AND linh_legacy_ref     = p_inh_legacy_ref
   AND inh_refno           = linh_refno;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'VALIDATE';
ct                   VARCHAR2(30) := 'DL_HEM_ASSETS';
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
l_inh_refno                NUMBER(10);
l_asco_code                VARCHAR2(10);
l_asco_asset_val_reqd_ind  VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_assets.dataload_validate');
    fsc_utils.debug_message( 's_dl_hem_assets.dataload_validate',3);
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
          cs   := p1.lasse_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
          l_asco_code               := NULL;
          l_asco_asset_val_reqd_ind := NULL;
--
-- **************************************************************************
--
-- Validation checks required
--
--
-- Check an Income Headers exists
--
          l_inh_refno := NULL;
--
           OPEN c_chk_inh_exists(p1.lasse_inh_legacy_ref);
          FETCH c_chk_inh_exists INTO l_inh_refno;
--
          IF (c_chk_inh_exists%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',473);
          END IF;
--
          CLOSE c_chk_inh_exists;
--
-- **************************************************************************
--
-- The Asset Code must exist on the Asset Codes Table
--
           OPEN c_chk_asco(p1.lasse_asco_code);
--
          FETCH c_chk_asco INTO l_asco_code,
                                l_asco_asset_val_reqd_ind;
--
          IF (c_chk_asco%NOTFOUND) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',24);
          END IF;
--
          CLOSE c_chk_asco;
--
-- **************************************************************************
--
-- Percentage Owned must be greater than 0 and less than or equal to 100
--
          IF (p1.lasse_percentage_owned IS NOT NULL) THEN
--
           IF (p1.lasse_percentage_owned NOT BETWEEN 0 AND 100) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',25);
           END IF;
--
          END IF;
--
-- **************************************************************************
--
-- All mandatory fields are supplied
--
-- Value of Asset
--
          IF (p1.lasse_amount IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',26);
          END IF;
--
-- **************************************************************************
--
-- If the Asset Code has Asset Value set to ‘M’ then Asset Value and Asset Income
-- must be supplied. If set to ‘N’ then Asset Value must not be supplied. If set 
-- to ‘O’ then Asset Value is optional.
--
          IF (l_asco_asset_val_reqd_ind = 'M') THEN
--
           IF (   p1.lasse_amount        IS NULL
               OR p1.lasse_annual_income IS NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',486);
--
           END IF;
--
          ELSIF (l_asco_asset_val_reqd_ind = 'N') THEN
--
              IF (   p1.lasse_amount        IS NOT NULL
                  OR p1.lasse_annual_income IS NOT NULL) THEN
--
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',487);
--
              END IF;
--   
          END IF;
--
-- **************************************************************************
--
-- At least one of Asset Income or Asset Value must be supplied
--
          IF (    p1.lasse_amount        IS NULL
              AND p1.lasse_annual_income IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',488);
--
          END IF;
--
-- **************************************************************************
--
-- If Asset Value has been supplied then Percentage Owned must be supplied
--
          IF (    p1.lasse_amount           IS NOT NULL
              AND p1.lasse_percentage_owned IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',489);
--
          END IF;
--
-- **************************************************************************
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
                          p_date           IN date) 
IS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       lasse_dlb_batch_id,
       lasse_dl_seqno,
       lasse_dl_load_status,
       lasse_refno
  FROM dl_hem_assets
 WHERE lasse_dlb_batch_id    = p_batch_id
   AND lasse_dl_load_status  = 'C';
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
ct                   VARCHAR2(30) := 'DL_HEM_ASSETS';
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
l_pro_refno      NUMBER(10);
i                INTEGER :=0;
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_assets.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_assets.dataload_delete',3 );
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
          cs   := p1.lasse_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from Assets table
--
          DELETE 
            FROM assets
           WHERE asse_refno = p1.lasse_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('ASSETS');
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
END s_dl_hem_assets;
/

