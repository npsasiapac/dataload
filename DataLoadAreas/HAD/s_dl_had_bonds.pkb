--
CREATE OR REPLACE PACKAGE BODY s_dl_had_bonds
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   05-FEB-2009  Initial Creation.
--
--  2.0     5.15.0    VS   17-apr-2009  Changed Status Code CLA to CLM.
--
--  3.0     5.15.0    VS   14-MAY-2009  Changed Status Code PRE to PSN
--                                      and REF to RFN.
--
--  4.0     5.15.0    VS   11-DEC-2009  Defect 2897 Fix. Disable/Enable
--                                      BND_BR_I in CREATE Process
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
  UPDATE dl_had_bonds
  SET    lbnd_dl_load_status = p_status
  WHERE  rowid               = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_had_bonds');
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
       LBND_DLB_BATCH_ID,
       LBND_DL_SEQNO,
       LBND_DL_LOAD_STATUS,
       LBND_ACHO_LEGACY_REF,
       LBND_SCO_CODE,
       LBND_STATUS_DATE,
       LBND_AUN_CODE,
       NVL(LBND_CREATED_BY,'DATALOAD') LBND_CREATED_BY,
       NVL(LBND_CREATED_DATE,SYSDATE)  LBND_CREATED_DATE,
       LBND_REFERENCE,
       LBND_BOND_LODGEMENT_NUMBER,
       LBND_BOND_AMOUNT,
       LBND_CONTRIBUTION_AMOUNT,
       LBND_HRV_BOVR_CODE,
       LBND_OVERRIDE_AMOUNT,
       LBND_OVERRIDE_DATE,
       LBND_OVERRIDE_USERNAME,
       LBND_REFUND_AMOUNT,
       LBND_IPP_SHORTNAME,
       LBND_HRV_BCR_CODE,
       LBND_CLAIM_AMOUNT,
       LBND_REFNO
  FROM dl_had_bonds
 WHERE lbnd_dlb_batch_id   = p_batch_id
   AND lbnd_dl_load_status = 'V';
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
CURSOR get_ipp_refno(p_ipp_shortname VARCHAR2)
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HAD_BONDS';
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
l_ipp_refno       NUMBER(10);
--
--
-- ***********************************************************************
--
BEGIN
--
    execute immediate 'alter trigger BND_BR_I disable';
--
    fsc_utils.proc_start('s_dl_had_bonds.dataload_create');
    fsc_utils.debug_message('s_dl_had_bonds.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lbnd_dl_seqno;
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
           OPEN get_acho_reference(p1.lbnd_acho_legacy_ref);
          FETCH get_acho_reference INTO l_acho_reference;
          CLOSE get_acho_reference;
--
--
-- Get ipp_refno
--
          l_ipp_refno := NULL;
--
           OPEN get_ipp_refno(p1.lbnd_ipp_shortname);
          FETCH get_ipp_refno INTO l_ipp_refno;
          CLOSE get_ipp_refno;
--
--
-- Insert into relevent table
--
--
-- Insert into BONDS
--
--
          INSERT /* +APPEND */ INTO BONDS(BND_REFNO,
                            BND_SCO_CODE,
                            BND_STATUS_DATE,
                            BND_AUN_CODE,
                            BND_CREATED_BY,
                            BND_CREATED_DATE,
                            BND_ACHO_REFERENCE,
                            BND_REFERENCE,
                            BND_BOND_LODGEMENT_NUMBER,
                            BND_BOND_AMOUNT,
                            BND_CONTRIBUTION_AMOUNT,
                            BND_HRV_BOVR_CODE,
                            BND_OVERRIDE_AMOUNT,
                            BND_OVERRIDE_DATE,
                            BND_OVERRIDE_USERNAME,
                            BND_REFUND_AMOUNT,
                            BND_IPP_REFNO,
                            BND_HRV_BCR_CODE,
                            BND_CLAIM_AMOUNT 
                           )
--
                    VALUES (p1.lbnd_refno,
                            p1.LBND_SCO_CODE,
                            p1.LBND_STATUS_DATE,
                            p1.LBND_AUN_CODE,
                            p1.LBND_CREATED_BY,
                            p1.LBND_CREATED_DATE,
                            l_acho_reference,
                            p1.LBND_REFERENCE,
                            p1.LBND_BOND_LODGEMENT_NUMBER,
                            p1.LBND_BOND_AMOUNT,
                            p1.LBND_CONTRIBUTION_AMOUNT,
                            p1.LBND_HRV_BOVR_CODE,
                            p1.LBND_OVERRIDE_AMOUNT,
                            p1.LBND_OVERRIDE_DATE,
                            p1.LBND_OVERRIDE_USERNAME,
                            p1.LBND_REFUND_AMOUNT,
                            l_ipp_refno,
                            p1.LBND_HRV_BCR_CODE,
                            p1.LBND_CLAIM_AMOUNT
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('BONDS');
--
    execute immediate 'alter trigger BND_BR_I enable';
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
       LBND_DLB_BATCH_ID,
       LBND_DL_SEQNO,
       LBND_DL_LOAD_STATUS,
       LBND_ACHO_LEGACY_REF,
       LBND_SCO_CODE,
       LBND_STATUS_DATE,
       LBND_AUN_CODE,
       NVL(LBND_CREATED_BY,'DATALOAD') LBND_CREATED_BY,
       NVL(LBND_CREATED_DATE,SYSDATE)  LBND_CREATED_DATE,
       LBND_REFERENCE,
       LBND_BOND_LODGEMENT_NUMBER,
       LBND_BOND_AMOUNT,
       LBND_CONTRIBUTION_AMOUNT,
       LBND_HRV_BOVR_CODE,
       LBND_OVERRIDE_AMOUNT,
       LBND_OVERRIDE_DATE,
       LBND_OVERRIDE_USERNAME,
       LBND_REFUND_AMOUNT,
       LBND_IPP_SHORTNAME,
       LBND_HRV_BCR_CODE,
       LBND_CLAIM_AMOUNT,
       LBND_REFNO
  FROM dl_had_bonds
 WHERE lbnd_dlb_batch_id    = p_batch_id
   AND lbnd_dl_load_status in ('L','F','O');
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
CURSOR get_acas_aun_type
IS
SELECT TRIM(pva.pva_char_value)
  FROM parameter_values            pva,
       area_codes                  arc,
       parameter_definition_usages pdu
 WHERE pdu.pdu_pdf_param_type  = 'SYSTEM'
   AND arc.arc_pgp_refno       = pdu.pdu_pgp_refno
   AND pdu.pdu_pdf_name        = pva.pva_pdu_pdf_name
   AND pdu.pdu_pdf_param_type  = pva.pva_pdu_pdf_param_type
   AND pdu.pdu_pob_table_name  = pva.pva_pdu_pob_table_name
   AND pdu.pdu_pgp_refno       = pva.pva_pdu_pgp_refno
   AND pdu.pdu_display_seqno   = pva.pva_pdu_display_seqno
   AND pdu.pdu_pdf_name        = 'ADVCASE_AUN_TYPE';
--
--
-- ***********************************************************************
--
CURSOR chk_acas_aun_exists(p_aun_code VARCHAR2, 
                           p_aun_type VARCHAR2)
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code     = p_aun_code
   AND aun_auy_code = p_aun_type;
--
--
-- ***********************************************************************
--
CURSOR chk_bnd_ref_exists(p_bnd_reference VARCHAR2)
IS
SELECT 'X'
  FROM bonds
 WHERE bnd_reference = p_bnd_reference;
--
--
-- ***********************************************************************
--
CURSOR chk_bnd_ipp_exists(p_ipp_shortname VARCHAR2)
IS
SELECT 'X'
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname;
--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HAD_BONDS';
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
l_acas_aun_type         VARCHAR2(255);
l_bnd_aun_exists        VARCHAR2(1);
l_bnd_ref_exists        VARCHAR2(1);
l_bnd_ipp_exists        VARCHAR2(1);
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
    fsc_utils.proc_start('s_dl_had_bonds.dataload_validate');
    fsc_utils.debug_message('s_dl_had_bonds.dataload_validate',3);
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
          cs   := p1.lbnd_dl_seqno;
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
          IF (p1.lbnd_acho_legacy_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',157);
          ELSE
--
             l_acho_reference := NULL;
--
              OPEN chk_acho_exists(p1.lbnd_acho_legacy_ref);
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
-- Check status code LBND_SCO_CODE has been supplied and is valid
--
--
          IF (p1.lbnd_sco_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',47);
--
          ELSIF (p1.lbnd_sco_code NOT IN ('BNI','ISS','PSN','CLM','RFN','CAN')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',14);
--
          END IF;
--
-- ***********
--
-- Check Status date LBND_STATUS_DATE has been supplied
--
-- 
         IF (p1.lbnd_status_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',16);
         END IF;
--
-- ***********
--
-- Check Admin Unit LBND_AUN_CODE is supplied and valid
-- 
-- The Admin Unit must exist on Admin Units Table and must be the same type 
-- as the value held in the ADVCASE_AUN_TYPE parameter.
--
          IF (p1.lbnd_aun_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',189);
--
          ELSE
--
             l_acas_aun_type := NULL;
--
              OPEN get_acas_aun_type;
             FETCH get_acas_aun_type INTO l_acas_aun_type;
             CLOSE get_acas_aun_type;
--
             IF (l_acas_aun_type IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',143);
-- 
             ELSE
--
                l_bnd_aun_exists := NULL;
--
                 OPEN chk_acas_aun_exists (p1.lbnd_aun_code, l_acas_aun_type);
                FETCH chk_acas_aun_exists INTO l_bnd_aun_exists;
                CLOSE chk_acas_aun_exists;
--
              IF (l_bnd_aun_exists IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',127);
              END IF;
--
             END IF; 
--
          END IF;
--
-- ***********
--
-- Check Bond Reference LBND_REFERENCE doesn't already exist if supplied
--
-- 
         IF (p1.lbnd_reference IS NOT NULL) THEN
--
          l_bnd_ref_exists := NULL;
--
           OPEN chk_bnd_ref_exists (p1.lbnd_reference);
          FETCH chk_bnd_ref_exists INTO l_bnd_ref_exists;
          CLOSE chk_bnd_ref_exists;
--
          IF (l_bnd_ref_exists IS NOT NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',190);
          END IF;
--
         END IF;
--
-- ***********
--
-- Check Interested Party ShortName LBND_IPP_SHORTNAME is valid if supplied
--
-- 
         IF (p1.lbnd_ipp_shortname IS NOT NULL) THEN
--
          l_bnd_ipp_exists := NULL;
--
           OPEN chk_bnd_ipp_exists (p1.lbnd_ipp_shortname);
          FETCH chk_bnd_ipp_exists INTO l_bnd_ipp_exists;
          CLOSE chk_bnd_ipp_exists;
--
          IF (l_bnd_ipp_exists IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',191);
          END IF;
--
         END IF;
--
--
-- ***********************************************************************
--
-- All reference values supplied are valid
-- 
--
-- Check Override reason Code LBND_HRV_BOVR_CODE is valid if supplied
--
          IF (p1.LBND_HRV_BOVR_CODE IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('BONDOVERRIDERSN',p1.lbnd_hrv_bovr_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',192);
           END IF;
--
          END IF;
--
-- ***********
--
-- Check Claim reason Code LBND_HRV_BCR_CODE is valid if supplied
--
          IF (p1.LBND_HRV_BCR_CODE IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('????????',p1.lbnd_hrv_bcr_code,'Y')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',193);
           END IF;
--
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
PROCEDURE dataload_delete(p_batch_id       IN VARCHAR2,
                          p_date           IN date) IS
--
CURSOR c1 is
SELECT ROWID REC_ROWID,
       LBND_DLB_BATCH_ID,
       LBND_DL_SEQNO,
       LBND_DL_LOAD_STATUS,
       LBND_REFNO
  FROM dl_had_bonds
 WHERE lbnd_dlb_batch_id   = p_batch_id
   AND lbnd_dl_load_status = 'C';
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
ct         VARCHAR2(30) := 'DL_HAD_BONDS';
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
    fsc_utils.proc_start('s_dl_had_bonds.dataload_delete');
    fsc_utils.debug_message('s_dl_had_bonds.dataload_delete',3 );
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
          cs   := p1.lbnd_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
--
--
-- Delete from bonds table
--
          DELETE 
            FROM bonds
           WHERE bnd_refno = p1.lbnd_refno;
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BONDS');
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
END s_dl_had_bonds;
/

