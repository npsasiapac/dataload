SET FEEDBACK OFF;

CREATE OR REPLACE PACKAGE BODY s_dl_hcs_business_actions
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--
--      1.0  5.16.1    VS   24-SEP-2009  Initial Version
--
--      1.1  5.16.1    VS   28-OCT-2009  Error Code Change 620 to 627
--
--      1.2  5.16.1    VS   02-DEC-2009  Install problem identified in DHMERM02.
--                                       Column LBAN_REFERENCE not required in the
--                                       validate process.
--      1.3  6.17.1    PL   11-SEP-2018  Changed TCY to use TCY_ALT_REF
-- ***********************************************************************   
--  
--  declare package variables AND constants
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_hcs_business_actions
     SET lban_dl_load_status = p_status
   WHERE rowid               = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hcs_business_actions');
          RAISE;
--
END set_record_status_flag;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LBAN_DLB_BATCH_ID,
       LBAN_DL_SEQNO,
       LBAN_DL_LOAD_STATUS,
       LBAN_ALT_REF,
       LBAN_OBJ_LEGACY_REF,
       LBAN_OBJ_SECONDARY_REF,
       LBAN_LEGACY_TYPE,
       LBAN_TYPE,
       LBAN_BRO_CODE,
       LBAN_AUN_CODE_RESPONSIBLE,
       LBAN_SCO_CODE,
       LBAN_STATUS_DATE,
       NVL(LBAN_CREATED_BY,'DATALOAD') LBAN_CREATED_BY,
       NVL(LBAN_CREATED_DATE, SYSDATE) LBAN_CREATED_DATE,
       LBAN_USR_USERNAME,
       LBAN_BAN_ALT_REF,
       LBAN_TARGET_DATE,
       LBAN_LAS_LEA_START_DATE,
       LBAN_LAS_START_DATE
  FROM dl_hcs_business_actions
 WHERE lban_dlb_batch_id   = p_batch_id
   AND lban_dl_load_status = 'V';
--
-- ************************************************************************************
--
CURSOR get_par_refno(p_par_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
-- ************************************************************************************
--
CURSOR get_pro_refno(p_pro_propref VARCHAR2) 
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ************************************************************************************
--
CURSOR get_aun_code(p_aun_code VARCHAR2) 
IS
SELECT aun_code
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
-- ************************************************************************************
--
CURSOR get_tcy_refno(p_tcy_refno VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_refno = p_tcy_refno; 
--
-- ************************************************************************************
--
CURSOR get_tcy_alt_refno(p_tcy_refno VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_refno; -- 1.3
--
-- ************************************************************************************
--
CURSOR get_ipp_refno(p_ipp_shortname VARCHAR2,
                     p_ipp_ipt_code  VARCHAR2)   
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipp_ipt_code;
--
-- ************************************************************************************
--
CURSOR get_app_refno(p_app_legacy_ref VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;
--
-- ************************************************************************************
--
CURSOR get_peg_code(p_peg_code VARCHAR2) 
IS
SELECT peg_code
  FROM people_groups
 WHERE peg_code = p_peg_code;
--
-- ************************************************************************************
--
CURSOR get_srq_no(p_srq_legacy_refno VARCHAR2) 
IS
SELECT srq_no
FROM   service_requests
WHERE  srq_legacy_refno = p_srq_legacy_refno;
--
-- ************************************************************************************
--
CURSOR get_cos_code(p_cos_code VARCHAR2) 
IS
SELECT cos_code
  FROM contractor_sites
 WHERE cos_code = p_cos_code;
--
-- ************************************************************************************
--
--CURSOR get_ban_ban_reference(p_ban_ban_alt_ref VARCHAR2) 
--IS
--SELECT lban_reference
--  FROM dl_hcs_business_actions
-- WHERE lban_alt_ref = p_ban_ban_alt_ref;
--
-- ************************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'CREATE';
ct       		VARCHAR2(30) := 'DL_HCS_BUSINESS_ACTIONS';
cs       		INTEGER;
ce	   		VARCHAR2(200);
l_id            	ROWID;
l_an_tab 		VARCHAR2(1);
--
-- Other variables
--
i	              	INTEGER := 0;
--
--
l_par_refno             NUMBER(8);
l_pro_refno             NUMBER(10);
l_las_pro_refno         NUMBER(10);
l_aun_code              VARCHAR2(20);
l_tcy_refno		NUMBER(8);
l_ipp_refno             NUMBER(10);
l_app_refno             NUMBER(10);
l_peg_code              VARCHAR2(10);
l_srq_no                NUMBER(10);
l_cos_code              VARCHAR2(15);
l_adr_refno             NUMBER(10);
l_reusable_refno        NUMBER(20);
l_ban_ban_reference     NUMBER(10);
--
--
BEGIN
--
    execute immediate 'alter trigger BAN_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hcs_business_actions.dataload_create');
    fsc_utils.debug_message( 's_dl_hcs_business_actions.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs := p1.lban_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
          l_par_refno     := NULL;
          l_pro_refno     := NULL;
          l_aun_code      := NULL;
          l_tcy_refno     := NULL;
          l_ipp_refno     := NULL;
          l_app_refno     := NULL;
          l_peg_code      := NULL;
          l_srq_no        := NULL;
          l_cos_code      := NULL;          
          l_las_pro_refno := NULL;
--
          l_ban_ban_reference := NULL;
--
--
--
          IF (p1.LBAN_LEGACY_TYPE = 'PAR') THEN
--
            OPEN get_par_refno(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_par_refno INTO l_par_refno;
           CLOSE get_par_refno;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'PRO') THEN
--
               OPEN get_pro_refno(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_pro_refno INTO l_pro_refno;
              CLOSE get_pro_refno;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'AUN') THEN
--
               OPEN get_aun_code(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_aun_code INTO l_aun_code;
              CLOSE get_aun_code;
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'TCY') THEN
--
               OPEN get_tcy_refno(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_tcy_refno INTO l_tcy_refno;
              CLOSE get_tcy_refno;
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'TCA') THEN
--
               OPEN get_tcy_alt_refno(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_tcy_alt_refno INTO l_tcy_refno;
              CLOSE get_tcy_alt_refno;
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'IPP') THEN
--
               OPEN get_ipp_refno(p1.LBAN_OBJ_LEGACY_REF,p1.LBAN_OBJ_SECONDARY_REF);
              FETCH get_ipp_refno INTO l_ipp_refno;
              CLOSE get_ipp_refno;
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'APP') THEN
--
               OPEN get_app_refno(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_app_refno INTO l_app_refno;
              CLOSE get_app_refno;
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'PEG') THEN
--
               OPEN get_peg_code(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_peg_code INTO l_peg_code;
              CLOSE get_peg_code;
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'SRQ') THEN
--
               OPEN get_srq_no(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_srq_no INTO l_srq_no;
              CLOSE get_srq_no;
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'COS') THEN
--
               OPEN get_cos_code(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_cos_code INTO l_cos_code;
              CLOSE get_cos_code;
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'LAS') THEN
--
               OPEN get_pro_refno(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_pro_refno INTO l_las_pro_refno;
              CLOSE get_pro_refno;
--
          END IF;
--
--
          INSERT INTO BUSINESS_ACTIONS(BAN_REFERENCE,
                                       BAN_TYPE,
                                       BAN_BRO_CODE,
                                       BAN_AUN_CODE_RESPONSIBLE,
                                       BAN_SCO_CODE,
                                       BAN_STATUS_DATE,
                                       BAN_REUSABLE_REFNO,
                                       BAN_CREATED_BY,
                                       BAN_CREATED_DATE,
                                       BAN_PAR_REFNO,
                                       BAN_PRO_REFNO,
                                       BAN_AUN_CODE,
                                       BAN_TCY_REFNO,
                                       BAN_IPP_REFNO,
                                       BAN_APP_REFNO,
                                       BAN_PEG_CODE,
                                       BAN_SRQ_NO,
                                       BAN_COS_CODE,
                                       BAN_ADR_REFNO,
                                       BAN_USR_USERNAME,
                                       BAN_BAN_REFERENCE,
                                       BAN_TARGET_DATE,
                                       BAN_LAS_LEA_PRO_REFNO,
                                       BAN_LAS_LEA_START_DATE,
                                       BAN_LAS_START_DATE
                                      )
--
                               VALUES (p1.LBAN_ALT_REF,
                                       p1.LBAN_TYPE,
                                       p1.LBAN_BRO_CODE,
                                       p1.LBAN_AUN_CODE_RESPONSIBLE,
                                       p1.LBAN_SCO_CODE,
                                       p1.LBAN_STATUS_DATE,
                                       reusable_refno_seq.NEXTVAL,
                                       p1.LBAN_CREATED_BY,
                                       p1.LBAN_CREATED_DATE,
                                       l_par_refno,
                                       l_pro_refno,
                                       l_aun_code,
                                       l_tcy_refno,
                                       l_ipp_refno,
                                       l_app_refno,
                                       l_peg_code,
                                       l_srq_no,
                                       l_cos_code,
                                       l_adr_refno,
                                       p1.LBAN_USR_USERNAME,
                                       p1.LBAN_BAN_ALT_REF,
                                       p1.LBAN_TARGET_DATE,
                                       l_las_pro_refno,
                                       p1.LBAN_LAS_LEA_START_DATE,
                                       p1.LBAN_LAS_START_DATE
                                      );
--
--
-- keep a count of the rows processed and commit after every 5000
--
          i := i+1; 
--
          IF MOD(i,5000)=0 THEN 
           COMMIT; 
          END If;
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
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUSINESS_ACTIONS');
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
            RAISE;
--
    execute immediate 'alter trigger BAN_BR_I enable';
--
END dataload_create;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LBAN_DLB_BATCH_ID,
       LBAN_DL_SEQNO,
       LBAN_DL_LOAD_STATUS,
       LBAN_ALT_REF,
       LBAN_OBJ_LEGACY_REF,
       LBAN_OBJ_SECONDARY_REF,
       LBAN_LEGACY_TYPE,
       LBAN_TYPE,
       LBAN_BRO_CODE,
       LBAN_AUN_CODE_RESPONSIBLE,
       LBAN_SCO_CODE,
       LBAN_STATUS_DATE,
       NVL(LBAN_CREATED_BY,'DATALOAD') LBAN_CREATED_BY,
       NVL(LBAN_CREATED_DATE, SYSDATE) LBAN_CREATED_DATE,
       LBAN_USR_USERNAME,
       LBAN_BAN_ALT_REF,
       LBAN_TARGET_DATE,
       LBAN_LAS_LEA_START_DATE,
       LBAN_LAS_START_DATE
  FROM dl_hcs_business_actions
 WHERE lban_dlb_batch_id   = p_batch_id
   AND lban_dl_load_status in ('L','F','O');
--
-- ************************************************************************************
--
CURSOR get_par_refno(p_par_per_alt_ref VARCHAR2) 
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
-- ************************************************************************************
--
CURSOR get_pro_refno(p_pro_propref VARCHAR2) 
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- ************************************************************************************
--
CURSOR get_aun_code(p_aun_code VARCHAR2) 
IS
SELECT aun_code
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
-- ************************************************************************************
--
CURSOR get_tcy_refno(p_tcy_refno VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_refno = p_tcy_refno;
--
-- ************************************************************************************
--
CURSOR get_tcy_alt_ref(p_tcy_refno VARCHAR2) 
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_refno;  -- 1.3
--
-- ************************************************************************************
--
CURSOR get_ipp_refno(p_ipp_shortname VARCHAR2,
                     p_ipp_ipt_code  VARCHAR2)   
IS
SELECT ipp_refno
  FROM interested_parties
 WHERE ipp_shortname = p_ipp_shortname
   AND ipp_ipt_code  = p_ipp_ipt_code;
--
-- ************************************************************************************
--
CURSOR get_app_refno(p_app_legacy_ref VARCHAR2) 
IS
SELECT app_refno
  FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;
--
-- ************************************************************************************
--
CURSOR get_peg_code(p_peg_code VARCHAR2) 
IS
SELECT peg_code
  FROM people_groups
 WHERE peg_code = p_peg_code;
--
-- ************************************************************************************
--
CURSOR get_srq_no(p_srq_legacy_refno VARCHAR2) 
IS
SELECT srq_no
FROM   service_requests
WHERE  srq_legacy_refno = p_srq_legacy_refno;
--
-- ************************************************************************************
--
CURSOR get_cos_code(p_cos_code VARCHAR2) 
IS
SELECT cos_code
  FROM contractor_sites
 WHERE cos_code = p_cos_code;
--
-- ************************************************************************************
--
CURSOR chk_las_exists(p_las_pro_propref    VARCHAR2,
                      p_las_lea_start_date DATE,
                      p_las_start_date     DATE) 
IS
SELECT 'X'
  FROM lease_assignments a,
       properties        b
 WHERE b.pro_propref        = p_las_pro_propref
   AND a.las_lea_pro_refno  = b.pro_refno
   AND a.las_lea_start_date = p_las_lea_start_date
   AND a.las_start_date     = p_las_start_date;
--
-- ************************************************************************************
--
CURSOR chk_bro_code(p_bro_code VARCHAR2) 
IS
SELECT 'X'
  FROM business_reasons
 WHERE bro_code = p_bro_code;
--
-- ************************************************************************************
--
CURSOR chk_ban_exists(p_ban_reference NUMBER) 
IS
SELECT 'X'
  FROM business_actions
 WHERE ban_reference = p_ban_reference;
--
-- ************************************************************************************
--
CURSOR chk_sco_code(p_sco_code VARCHAR2) 
IS
SELECT 'X'
  FROM status_codes
 WHERE sco_code = p_sco_code;
--
-- ************************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd      		DATE;
cp       		VARCHAR2(30) := 'VALIDATE';
ct       		VARCHAR2(30) := 'DL_HCS_BUSINESS_ACTIONS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id            	ROWID;
--
-- Other Constants
--
l_bro_exists      	VARCHAR2(1);
l_resp_aun_code      	VARCHAR2(20);
l_sco_exists      	VARCHAR2(1);
--
l_par_refno             NUMBER(8);
l_pro_refno             NUMBER(10);
l_las_pro_refno         NUMBER(10);
l_aun_code              VARCHAR2(20);
l_tcy_refno		NUMBER(8);
l_ipp_refno             NUMBER(10);
l_app_refno             NUMBER(10);
l_peg_code              VARCHAR2(10);
l_srq_no                NUMBER(10);
l_cos_code              VARCHAR2(15);
l_adr_refno             NUMBER(10);
--
l_las_fields_supplied   VARCHAR2(1);
l_las_exists            VARCHAR2(1);
l_ban_exists            VARCHAR2(1);
l_ban_ban_exists        VARCHAR2(1);
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hcs_business_actions.dataload_validate');
    fsc_utils.debug_message('s_dl_hcs_business_actions.dataload_validate',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lban_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
--
-- ************************************************************************************
--
-- CHECK THAT THE OBJECT LEGACY REFERENCE IS VALID
--
--
          l_par_refno     := NULL;
          l_pro_refno     := NULL;
          l_aun_code      := NULL;
          l_tcy_refno     := NULL;
          l_ipp_refno     := NULL;
          l_app_refno     := NULL;
          l_peg_code      := NULL;
          l_srq_no        := NULL;
          l_cos_code      := NULL;
          l_las_pro_refno := NULL;
--
          l_las_fields_supplied := 'Y';
--
--
          IF (p1.LBAN_LEGACY_TYPE = 'PAR') THEN
--
            OPEN get_par_refno(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_par_refno INTO l_par_refno;
           CLOSE get_par_refno;
--
           IF (l_par_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',590);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'PRO') THEN
--
            OPEN get_pro_refno(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_pro_refno INTO l_pro_refno;
           CLOSE get_pro_refno;
--
           IF (l_pro_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',591);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'AUN') THEN
--
            OPEN get_aun_code(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_aun_code INTO l_aun_code;
           CLOSE get_aun_code;
--
           IF (l_aun_code IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',592);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'TCY') THEN
--
            OPEN get_tcy_refno(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_tcy_refno INTO l_tcy_refno;
           CLOSE get_tcy_refno;
--
           IF (l_tcy_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',593);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'TCA') THEN
--
            OPEN get_tcy_alt_ref(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_tcy_alt_ref INTO l_tcy_refno;
           CLOSE get_tcy_alt_ref;
--
           IF (l_tcy_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',593);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'IPP') THEN
--
            OPEN get_ipp_refno(p1.LBAN_OBJ_LEGACY_REF,p1.LBAN_OBJ_SECONDARY_REF);
           FETCH get_ipp_refno INTO l_ipp_refno;
           CLOSE get_ipp_refno;
--
           IF (l_ipp_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',594);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'APP') THEN
--
            OPEN get_app_refno(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_app_refno INTO l_app_refno;
           CLOSE get_app_refno;
--
           IF (l_app_refno IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',595);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'PEG') THEN
--
            OPEN get_peg_code(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_peg_code INTO l_peg_code;
           CLOSE get_peg_code;
--
           IF (l_peg_code IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',596);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'SRQ') THEN
--
            OPEN get_srq_no(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_srq_no INTO l_srq_no;
           CLOSE get_srq_no;
--
           IF (l_srq_no IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',597);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'COS') THEN
--
            OPEN get_cos_code(p1.LBAN_OBJ_LEGACY_REF);
           FETCH get_cos_code INTO l_cos_code;
           CLOSE get_cos_code;
--
           IF (l_cos_code IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',598);
           END IF;
--
--
          ELSIF (p1.LBAN_LEGACY_TYPE = 'LAS') THEN
--
               OPEN get_pro_refno(p1.LBAN_OBJ_LEGACY_REF);
              FETCH get_pro_refno INTO l_las_pro_refno;
              CLOSE get_pro_refno;
--
              IF (l_las_pro_refno IS NULL) THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',599);
              END IF;
--
              IF (   p1.LBAN_LAS_LEA_START_DATE IS NULL
                  OR p1.LBAN_LAS_START_DATE     IS NULL) THEN
--
               l_las_fields_supplied := 'N';
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',600);
--
              END IF;
--
              IF (    l_las_pro_refno IS NOT NULL
                  AND l_las_fields_supplied = 'Y') THEN
--
                OPEN chk_las_exists(p1.LBAN_OBJ_LEGACY_REF, p1.LBAN_LAS_LEA_START_DATE, p1.LBAN_LAS_START_DATE);
               FETCH chk_las_exists INTO l_las_exists;
               CLOSE chk_las_exists;
--
               IF (l_las_exists IS NULL) THEN
                l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',601);
               END IF;
--
              END IF;
--
          END IF;
--
--
-- ************************************************************************************
--
-- CHECK LEGACY TYPE IS VALID
--
          IF (p1.LBAN_LEGACY_TYPE IS NOT NULL) THEN
--
           IF (p1.LBAN_LEGACY_TYPE NOT IN ('PAR','PRO','AUN','SRQ','IPP','APP','PEG','COS','LAS','TCY','TCA')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',602);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE ACTION TYPE IS VALID. C - Contact Business Action, 
--                                 N - Non Contact Business Action
--
--
          IF (p1.LBAN_TYPE IS NOT NULL) THEN
--
           IF (p1.LBAN_TYPE NOT IN ('C','N')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',603);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE BUSINESS REASON IS VALID
--
          l_bro_exists := NULL;
--
          IF (p1.LBAN_BRO_CODE IS NOT NULL) THEN
--
            OPEN chk_bro_code(p1.LBAN_BRO_CODE);
           FETCH chk_bro_code INTO l_bro_exists;
           CLOSE chk_bro_code;
--
           IF (l_bro_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',604);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE RESPONSIBLE ADMIN UNIT IS VALID
--
          l_resp_aun_code := NULL;
--
          IF (p1.LBAN_AUN_CODE_RESPONSIBLE IS NOT NULL) THEN
--
            OPEN get_aun_code(p1.LBAN_AUN_CODE_RESPONSIBLE);
           FETCH get_aun_code INTO l_resp_aun_code;
           CLOSE get_aun_code;
--
           IF (l_resp_aun_code IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',605);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE STATUS CODE IS VALID
--
          l_sco_exists := NULL;
--
          IF (p1.LBAN_SCO_CODE IS NOT NULL) THEN
--
            OPEN chk_sco_code(p1.LBAN_SCO_CODE);
           FETCH chk_sco_code INTO l_sco_exists;
           CLOSE chk_sco_code;
--
           IF (   l_sco_exists IS NULL
               OR p1.LBAN_SCO_CODE NOT IN ('CUR','COM','HLD','CAN','CLO')) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',606);
--
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE LBAN_ALT_REF doesn't already exists
--
          l_ban_exists := NULL;
--
           OPEN chk_ban_exists(p1.LBAN_ALT_REF);
          FETCH chk_ban_exists INTO l_ban_exists;
          CLOSE chk_ban_exists;
--
          IF (l_ban_exists IS NOT NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',607);
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE LBAN_BAN_ALT_REF exists
--
          IF (p1.LBAN_BAN_ALT_REF IS NOT NULL) THEN
--
           l_ban_ban_exists := NULL;
--
            OPEN chk_ban_exists(p1.LBAN_BAN_ALT_REF);
           FETCH chk_ban_exists INTO l_ban_ban_exists;
           CLOSE chk_ban_exists;
--
           IF (l_ban_ban_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',627);
           END IF;
--
          END IF;
--
--
-- ************************************************************************************
--
-- Now UPDATE the record count AND error code
--
          IF l_errors = 'F' THEN
           l_error_ind := 'Y';
          ELSE
             l_error_ind := 'N';
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
          set_record_status_flag(l_id,l_errors);
--
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
    COMMIT;
--
    fsc_utils.proc_END;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       LBAN_DLB_BATCH_ID,
       LBAN_DL_SEQNO,
       LBAN_DL_LOAD_STATUS,
       LBAN_ALT_REF
  FROM dl_hcs_business_actions
 WHERE lban_dlb_batch_id   = p_batch_id
   AND lban_dl_load_status = 'C';
--
-- ************************************************************************************
--
-- Constants for process_summary
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'DELETE';
ct       		VARCHAR2(30) := 'DL_HCS_BUSINESS_ACTIONS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id                 	ROWID;
l_an_tab 		VARCHAR2(1);
--
-- Other Variables
--
i                 	INTEGER := 0;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hcs_business_actions.dataload_delete');
    fsc_utils.debug_message( 's_dl_hcs_business_actions.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lban_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
          DELETE 
            FROM business_actions
           WHERE ban_reference = p1.lban_alt_ref;
--
--
-- keep a count of the rows processed and commit after every 1000
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ROLLBACK TO SP1;
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
                  set_record_status_flag(l_id,'C');
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
    COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUSINESS_ACTIONS');
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
            RAISE;
--
END dataload_delete;
--
--
END s_dl_hcs_business_actions;
/
