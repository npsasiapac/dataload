CREATE OR REPLACE PACKAGE BODY HOU.s_dl_hra_subsidy_income_items
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  1.0     5.15.0    VS   06-JAN-2009  Initial Creation.
--
--  1.1     5.15.0    VS   05-MAY-2009  Disable trigger SUIT_BR_IU on
--                                      CREATE process to stop the supplied
--                                      created_by and created_date from
--                                      being over written.
--
--  1.2     5.15.0    VS   18-MAY-2009  Use the supplied Subsidy Review Legacy
--                                      Reference to link it to right subsidy
--                                      review.
--
--  1.3     5.15.0    VS   18-MAY-2009  Remove chk_hop_exists check as it is
--                                      stopping non main tenant household persons
--                                      from loading.
--
--  1.4     5.15.0    VS   07-JUN-2010  Defect 4730 - Validation against subsidy
--                                      income type mappings added
--
--  1.5     5.15.0    MT   18-MAY-2011  V16: Add support for par_per_alt_ref
--  1.6     6.4.0     PH   29-JUN-2011  Amended code to get surv_refno
--                                      Removed disable/enable of trigger and
--                                      perform a post insert update instead
--  1.7     6.5.0     PH   24-FEB-2012  Legacy Ref now held in subsidy
--                                      reviews, removed call to dl table
--
-- 1.8     6.8.0    MM    14-OCT-2013  Amended lsuit_suar_subp_asca_cod and
--                                     lsuit_suar_code so that it checks
--                                      assessment_categories, and not the
--                                     FRV
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
  UPDATE dl_hra_subsidy_income_items
  SET    lsuit_dl_load_status = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_subsidy_income_items');
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
       LSUIT_DLB_BATCH_ID,
       LSUIT_DL_SEQNO,
       LSUIT_DL_LOAD_STATUS,
       LSUIT_SURV_LEGACY_REF,
       LSUIT_PAR_REFNO,
       LSUIT_PAR_PER_ALT_REF,
       LSUIT_HSIT_CODE,
       LSUIT_ELIGIBILITY_AMOUNT,
       LSUIT_SUBSIDY_CALC_AMOUNT,
       LSUIT_OVERRIDDEN_INCOME_IND,
       NVL(LSUIT_CREATED_DATE, SYSDATE)  LSUIT_CREATED_DATE,
       NVL(LSUIT_CREATED_BY, 'DATALOAD') LSUIT_CREATED_BY,
       LSUIT_RENT_PAYABLE_CONTRIB,
       LSUIT_SUAR_CODE,
       LSUIT_PERCENTAGE,
       LSUIT_SUAR_SUBP_SEQ,
       LSUIT_SUAR_SUBP_ASCA_CODE,
       LSUIT_SIOR_CODE
  FROM dl_hra_subsidy_income_items
 WHERE lsuit_dlb_batch_id    = p_batch_id
   AND lsuit_dl_load_status  = 'V';
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR get_par_refno(p_par_refno NUMBER)
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_refno;

CURSOR get_par_refno_alt(p_par_per_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
CURSOR get_surv_refno(p_surv_legacy_ref VARCHAR2)
IS
SELECT surv_refno
  FROM subsidy_reviews
 WHERE surv_legacy_ref = p_surv_legacy_ref ;

--
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_SUBSIDY_INCOME_ITEMS';
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
l_tcy_refno                NUMBER(10);
l_suap_refno               NUMBER(10);
l_surv_refno               NUMBER(10);
l_par_refno                NUMBER(8);
--
--
-- ***********************************************************************
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_income_items.dataload_create');
    fsc_utils.debug_message('s_dl_hra_subsidy_income_items.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lsuit_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
-- Main processing
--
--
--
          l_par_refno  := NULL;
          l_surv_refno := NULL;
--
   if p1.lsuit_par_refno is not null then
          OPEN get_par_refno(p1.lsuit_par_refno);
          FETCH get_par_refno INTO l_par_refno;
          CLOSE get_par_refno;
   elsif p1.lsuit_par_per_alt_ref is not null then
          OPEN get_par_refno_alt(p1.lsuit_par_per_alt_ref);
          FETCH get_par_refno_alt INTO l_par_refno;
          CLOSE get_par_refno_alt;
   end if;
--
          OPEN get_surv_refno(p1.lsuit_surv_legacy_ref);
          FETCH get_surv_refno INTO l_surv_refno;
          CLOSE get_surv_refno;
--
-- Insert int relevent table
--
          INSERT /* +APPEND */ INTO subsidy_income_items(SUIT_SURV_REFNO,
                                           SUIT_HSIT_CODE,
                                           SUIT_PAR_REFNO,
                                           SUIT_ELIGIBILITY_CALC_AMOUNT,
                                           SUIT_SUBSIDY_CALC_AMOUNT,
                                           SUIT_OVERRIDDEN_INCOME_IND,
                                           SUIT_CREATED_DATE,
                                           SUIT_CREATED_BY,
                                           SUIT_RENT_PAYABLE_CONTRIB,
                                           SUIT_SUAR_CODE,
                                           SUIT_PERCENTAGE,
                                           SUIT_SUAR_SUBP_SEQ,
                                           SUIT_SUAR_SUBP_ASCA_CODE,
                                           SUIT_SIOR_CODE
                                          )
--
                                    VALUES(l_surv_refno,
                                           p1.lsuit_hsit_code,
                                           l_par_refno,
                                           p1.lsuit_eligibility_amount,
                                           p1.lsuit_subsidy_calc_amount,
                                           p1.lsuit_overridden_income_ind,
                                           p1.lsuit_created_date,
                                           p1.lsuit_created_by,
                                           p1.lsuit_rent_payable_contrib,
                                           p1.lsuit_suar_code,
                                           p1.lsuit_percentage,
                                           p1.lsuit_suar_subp_seq,
                                           p1.lsuit_suar_subp_asca_code,
                                           p1.lsuit_sior_code
                                          );
--
         UPDATE   subsidy_income_items
            SET   suit_created_date = p1.lsuit_created_date
                , suit_created_by   = p1.lsuit_created_by
          WHERE   suit_surv_refno   = l_surv_refno
            AND   suit_par_refno    = l_par_refno
            AND   suit_hsit_code    = p1.lsuit_hsit_code;
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
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_INCOME_ITEMS');
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
       LSUIT_DLB_BATCH_ID,
       LSUIT_DL_SEQNO,
       LSUIT_DL_LOAD_STATUS,
       LSUIT_SURV_LEGACY_REF,
       LSUIT_PAR_REFNO,
       LSUIT_PAR_PER_ALT_REF,
       LSUIT_HSIT_CODE,
       LSUIT_ELIGIBILITY_AMOUNT,
       LSUIT_SUBSIDY_CALC_AMOUNT,
       LSUIT_OVERRIDDEN_INCOME_IND,
       LSUIT_CREATED_DATE,
       LSUIT_CREATED_BY,
       LSUIT_RENT_PAYABLE_CONTRIB,
       LSUIT_SUAR_CODE,
       LSUIT_PERCENTAGE,
       LSUIT_SUAR_SUBP_SEQ,
       LSUIT_SUAR_SUBP_ASCA_CODE,
       LSUIT_SIOR_CODE
  FROM dl_hra_subsidy_income_items
 WHERE lsuit_dlb_batch_id    = p_batch_id
   AND lsuit_dl_load_status in ('L','F','O');
--
-- ***********************************************************************
--
-- Additional Cursors
--
--
CURSOR chk_surv_exists(p_surv_legacy_ref VARCHAR2)
IS
SELECT 'X'
  FROM subsidy_reviews
 WHERE surv_legacy_ref = p_surv_legacy_ref;
--
--
-- ***********************************************************************
--
CURSOR chk_par_refno(p_par_refno NUMBER)
IS
SELECT par_refno
  FROM parties
 WHERE par_refno = p_par_refno;
--
CURSOR chk_par_refno_alt(p_par_per_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref;
--
--
-- ***********************************************************************
--
CURSOR chk_suit_exists(p_surv_legacy_ref VARCHAR2,
                       p_par_refno       NUMBER,
                       p_hsit_code       VARCHAR2)
IS
SELECT 'X'
  FROM subsidy_income_items
      ,dl_hra_subsidy_reviews
 WHERE suit_surv_refno  = lsurv_refno
   AND lsurv_legacy_ref = p_surv_legacy_ref
   AND suit_par_refno   = p_par_refno
   AND suit_hsit_code   = p_hsit_code;
--
-- ***********************************************************************
--
--
CURSOR chk_sub_ass_rule_exists(p_suar_code           VARCHAR2,
                               p_suar_subp_asca_code VARCHAR2,
                               p_suar_subp_seq       NUMBER)
IS
SELECT 'X'
  FROM subsidy_assessment_rules
 WHERE suar_code           = p_suar_code
   AND suar_subp_asca_code = p_suar_subp_asca_code
   AND suar_subp_seq       = p_suar_subp_seq;
--
-- ***********************************************************************
--
--
CURSOR chk_sitm_exists(p_sitm_hsit_code      VARCHAR2,
                       p_sitm_subp_asca_code VARCHAR2,
                       p_sitm_subp_seq       NUMBER)
IS
SELECT DISTINCT 'X'
  FROM subsidy_income_type_mappings
 WHERE sitm_hsit_code          = p_sitm_hsit_code
   AND sitm_subp_hrv_asca_code = p_sitm_subp_asca_code
   AND sitm_subp_seq           = p_sitm_subp_seq;
--
-- ***********************************************************************
--
--
CURSOR chk_subp_exists(p_subp_asca_code VARCHAR2,
                       p_subp_seq       NUMBER)
IS
SELECT 'X'
  FROM subsidy_policies
 WHERE subp_hrv_asca_code = p_subp_asca_code
   AND subp_seq           = p_subp_seq;

-- ***********************************************************************
--
-- Moved check from First Ref Values to SUBSIDY_ASSESSMENT_CATEGORIES, as
-- cofiguration of the data has changed
--
CURSOR chk_asca_exists(cp_code VARCHAR2)
IS
SELECT 'X'
  FROM SUBSIDY_ASSESSMENT_CATEGORIES
 WHERE ASCA_CODE = cp_code;
 
--
--
--
-- ***********************************************************************
--
--
--CURSOR chk_hop_exists(p_tcy_refno  NUMBER,
--                      p_par_refno  NUMBER,
--                      p_start_date DATE)
--IS
--SELECT 'X'
--  FROM household_persons a,
--       tenancy_instances b
-- WHERE a.hop_par_refno = p_par_refno
--   AND b.tin_hop_refno = a.hop_refno
--   AND b.tin_tcy_refno = p_tcy_refno
--   AND p_start_date BETWEEN b.tin_start_date
--                    AND NVL(b.tin_end_date, p_start_date +1);
--
--
-- ***********************************************************************
--

-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_INCOME_ITEMS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists             VARCHAR2(1);
l_asca_exists         VARCHAR2(1);
l_par_refno             NUMBER(8);
l_surv_exists           VARCHAr2(1);
l_hsit_code_exists      VARCHAR2(1);
l_suit_exists           VARCHAR2(1);
l_sub_ass_rule_exists   VARCHAR2(1);
l_hop_exists            VARCHAR2(1);
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
i                    INTEGER :=0;
l_sitm_exists           VARCHAR2(1);
l_subp_exists           VARCHAR2(1);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_income_items.dataload_validate');
    fsc_utils.debug_message('s_dl_hra_subsidy_income_items.dataload_validate',3);
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
          cs   := p1.lsuit_dl_seqno;
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
-- Check Subsidy Review Legacy Reference has been supplied and is valid
--
--
          IF (p1.lsuit_surv_legacy_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',274);
--
          ELSE
--
             l_surv_exists := NULL;
--
              OPEN chk_surv_exists (p1.lsuit_surv_legacy_ref);
             FETCH chk_surv_exists INTO l_surv_exists;
             CLOSE chk_surv_exists;
--
             IF (l_surv_exists IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',72);
             END IF;
--
          END IF;
--
-- ********************************************
--
-- Check One Person Ref, and only one, is supplied.  And it is valid.
--
--  par_per_alt_ref
          IF (p1.lsuit_par_refno IS NULL) and  (p1.lsuit_par_per_alt_ref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',73);
          elsIF (p1.lsuit_par_refno IS NOT NULL) and  (p1.lsuit_par_per_alt_ref IS NOT NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',73);
          ELSE
--
             l_par_refno := NULL;
--
             IF p1.lsuit_par_refno is not null then
               OPEN chk_par_refno(p1.lsuit_par_refno);
               FETCH chk_par_refno INTO l_par_refno;
               CLOSE chk_par_refno;
             ELSE
               OPEN chk_par_refno_alt(p1.lsuit_par_per_alt_ref);
               FETCH chk_par_refno_alt INTO l_par_refno;
               CLOSE chk_par_refno_alt;
             END IF;

             IF (l_par_refno IS NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',868);
             END IF;
          END IF;
--
--
-- ********************************************
--
-- Subsidy Income Type Code SUIT_HSIT_CODE has been supplied and is valid
--
          l_hsit_code_exists := NULL;
--
          IF (p1.lsuit_hsit_code IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',74);
--
          ELSIF (NOT s_dl_hem_utils.exists_frv('SUBINCTYPE',p1.lsuit_hsit_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',75);
              l_hsit_code_exists := 'N';
--
          END IF;
--
--
-- ********************************************
--
-- Check Subsidy Income Eligibility Amount SUIT_ELIGIBILITY_AMOUNT has been supplied
--
--
          IF (p1.lsuit_eligibility_amount IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',76);
          END IF;
--
--
-- ********************************************
--
-- Check Income Subsidy Amount SUIT_INCOME_CALC_AMOUNT has been supplied
--
--
--          IF (p1.lsuit_incom_calc_amount IS NULL) THEN
--           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',77);
--          END IF;
--
--
-- ********************************************
--
-- Check Income Overidden Indicator SUIT_OVERRIDDEN_INCOME_IND has been supplied and is valid
--
--
          IF (p1.lsuit_overridden_income_ind IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',78);
--
          ELSIF (p1.lsuit_overridden_income_ind NOT IN ('Y','N')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',79);
--
          END IF;
--
-- ********************************************
--
-- Check to see if subsidy income items record already exists for the
-- subsidy review/par refno/income type code supplied.
--
--
          IF (    l_surv_exists      IS NOT NULL
              AND l_par_refno        IS NOT NULL
              AND l_hsit_code_exists IS NOT NULL) THEN
--
             l_suit_exists := NULL;
--
             OPEN  chk_suit_exists(p1.lsuit_surv_legacy_ref,l_par_refno, p1.lsuit_hsit_code);
             FETCH chk_suit_exists INTO l_suit_exists;
             CLOSE chk_suit_exists;
--
             IF (l_suit_exists IS NOT NULL) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',276);
             END IF;
--
          END IF;
--
--
-- ********************************************
--
-- Check a record exists on Subsidy Assessment Rules table
-- for the Subsidy Assessment Rule LSUIT_SUAR_CODE,
-- Subsidy Policy Category LSUIT_SUAR_SUBP_ASCA_CODE
-- and Sequence LSUIT_SUAR_SUBP_SEQ supplied.
--
          IF (    p1.lsuit_suar_code           IS NOT NULL
              AND p1.lsuit_suar_subp_asca_code IS NOT NULL
              AND p1.lsuit_suar_subp_seq       IS NOT NULL) THEN
--
           l_sub_ass_rule_exists := NULL;
--
            OPEN chk_sub_ass_rule_exists(p1.lsuit_suar_code, p1.lsuit_suar_subp_asca_code,p1.lsuit_suar_subp_seq);
           FETCH chk_sub_ass_rule_exists INTO l_sub_ass_rule_exists;
           CLOSE chk_sub_ass_rule_exists;
--
           IF (l_sub_ass_rule_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',80);
           END IF;
--
          END IF;
--
--
-- ********************************************
--
-- Check a record exists on Subsidy Income Type Mappings table
--
          IF (    p1.lsuit_hsit_code           IS NOT NULL
              AND p1.lsuit_suar_subp_asca_code IS NOT NULL
              AND p1.lsuit_suar_subp_seq       IS NOT NULL) THEN
--
           l_sitm_exists := NULL;
--
            OPEN chk_sitm_exists(p1.lsuit_hsit_code, p1.lsuit_suar_subp_asca_code,p1.lsuit_suar_subp_seq);
           FETCH chk_sitm_exists INTO l_sitm_exists;
           CLOSE chk_sitm_exists;
--
           IF (l_sitm_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',375);
           END IF;
--
          END IF;
--
--
-- ********************************************
--
-- Check a record exists on Subsidy Policies table
--
          IF (    p1.lsuit_suar_subp_asca_code IS NOT NULL
              AND p1.lsuit_suar_subp_seq       IS NOT NULL) THEN
--
           l_subp_exists := NULL;
--
            OPEN chk_subp_exists(p1.lsuit_suar_subp_asca_code,p1.lsuit_suar_subp_seq);
           FETCH chk_subp_exists INTO l_subp_exists;
           CLOSE chk_subp_exists;
--
           IF (l_subp_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',376);
           END IF;
--
          END IF;
--
--
-- ********************************************
--
-- All reference values supplied are valid
--
-- Subsidy Assessment Code
--
--          IF (p1.lsuit_suar_code IS NOT NULL) THEN
----
--           IF (NOT s_dl_hem_utils.exists_frv('SUBASSCAT',p1.lsuit_suar_code,'Y')) THEN
--              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',82);
--           END IF;
----
--          END IF;
----

        IF (p1.lsuit_suar_code IS NOT NULL) THEN
           
--
          OPEN chk_asca_exists(p1.lsuit_suar_code);
         FETCH chk_asca_exists INTO l_asca_exists;
         IF chk_asca_exists%NOTFOUND
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',82);
         END IF;
         CLOSE chk_asca_exists;

        END IF;


-- Subsidy Policy Category Code
--
        IF (p1.lsuit_suar_subp_asca_code IS NOT NULL) THEN
           
--
          OPEN chk_asca_exists(p1.lsuit_suar_subp_asca_code);
         FETCH chk_asca_exists INTO l_asca_exists;
         IF chk_asca_exists%NOTFOUND
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',62);
         END IF;
         CLOSE chk_asca_exists;

        END IF;

--
-- Subsidy Override Reason
--
          IF (p1.lsuit_sior_code IS NOT NULL) THEN
--
           IF (NOT s_dl_hem_utils.exists_frv('SUB_INC_ORIDE_RSN',p1.lsuit_sior_code,'Y')) THEN
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',83);
           END IF;
--
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
PROCEDURE dataload_delete(p_batch_id       IN VARCHAR2,
                          p_date           IN date) IS
--
CURSOR c1 is
SELECT rowid REC_ROWID,
       LSUIT_DLB_BATCH_ID,
       LSUIT_DL_SEQNO,
       LSUIT_DL_LOAD_STATUS,
       LSUIT_PAR_REFNO,
       LSUIT_PAR_PER_ALT_REF,
       LSUIT_SURV_LEGACY_REF,
       LSUIT_HSIT_CODE
  FROM dl_hra_subsidy_income_items
 WHERE lsuit_dlb_batch_id   = p_batch_id
   AND lsuit_dl_load_status = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
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
ct       VARCHAR2(30) := 'DL_HRA_SUBSIDY_INCOME_ITEMS';
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
l_surv_refno     NUMBER(10);
--
-- ***********************************************************************
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hra_subsidy_income_items.dataload_delete');
    fsc_utils.debug_message('s_dl_hra_subsidy_income_items.dataload_delete',3 );
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
          cs   := p1.lsuit_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i + 1;
          l_surv_refno := NULL;
--
          OPEN get_surv_refno(p1.lsuit_surv_legacy_ref);
          FETCH get_surv_refno INTO l_surv_refno;
          CLOSE get_surv_refno;
--
--
-- Delete from table
--
          DELETE
            FROM subsidy_income_items
           WHERE suit_surv_refno = l_surv_refno
             AND suit_hsit_code  = p1.lsuit_hsit_code
             AND suit_par_refno  = (select par_refno from parties
                                    where par_refno = decode(p1.lsuit_par_refno,null,par_refno,p1.lsuit_par_refno)
                                     and par_per_alt_ref = decode(p1.lsuit_par_per_alt_ref,null,par_per_alt_ref,p1.lsuit_par_per_alt_ref)
                                    );
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
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('SUBSIDY_INCOME_ITEMS');
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
END s_dl_hra_subsidy_income_items;
/