CREATE OR REPLACE PACKAGE BODY s_dl_mad_contact_details
AS
--
-- ************************************************************************************
--
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION  DB VER   WHO  WHEN         WHY
--
--  1.0      6.3.0    VRS  03-FEB-2011  Initial Release
--
--  2.0      6.7.0    VRS  20-MAR-2013  Addition of question to end existing contact detail
--                                      record of the same contact method being loaded.
--  3.0      6.10     AJ   04-MAR-2013  Amended to MAD (Multi Area Dataload) dataload area
--  3.1      6.10     AJ   01-JUN-2015  Show Errors added at bottom
--  3.2      6.13     AJ   13-MAY-2016  ORG Reference Type for Organisation added create/validate
--  3.3      6.13     AJ   24-MAY-2016  1)ORG Ref Type for Organisation added delete
--                                      2)OCC Ref Type for Organisation Contact Contact Details
--                                        for create added
--  3.4      6.13     AJ   16-JUN-2016  Further Changes to ORG Ref Type for Organisation
--                                      Mandatory field checks for Legacy Reference
--                                      Validation checks added for contact method options
--  3.5      6.13     AJ   15-JUL-2016  1)Validation and Delete added for OCC Ref Type
--                                      2)OCC Ref Type for Organisation Contact Contact Details
--                                        for create added
--  3.6      6.13     AJ   10-AUG-2016  Create Validate and Delete added for OC2 Contact Details for
--                                      Organisation Contact but using par_refno for Organisation
--  3.7      6.15     AJ   09-MAR-2017  Added Organisations Contacts extra fields for new create
--                                      into (organisations_cotacts) and new batch question
--  3.8      6.15     AJ   28-MAR-2017  1) Amended Organisations Contacts create now fields not batch
--                                      question to determine what to do lcde_occ_create or lcde_oco_update
--                                      2) Delete for OCC and OC2 amended during testing update wrong
--                                      3) Tested changes appears to work
--  3.9      6.18     PL   19-AUG-2019  Set Telephone number field
--  3.10     6.20     PL   26-OCT-2020  Current ind on org.
--  declare package variables AND constants
--
-- ************************************************************************************
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_mad_contact_details
     SET lcde_dl_load_status = p_status
   WHERE rowid               = p_rowid;
  --
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_mad_contact_details');
          RAISE;
  --
END set_record_status_flag;
--
-- ************************************************************************************
--
PROCEDURE dataload_create(p_batch_id  IN VARCHAR2,
                          p_date      IN DATE)
AS
--
CURSOR c1
IS
SELECT rowid rec_rowid,
       lcde_dlb_batch_id,
       lcde_dl_seqno,
       lcde_dl_load_status,
       lcde_legacy_ref,
       lcde_legacy_type,
       lcde_start_date,
       NVL(lcde_created_date,SYSDATE)  lcde_created_date,
       NVL(lcde_created_by,'DATALOAD') lcde_created_by,
       lcde_contact_value,
       lcde_frv_cme_code,
       lcde_contact_name,
       lcde_end_date,
       lcde_precedence,
       lcde_frv_comm_pref_code,
       lcde_allow_texts,
       lcde_secondary_ref,
       lcde_comments,
       lcde_oco_forename,
       lcde_oco_surname,
       lcde_oco_frv_title,
       lcde_oco_update,
       lcde_oco_create,
       lcde_oco_start_date,
       lcde_oco_end_date,
       lcde_oco_signatory_ind,
       lcde_oco_frv_ocr_code,
       lcde_oco_frv_opl_code,
       lcde_oco_comments,
       lcde_oco_refno,
       lcde_oco_created,
       lcde_refno
  FROM dl_mad_contact_details
 WHERE lcde_dlb_batch_id   = p_batch_id
   AND lcde_dl_load_status = 'V';
--
-- *************************************
CURSOR c_pro_refno (p_pro_propref VARCHAR2)
IS
SELECT pro_refno
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- *************************************
CURSOR c_par_refno (p_par_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- *************************************
CURSOR c_bde_refno(p_bde_bank_name   VARCHAR2,
                   p_bde_branch_name VARCHAR2)
IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name   = p_bde_bank_name
   AND bde_branch_name = p_bde_branch_name;
--
-- *************************************
CURSOR c_srq_refno (p_srq_alt_ref VARCHAR2)
IS
SELECT srq_no
  FROM service_requests
 WHERE srq_legacy_refno = p_srq_alt_ref;
--
-- *************************************
CURSOR c_org_refno(p_org_short_name   VARCHAR2
                  ,p_org_frv_oty_code VARCHAR2) IS
SELECT par_refno
  FROM parties
 WHERE par_org_short_name = p_org_short_name
   AND par_org_frv_oty_code = p_org_frv_oty_code
   AND par_type = 'ORG'
   AND par_org_current_ind = 'Y';  --3.20
--
-- *************************************
CURSOR c_org_ct_refno(p_org_short_name   VARCHAR2
                     ,p_org_frv_oty_code VARCHAR2
                     ,p_oco_forename     VARCHAR2
                     ,p_oco_surname      VARCHAR2) IS
SELECT oc.oco_refno, oc.oco_par_refno
  FROM parties p
      ,organisation_contacts oc
 WHERE p.par_org_short_name = p_org_short_name
   AND p.par_org_frv_oty_code = p_org_frv_oty_code
   AND p.par_type = 'ORG'
   AND p.par_refno = oc.oco_par_refno
   AND oc.oco_forename = p_oco_forename
   AND oc.oco_surname = p_oco_surname
   AND p.par_org_current_ind = 'Y';--3.20
--
--
-- *************************************
CURSOR c_org_ct2_refno(p_org_refno    VARCHAR2
                      ,p_oco_forename VARCHAR2
                      ,p_oco_surname  VARCHAR2) IS
SELECT oco_refno
  FROM organisation_contacts oc
 WHERE oc.oco_par_refno = p_org_refno
   AND oc.oco_forename = p_oco_forename
   AND oc.oco_surname = p_oco_surname;
--
-- *************************************
CURSOR get_cde_refno
IS
SELECT cde_refno.nextval
  FROM dual;
--
-- *************************************
CURSOR get_oco_refno
IS
SELECT oco_refno_seq.nextval
  FROM dual;
--
-- *************************************
CURSOR get_org_ct_refno(p_org_short_name   VARCHAR2
                       ,p_org_frv_oty_code VARCHAR2) IS
SELECT p.par_refno
  FROM parties p
 WHERE p.par_org_short_name = p_org_short_name
   AND p.par_org_frv_oty_code = p_org_frv_oty_code
   AND p.par_type = 'ORG'
   AND p.par_org_current_ind = 'Y';--3.20
--
--
-- *************************************
CURSOR get_reusable_refno
IS
SELECT reusable_refno_seq.nextval
FROM dual;
--
-- *************************************
CURSOR ckh_ct_update(p_signatory_ind VARCHAR2
                    ,p_start_date    DATE
                    ,p_frv_ocr_code  VARCHAR2
                    ,p_frv_opl_code  VARCHAR2
                    ,p_frv_title     VARCHAR2
                    ,p_end_date      DATE
                    ,p_comments      VARCHAR2
                    ,p_oco_refno     NUMBER)  IS
SELECT 'X'
FROM organisation_contacts
WHERE oco_refno = p_oco_refno
  AND oco_signatory_ind = p_signatory_ind
  AND trunc(oco_start_date) = p_start_date
  AND nvl(oco_frv_ocr_code,'A') = nvl(p_frv_ocr_code,'A')
  AND nvl(oco_frv_opl_code,'A') = nvl(p_frv_opl_code,'A')
  AND nvl(oco_frv_title,'A') = nvl(p_frv_title,'A')
  AND nvl(oco_end_date,'01-DEC-2999') = nvl(p_end_date,'01-DEC-2999')
  AND nvl(oco_comments,'A') = nvl(p_comments,'A');
--
-- *************************************
-- Constants FOR process_summary
--
cb          VARCHAR2(30);
cd          DATE;
cp          VARCHAR2(30) := 'CREATE';
ct          VARCHAR2(30) := 'DL_MAD_CONTACT_DETAILS';
cs          INTEGER;
ce          VARCHAR2(200);
l_id        ROWID;
l_an_tab    VARCHAR2(1);
--
-- Other variables
i                 INTEGER:=0;
l_pro_refno       NUMBER(10);
l_aun_code        VARCHAR2(20);
l_par_refno       NUMBER(8);
l_bde_refno       NUMBER(10);
l_cos_code        VARCHAR2(20);
l_srq_no          NUMBER(10);
l_peg_code        VARCHAR2(10);
l_cse_contact     VARCHAR2(30);
l_oco_refno       NUMBER(8);
l_cde_refno       NUMBER(10);
l_reusable_refno  NUMBER(10);
l_answer          VARCHAR2(1);  -- end old
l_org_refno       NUMBER(8);
l_ct_update       VARCHAR2(1);
--
BEGIN
--
 fsc_utils.proc_start('s_dl_mad_contact_details.dataload_create');
 fsc_utils.debug_message('s_dl_mad_contact_details.dataload_create',3);
--
 cb := p_batch_id;
 cd := p_date;
 l_answer  := NULL;
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the batch questions
--
 l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs   := p1.lcde_dl_seqno;
  l_id := p1.rec_rowid;
--
  SAVEPOINT SP1;
--
  l_pro_refno   := NULL;
  l_aun_code    := NULL;
  l_par_refno   := NULL;
  l_bde_refno   := NULL;
  l_cos_code    := NULL;
  l_srq_no      := NULL;
  l_peg_code    := NULL;
  l_cse_contact := NULL;
  l_oco_refno   := NULL;
  l_cde_refno   := NULL;
  l_reusable_refno   := NULL;
  l_org_refno   := NULL;
  l_ct_update   := NULL;
--
-- Get the Relevant Object
--
  IF (p1.lcde_legacy_type = 'PRO')THEN
--
   OPEN c_pro_refno(p1.lcde_legacy_ref);
   FETCH c_pro_refno INTO l_pro_refno;
   CLOSE c_pro_refno;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_pro_refno = l_pro_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'AUN') THEN
--
   l_aun_code := p1.lcde_legacy_ref;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_aun_code = l_aun_code
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'PAR') THEN
--
   OPEN c_par_refno(p1.lcde_legacy_ref);
   FETCH c_par_refno INTO l_par_refno;
   CLOSE c_par_refno;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_par_refno = l_par_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'PRF') THEN
--
   l_par_refno := p1.lcde_legacy_ref;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_par_refno = l_par_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'BDE') THEN
--
   OPEN c_bde_refno(p1.lcde_legacy_ref,p1.lcde_secondary_ref);
   FETCH c_bde_refno INTO l_bde_refno;
   CLOSE c_bde_refno;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_bde_refno = l_bde_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'COS') THEN
--
   l_cos_code    := p1.lcde_legacy_ref;
   l_cse_contact := p1.lcde_secondary_ref;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_cos_code = l_cos_code
   AND NVL(cde_cse_contact, 'XYZ') = NVL(l_cse_contact,'XYZ')
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'SRQ') THEN
--
   OPEN c_srq_refno(p1.lcde_legacy_ref);
   FETCH c_srq_refno INTO l_srq_no;
   CLOSE c_srq_refno;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_srq_no = l_srq_no
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'PEG') THEN
--
   l_peg_code := p1.lcde_legacy_ref;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_peg_code = l_peg_code
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'ORG') THEN
--
   OPEN c_org_refno(p1.lcde_legacy_ref,p1.lcde_secondary_ref);
   FETCH c_org_refno INTO l_par_refno;
   CLOSE c_org_refno;
--
   IF nvl(l_answer,'N') = 'Y' THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_par_refno = l_par_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'OCC') THEN
--
   OPEN c_org_ct_refno(p1.lcde_legacy_ref,p1.lcde_secondary_ref,
                       p1.lcde_oco_forename,p1.lcde_oco_surname);
   FETCH c_org_ct_refno INTO l_oco_refno,l_org_refno;
   CLOSE c_org_ct_refno;
--
   IF (nvl(l_answer,'N') = 'Y' AND l_oco_refno IS NOT NULL) THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_oco_refno = l_oco_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
   IF (l_oco_refno IS NULL) THEN
--
    OPEN get_org_ct_refno(p1.lcde_legacy_ref,p1.lcde_secondary_ref);
    FETCH get_org_ct_refno INTO l_org_refno;
    CLOSE get_org_ct_refno;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'OC2') THEN
--
   OPEN c_org_ct2_refno(p1.lcde_legacy_ref
                       ,p1.lcde_oco_forename
                       ,p1.lcde_oco_surname);
   FETCH c_org_ct2_refno INTO l_oco_refno;
   CLOSE c_org_ct2_refno;
--
   IF (nvl(l_answer,'N') = 'Y' AND l_oco_refno IS NOT NULL) THEN
--
    UPDATE contact_details
       SET cde_end_date = p1.lcde_start_date - 1
     WHERE cde_oco_refno = l_oco_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date IS NULL;
--
   END IF;
--
   l_org_refno := p1.lcde_legacy_ref;
--
  END IF;
--
-- insert into Organisation Contacts record where Organisation Contact
-- not found for OCC and OC2 record types only needed before Contact details
-- created so this should create the Organisation Contact on the 1st record
-- and subsequently find it on the others for the same contact
--
  IF (l_oco_refno IS NULL                AND
      p1.lcde_oco_create = 'Y'           AND
      p1.lcde_legacy_type IN('OCC','OC2')   ) THEN
--
   OPEN get_reusable_refno;
   FETCH get_reusable_refno INTO l_reusable_refno;
   CLOSE get_reusable_refno;
--
   OPEN get_oco_refno;
   FETCH get_oco_refno INTO l_oco_refno;
   CLOSE get_oco_refno;
--
   INSERT INTO organisation_contacts
              (oco_refno,
               oco_par_refno,
               oco_forename,
               oco_surname,
               oco_signatory_ind,
               oco_start_date,
               oco_reusable_refno,
               oco_created_date,
               oco_created_by,
               oco_frv_ocr_code,
               oco_frv_opl_code,
               oco_frv_title,
               oco_end_date,
               oco_comments
              )
        VALUES
              (l_oco_refno,
               l_org_refno,
               p1.lcde_oco_forename,
               p1.lcde_oco_surname,
               p1.lcde_oco_signatory_ind,
               p1.lcde_oco_start_date,
               l_reusable_refno,
               p1.lcde_created_date,
               p1.lcde_created_by,
               p1.lcde_oco_frv_ocr_code,
               p1.lcde_oco_frv_opl_code,
               p1.lcde_oco_frv_title,
               p1.lcde_oco_end_date,
               p1.lcde_oco_comments
              );
--
   UPDATE dl_mad_contact_details
      SET lcde_oco_created = 'Y'
    WHERE lcde_dlb_batch_id = p_batch_id
      AND rowid = p1.rec_rowid;
--
  END IF;
--
-- Update Organisation Contacts record where Organisation Contact
-- found for OCC and OC2 record types and details supplied are different
-- and lcde_oco_update is set to Y first check if fields different
--
  IF (l_oco_refno IS NOT NULL            AND
      p1.lcde_oco_update = 'Y'           AND
      p1.lcde_legacy_type IN('OCC','OC2')   ) THEN
--
   OPEN ckh_ct_update(p1.lcde_oco_signatory_ind
                     ,p1.lcde_oco_start_date
                     ,p1.lcde_oco_frv_ocr_code
                     ,p1.lcde_oco_frv_opl_code
                     ,p1.lcde_oco_frv_title
                     ,p1.lcde_oco_end_date
                     ,p1.lcde_oco_comments
                     ,l_oco_refno);
   FETCH ckh_ct_update INTO l_ct_update;
   CLOSE ckh_ct_update;
--
   IF (l_ct_update IS NULL) THEN
--
    UPDATE organisation_contacts
    SET oco_modified_date = sysdate
       ,oco_modified_by = 'DATALOAD'
       ,oco_signatory_ind = p1.lcde_oco_signatory_ind
       ,oco_start_date = p1.lcde_oco_start_date
       ,oco_frv_ocr_code = p1.lcde_oco_frv_ocr_code
       ,oco_frv_opl_code = p1.lcde_oco_frv_opl_code
       ,oco_frv_title = p1.lcde_oco_frv_title
       ,oco_end_date = p1.lcde_oco_end_date
       ,oco_comments = p1.lcde_oco_comments
    WHERE oco_refno = l_oco_refno;
--
   END IF;
  END IF;
--
-- Get cde_refno first and store for delete then do the insert into contact_details
--
  OPEN get_cde_refno;
  FETCH get_cde_refno INTO l_cde_refno;
  CLOSE get_cde_refno;
--
  INSERT INTO contact_details
             (cde_refno,
              cde_start_date,
              cde_created_date,
              cde_created_by,
              cde_contact_value,
              cde_frv_cme_code,
              cde_contact_name,
              cde_end_date,
              cde_pro_refno,
              cde_aun_code,
              cde_par_refno,
              cde_bde_refno,
              cde_cos_code,
              cde_cse_contact,
              cde_srq_no,
              cde_peg_code,
              cde_precedence,
              cde_frv_comm_pref_code,
              cde_allow_texts,
              cde_comments,
              cde_oco_refno,
        cde_telephone_no
             )
       VALUES
             (l_cde_refno,
              p1.lcde_start_date,
              p1.lcde_created_date,
              p1.lcde_created_by,
              p1.lcde_contact_value,
              p1.lcde_frv_cme_code,
              p1.lcde_contact_name,
              p1.lcde_end_date,
              l_pro_refno,
              l_aun_code,
              l_par_refno,
              l_bde_refno,
              l_cos_code,
              l_cse_contact,
              l_srq_no,
              l_peg_code,
              p1.lcde_precedence,
              p1.lcde_frv_comm_pref_code,
              p1.lcde_allow_texts,
              p1.lcde_comments,
              l_oco_refno,
        CASE WHEN p1.lcde_frv_cme_code = 'EMAIL' THEN '' ELSE p1.lcde_contact_value END --VSTS28044
             );
--
  IF (l_cde_refno IS NOT NULL) THEN
--
   UPDATE dl_mad_contact_details
      SET lcde_refno = l_cde_refno
    WHERE lcde_dlb_batch_id = p_batch_id
      AND rowid = p1.rec_rowid;
--
  END IF;
--
  IF (l_oco_refno IS NOT NULL) THEN
--
   UPDATE dl_mad_contact_details
      SET lcde_oco_refno = l_oco_refno
    WHERE lcde_dlb_batch_id = p_batch_id
      AND rowid = p1.rec_rowid;
--
  END IF;
--
-- Set the data load statuses
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed AND COMMIT after every 5000
--
  i := i+1;
--
  IF MOD(i,5000) =0 THEN
   COMMIT;
  END IF;
--
  EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  END;
--
 END LOOP;
--
COMMIT;
--
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ORGANISATION_CONTACTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('DL_MAD_CONTACT_DETAILS');
--
fsc_utils.proc_END;
--
EXCEPTION
 WHEN OTHERS THEN
 s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
 RAISE;
--
END dataload_create;
--
-- ************************************************************************************
--
PROCEDURE dataload_validate(p_batch_id  IN VARCHAR2,
                            p_date      IN DATE)
AS
--
CURSOR c1
IS
SELECT rowid rec_rowid,
       lcde_dlb_batch_id,
       lcde_dl_seqno,
       lcde_dl_load_status,
       lcde_legacy_ref,
       lcde_legacy_type,
       lcde_start_date,
       NVL(lcde_created_date,SYSDATE)  lcde_created_date,
       NVL(lcde_created_by,'DATALOAD') lcde_created_by,
       lcde_contact_value,
       lcde_frv_cme_code,
       lcde_contact_name,
       lcde_end_date,
       lcde_precedence,
       lcde_frv_comm_pref_code,
       lcde_allow_texts,
       lcde_secondary_ref,
       lcde_comments,
       lcde_oco_forename,
       lcde_oco_surname,
       lcde_oco_frv_title,
       lcde_oco_update,
       lcde_oco_create,
       lcde_oco_start_date,
       lcde_oco_end_date,
       lcde_oco_signatory_ind,
       lcde_oco_frv_ocr_code,
       lcde_oco_frv_opl_code,
       lcde_oco_comments,
       lcde_oco_refno,
       lcde_oco_created,
       lcde_refno
  FROM dl_mad_contact_details
 WHERE lcde_dlb_batch_id   = p_batch_id
   AND lcde_dl_load_status IN ('L','F','O');
--
-- *************************************
CURSOR c_pro_refno (p_pro_propref VARCHAR2)
IS
SELECT 'X'
  FROM properties
 WHERE pro_propref = p_pro_propref;
--
-- *************************************
CURSOR c_aun_code (p_aun_code VARCHAR2)
IS
SELECT 'X'
  FROM admin_units
 WHERE aun_code = p_aun_code;
--
-- *************************************
CURSOR c_par_refno (p_par_alt_ref VARCHAR2)
IS
SELECT 'X'
  FROM parties
 WHERE par_per_alt_ref = p_par_alt_ref;
--
-- *************************************
CURSOR c_prf_refno (p_par_refno VARCHAR2)
IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_par_refno;
--
-- *************************************
CURSOR c_bde_refno (p_bde_bank_name   VARCHAR2,
                    p_bde_branch_name VARCHAR2)
IS
SELECT 'X'
  FROM bank_details
 WHERE bde_bank_name   = p_bde_bank_name
   AND bde_branch_name = p_bde_branch_name;
--
-- *************************************
CURSOR c_cos_code (p_cos_code VARCHAR2)
IS
SELECT 'X'
  FROM contractor_sites
 WHERE cos_code = p_cos_code;
--
-- *************************************
CURSOR c_srq_refno (p_srq_alt_ref VARCHAR2)
IS
SELECT 'X'
  FROM service_requests
 WHERE srq_legacy_refno = p_srq_alt_ref;
--
-- *************************************
CURSOR c_peg_code (p_peg_code VARCHAR2)
IS
SELECT 'X'
  FROM people_groups
 WHERE peg_code = p_peg_code;
--
-- *************************************
CURSOR c_csc_code (p_cos_code     VARCHAR2,
                   p_contact_name VARCHAR2)
IS
SELECT 'X'
  FROM con_site_contacts
 WHERE csc_cos_code = p_cos_code
   AND csc_contact  = p_contact_name;
--
-- *************************************
CURSOR c_conm_code (p_conm_code VARCHAR2)
IS
SELECT 'X'
  FROM contact_methods
 WHERE conm_code = p_conm_code;
--
-- *************************************
CURSOR chk_org_refno(p_org_short_name   VARCHAR2
                    ,p_org_frv_oty_code VARCHAR2) IS
SELECT 'X'
  FROM parties
 WHERE par_org_short_name   = p_org_short_name
  AND  par_org_frv_oty_code = p_org_frv_oty_code;
--
-- *************************************
CURSOR chk_org_count(p_org_short_name   VARCHAR2
                    ,p_org_frv_oty_code VARCHAR2) IS
SELECT count(*)
  FROM parties
 WHERE par_org_short_name   = p_org_short_name
  AND  par_org_frv_oty_code = p_org_frv_oty_code
  AND  par_org_current_ind = 'Y';
--
-- *************************************
CURSOR chk_org_ct_refno(p_org_short_name   VARCHAR2
                       ,p_org_frv_oty_code VARCHAR2
                       ,p_oco_forename     VARCHAR2
                       ,p_oco_surname      VARCHAR2) IS
SELECT 'X'
  FROM parties p
      ,organisation_contacts oc
 WHERE p.par_org_short_name = p_org_short_name
  AND  p.par_org_frv_oty_code = p_org_frv_oty_code
  AND  p.par_refno = oc.oco_par_refno
  AND  oc.oco_forename = p_oco_forename
  AND  oc.oco_surname = p_oco_surname;
--
-- *************************************
CURSOR chk_org_ct_count(p_org_short_name   VARCHAR2
                       ,p_org_frv_oty_code VARCHAR2
                       ,p_oco_forename     VARCHAR2
                       ,p_oco_surname      VARCHAR2) IS
SELECT count(*)
  FROM parties p
      ,organisation_contacts oc
 WHERE p.par_org_short_name = p_org_short_name
  AND  p.par_org_frv_oty_code = p_org_frv_oty_code
  AND  p.par_refno = oc.oco_par_refno
  AND  oc.oco_forename = p_oco_forename
  AND  oc.oco_surname = p_oco_surname;
--
-- *************************************
CURSOR c_org_refno (p_org_refno VARCHAR2)
IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_org_refno
   AND par_type = 'ORG'
   AND par_org_current_ind = 'Y';--3.20
--
--
-- *************************************
CURSOR c_org_ct2_refno(p_org_refno    VARCHAR2
                      ,p_oco_forename VARCHAR2
                      ,p_oco_surname  VARCHAR2) IS
SELECT 'X'
  FROM organisation_contacts oc
 WHERE oc.oco_par_refno = p_org_refno
  AND  oc.oco_forename = p_oco_forename
  AND  oc.oco_surname = p_oco_surname;
--
-- *************************************
CURSOR c_conm(p_lconm_code VARCHAR2)  IS
SELECT conm_current_ind
      ,conm_code
      ,conm_digits_only_ind
      ,conm_value_min_length
      ,conm_value_max_length
      ,conm_spaces_allow_ind
  FROM contact_methods
 WHERE conm_code = p_lconm_code;
--
-- *************************************
CURSOR c_conm_spaces(p_dl_conm_value VARCHAR2
                    ,p_dlb_batch_id  VARCHAR2
                    ,p_dl_seqno      NUMBER
                    ,p_llconm_code   VARCHAR2)  IS
SELECT 'X'
  FROM dl_mad_contact_details
 WHERE lcde_dlb_batch_id = p_dlb_batch_id
  AND  lcde_dl_seqno = p_dl_seqno
  AND  lcde_frv_cme_code = p_llconm_code
  AND  lcde_contact_value = p_dl_conm_value
  AND  NVL(p_dl_conm_value,'') NOT LIKE '% %';
--
-- *************************************
-- constants FOR error process
--
cb           VARCHAR2(30);
cd           DATE;
cp           VARCHAR2(30) := 'VALIDATE';
ct           VARCHAR2(30) := 'DL_MAD_CONTACT_DETAILS';
cs           INTEGER;
ce           VARCHAR2(200);
l_id         ROWID;
--
-- Other Variables
--
l_cos_exists         VARCHAR2(1);
l_csc_exists         VARCHAR2(1);
l_pro_exists         VARCHAR2(1);
l_aun_exists         VARCHAR2(1);
l_par_exists         VARCHAR2(1);
l_bde_exists         VARCHAR2(1);
l_srq_exists         VARCHAR2(1);
l_peg_exists         VARCHAR2(1);
l_cme_exists         VARCHAR2(1);
l_org_exists         VARCHAR2(1);
l_org_count          INTEGER:=0;
l_occ_exists         VARCHAR2(1);
l_occ_count          INTEGER:=0;
i                    INTEGER:=0;
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
--
l_email_valid        VARCHAR2(1);
--
-- Variables for contact details check
--
l_whole_part_count   NUMBER(10);
l_fract_part_count   NUMBER(10);
li                   INTEGER := 0;
l_conm_code_out      VARCHAR2(10);
l_conm_cur           VARCHAR2(1);
l_conm_dig           VARCHAR2(1);
l_conm_min_len       NUMBER(3,0);
l_conm_max_len       NUMBER(3,0);
l_conm_spaces        VARCHAR2(1);
l_chk_conm_spaces    VARCHAR2(1);
l_char contact_details.cde_contact_value%TYPE;
--
BEGIN
--
 fsc_utils.proc_start('s_dl_mad_contact_details.dataload_validate');
 fsc_utils.debug_message('s_dl_mad_contact_details.dataload_validate',3);
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
  cs   := p1.lcde_dl_seqno;
  l_id := p1.rec_rowid;
  l_errors    := 'V';
  l_error_ind := 'N';
--
  l_pro_exists := NULL;
  l_cos_exists := NULL;
  l_csc_exists := NULL;
  l_aun_exists := NULL;
  l_par_exists := NULL;
  l_bde_exists := NULL;
  l_srq_exists := NULL;
  l_peg_exists := NULL;
  l_cme_exists := NULL;
  l_org_exists := NULL;
  l_occ_exists := NULL;
  l_conm_code_out   := NULL;
  l_conm_cur        := NULL;
  l_conm_dig        := NULL;
  l_conm_min_len    := NULL;
  l_conm_max_len    := NULL;
  l_conm_spaces     := NULL;
  l_chk_conm_spaces := NULL;
  l_char            := NULL;
--
-- *************************************
-- Check the Legacy Type is Valid
--
  IF p1.lcde_legacy_type NOT IN ('PRO', 'AUN', 'PAR', 'PRF', 'BDE', 'COS', 'SRQ', 'PEG'
                                ,'ORG','OCC','OC2') THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',691);
  END IF;
-- *************************************
-- Check legacy record exists
--
  IF (p1.lcde_legacy_type = 'PRO') THEN
--
   OPEN c_pro_refno(p1.lcde_legacy_ref);
   FETCH c_pro_refno INTO l_pro_exists;
--
   IF (c_pro_refno%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',30);
   END IF;
--
   CLOSE c_pro_refno;
--
  END IF;
-- *********************************************
  IF (p1.lcde_legacy_type = 'AUN') THEN
--
   OPEN c_aun_code(p1.lcde_legacy_ref);
   FETCH c_aun_code INTO l_aun_exists;
--
   IF (c_aun_code%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',50);
   END IF;
--
   CLOSE c_aun_code;
--
  END IF;
--- *********************************************
  IF (p1.lcde_legacy_type = 'PAR') THEN
--
   OPEN c_par_refno(p1.lcde_legacy_ref);
   FETCH c_par_refno INTO l_par_exists;
--
   IF (c_par_refno%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',207);
   END IF;
--
   CLOSE c_par_refno;
--
  END IF;
-- *********************************************
  IF (p1.lcde_legacy_type = 'PRF') THEN
--
   OPEN c_prf_refno(p1.lcde_legacy_ref);
   FETCH c_prf_refno INTO l_par_exists;
--
   IF (c_prf_refno%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',208);
   END IF;
--
   CLOSE c_prf_refno;
--
  END IF;
-- *********************************************
  IF (p1.lcde_legacy_type = 'BDE') THEN
--
   OPEN c_bde_refno(p1.lcde_legacy_ref, p1.lcde_secondary_ref);
   FETCH c_bde_refno INTO l_bde_exists;
--
   IF (c_bde_refno%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',692);
   END IF;
--
   CLOSE c_bde_refno;
--
  END IF;
-- *********************************************
  IF (p1.lcde_legacy_type = 'COS') THEN
--
   OPEN c_cos_code(p1.lcde_legacy_ref);
   FETCH c_cos_code INTO l_cos_exists;
--
   IF (c_cos_code%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',718);
   END IF;
--
   CLOSE c_cos_code;
--
-- Now check that contact name supplied exists in con_site_contacts table
--
   IF (    p1.lcde_secondary_ref IS NOT NULL
       AND l_cos_exists          IS NOT NULL) THEN
--
    OPEN c_csc_code(p1.lcde_legacy_ref, p1.lcde_secondary_ref);
    FETCH c_csc_code INTO l_csc_exists;
--
    IF (c_csc_code%NOTFOUND) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',752);
    END IF;
--
    CLOSE c_csc_code;
--
   END IF;
--
  END IF;
-- *********************************************
  IF (p1.lcde_legacy_type = 'SRQ') THEN
--
   OPEN c_srq_refno(p1.lcde_legacy_ref);
   FETCH c_srq_refno INTO l_srq_exists;
--
   IF (c_srq_refno%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',693);
   END IF;
--
   CLOSE c_srq_refno;
--
  END IF;
-- *********************************************
  IF (p1.lcde_legacy_type = 'PEG') THEN
--
   OPEN c_peg_code(p1.lcde_legacy_ref);
   FETCH c_peg_code INTO l_peg_exists;
--
   IF (c_peg_code%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',596);
   END IF;
--
   CLOSE c_peg_code;
--
  END IF;
-- *********************************************
  IF (p1.lcde_legacy_type = 'ORG') THEN   -- AJ3
--
   IF (p1.lcde_legacy_ref IS NULL OR p1.lcde_secondary_ref IS NULL) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',920);
   END IF;
--
   IF (p1.lcde_legacy_ref IS NOT NULL AND p1.lcde_secondary_ref IS NOT NULL) THEN -- AJ2
--
    OPEN chk_org_refno(p1.lcde_legacy_ref, p1.lcde_secondary_ref);
    FETCH chk_org_refno into l_org_exists;
    CLOSE chk_org_refno;
--
    IF (l_org_exists IS NULL) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',938);
    END IF;
--
-- IF Organisation supplied exists on parties
--
    IF (l_org_exists IS NOT NULL) THEN -- AJ1
     OPEN chk_org_count(p1.lcde_legacy_ref, p1.lcde_secondary_ref);
     FETCH chk_org_count into l_org_count;
     CLOSE chk_org_count;
--
     IF (l_org_count > 1) THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',895);
     END IF;
--
    END IF;  -- AJ1
   END IF;  -- AJ2
  END IF;  -- AJ3
-- *********************************************
  IF (p1.lcde_legacy_type = 'OCC') THEN   -- AJ3
--
   IF (p1.lcde_secondary_ref IS NULL
    OR p1.lcde_oco_forename IS NULL
    OR p1.lcde_oco_surname IS NULL) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',924);
   END IF;
--
   IF (p1.lcde_legacy_ref IS NOT NULL AND p1.lcde_secondary_ref IS NOT NULL
   AND p1.lcde_oco_forename IS NOT NULL AND p1.lcde_oco_surname IS NOT NULL) THEN -- AJ2
--
    OPEN chk_org_ct_refno(p1.lcde_legacy_ref
                         ,p1.lcde_secondary_ref
                         ,p1.lcde_oco_forename
                         ,p1.lcde_oco_surname);
    FETCH chk_org_ct_refno into l_occ_exists;
    CLOSE chk_org_ct_refno;
--
-- only error if not found when Organisation Contacts Create Marker is not Y
--
    IF nvl(p1.lcde_oco_create,'N') ='N' THEN
     IF (l_occ_exists IS NULL) THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',937);
     END IF;
    END IF;
--
-- IF Organisation Contact exists against Organisation
--
    IF (l_occ_exists IS NOT NULL) THEN
     OPEN chk_org_ct_count(p1.lcde_legacy_ref
                          ,p1.lcde_secondary_ref
                          ,p1.lcde_oco_forename
                          ,p1.lcde_oco_surname);
     FETCH chk_org_ct_count into l_occ_count;
     CLOSE chk_org_ct_count;
--
     IF (l_occ_count > 1) THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',895);
     END IF;
    END IF;
--
-- IF Organisation supplied exists on parties and is unique
--
    IF (l_occ_exists IS NULL) THEN
     OPEN chk_org_count(p1.lcde_legacy_ref, p1.lcde_secondary_ref);
     FETCH chk_org_count into l_org_count;
     CLOSE chk_org_count;
--
     IF (l_org_count > 1) THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',192);
     END IF;
--
     IF (l_org_count = 0) THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',193);
     END IF;
    END IF;
   END IF;  -- AJ2
  END IF;  -- AJ3
-- *********************************************
  IF (p1.lcde_legacy_type = 'OC2') THEN -- AJ3
--
   IF (p1.lcde_oco_forename IS NULL
    OR p1.lcde_oco_surname  IS NULL) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',936);
   END IF;
--
   IF (p1.lcde_legacy_ref IS NOT NULL) THEN
    OPEN c_org_refno(p1.lcde_legacy_ref);
    FETCH c_org_refno INTO l_org_exists;
    CLOSE c_org_refno;
--
    IF (l_org_exists IS NULL) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',938);
    END IF;
   END IF;
--
   IF (p1.lcde_legacy_ref   IS NOT NULL
   AND p1.lcde_oco_forename IS NOT NULL
   AND p1.lcde_oco_surname  IS NOT NULL) THEN -- AJ2
--
    OPEN c_org_ct2_refno(p1.lcde_legacy_ref
                        ,p1.lcde_oco_forename
                        ,p1.lcde_oco_surname);
    FETCH c_org_ct2_refno into l_occ_exists;
    CLOSE c_org_ct2_refno;
--
-- only error if not found when Organisation Contacts Create Marker is not Y
--
    IF nvl(p1.lcde_oco_create,'N') ='N' THEN
     IF (l_occ_exists IS NULL) THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',937);
     END IF;
    END IF;
   END IF; -- AJ2
  END IF; -- AJ3
-- *********************************************
-- Further Joint Checks for Organisation Contacts
--
  IF (p1.lcde_legacy_type IN ('OCC','OC2')) THEN
--
   IF nvl(p1.lcde_oco_update,'N') NOT IN ('N','Y') THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',184);
   END IF;
--
   IF nvl(p1.lcde_oco_create,'N') NOT IN ('N','Y') THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',185);
   END IF;
--
   IF ( p1.lcde_oco_update = 'Y'  OR
        p1.lcde_oco_create = 'Y'    )  THEN
--
    IF (p1.lcde_oco_start_date IS NULL) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',186);
    END IF;
--
    IF (p1.lcde_oco_signatory_ind IS NULL) THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',187);
    END IF;
--
   END IF;
--
   IF (NOT s_dl_hem_utils.exists_frv('TITLE',p1.lcde_oco_frv_title,'Y')) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',188);
   END IF;
--
   IF (p1.lcde_oco_start_date > nvl(p1.lcde_oco_end_date,p1.lcde_oco_start_date + 1)) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',189);
   END IF;
--
   IF (NOT s_dl_hem_utils.exists_frv('ORG_CONTACT_ROLE',p1.lcde_oco_frv_ocr_code,'Y')) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',190);
   END IF;
--
   IF (NOT s_dl_hem_utils.exists_frv('ORG_PREF_LANGUAGE',p1.lcde_oco_frv_opl_code,'Y')) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',191);
   END IF;
--
   IF (p1.lcde_oco_update = 'Y'  AND p1.lcde_oco_create = 'Y')  THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',194);
   END IF;
--
  END IF;
-- *********************************************
--
-- Check the Contact Method is Valid in table contact_methods
--
  IF (p1.lcde_frv_cme_code IS NOT NULL) THEN  -- Top
--
   OPEN c_conm_code(p1.lcde_frv_cme_code);
   FETCH c_conm_code INTO l_cme_exists;
   IF (c_conm_code%NOTFOUND) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',753);
   END IF;
   CLOSE c_conm_code;
--
-- further checks only if contact method found
--
   IF (p1.lcde_frv_cme_code != 'EMAIL' AND l_cme_exists IS NOT NULL) THEN -- L1
--
    li := LENGTH(p1.lcde_contact_value);
--
    OPEN c_conm(p1.lcde_frv_cme_code);
    FETCH c_conm INTO l_conm_cur
                     ,l_conm_code_out
                     ,l_conm_dig
                     ,l_conm_min_len
                     ,l_conm_max_len
                     ,l_conm_spaces;
    CLOSE c_conm;
--
-- check that only contains digits if set to Y (l_conm_dig)
--
    IF l_conm_dig = 'Y'  THEN
     l_char := SUBSTR(p1.lcde_contact_value,li,1);
     IF l_char NOT IN ('0','1','2','3','4','5','6','7','8','9') THEN
      l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',830);
     END IF;
    END IF;
--
-- check the contact value length conforms to min and max lengths specified
-- l_conm_min_len and l_conm_max_len
--
    IF NVL(l_conm_min_len,li) > li THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',831);
    END IF;
--
    IF NVL(l_conm_max_len,li) < li THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',832);
    END IF;
--
-- check that contact values does not contain spaces if set (l_conm_spaces)
--
    IF l_conm_spaces = 'N' THEN
     OPEN c_conm_spaces(p1.lcde_contact_value
                       ,p1.lcde_dlb_batch_id
                       ,p1.lcde_dl_seqno
                       ,p1.lcde_frv_cme_code);
     FETCH c_conm_spaces INTO l_chk_conm_spaces;
     IF c_conm_spaces%NOTFOUND
      THEN
       l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',833);
     END IF;
     CLOSE c_conm_spaces;
    END IF;
--
-- check that contact method is current (l_conm_cur)
--
    IF l_conm_cur = 'N' THEN
     l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',834);
    END IF;
--
   END IF;    -- L1
  END IF;   -- Top
-- *********************************************
--
-- Check All Other Mandatory Fields
--
  IF (p1.lcde_start_date IS NULL) THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',541);
  END IF;
--
  IF (p1.lcde_legacy_ref IS NULL) THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',921);
  END IF;
--
  IF (p1.lcde_contact_value IS NULL) THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',922);
  END IF;
--
  IF (p1.lcde_frv_cme_code IS NULL) THEN
   l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',923);
  END IF;
-- *********************************************
--
-- Check that if the End Date is Supplied it's after the Start Date
--
  IF (    p1.lcde_start_date IS NOT NULL
      AND p1.lcde_end_date   IS NOT NULL) THEN
--
   IF (p1.lcde_start_date <= p1.lcde_end_date) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',542);
   END IF;
  END IF;
-- *********************************************
-- check email is valid
--
  l_email_valid := 'Y';
--
  IF (p1.lcde_frv_cme_code = 'EMAIL') THEN
--
-- An @ must be supplied
--
   IF (INSTR(p1.lcde_contact_value, '@') = '0') THEN
    l_email_valid := 'N';
   END IF;
--
-- Only 1 @ must be supplied
--
   IF (INSTR(p1.lcde_contact_value, '@',1,2) != '0') THEN
    l_email_valid := 'N';
   END IF;
--
-- There must be data BEFORE the @
--
   IF (INSTR(p1.lcde_contact_value, '@') = '1') THEN
    l_email_valid := 'N';
   END IF;
--
-- There must be data AFTER the @
--
   IF (INSTR(p1.lcde_contact_value, '@') = LENGTH(p1.lcde_contact_value)) THEN
    l_email_valid := 'N';
   END IF;
--
-- At least one '.' must be specified AFTER the @
--
   IF (INSTR(p1.lcde_contact_value, '.', INSTR(p1.lcde_contact_value,'@')) = '0') THEN
    l_email_valid := 'N';
   END IF;
--
-- 1 or more characters either side of the '.' must be specified - check the '.' is not the last character
--
   IF (INSTR(p1.lcde_contact_value, '.', INSTR(p1.lcde_contact_value,'@')) = LENGTH(p1.lcde_contact_value)) THEN
    l_email_valid := 'N';
   END IF;
--
-- 1 or more characters either side of the '.' must be specified - check the '.' is not the first character
--
   IF (INSTR(p1.lcde_contact_value, '.', INSTR(p1.lcde_contact_value,'@')) = (INSTR(p1.lcde_contact_value,'@') +1)) THEN
    l_email_valid := 'N';
   END IF;
--
   IF (l_email_valid = 'N') THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',754);
   END IF;
--
  END IF; --p1.lcde_frv_cme_code = EMAIL
-- *********************************************
--
-- If supplied Comm Pref Code LCDE_FRV_COMM_PREF_CODE is valid
--
  IF (p1.lcde_frv_comm_pref_code IS NOT NULL) THEN
   IF (NOT s_dl_hem_utils.exists_frv('TEL_COMM_PREF',p1.lcde_frv_comm_pref_code,'Y')) THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',755);
   END IF;
  END IF;
-- *********************************************
--
-- Now UPDATE the record count and error code
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
-- keep a count of the rows processed and commit after every 1000
--
  i := i+1;
  IF MOD(i,1000)=0 THEN
   COMMIT;
  END IF;
--
  EXCEPTION
  WHEN OTHERS THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
   set_record_status_flag(l_id,'O');
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
  RAISE;
--
END dataload_validate;
--
-- *********************************************
--
PROCEDURE dataload_delete (p_batch_id IN VARCHAR2,
                           p_date     IN DATE)
AS
--
CURSOR c1
IS
SELECT rowid rec_rowid,
       lcde_dlb_batch_id,
       lcde_dl_seqno,
       lcde_dl_load_status,
       lcde_legacy_ref,
       lcde_legacy_type,
       lcde_start_date,
       NVL(lcde_created_date,SYSDATE)  lcde_created_date,
       NVL(lcde_created_by,'DATALOAD') lcde_created_by,
       lcde_contact_value,
       lcde_frv_cme_code,
       lcde_contact_name,
       lcde_end_date,
       lcde_precedence,
       lcde_frv_comm_pref_code,
       lcde_allow_texts,
       lcde_secondary_ref,
       lcde_comments,
       lcde_oco_forename,
       lcde_oco_surname,
       lcde_oco_frv_title,
       lcde_oco_update,
       lcde_oco_create,
       lcde_oco_start_date,
       lcde_oco_end_date,
       lcde_oco_signatory_ind,
       lcde_oco_frv_ocr_code,
       lcde_oco_frv_opl_code,
       lcde_oco_comments,
       lcde_oco_refno,
       lcde_oco_created,
       lcde_refno
  FROM dl_mad_contact_details
 WHERE lcde_dlb_batch_id   = p_batch_id
   AND lcde_dl_load_status = 'C';
--
-- *********************************************
-- PRO
CURSOR c_pro_refno (p_propref       VARCHAR2,
                    p_start_date    DATE,
                    p_contact_value VARCHAR2,
                    p_method        VARCHAR2)
IS
SELECT cde_refno,
       pro_refno
  FROM properties,
       contact_details
 WHERE pro_propref       = p_propref
   AND pro_refno         = cde_pro_refno
   AND cde_start_date    = p_start_date
   AND cde_contact_value = p_contact_value
   AND cde_frv_cme_code  = p_method;
--
-- *********************************************
-- AUN
CURSOR c_aun_code (p_aun_code      VARCHAR2,
                   p_start_date    DATE,
                   p_contact_value VARCHAR2,
                   p_method        VARCHAR2)
IS
SELECT cde_refno
  FROM contact_details
 WHERE cde_aun_code      = p_aun_code
   AND cde_start_date    = p_start_date
   AND cde_contact_value = p_contact_value
   AND cde_frv_cme_code  = p_method;
--
-- *********************************************
-- PAR
CURSOR c_par_refno (p_par_alt_ref   VARCHAR2,
                    p_start_date    DATE,
                    p_contact_value VARCHAR2,
                    p_method        VARCHAR2)
IS
SELECT cde_refno,
       par_refno
  FROM parties,
       contact_details
 WHERE par_per_alt_ref   = p_par_alt_ref
   AND par_refno         = cde_par_refno
   AND cde_start_date    = p_start_date
   AND cde_contact_value = p_contact_value
   AND cde_frv_cme_code  = p_method;
--
-- *********************************************
-- PRF
CURSOR c_prf_refno (p_par_refno     VARCHAR2,
                    p_start_date    DATE,
                    p_contact_value VARCHAR2,
                    p_method        VARCHAR2)
IS
SELECT cde_refno
  FROM parties,
       contact_details
 WHERE par_refno         = p_par_refno
   AND par_refno         = cde_par_refno
   AND cde_start_date    = p_start_date
   AND cde_contact_value = p_contact_value
   AND cde_frv_cme_code  = p_method;
--
-- *********************************************
-- BDE
CURSOR c_bde_refno (p_bde_bank_name   VARCHAR2,
                    p_bde_branch_name VARCHAR2,
                    p_start_date      DATE,
                    p_contact_value   VARCHAR2,
                    p_method          VARCHAR2)
IS
SELECT cde_refno,
       bde_refno
  FROM bank_details,
       contact_details
 WHERE bde_bank_name     = p_bde_bank_name
   AND bde_branch_name   = p_bde_branch_name
   AND bde_refno         = cde_bde_refno
   AND cde_start_date    = p_start_date
   AND cde_contact_value = p_contact_value
   AND cde_frv_cme_code  = p_method;
--
-- *********************************************
-- COS
CURSOR c_cos_code (p_cos_code      VARCHAR2,
                   p_cse_contact   VARCHAR2,
                   p_start_date    DATE,
                   p_contact_value VARCHAR2,
                   p_method        VARCHAR2)
IS
SELECT cde_refno
  FROM contact_details
 WHERE cde_cos_code                = p_cos_code
   AND NVL(cde_cse_contact, 'XYZ') = NVL(p_cse_contact,'XYZ')
   AND cde_start_date              = p_start_date
   AND cde_contact_value           = p_contact_value
   AND cde_frv_cme_code            = p_method;
--
-- *********************************************
-- SRQ
CURSOR c_srq_refno (p_srq_alt_ref   VARCHAR2,
                    p_start_date    DATE,
                    p_contact_value VARCHAR2,
                    p_method        VARCHAR2)
IS
SELECT cde_refno,
       srq_no
  FROM service_requests,
       contact_details
 WHERE srq_legacy_refno  = p_srq_alt_ref
   AND srq_no            = cde_srq_no
   AND cde_start_date    = p_start_date
   AND cde_contact_value = p_contact_value
   AND cde_frv_cme_code  = p_method;
--
-- *********************************************
-- PEG
CURSOR c_peg_code (p_peg_code      VARCHAR2,
                   p_start_date    DATE,
                   p_contact_value VARCHAR2,
                   p_method        VARCHAR2)
IS
SELECT cde_refno
  FROM contact_details
 WHERE cde_peg_code      = p_peg_code
   AND cde_start_date    = p_start_date
   AND cde_contact_value = p_contact_value
   AND cde_frv_cme_code  = p_method;
--
-- *********************************************
-- ORG
CURSOR c_org_refno (p_org_short_name   VARCHAR2
                   ,p_org_frv_oty_code VARCHAR2
                   ,p_start_date       DATE
                   ,p_contact_value    VARCHAR2
                   ,p_method           VARCHAR2)
IS
SELECT cde_refno,
       par_refno
  FROM parties,
       contact_details
 WHERE par_org_short_name   = p_org_short_name
   AND par_org_frv_oty_code = p_org_frv_oty_code
   AND par_refno            = cde_par_refno
   AND cde_start_date       = p_start_date
   AND cde_contact_value    = p_contact_value
   AND cde_frv_cme_code     = p_method;
--
-- *********************************************
-- OCC
CURSOR c_oco_org_refno(p_org_short_name   VARCHAR2
                      ,p_org_frv_oty_code VARCHAR2
                      ,p_start_date       DATE
                      ,p_contact_value    VARCHAR2
                      ,p_method           VARCHAR2
                      ,p_oco_forename     VARCHAR2
                      ,p_oco_surname      VARCHAR2) IS
SELECT cd.cde_refno,
       oc.oco_refno
  FROM parties p
      ,organisation_contacts oc
      ,contact_details cd
 WHERE p.par_org_short_name = p_org_short_name
  AND  p.par_org_frv_oty_code = p_org_frv_oty_code
  AND  p.par_refno = oc.oco_par_refno
  AND  oc.oco_forename = p_oco_forename
  AND  oc.oco_surname = p_oco_surname
  AND  oc.oco_refno = cd.cde_oco_refno
  AND  cd.cde_start_date = p_start_date
  AND  cd.cde_contact_value = p_contact_value
  AND  cd.cde_frv_cme_code = p_method;
--
-- *********************************************
-- OC2
CURSOR c_oco_ct2_refno(p_orgct_refno      VARCHAR2
                      ,p_start_date       DATE
                      ,p_contact_value    VARCHAR2
                      ,p_method           VARCHAR2
                      ,p_oco_forename     VARCHAR2
                      ,p_oco_surname      VARCHAR2) IS
SELECT cd.cde_refno,
       oc.oco_refno
  FROM parties p
      ,organisation_contacts oc
      ,contact_details cd
 WHERE p.par_refno = p_orgct_refno
  AND  p.par_refno = oc.oco_par_refno
  AND  oc.oco_forename = p_oco_forename
  AND  oc.oco_surname = p_oco_surname
  AND  oc.oco_refno = cd.cde_oco_refno
  AND  cd.cde_start_date = p_start_date
  AND  cd.cde_contact_value = p_contact_value
  AND  cd.cde_frv_cme_code = p_method;
--
-- *********************************************
--
-- Constants FOR process_summary
--
cb                VARCHAR2(30);
cd                DATE;
cp                VARCHAR2(30) := 'DELETE';
ct                VARCHAR2(30) := 'DL_MAD_CONTACT_DETAILS';
cs                INTEGER;
ce                VARCHAR2(200);
l_id              ROWID;
l_an_tab          VARCHAR2(1);
i                 INTEGER := 0;
l_cde_refno       NUMBER(10);
--
l_answer          VARCHAR2(1);
--
l_pro_refno       NUMBER(10);
l_aun_code        VARCHAR2(20);
l_par_refno       NUMBER(10);
l_bde_refno       NUMBER(10);
l_cos_code        VARCHAR2(20);
l_srq_no          NUMBER(10);
l_peg_code        VARCHAR2(10);
l_cse_contact     VARCHAR2(30);
l_oco_refno       NUMBER(8);
--
BEGIN
--
 fsc_utils.proc_start('s_dl_mad_contact_details.dataload_DELETE');
 fsc_utils.debug_message( 's_dl_mad_contact_details.dataload_DELETE',3 );
--
 cb := p_batch_id;
 cd := p_date;
--
 s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'End existing contact Method'
--
 l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
  cs   := p1.lcde_dl_seqno;
  l_id := p1.rec_rowid;
--
  SAVEPOINT SP1;
--
  l_cde_refno   := NULL;
--
  l_pro_refno   := NULL;
  l_aun_code    := NULL;
  l_par_refno   := NULL;
  l_bde_refno   := NULL;
  l_cos_code    := NULL;
  l_srq_no      := NULL;
  l_peg_code    := NULL;
  l_cse_contact := NULL;
  l_oco_refno   := NULL;
--
-- Get the Relevant Object and Contact Detail Record
-- Can only bypass and use cde_refno stored on create if
-- no other references are being fetched for example PRO also gets
-- the pro_refno as well
--
  IF (p1.lcde_legacy_type = 'PRO') THEN
--
   OPEN  c_pro_refno(p1.lcde_legacy_ref,
                     p1.lcde_start_date,
                     p1.lcde_contact_value,
                     p1.lcde_frv_cme_code);
   FETCH c_pro_refno INTO l_cde_refno, l_pro_refno;
   CLOSE c_pro_refno;
--
   IF (l_answer = 'Y') THEN
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_pro_refno    = l_pro_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'AUN') THEN
--
   IF (p1.lcde_refno IS NULL) THEN
    OPEN c_aun_code (p1.lcde_legacy_ref,
                     p1.lcde_start_date,
                     p1.lcde_contact_value,
                     p1.lcde_frv_cme_code);
--
    FETCH c_aun_code INTO l_cde_refno;
    CLOSE c_aun_code;
   ELSE
    l_cde_refno := p1.lcde_refno;
   END IF;
--
   l_aun_code := p1.lcde_legacy_ref;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_aun_code     = l_aun_code
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'PAR') THEN
--
   OPEN c_par_refno(p1.lcde_legacy_ref,
                    p1.lcde_start_date,
                    p1.lcde_contact_value,
                    p1.lcde_frv_cme_code);
--
   FETCH c_par_refno INTO l_cde_refno, l_par_refno;
   CLOSE c_par_refno;
--
   IF (l_answer = 'Y') THEN
--
   UPDATE contact_details
      SET cde_end_date = NULL
    WHERE cde_par_refno    = l_par_refno
      AND cde_frv_cme_code = p1.lcde_frv_cme_code
      AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'PRF') THEN
--
   IF (p1.lcde_refno IS NULL) THEN
    OPEN c_prf_refno(p1.lcde_legacy_ref,
                     p1.lcde_start_date,
                     p1.lcde_contact_value,
                     p1.lcde_frv_cme_code);
--
    FETCH c_prf_refno INTO l_cde_refno;
    CLOSE c_prf_refno;
   ELSE
    l_cde_refno := p1.lcde_refno;
   END IF;
--
   l_par_refno := p1.lcde_legacy_ref;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_par_refno    = l_par_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'BDE') THEN
--
   OPEN c_bde_refno(p1.lcde_legacy_ref,
                    p1.lcde_secondary_ref,
                    p1.lcde_start_date,
                    p1.lcde_contact_value,
                    p1.lcde_frv_cme_code);
--
   FETCH c_bde_refno INTO l_cde_refno, l_bde_refno;
   CLOSE c_bde_refno;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_bde_refno    = l_par_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'COS') THEN
--
   IF (p1.lcde_refno IS NULL) THEN
    OPEN c_cos_code (p1.lcde_legacy_ref,
                     p1.lcde_secondary_ref,
                     p1.lcde_start_date,
                     p1.lcde_contact_value,
                     p1.lcde_frv_cme_code);
--
    FETCH c_cos_code INTO l_cde_refno;
    CLOSE c_cos_code;
   ELSE
    l_cde_refno := p1.lcde_refno;
   END IF;
--
   l_cos_code    := p1.lcde_legacy_ref;
   l_cse_contact := p1.lcde_secondary_ref;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_cos_code                = l_cos_code
       AND NVL(cde_cse_contact, 'XYZ') = NVL(l_cse_contact,'XYZ')
       AND cde_frv_cme_code            = p1.lcde_frv_cme_code
       AND cde_end_date                = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'SRQ') THEN
--
   OPEN c_srq_refno(p1.lcde_legacy_ref,
                    p1.lcde_start_date,
                    p1.lcde_contact_value,
                    p1.lcde_frv_cme_code);
--
   FETCH c_srq_refno INTO l_cde_refno, l_srq_no;
   CLOSE c_srq_refno;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_srq_no       = l_srq_no
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'PEG') THEN
--
   IF (p1.lcde_refno IS NULL) THEN
    OPEN c_peg_code(p1.lcde_legacy_ref,
                    p1.lcde_start_date,
                    p1.lcde_contact_value,
                    p1.lcde_frv_cme_code);
--
    FETCH c_peg_code INTO l_cde_refno;
    CLOSE c_peg_code;
   ELSE
    l_cde_refno := p1.lcde_refno;
   END IF;
--
   l_peg_code := p1.lcde_legacy_ref;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_peg_code     = l_peg_code
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'ORG') THEN
--
   OPEN c_org_refno(p1.lcde_legacy_ref,
                    p1.lcde_secondary_ref,
                    p1.lcde_start_date,
                    p1.lcde_contact_value,
                    p1.lcde_frv_cme_code);
--
   FETCH c_org_refno INTO l_cde_refno, l_par_refno;
   CLOSE c_org_refno;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_par_refno    = l_par_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'OCC') THEN
--
   IF (p1.lcde_refno IS NULL OR p1.lcde_oco_refno IS NULL) THEN
    OPEN c_oco_org_refno(p1.lcde_legacy_ref,
                         p1.lcde_secondary_ref,
                         p1.lcde_start_date,
                         p1.lcde_contact_value,
                         p1.lcde_frv_cme_code,
                         p1.lcde_oco_forename,
                         p1.lcde_oco_surname);
--
    FETCH c_oco_org_refno INTO l_cde_refno, l_oco_refno;
    CLOSE c_oco_org_refno;
   ELSE
    l_cde_refno := p1.lcde_refno;
    l_oco_refno := p1.lcde_oco_refno;
   END IF;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_oco_refno    = l_oco_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  ELSIF (p1.lcde_legacy_type = 'OC2') THEN
--
   IF (p1.lcde_refno IS NULL OR p1.lcde_oco_refno IS NULL) THEN
    OPEN c_oco_ct2_refno(p1.lcde_legacy_ref,
                         p1.lcde_start_date,
                         p1.lcde_contact_value,
                         p1.lcde_frv_cme_code,
                         p1.lcde_oco_forename,
                         p1.lcde_oco_surname);
--
    FETCH c_oco_ct2_refno INTO l_cde_refno, l_oco_refno;
    CLOSE c_oco_ct2_refno;
   ELSE
    l_cde_refno := p1.lcde_refno;
    l_oco_refno := p1.lcde_oco_refno;
   END IF;
--
   IF (l_answer = 'Y') THEN
--
    UPDATE contact_details
       SET cde_end_date = NULL
     WHERE cde_oco_refno    = l_oco_refno
       AND cde_frv_cme_code = p1.lcde_frv_cme_code
       AND cde_end_date     = p1.lcde_start_date - 1;
--
   END IF;
--
  END IF;
--
-- Now Perform the Deletes
--
  DELETE
  FROM contact_details
  WHERE cde_refno = l_cde_refno;
--
-- Now need to delete Organisation Contact if created by this process
--
  DELETE
  FROM organisation_contacts
  WHERE oco_refno = l_oco_refno
  AND p1.lcde_oco_created = 'Y';
--
-- Update Record and Processed Count
--
  s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'V');
--
  i := i +1;
--
  IF mod(i,1000) = 0 THEN
   commit;
  END IF;
--
  EXCEPTION
   WHEN OTHERS THEN
    ROLLBACK TO SP1;
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    set_record_status_flag(l_id,'C');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
 END LOOP;
--
--
COMMIT;
--
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
--
fsc_utils.proc_end;
--
EXCEPTION
 WHEN OTHERS THEN
  s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
--
END dataload_delete;
--
END s_dl_mad_contact_details;
/

show errors

