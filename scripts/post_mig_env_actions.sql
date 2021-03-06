ALTER TRIGGER PAR_CRN_BR_IU DISABLE;
ALTER TRIGGER ALS_AR_U DISABLE;

UPDATE parties
   SET par_per_alt_ref = 'AMELIE'
 WHERE par_refno = 20000563;
 
UPDATE parties
   SET par_per_alt_ref = 'YOURPL'
 WHERE par_refno = 20000260;

UPDATE first_ref_values
   SET frv_current_ind = 'Y'
 WHERE frv_frd_domain = 'VERIFIEDTYPE'
   AND frv_code = 'MIGRATION';

CREATE OR REPLACE PROCEDURE insert_param_defn_usage(p_pdf_name       IN   VARCHAR2,
                                                    p_table_name     IN   VARCHAR2,
                                                    p_mandatory_ind  IN   VARCHAR2,
                                                    p_ele_code       IN   VARCHAR2,
                                                    p_aet_code       IN   VARCHAR2,
                                                    p_org_code       IN   VARCHAR2,
                                                    p_bro_code_act   IN   VARCHAR2,
                                                    p_bro_code_con   IN   VARCHAR2,
                                                    p_rac_type       IN   VARCHAR2,
                                                    p_adre_code      IN   VARCHAR2,
                                                    p_pptp_code      IN   VARCHAR2,
                                                    p_ara_code       IN   VARCHAR2,
                                                    p_hoop_code      IN   VARCHAR2,
                                                    p_ipt_code       IN   VARCHAR2,
                                                    p_conm_code      IN   VARCHAR2,
                                                    p_default        IN   VARCHAR2,
                                                    p_seq_no         IN   VARCHAR2,
                                                    p_required_ind   IN   VARCHAR2)
IS
   l_pgp_refno      elements.ele_pgp_refno%TYPE;
   l_exists         VARCHAR2(1);

   CURSOR C_PGP_REFNO IS
      SELECT ele_pgp_refno pgp_refno
        FROM elements
       WHERE p_ele_code IS NOT NULL
         AND ele_code = p_ele_code
       UNION 
      SELECT aet_pgp_refno pgp_refno
        FROM action_event_types
       WHERE p_aet_code IS NOT NULL
         AND aet_code = p_aet_code
       UNION
      SELECT otpg_pgp_refno pgp_refno
        FROM organisation_type_param_groups
       WHERE otpg_frv_oty_domain = 'ORG_TYPE'
         AND p_org_code IS NOT NULL
         AND otpg_frv_oty_code = p_org_code
       UNION
      SELECT bro_pgp_action_refno pgp_refno
        FROM business_reasons
       WHERE p_bro_code_act IS NOT NULL
         AND bro_code = p_bro_code_act
       UNION
      SELECT bro_pgp_contact_refno pgp_refno
        FROM business_reasons
       WHERE p_bro_code_con IS NOT NULL
         AND bro_code = p_bro_code_con
       UNION
      SELECT adre_pgp_refno
        FROM address_registers
       WHERE p_adre_code IS NOT NULL
         AND adre_code = p_adre_code
       UNION
      SELECT pptp_pgp_refno
        FROM prevention_payment_types
       WHERE p_pptp_code IS NOT NULL
         AND pptp_code = p_pptp_code
       UNION
      SELECT ofat_pgp_refno pgp_refno
        FROM other_fields_account_types
       WHERE p_rac_type IS NOT NULL
         AND ofat_code = p_rac_type
       UNION
      SELECT ara_pgp_refno
        FROM arrears_actions
       WHERE p_ara_code IS NOT NULL
         AND ara_code = p_ara_code
       UNION
      SELECT hoop_pgp_refno
        FROM housing_options
       WHERE p_hoop_code IS NOT NULL
         AND hoop_code = p_hoop_code
       UNION
      SELECT ipt_pgp_refno
        FROM interested_party_types
       WHERE p_ipt_code IS NOT NULL
         AND ipt_code = p_ipt_code
       UNION
      SELECT conm_pgp_refno
        FROM contact_methods
       WHERE p_conm_code IS NOT NULL
         AND conm_code = p_conm_code;
         
   CURSOR C_PARAM_VALS_EXIST(p_pdf_name   VARCHAR2,
                             p_pgp_refno  NUMBER) IS
      SELECT 'Y'
        FROM parameter_values
       WHERE pva_pdu_pdf_name = p_pdf_name
         AND pva_pdu_pdf_param_type = 'OTHER FIELDS'
         AND pva_pdu_pgp_refno = NVL(p_pgp_refno, -1);
         
   CURSOR C_PGP_SEQ_NO_EXISTS(p_pdf_name      VARCHAR2,
                              p_table_name    VARCHAR2,
                              p_pgp_refno     NUMBER,
                              p_seq_no        NUMBER) IS
      SELECT 'Y'
        FROM parameter_definition_usages
       WHERE pdu_pdf_param_type = 'OTHER FIELDS'
         AND pdu_pgp_refno = NVL(p_pgp_refno, -1)
         AND pdu_pob_table_name = p_table_name
         AND pdu_display_seqno = p_seq_no
         AND pdu_pdf_name != p_pdf_name;
BEGIN
   OPEN C_PGP_REFNO;
   FETCH C_PGP_REFNO into l_pgp_refno;
   CLOSE C_PGP_REFNO;

   IF p_required_ind = 'Y'
   THEN
      OPEN C_PARAM_VALS_EXIST(p_pdf_name,
                              l_pgp_refno);
      FETCH C_PARAM_VALS_EXIST INTO l_exists;
      CLOSE C_PARAM_VALS_EXIST;
      
      IF NVL(l_exists, 'N') = 'Y'
      THEN
         DELETE
           FROM parameter_values
          WHERE pva_pdu_pdf_name = p_pdf_name
            AND pva_pdu_pdf_param_type = 'OTHER FIELDS'
            AND pva_pdu_pob_table_name = NVL(p_table_name, 'NULL')
            AND pva_pdu_pgp_refno = NVL(l_pgp_refno, -1);
      END IF;
      
      l_exists := 'N';
      
      OPEN C_PGP_SEQ_NO_EXISTS(p_pdf_name,
                               NVL(p_table_name, 'NULL'),
                               NVL(l_pgp_refno, -1),
                               p_seq_no);
      FETCH C_PGP_SEQ_NO_EXISTS INTO l_exists;
      CLOSE C_PGP_SEQ_NO_EXISTS;
      
      IF l_exists = 'Y'
      THEN
         UPDATE parameter_definition_usages
            SET pdu_display_seqno = ABS(pdu_display_seqno) * -1
          WHERE pdu_pdf_param_type = 'OTHER FIELDS'
            AND pdu_pob_table_name = NVL(p_table_name, 'NULL')
            AND pdu_pgp_refno = NVL(l_pgp_refno, -1);
      END IF;
   
      MERGE INTO parameter_definition_usages tgt
      USING (SELECT p_pdf_name pdu_pdf_name,
                    p_mandatory_ind pdu_required_ind,
                    NVL(p_table_name, 'NULL') pdu_pob_table_name,
                    NVL(l_pgp_refno, 0) pdu_pgp_refno,
                    p_default pdu_default,
                    p_seq_no pdu_display_seqno
               FROM dual) src
         ON (    tgt.pdu_pdf_name = src.pdu_pdf_name
             AND tgt.pdu_pdf_param_type = 'OTHER FIELDS'
             AND tgt.pdu_pob_table_name = src.pdu_pob_table_name
             AND NVL(tgt.pdu_pgp_refno, 0) = src.pdu_pgp_refno)
       WHEN MATCHED THEN
          UPDATE
             SET tgt.pdu_display_seqno = src.pdu_display_seqno,
                 tgt.pdu_required_ind = src.pdu_required_ind,
                 tgt.pdu_default = src.pdu_default
       WHEN NOT MATCHED THEN
          INSERT(pdu_pdf_name,
                 pdu_pdf_param_type,
                 pdu_required_ind,
                 pdu_pob_table_name,
                 pdu_pgp_refno,
                 pdu_default,
                 pdu_display_seqno)
          VALUES(src.pdu_pdf_name,
                 'OTHER FIELDS',
                 src.pdu_required_ind,
                 src.pdu_pob_table_name,
                 NVL(src.pdu_pgp_refno, -1),
                 src.pdu_default,
                 src.pdu_display_seqno);
   ELSE
      DELETE
        FROM parameter_values_history
       WHERE pvh_pva_pdu_pdf_name = p_pdf_name
         AND pvh_pva_pdu_pdf_param_type = 'OTHER FIELDS'
         AND pvh_pva_pdu_pob_table_name = p_table_name
         AND pvh_pva_pdu_pgp_refno = NVL(l_pgp_refno, -1);
   
      DELETE
        FROM parameter_values
       WHERE pva_pdu_pdf_name = p_pdf_name
         AND pva_pdu_pdf_param_type = 'OTHER FIELDS'
         AND NVL(pva_pdu_pob_table_name, 'x') = NVL(p_table_name, 'x')
         AND pva_pdu_pgp_refno = NVL(l_pgp_refno, -1);

      DELETE
        FROM parameter_definition_usages
       WHERE pdu_pdf_name = p_pdf_name
         AND pdu_pdf_param_type = 'OTHER FIELDS'
         AND NVL(pdu_pob_table_name, 'x') = NVL(p_table_name, 'x')
         AND pdu_pgp_refno = NVL(l_pgp_refno, -1)
         AND pdu_display_seqno = p_seq_no;
   END IF;
END insert_param_defn_usage;
/

exec insert_param_defn_usage('CRN', 'PARTIES', 'N', null, null, null, null, null, null, null, null, null, null, null, null, null, 400, 'Y');

DECLARE
   l_vru_id   validation_rules.vru_id%TYPE;
   
   CURSOR C_VRU_EXISTS(p_vru_name   VARCHAR2) IS
      SELECT vru_id
        FROM validation_rules
       WHERE vru_name = p_vru_name
         AND vru_type = 'TABLE';
   
BEGIN
   OPEN C_VRU_EXISTS('CHPAGREE VALUE');
   FETCH C_VRU_EXISTS INTO l_vru_id;
   CLOSE C_VRU_EXISTS;
   
   IF l_vru_id IS NULL
   THEN
      l_vru_id := vru_id_seq.nextval;
   END IF;
   
   MERGE INTO validation_rules tgt
   USING (SELECT l_vru_id vru_id,
                 'CHPAGREE VALUE' vru_name,
                 'TABLE' vru_type,
                 'Value attached to CHP Agreement' vru_comments,
                 'Y' vru_display_lov,
                 'ATT_CODE' vru_column_1,
                 'Attribute Code' vru_column_1_title,
                 'ATT_DESCRIPTION' vru_column_2,
                 'Attribute Description' vru_column_2_title,
                 'FROM attributes att INNER JOIN property_elements pel ON pel.pel_ele_code = att.att_ele_code and pel.pel_att_code = att.att_code AND pel.pel_ele_code = ''CHPAGREE'' AND TRUNC(SYSDATE) BETWEEN TRUNC(pel.pel_start_date) AND NVL(TRUNC(pel.pel_end_date), SYSDATE + 1)  INNER JOIN nominations nom ON nom.nom_pro_refno = pel.pel_pro_refno AND nom.nom_refno = NVL(v(''P195_NOM_REFNO''), v(''P135_NOM_REFNO''))' vru_from_clause
            FROM dual) src
      ON (    tgt.vru_name = src.vru_name
          AND tgt.vru_type = src.vru_type
          AND tgt.vru_comments = src.vru_comments)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.vru_display_lov = src.vru_display_lov,
              tgt.vru_column_1 = src.vru_column_1,
              tgt.vru_column_1_title = src.vru_column_1_title,
              tgt.vru_column_2 = src.vru_column_2,
              tgt.vru_column_2_title = src.vru_column_2_title,
              tgt.vru_from_clause = src.vru_from_clause
    WHEN NOT MATCHED THEN
       INSERT(vru_id,
              vru_name,
              vru_type,
              vru_comments,
              vru_display_lov,
              vru_column_1,
              vru_column_1_title,
              vru_column_2,
              vru_column_2_title,
              vru_from_clause)
       VALUES(src.vru_id,
              src.vru_name,
              src.vru_type,
              src.vru_comments,
              src.vru_display_lov,
              src.vru_column_1,
              src.vru_column_1_title,
              src.vru_column_2,
              src.vru_column_2_title,
              src.vru_from_clause);

   UPDATE parameter_definitions
      SET pdf_vru_id = l_vru_id
    WHERE pdf_name = 'NOM_OFFER_PROP_AGREE';
    
   l_vru_id := NULL;
    
   OPEN C_VRU_EXISTS('GRPHOUSE VALUES');
   FETCH C_VRU_EXISTS INTO l_vru_id;
   CLOSE C_VRU_EXISTS;
   
   IF l_vru_id IS NULL
   THEN
      l_vru_id := vru_id_seq.nextval;
   END IF;
   
   MERGE INTO validation_rules tgt
   USING (SELECT l_vru_id vru_id,
                 'GRPHOUSE VALUES' vru_name,
                 'TABLE' vru_type,
                 'TEXT_ID values for GRPHOUSE elements' vru_comments,
                 'Y' vru_display_lov,
                 'PVA_CHAR_VALUE' vru_column_1,
                 'Text ID' vru_column_1_title,
                 'PVA_CHAR_VALUE' vru_column_2,
                 'Text ID' vru_column_2_title,
                 'FROM nominations nom INNER JOIN property_elements pel on pel.pel_pro_refno = nom.nom_pro_refno and pel.pel_ele_code LIKE ''GRPHOUSE_%'' AND TRUNC(SYSDATE) BETWEEN TRUNC(pel.pel_start_date) AND NVL(TRUNC(pel.pel_end_date), SYSDATE + 1) INNER JOIN elements ele ON ele.ele_code = pel.pel_ele_code INNER JOIN parameter_values pva ON pva.pva_reusable_refno = pel.pel_reusable_refno AND pva.pva_pdu_pgp_refno = ele.ele_pgp_refno AND pva.pva_pdu_pdf_name = ''TEXT_ID'' AND pva.pva_pdu_pob_table_name = ''NULL'' WHERE nom.nom_refno = NVL(v(''P195_NOM_REFNO''), v(''P135_NOM_REFNO''))' vru_from_clause
            FROM dual) src
      ON (    tgt.vru_name = src.vru_name
          AND tgt.vru_type = src.vru_type
          AND tgt.vru_comments = src.vru_comments)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.vru_display_lov = src.vru_display_lov,
              tgt.vru_column_1 = src.vru_column_1,
              tgt.vru_column_1_title = src.vru_column_1_title,
              tgt.vru_column_2 = src.vru_column_2,
              tgt.vru_column_2_title = src.vru_column_2_title,
              tgt.vru_from_clause = src.vru_from_clause
    WHEN NOT MATCHED THEN
       INSERT(vru_id,
              vru_name,
              vru_type,
              vru_comments,
              vru_display_lov,
              vru_column_1,
              vru_column_1_title,
              vru_column_2,
              vru_column_2_title,
              vru_from_clause)
       VALUES(src.vru_id,
              src.vru_name,
              src.vru_type,
              src.vru_comments,
              src.vru_display_lov,
              src.vru_column_1,
              src.vru_column_1_title,
              src.vru_column_2,
              src.vru_column_2_title,
              src.vru_from_clause);
              
   UPDATE parameter_definitions
      SET pdf_vru_id = l_vru_id
    WHERE pdf_name = 'GRPHOUSE';
END;
/

DROP PROCEDURE insert_param_defn_usage;
   
COMMIT;