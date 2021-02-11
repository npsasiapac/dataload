SPOO op_config_install.log
SET LINESIZE 32000
SET LONG 32000

SET ECHO OFF;
SET DEFINE OFF;
SET FEEDBACK OFF;
SET SERVEROUTPUT ON;

CREATE OR REPLACE PROCEDURE create_question(p_que_refno                   IN   questions.que_refno%TYPE,
                                            p_que_type                    IN   questions.que_type%TYPE,
                                            p_que_datatype                IN   questions.que_datatype%TYPE,
                                            p_que_qgr_code                IN   questions.que_qgr_code%TYPE,
                                            p_que_qgr_question_category   IN   questions.que_qgr_question_category%TYPE,
                                            p_que_disable_index_ind       IN   questions.que_disable_index_ind%TYPE,
                                            p_que_parent_child_ind        IN   questions.que_parent_child_ind%TYPE,
                                            p_que_call_procedure_ind      IN   questions.que_call_procedure_ind%TYPE,
                                            p_que_prop_related_ind        IN   questions.que_prop_related_ind%TYPE,
                                            p_que_history_reqd_ind        IN   questions.que_history_reqd_ind%TYPE,
                                            p_que_show_summary_ind        IN   questions.que_show_summary_ind%TYPE,
                                            p_que_slist_warning_ind       IN   questions.que_slist_warning_ind%TYPE,
                                            p_que_model_ind               IN   questions.que_model_ind%TYPE,
                                            p_que_seqno                   IN   questions.que_seqno%TYPE,
                                            p_que_summary_seqno           IN   questions.que_summary_seqno%TYPE,
                                            p_que_group_seqno             IN   questions.que_group_seqno%TYPE,
                                            p_que_summary_description     IN   questions.que_summary_description%TYPE,
                                            p_que_question_description    IN   questions.que_question_description%TYPE,
                                            p_que_required_ind            IN   questions.que_required_ind%TYPE,
                                            p_que_que_refno               IN   questions.que_que_refno%TYPE)
IS

BEGIN
   MERGE INTO questions tgt
   USING (SELECT p_que_refno que_refno,
                 p_que_type que_type,
                 p_que_datatype que_datatype,
                 p_que_qgr_code que_qgr_code,
                 p_que_qgr_question_category que_qgr_question_category,
                 p_que_disable_index_ind que_disable_index_ind,
                 p_que_parent_child_ind que_parent_child_ind,
                 p_que_call_procedure_ind que_call_procedure_ind,
                 p_que_prop_related_ind que_prop_related_ind,
                 p_que_history_reqd_ind que_history_reqd_ind,
                 p_que_show_summary_ind que_show_summary_ind,
                 p_que_slist_warning_ind que_slist_warning_ind,
                 p_que_model_ind que_model_ind,
                 p_que_seqno que_seqno,
                 p_que_summary_seqno que_summary_seqno,
                 p_que_group_seqno que_group_seqno,
                 p_que_summary_description que_summary_description,
                 p_que_question_description que_question_description,
                 p_que_required_ind que_required_ind,
                 p_que_que_refno que_que_refno
            FROM dual) src
      ON (src.que_refno = tgt.que_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.que_type = src.que_type,
              tgt.que_datatype = src.que_datatype,
              tgt.que_qgr_code = src.que_qgr_code,
              tgt.que_qgr_question_category = src.que_qgr_question_category,
              tgt.que_disable_index_ind = src.que_disable_index_ind,
              tgt.que_parent_child_ind = src.que_parent_child_ind,
              tgt.que_call_procedure_ind = src.que_call_procedure_ind,
              tgt.que_prop_related_ind = src.que_prop_related_ind,
              tgt.que_history_reqd_ind = src.que_history_reqd_ind,
              tgt.que_show_summary_ind = src.que_show_summary_ind,
              tgt.que_slist_warning_ind = src.que_slist_warning_ind,
              tgt.que_model_ind = src.que_model_ind,
              tgt.que_seqno = src.que_seqno,
              tgt.que_summary_seqno = src.que_summary_seqno,
              tgt.que_group_seqno = src.que_group_seqno,
              tgt.que_summary_description = src.que_summary_description,
              tgt.que_question_description = src.que_question_description,
              tgt.que_required_ind = src.que_required_ind,
              tgt.que_que_refno = src.que_que_refno
    WHEN NOT MATCHED THEN
       INSERT(que_refno,
              que_type,
              que_datatype,
              que_qgr_code,
              que_qgr_question_category,
              que_disable_index_ind,
              que_parent_child_ind,
              que_call_procedure_ind,
              que_prop_related_ind,
              que_history_reqd_ind,
              que_show_summary_ind,
              que_slist_warning_ind,
              que_model_ind,
              que_seqno,
              que_summary_seqno,
              que_group_seqno,
              que_summary_description,
              que_question_description,
              que_required_ind,
              que_que_refno)
       VALUES(src.que_refno,
              src.que_type,
              src.que_datatype,
              src.que_qgr_code,
              src.que_qgr_question_category,
              src.que_disable_index_ind,
              src.que_parent_child_ind,
              src.que_call_procedure_ind,
              src.que_prop_related_ind,
              src.que_history_reqd_ind,
              src.que_show_summary_ind,
              src.que_slist_warning_ind,
              src.que_model_ind,
              src.que_seqno,
              src.que_summary_seqno,
              src.que_group_seqno,
              src.que_summary_description,
              src.que_question_description,
              src.que_required_ind,
              src.que_que_refno);
          
END create_question;
/

CREATE OR REPLACE PROCEDURE cre_derived_question_clauses(p_dqc_dqh_refno   IN  derived_question_clauses.dqc_dqh_refno%TYPE,
                                                         p_dqc_seqno       IN  derived_question_clauses.dqc_seqno%TYPE,
                                                         p_dqc_sql_text    IN  derived_question_clauses.dqc_sql_text%TYPE)
IS

BEGIN
   MERGE INTO derived_question_clauses tgt
   USING (SELECT p_dqc_dqh_refno dqc_dqh_refno,
                 p_dqc_seqno dqc_seqno,
                 p_dqc_sql_text dqc_sql_text
            FROM dual) src
      ON (    tgt.dqc_seqno = src.dqc_seqno
          AND tgt.dqc_dqh_refno = src.dqc_dqh_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.dqc_sql_text = src.dqc_sql_text
    WHEN NOT MATCHED THEN
       INSERT(dqc_dqh_refno,
              dqc_seqno,
              dqc_sql_text)
       VALUES(src.dqc_dqh_refno,
              src.dqc_seqno,
              src.dqc_sql_text);
END cre_derived_question_clauses;
/

CREATE OR REPLACE PROCEDURE cre_derived_question_usage(p_dqu_dqh_refno   IN   derived_question_usages.dqu_dqh_refno%TYPE,
                                                       p_dqu_que_refno   IN   derived_question_usages.dqu_que_refno%TYPE,
                                                       p_dqu_dqc_refno   IN   derived_question_usages.dqu_dqc_seqno%TYPE)
IS

BEGIN
   MERGE INTO derived_question_usages tgt
   USING (SELECT p_dqu_dqh_refno dqu_dqh_refno,
                 p_dqu_que_refno dqu_que_refno,
                 p_dqu_dqc_refno dqu_dqc_seqno
            FROM dual) src
      ON (    src.dqu_dqh_refno = tgt.dqu_dqh_refno
          AND src.dqu_que_refno = tgt.dqu_que_refno
          AND NVL(src.dqu_dqc_seqno, -1) = NVL(tgt.dqu_dqc_seqno, -1))
    WHEN NOT MATCHED THEN
       INSERT(dqu_dqh_refno,
              dqu_que_refno,
              dqu_dqc_seqno)
       VALUES(src.dqu_dqh_refno,
              src.dqu_que_refno,
              src.dqu_dqc_seqno);
END cre_derived_question_usage;
/

CREATE OR REPLACE PROCEDURE cre_que_perm_resp(p_qpr_refno         IN   question_permitted_responses.qpr_refno%TYPE,
                                              p_qpr_description   IN   question_permitted_responses.qpr_description%TYPE,
                                              p_qpr_que_refno     IN   question_permitted_responses.qpr_que_refno%TYPE,
                                              p_qpr_code          IN   question_permitted_responses.qpr_code%TYPE)
IS

BEGIN
   MERGE INTO question_permitted_responses tgt
   USING (SELECT p_qpr_refno qpr_refno,
                 p_qpr_description qpr_description,
                 p_qpr_que_refno qpr_que_refno,
                 p_qpr_code qpr_code
            FROM dual) src
      ON (src.qpr_refno = tgt.qpr_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.qpr_description = src.qpr_description,
              tgt.qpr_que_refno = src.qpr_que_refno,
              tgt.qpr_code = src.qpr_code
    WHEN NOT MATCHED THEN
       INSERT(qpr_refno,
              qpr_description,
              qpr_que_refno,
              qpr_code)
       VALUES(src.qpr_refno,
              src.qpr_description,
              src.qpr_que_refno,
              src.qpr_code);
END cre_que_perm_resp;
/

CREATE OR REPLACE PROCEDURE insert_fr_values(p_frd_domain   IN   first_ref_values.frv_frd_domain%TYPE,
                                             p_frv_code     IN   first_ref_values.frv_code%TYPE,
                                             p_frv_name     IN   first_ref_values.frv_name%TYPE,
                                             p_frv_sequence IN   first_ref_values.frv_sequence%TYPE)
IS
   L_exists     VARCHAR2(1);
   
   CURSOR C_FR_DOMAIN_EXISTS IS
      SELECT 'Y'
        FROM first_ref_domains
       WHERE frd_domain = p_frd_domain;
BEGIN
   
   OPEN C_FR_DOMAIN_EXISTS;
   FETCH C_FR_DOMAIN_EXISTS INTO L_exists;
   CLOSE C_FR_DOMAIN_EXISTS;
   
   IF NVL(L_exists, 'N') = 'Y'
   THEN      
      MERGE INTO first_ref_values tgt
      USING (SELECT p_frd_domain frv_frd_domain,
                    p_frv_code frv_code,
                    p_frv_name frv_name,
                    p_frv_sequence frv_sequence
               FROM dual) src
         ON (    tgt.frv_frd_domain = src.frv_frd_domain
             AND tgt.frv_code = src.frv_code)
       WHEN MATCHED THEN
          UPDATE
             SET tgt.frv_name = src.frv_name,
                 tgt.frv_sequence = src.frv_sequence,
                 tgt.frv_default_ind = 'N',
                 tgt.frv_current_ind = 'Y'
       WHEN NOT MATCHED THEN
          INSERT(frv_frd_domain,
                 frv_code,
                 frv_name,
                 frv_usage,
                 frv_sequence)
          VALUES(src.frv_frd_domain,
                 src.frv_code,
                 src.frv_name,
                 'USR',
                 src.frv_sequence);
   END IF;
END insert_fr_values;
/

CREATE OR REPLACE PROCEDURE cre_acpg(p_acpg_code          IN   applic_cat_pri_sch_group_rules.acpg_code%TYPE,
                                     p_acpg_description   IN   applic_cat_pri_sch_group_rules.acpg_description%TYPE,
                                     p_acpg_current_ind   IN   applic_cat_pri_sch_group_rules.acpg_current_ind%TYPE,
                                     p_acpg_sequence      IN   applic_cat_pri_sch_group_rules.acpg_sequence%TYPE)
IS

BEGIN
   MERGE INTO applic_cat_pri_sch_group_rules tgt
   USING (SELECT p_acpg_code acpg_code,
                 p_acpg_description acpg_description,
                 p_acpg_current_ind acpg_current_ind,
                 p_acpg_sequence acpg_sequence
            FROM dual) src
      ON (src.acpg_code = tgt.acpg_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.acpg_description = src.acpg_description,
              tgt.acpg_sequence = src.acpg_sequence
    WHEN NOT MATCHED THEN
       INSERT(acpg_code,
              acpg_description,
              acpg_current_ind,
              acpg_sequence)
       VALUES(src.acpg_code,
              src.acpg_description,
              src.acpg_current_ind,
              src.acpg_sequence);
END cre_acpg;
/
 
CREATE OR REPLACE PROCEDURE cre_acrq(p_acrq_acpg_code   IN   applic_catgroup_rule_questions.acrq_acpg_code%TYPE,
                                     p_acrq_que_refno   IN   applic_catgroup_rule_questions.acrq_que_refno%TYPE,
                                     p_acrq_sequence    IN   applic_catgroup_rule_questions.acrq_sequence%TYPE,
                                     p_acrq_group_no    IN   applic_catgroup_rule_questions.acrq_group_no%TYPE)
IS

BEGIN
   MERGE INTO applic_catgroup_rule_questions tgt
   USING (SELECT p_acrq_acpg_code acrq_acpg_code,
                 p_acrq_que_refno acrq_que_refno,
                 p_acrq_sequence acrq_sequence,
                 p_acrq_group_no acrq_group_no
            FROM dual) src
      ON (    src.acrq_acpg_code = tgt.acrq_acpg_code
          AND src.acrq_que_refno = tgt.acrq_que_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.acrq_sequence = src.acrq_sequence,
              tgt.acrq_group_no = src.acrq_group_no
    WHEN NOT MATCHED THEN
       INSERT(acrq_acpg_code,
              acrq_que_refno,
              acrq_sequence,
              acrq_group_no)
       VALUES(src.acrq_acpg_code,
              src.acrq_que_refno,
              src.acrq_sequence,
              src.acrq_group_no);
END cre_acrq;
/

CREATE OR REPLACE PROCEDURE cre_accg(p_accg_refno                    IN   applic_cat_rule_conditions.accg_refno%TYPE,
                                     p_accg_type                     IN   applic_cat_rule_conditions.accg_type%TYPE,
                                     p_accg_acrq_acpg_code           IN   applic_cat_rule_conditions.accg_acrq_acpg_code%TYPE,
                                     p_accg_acrq_que_refno           IN   applic_cat_rule_conditions.accg_acrq_que_refno%TYPE,
                                     p_accg_acpr_acps_hrv_prs_code   IN   applic_cat_rule_conditions.accg_acpr_acps_hrv_prs_code%TYPE,
                                     p_accg_acpr_acps_hrv_apc_code   IN   applic_cat_rule_conditions.accg_acpr_acps_hrv_apc_code%TYPE,
                                     p_accg_acpr_que_refno           IN   applic_cat_rule_conditions.accg_acpr_que_refno%TYPE,
                                     p_accg_min_value                IN   applic_cat_rule_conditions.accg_min_value%TYPE,
                                     p_accg_max_value                IN   applic_cat_rule_conditions.accg_max_value%TYPE)
IS

BEGIN
   MERGE INTO applic_cat_rule_conditions tgt
   USING (SELECT p_accg_refno accg_refno,
                 p_accg_type accg_type,
                 p_accg_acrq_acpg_code accg_acrq_acpg_code,
                 p_accg_acrq_que_refno accg_acrq_que_refno,
                 p_accg_acpr_acps_hrv_prs_code accg_acpr_acps_hrv_prs_code,
                 p_accg_acpr_acps_hrv_apc_code accg_acpr_acps_hrv_apc_code,
                 p_accg_acpr_que_refno accg_acpr_que_refno,
                 p_accg_min_value accg_min_value,
                 p_accg_max_value accg_max_value
            FROM dual) src
      ON (src.accg_refno = tgt.accg_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.accg_type = src.accg_type,
              tgt.accg_acrq_acpg_code = src.accg_acrq_acpg_code,
              tgt.accg_acrq_que_refno = src.accg_acrq_que_refno,
              tgt.accg_acpr_acps_hrv_prs_code = src.accg_acpr_acps_hrv_prs_code,
              tgt.accg_acpr_acps_hrv_apc_code = src.accg_acpr_acps_hrv_apc_code,
              tgt.accg_acpr_que_refno = src.accg_acpr_que_refno,
              tgt.accg_min_value = src.accg_min_value,
              tgt.accg_max_value = src.accg_max_value
    WHEN NOT MATCHED THEN
       INSERT(accg_refno,
              accg_type,
              accg_acrq_acpg_code,
              accg_acrq_que_refno,
              accg_acpr_acps_hrv_prs_code,
              accg_acpr_acps_hrv_apc_code,
              accg_acpr_que_refno,
              accg_min_value,
              accg_max_value)
       VALUES(src.accg_refno,
              src.accg_type,
              src.accg_acrq_acpg_code,
              src.accg_acrq_que_refno,
              src.accg_acpr_acps_hrv_prs_code,
              src.accg_acpr_acps_hrv_apc_code,
              src.accg_acpr_que_refno,
              src.accg_min_value,
              src.accg_max_value);
END cre_accg;
/

CREATE OR REPLACE PROCEDURE cre_acps(p_acps_hrv_prs_code   IN  applic_category_pri_schemes.acps_hrv_prs_code%TYPE,
                                     p_acps_hrv_apc_code   IN  applic_category_pri_schemes.acps_hrv_apc_code%TYPE,
                                     p_acps_acpg_code      IN  applic_category_pri_schemes.acps_acpg_code%TYPE)
IS

BEGIN
   MERGE INTO applic_category_pri_schemes tgt
   USING (SELECT p_acps_hrv_prs_code acps_hrv_prs_code,
                 p_acps_hrv_apc_code acps_hrv_apc_code,
                 p_acps_acpg_code acps_acpg_code
            FROM dual) src
      ON (    src.acps_hrv_prs_code = tgt.acps_hrv_prs_code
          AND src.acps_hrv_apc_code = tgt.acps_hrv_apc_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.acps_acpg_code = src.acps_acpg_code,
              tgt.acps_current_ind = 'Y'
    WHEN NOT MATCHED THEN
       INSERT(acps_hrv_prs_code,
              acps_hrv_apc_code,
              acps_acpg_code)
       VALUES(src.acps_hrv_prs_code,
              src.acps_hrv_apc_code,
              src.acps_acpg_code);
END cre_acps;
/

CREATE OR REPLACE PROCEDURE cre_acpr(p_acpr_acps_hrv_prs_code   IN   applic_cat_pri_sch_rules.acpr_acps_hrv_prs_code%TYPE,
                                     p_acpr_acps_hrv_apc_code   IN   applic_cat_pri_sch_rules.acpr_acps_hrv_apc_code%TYPE,
                                     p_acpr_que_refno           IN   applic_cat_pri_sch_rules.acpr_que_refno%TYPE,
                                     p_acpr_sequence            IN   applic_cat_pri_sch_rules.acpr_sequence%TYPE,
                                     p_acpr_group_no            IN   applic_cat_pri_sch_rules.acpr_group_no%TYPE)
IS

BEGIN
   MERGE INTO applic_cat_pri_sch_rules tgt
   USING (SELECT p_acpr_acps_hrv_prs_code acpr_acps_hrv_prs_code,
                 p_acpr_acps_hrv_apc_code acpr_acps_hrv_apc_code,
                 p_acpr_que_refno acpr_que_refno, 
                 p_acpr_sequence acpr_sequence,
                 p_acpr_group_no acpr_group_no
            FROM dual) src
      ON (    tgt.acpr_acps_hrv_prs_code = src.acpr_acps_hrv_prs_code
          AND tgt.acpr_acps_hrv_apc_code = src.acpr_acps_hrv_apc_code
          AND tgt.acpr_que_refno = src.acpr_que_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.acpr_sequence = src.acpr_sequence,
              tgt.acpr_group_no = src.acpr_group_no
    WHEN NOT MATCHED THEN
       INSERT(acpr_acps_hrv_prs_code,
              acpr_acps_hrv_apc_code,
              acpr_que_refno,
              acpr_sequence,
              acpr_group_no)
       VALUES(src.acpr_acps_hrv_prs_code,
              src.acpr_acps_hrv_apc_code,
              src.acpr_que_refno,
              src.acpr_sequence,
              src.acpr_group_no);
END cre_acpr;
/

CREATE OR REPLACE PROCEDURE cre_slc(p_slc_code               IN   slist_categories.slc_code%TYPE,
                                    p_slc_description        IN   slist_categories.slc_description%TYPE,
                                    p_slc_default_priority   IN slist_categories.slc_default_priority%TYPE)
IS

BEGIN
   MERGE INTO slist_categories tgt
   USING (SELECT p_slc_code slc_code,
                 p_slc_description slc_description,
                 p_slc_default_priority slc_default_priority
            FROM dual) src
      ON (tgt.slc_code = src.slc_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.slc_description = src.slc_description,
              tgt.slc_default_priority = src.slc_default_priority,
              tgt.slc_current_ind = 'Y',
              tgt.slc_default_ind = 'N',
              tgt.slc_quota_ind = 'N'
    WHEN NOT MATCHED THEN
       INSERT(slc_code,
              slc_description,
              slc_default_priority)
       VALUES(src.slc_code,
              src.slc_description,
              src.slc_default_priority);
END cre_slc;
/

CREATE OR REPLACE PROCEDURE cre_scg(p_scg_slc_code       IN   slist_cat_appl_cat.scg_slc_code%TYPE,
                                    p_scg_hrv_apc_code   IN   slist_cat_appl_cat.scg_hrv_apc_code%TYPE,
                                    p_scg_priority       IN   slist_cat_appl_cat.scg_priority%TYPE)
IS

BEGIN
   MERGE INTO slist_cat_appl_cat tgt
   USING (SELECT p_scg_slc_code scg_slc_code,
                 p_scg_hrv_apc_code scg_hrv_apc_code,
                 p_scg_priority scg_priority
            FROM dual) src
      ON (    tgt.scg_slc_code = src.scg_slc_code
          AND tgt.scg_hrv_apc_code = src.scg_hrv_apc_code
          AND tgt.scg_priority = src.scg_priority)
    WHEN NOT MATCHED THEN
       INSERT(scg_slc_code,
              scg_hrv_apc_code,
              scg_priority)
       VALUES(src.scg_slc_code,
              src.scg_hrv_apc_code,
              src.scg_priority);
END cre_scg;
/

CREATE OR REPLACE PROCEDURE cre_dqr(p_dqr_refno            IN   dflt_question_restricts.dqr_refno%TYPE,
                                    p_dqr_seqno            IN   dflt_question_restricts.dqr_seqno%TYPE,
                                    p_dqr_auto_apply_ind   IN   dflt_question_restricts.dqr_auto_apply_ind%TYPE,
                                    p_dqr_que_refno        IN   dflt_question_restricts.dqr_que_refno%TYPE,
                                    p_dqr_slc_code         IN   dflt_question_restricts.dqr_slc_code%TYPE,
                                    p_dqr_operand          IN   dflt_question_restricts.dqr_operand%TYPE,
                                    p_dqr_qpr_refno        IN   dflt_question_restricts.dqr_qpr_refno%TYPE,
                                    p_dqr_default_answer   IN   dflt_question_restricts.dqr_default_answer%TYPE)
IS

BEGIN
   MERGE INTO dflt_question_restricts tgt
   USING (SELECT p_dqr_refno dqr_refno,
                 p_dqr_seqno dqr_seqno,
                 p_dqr_auto_apply_ind dqr_auto_apply_ind,
                 p_dqr_que_refno dqr_que_refno,
                 p_dqr_slc_code dqr_slc_code,
                 p_dqr_operand dqr_operand,
                 p_dqr_qpr_refno dqr_qpr_refno,
                 p_dqr_default_answer dqr_default_answer
            FROM dual) src
      ON (tgt.dqr_refno = src.dqr_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.dqr_seqno = src.dqr_seqno,
              tgt.dqr_auto_apply_ind = src.dqr_auto_apply_ind,
              tgt.dqr_que_refno = src.dqr_que_refno,
              tgt.dqr_slc_code = src.dqr_slc_code,
              tgt.dqr_operand = src.dqr_operand,
              tgt.dqr_qpr_refno = src.dqr_qpr_refno,
              tgt.dqr_default_answer = src.dqr_default_answer
    WHEN NOT MATCHED THEN
       INSERT(dqr_refno,
              dqr_seqno,
              dqr_auto_apply_ind,
              dqr_que_refno,
              dqr_slc_code,
              dqr_operand,
              dqr_qpr_refno,
              dqr_default_answer)
       VALUES(src.dqr_refno,
              src.dqr_seqno,
              src.dqr_auto_apply_ind,
              src.dqr_que_refno,
              src.dqr_slc_code,
              src.dqr_operand,
              src.dqr_qpr_refno,
              src.dqr_default_answer);
END cre_dqr;
/

CREATE OR REPLACE PROCEDURE cre_egr(p_egr_que_refno   IN   elig_group_rule_questions.egr_que_refno%TYPE,
                                    p_egr_heg_code    IN   elig_group_rule_questions.egr_heg_code%TYPE,
                                    p_egr_seqno       IN   elig_group_rule_questions.egr_seqno%TYPE,
                                    p_egr_min_value   IN   elig_group_rule_questions.egr_min_value%TYPE,
                                    p_egr_max_value   IN   elig_group_rule_questions.egr_max_value%TYPE,
                                    p_egr_answer      IN   elig_group_rule_questions.egr_answer%TYPE)
IS 

BEGIN
   MERGE INTO elig_group_rule_questions tgt
   USING (SELECT p_egr_que_refno egr_que_refno,
                 p_egr_heg_code egr_heg_code,
                 p_egr_seqno egr_seqno,
                 p_egr_min_value egr_min_value,
                 p_egr_max_value egr_max_value,
                 p_egr_answer egr_answer
            FROM dual) src
      ON (    tgt.egr_heg_code = src.egr_heg_code
          AND tgt.egr_que_refno = src.egr_que_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.egr_seqno = src.egr_seqno,
              tgt.egr_min_value = src.egr_max_value,
              tgt.egr_max_value = src.egr_max_value,
              tgt.egr_answer = src.egr_answer
    WHEN NOT MATCHED THEN
       INSERT(egr_que_refno,
              egr_heg_code,
              egr_seqno,
              egr_min_value,
              egr_max_value,
              egr_answer)
       VALUES(src.egr_que_refno,
              src.egr_heg_code,
              src.egr_seqno,
              src.egr_min_value,
              src.egr_max_value,
              src.egr_answer);
END cre_egr;
/

CREATE OR REPLACE PROCEDURE cre_mru(p_mru_refno          IN   matching_rules.mru_refno%TYPE,
                                    p_mru_ele_code       IN   matching_rules.mru_ele_code%TYPE,
                                    p_mru_type           IN   matching_rules.mru_type%TYPE,
                                    p_mru_hrv_prs_code   IN   matching_rules.mru_hrv_prs_code%TYPE,
                                    p_mru_description    IN   matching_rules.mru_description%TYPE,
                                    p_mru_current_ind    IN   matching_rules.mru_current_ind%TYPE,
                                    p_mru_seqno          IN   matching_rules.mru_seqno%TYPE,
                                    p_mru_que_refno      IN   matching_rules.mru_que_refno%TYPE)
IS

BEGIN
   MERGE INTO matching_rules tgt
   USING (SELECT p_mru_refno mru_refno,
                 p_mru_ele_code mru_ele_code,
                 p_mru_type mru_type,
                 p_mru_hrv_prs_code mru_hrv_prs_code,
                 p_mru_description mru_description,
                 p_mru_current_ind mru_current_ind,
                 p_mru_seqno mru_seqno,
                 p_mru_que_refno mru_que_refno
            FROM dual) src
      ON (tgt.mru_refno = src.mru_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.mru_ele_code = src.mru_ele_code,
              tgt.mru_type = src.mru_type,
              tgt.mru_hrv_prs_code = src.mru_hrv_prs_code,
              tgt.mru_description = src.mru_description,
              tgt.mru_current_ind = src.mru_current_ind,
              tgt.mru_seqno = src.mru_seqno,
              tgt.mru_que_refno = src.mru_que_refno
    WHEN NOT MATCHED THEN
       INSERT(mru_refno,
              mru_ele_code,
              mru_type,
              mru_hrv_prs_code,
              mru_description,
              mru_current_ind,
              mru_seqno,
              mru_que_refno)
       VALUES(src.mru_refno,
              src.mru_ele_code,
              src.mru_type,
              src.mru_hrv_prs_code,
              src.mru_description,
              src.mru_current_ind,
              src.mru_seqno,
              src.mru_que_refno);
END cre_mru;
/

CREATE OR REPLACE PROCEDURE cre_squ(p_squ_hrv_prs_code            IN   scheme_questions.squ_hrv_prs_code%TYPE,
                                    p_squ_que_refno               IN   scheme_questions.squ_que_refno%TYPE,
                                    p_squ_qgr_code                IN   scheme_questions.squ_qgr_code%TYPE,
                                    p_squ_qgr_question_category   IN   scheme_questions.squ_qgr_question_category%TYPE,
                                    p_squ_group_alt_seqno         IN   scheme_questions.squ_group_alt_seqno%TYPE,
                                    p_squ_required_ind            IN   scheme_questions.squ_required_ind%TYPE,
                                    p_squ_scheme_alt_seqno        IN   scheme_questions.squ_scheme_alt_seqno%TYPE,
                                    p_squ_summary_alt_seqno       IN   scheme_questions.squ_summary_alt_seqno%TYPE,
                                    p_squ_nomination_que_ind      IN   scheme_questions.squ_nomination_que_ind%TYPE)
IS

BEGIN
   MERGE INTO scheme_questions tgt
   USING (SELECT p_squ_hrv_prs_code squ_hrv_prs_code,
                 p_squ_que_refno squ_que_refno,
                 p_squ_qgr_code squ_qgr_code,
                 p_squ_qgr_question_category squ_qgr_question_category,
                 p_squ_group_alt_seqno squ_group_alt_seqno,
                 p_squ_required_ind squ_required_ind,
                 p_squ_scheme_alt_seqno squ_scheme_alt_seqno,
                 p_squ_summary_alt_seqno squ_summary_alt_seqno,
                 p_squ_nomination_que_ind squ_nomination_que_ind
            FROM dual) src
      ON (    tgt.squ_que_refno = src.squ_que_refno
          AND tgt.squ_hrv_prs_code = src.squ_hrv_prs_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.squ_qgr_code = src.squ_qgr_code,
              tgt.squ_qgr_question_category = src.squ_qgr_question_category,
              tgt.squ_group_alt_seqno = src.squ_group_alt_seqno,
              tgt.squ_required_ind = src.squ_required_ind,
              tgt.squ_scheme_alt_seqno = src.squ_scheme_alt_seqno,
              tgt.squ_summary_alt_seqno = src.squ_summary_alt_seqno,
              tgt.squ_nomination_que_ind = src.squ_nomination_que_ind
    WHEN NOT MATCHED THEN
       INSERT(squ_hrv_prs_code,
              squ_que_refno,
              squ_qgr_code,
              squ_qgr_question_category,
              squ_group_alt_seqno,
              squ_required_ind,
              squ_scheme_alt_seqno,
              squ_summary_alt_seqno,
              squ_nomination_que_ind)
       VALUES(src.squ_hrv_prs_code,
              src.squ_que_refno,
              src.squ_qgr_code,
              src.squ_qgr_question_category,
              src.squ_group_alt_seqno,
              src.squ_required_ind,
              src.squ_scheme_alt_seqno,
              src.squ_summary_alt_seqno,
              src.squ_nomination_que_ind);
END cre_squ;
/

CREATE OR REPLACE PROCEDURE cre_att(p_att_ele_code      IN   attributes.att_ele_code%TYPE,
                                    p_att_code          IN   attributes.att_code%TYPE,
                                    p_att_seqno         IN   attributes.att_seqno%TYPE,
                                    p_att_description   IN   attributes.att_description%TYPE)
IS

BEGIN
   MERGE INTO attributes tgt
   USING (SELECT p_att_ele_code att_ele_code,
                 p_att_code att_code,
                 p_att_seqno att_seqno,
                 p_att_description att_description
            FROM dual) src
      ON (    tgt.att_ele_code = src.att_ele_code
          AND tgt.att_code = src.att_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.att_seqno = src.att_seqno,
              tgt.att_description = src.att_description
    WHEN NOT MATCHED THEN
       INSERT(att_ele_code,
              att_code,
              att_seqno,
              att_description)
       VALUES(src.att_ele_code,
              src.att_code,
              src.att_seqno,
              src.att_description);
END cre_att;
/

CREATE OR REPLACE PROCEDURE cre_lar(p_lar_code          IN   lettings_areas.lar_code%TYPE,
                                    p_lar_description   IN   lettings_areas.lar_description%TYPE,
                                    p_lar_current_ind   IN   lettings_areas.lar_current_ind%TYPE,
                                    p_lar_seqno         IN   lettings_areas.lar_seqno%TYPE,
                                    p_lar_type          IN   lettings_areas.lar_type%TYPE,
                                    p_lar_lar_code      IN   lettings_areas.lar_lar_code%TYPE,
                                    p_lar_ele_code      IN   lettings_areas.lar_ele_code%TYPE,
                                    p_lar_att_code      IN   lettings_areas.lar_att_code%TYPE)
IS

BEGIN
   MERGE INTO lettings_areas tgt
   USING (SELECT p_lar_code lar_code,
                 p_lar_description lar_description,
                 p_lar_current_ind lar_current_ind,
                 p_lar_seqno lar_seqno,
                 p_lar_type lar_type,
                 p_lar_lar_code lar_lar_code,
                 p_lar_ele_code lar_ele_code,
                 p_lar_att_code lar_att_code
            FROM dual) src
      ON (tgt.lar_code = src.lar_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.lar_description = src.lar_description,
              tgt.lar_current_ind = src.lar_current_ind,
              tgt.lar_seqno = src.lar_seqno,
              tgt.lar_type = src.lar_type,
              tgt.lar_lar_code = src.lar_lar_code,
              tgt.lar_ele_code = src.lar_ele_code,
              tgt.lar_att_code = src.lar_att_code
    WHEN NOT MATCHED THEN
       INSERT(lar_code,
              lar_description,
              lar_current_ind,
              lar_seqno,
              lar_type,
              lar_lar_code,
              lar_ele_code,
              lar_att_code)
       VALUES(src.lar_code,
              src.lar_description,
              src.lar_current_ind,
              src.lar_seqno,
              src.lar_type,
              src.lar_lar_code,
              src.lar_ele_code,
              src.lar_att_code);
END cre_lar;
/

CREATE OR REPLACE PROCEDURE cre_dqh(p_dqh_refno         IN   derived_question_headers.dqh_refno%TYPE,
                                    p_dqh_description   IN   derived_question_headers.dqh_description%TYPE,
                                    p_dqh_datatype_ind  IN   derived_question_headers.dqh_datatype_ind%TYPE)
IS

BEGIN
   MERGE INTO derived_question_headers tgt
   USING (SELECT p_dqh_refno dqh_refno,
                 p_dqh_description dqh_description,
                 p_dqh_datatype_ind dqh_datatype_ind
            FROM dual) src
      ON (tgt.dqh_refno = src.dqh_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.dqh_description = src.dqh_description,
              tgt.dqh_datatype_ind = src.dqh_datatype_ind
    WHEN NOT MATCHED THEN
       INSERT(dqh_refno,
              dqh_description,
              dqh_datatype_ind)
       VALUES(src.dqh_refno,
              src.dqh_description,
              src.dqh_datatype_ind);
END cre_dqh;
/

CREATE OR REPLACE PROCEDURE cre_dqd(p_dqd_dqh_refno   IN   derived_question_details.dqd_dqh_refno%TYPE,
                                    p_dqd_seqno       IN   derived_question_details.dqd_seqno%TYPE,
                                    p_dqd_sql_text    IN   derived_question_details.dqd_sql_text%TYPE)
IS

BEGIN
   MERGE INTO derived_question_details tgt
   USING (SELECT p_dqd_dqh_refno dqd_dqh_refno,
                 p_dqd_seqno dqd_seqno,
                 p_dqd_sql_text dqd_sql_text
            FROM dual) src
      ON (    tgt.dqd_dqh_refno = src.dqd_dqh_refno
          AND tgt.dqd_seqno = src.dqd_seqno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.dqd_sql_text = src.dqd_sql_text
    WHEN NOT MATCHED THEN
       INSERT(dqd_dqh_refno,
              dqd_seqno,
              dqd_sql_text)
       VALUES(src.dqd_dqh_refno,
              src.dqd_seqno,
              src.dqd_sql_text);
END cre_dqd;
/

CREATE OR REPLACE PROCEDURE cre_rra(p_rra_code          IN   rejection_reasons.rra_code%TYPE,
                                    p_rra_description   IN   rejection_reasons.rra_description%TYPE,
                                    p_rra_current_ind   IN   rejection_reasons.rra_current_ind%TYPE,
                                    p_rra_sequence      IN   rejection_reasons.rra_sequence%TYPE)
IS

BEGIN
   MERGE INTO rejection_reasons tgt
   USING (SELECT p_rra_code rra_code,
                 p_rra_description rra_description,
                 p_rra_current_ind rra_current_ind,
                 p_rra_sequence rra_sequence
            FROM dual) src
      ON (tgt.rra_code = src.rra_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.rra_description = src.rra_description,
              tgt.rra_current_ind = src.rra_current_ind,
              tgt.rra_sequence = src.rra_sequence
    WHEN NOT MATCHED THEN
       INSERT(rra_code,
              rra_description,
              rra_current_ind,
              rra_sequence)
       VALUES(src.rra_code,
              src.rra_description,
              src.rra_current_ind,
              src.rra_sequence);
END cre_rra;
/

CREATE OR REPLACE PROCEDURE cre_hid(p_hid_region_id   IN   hidden_field_job_roles.hid_region_id%TYPE,
                                    p_hid_item_pos    IN   hidden_field_job_roles.hid_item_pos%TYPE,
                                    p_hid_jro_code    IN   hidden_field_job_roles.hid_jro_code%TYPE)
IS

BEGIN
   MERGE INTO hidden_field_job_roles tgt
   USING (SELECT p_hid_region_id hid_region_id,
                 p_hid_item_pos hid_item_pos,
                 p_hid_jro_code hid_jro_code
            FROM dual) src
      ON (    tgt.hid_region_id = src.hid_region_id
          AND tgt.hid_item_pos = src.hid_item_pos
          AND tgt.hid_jro_code = src.hid_jro_code)
    WHEN NOT MATCHED THEN
       INSERT(hid_region_id,
              hid_item_pos,
              hid_jro_code)
       VALUES(src.hid_region_id,
              src.hid_item_pos,
              src.hid_jro_code);
END cre_hid;
/

CREATE OR REPLACE PROCEDURE cre_tty(p_tty_code                      IN   tenancy_types.tty_code%TYPE,
                                    p_tty_description               IN   tenancy_types.tty_description%TYPE,
                                    p_tty_periodic_status           IN   tenancy_types.tty_periodic_status%TYPE,
                                    p_tty_current_ind               IN   tenancy_types.tty_current_ind%TYPE,
                                    p_tty_usage                     IN   tenancy_types.tty_usage%TYPE,
                                    p_tty_default_ind               IN   tenancy_types.tty_default_ind%TYPE,
                                    p_tty_seqno                     IN   tenancy_types.tty_seqno%TYPE,
                                    p_tty_rtb_eligible_ind          IN   tenancy_types.tty_rtb_eligible_ind%TYPE,
                                    p_tty_fair_rent_eligible_ind    IN   tenancy_types.tty_fair_rent_eligible_ind%TYPE,
                                    p_tty_anniversary_rent_ind      IN   tenancy_types.tty_anniversary_rent_ind%TYPE,
                                    p_tty_rta_eligible_ind          IN   tenancy_types.tty_rta_eligible_ind%TYPE,
                                    p_tty_water_charge_exempt_ind   IN   tenancy_types.tty_water_charge_exempt_ind%TYPE)
IS

BEGIN
   MERGE INTO tenancy_types tgt
   USING (SELECT p_tty_code tty_code,
                 p_tty_description tty_description,
                 p_tty_periodic_status tty_periodic_status,
                 p_tty_current_ind tty_current_ind,
                 p_tty_usage tty_usage,
                 p_tty_default_ind tty_default_ind,
                 p_tty_seqno tty_seqno,
                 p_tty_rtb_eligible_ind tty_rtb_eligible_ind,
                 p_tty_fair_rent_eligible_ind tty_fair_rent_eligible_ind,
                 p_tty_anniversary_rent_ind tty_anniversary_rent_ind,
                 p_tty_rta_eligible_ind tty_rta_eligible_ind,
                 p_tty_water_charge_exempt_ind tty_water_charge_exempt_ind
            FROM dual) src
      ON (tgt.tty_code = src.tty_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.tty_description = src.tty_description,
              tgt.tty_periodic_status = src.tty_periodic_status,
              tgt.tty_current_ind = src.tty_current_ind,
              tgt.tty_usage = src.tty_usage,
              tgt.tty_default_ind = src.tty_default_ind,
              tgt.tty_seqno = src.tty_seqno,
              tgt.tty_rtb_eligible_ind = src.tty_rtb_eligible_ind,
              tgt.tty_fair_rent_eligible_ind = src.tty_fair_rent_eligible_ind,
              tgt.tty_anniversary_rent_ind = src.tty_anniversary_rent_ind,
              tgt.tty_rta_eligible_ind = src.tty_rta_eligible_ind,
              tgt.tty_water_charge_exempt_ind = src.tty_water_charge_exempt_ind
    WHEN NOT MATCHED THEN
       INSERT(tty_code,
              tty_description,
              tty_periodic_status,
              tty_current_ind,
              tty_usage,
              tty_default_ind,
              tty_seqno,
              tty_rtb_eligible_ind,
              tty_fair_rent_eligible_ind,
              tty_anniversary_rent_ind,
              tty_rta_eligible_ind,
              tty_water_charge_exempt_ind)
       VALUES(src.tty_code,
              src.tty_description,
              src.tty_periodic_status,
              src.tty_current_ind,
              src.tty_usage,
              src.tty_default_ind,
              src.tty_seqno,
              src.tty_rtb_eligible_ind,
              src.tty_fair_rent_eligible_ind,
              src.tty_anniversary_rent_ind,
              src.tty_rta_eligible_ind,
              src.tty_water_charge_exempt_ind);
         
              
END cre_tty;
/

CREATE OR REPLACE PROCEDURE cre_ntt(p_ntt_code                 IN   note_types.ntt_code%TYPE,
                                    p_ntt_description          IN   note_types.ntt_description%TYPE,
                                    p_ntt_current_ind          IN   note_types.ntt_current_ind%TYPE,
                                    p_ntt_generate_alert_ind   IN   note_types.ntt_generate_alert_ind%TYPE,
                                    p_ntt_visiting_note_ind    IN   note_types.ntt_visiting_note_ind%TYPE)
IS

BEGIN
   MERGE INTO note_types tgt
   USING (SELECT p_ntt_code ntt_code,
                 p_ntt_description ntt_description,
                 p_ntt_current_ind ntt_current_ind,
                 p_ntt_generate_alert_ind ntt_generate_alert_ind,
                 p_ntt_visiting_note_ind ntt_visiting_note_ind
            FROM dual) src
      ON (tgt.ntt_code = src.ntt_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.ntt_description = src.ntt_description,
              tgt.ntt_current_ind = src.ntt_current_ind,
              tgt.ntt_generate_alert_ind = src.ntt_generate_alert_ind,
              tgt.ntt_visiting_note_ind = src.ntt_visiting_note_ind
    WHEN NOT MATCHED THEN
       INSERT(ntt_code,
              ntt_description,
              ntt_current_ind,
              ntt_generate_alert_ind,
              ntt_visiting_note_ind)
       VALUES(src.ntt_code,
              src.ntt_description,
              src.ntt_current_ind,
              src.ntt_generate_alert_ind,
              src.ntt_visiting_note_ind);
END cre_ntt;
/

CREATE OR REPLACE PROCEDURE cre_nto(p_nto_object_code   IN   note_type_objects.nto_object_code%TYPE,
                                    p_nto_ntt_code      IN   note_type_objects.nto_ntt_code%TYPE)
IS

BEGIN
   MERGE INTO note_type_objects tgt
   USING (SELECT p_nto_object_code nto_object_code,
                 p_nto_ntt_code nto_ntt_code
            FROM dual) src
      ON (    tgt.nto_object_code = src.nto_object_code
          AND tgt.nto_ntt_code = src.nto_ntt_code)
    WHEN NOT MATCHED THEN
       INSERT(nto_object_code,
              nto_ntt_code)
       VALUES(src.nto_object_code,
              src.nto_ntt_code);
END cre_nto;
/

CREATE OR REPLACE PROCEDURE cre_srp(p_srp_rpl_refno   IN   slist_cat_rehousing_policies.srp_rpl_refno%TYPE,
                                    p_srp_slc_code    IN   slist_cat_rehousing_policies.srp_slc_code%TYPE)
IS

BEGIN
   MERGE INTO slist_cat_rehousing_policies tgt
   USING (SELECT p_srp_rpl_refno srp_rpl_refno,
                 p_srp_slc_code srp_slc_code
            FROM dual) src
      ON (    tgt.srp_rpl_refno = src.srp_rpl_refno
          AND tgt.srp_slc_code = src.srp_slc_code)
    WHEN NOT MATCHED THEN
       INSERT(srp_rpl_refno,
              srp_slc_code)
       VALUES(src.srp_rpl_refno,
              src.srp_slc_code);
END cre_srp;
/

CREATE OR REPLACE PROCEDURE cre_pesr(p_pesr_code                    IN   person_search_rules.pesr_code%TYPE,
                                     p_pesr_description             IN   person_search_rules.pesr_description%TYPE,
                                     p_pesr_current_ind             IN   person_search_rules.pesr_current_ind%TYPE,
                                     p_pesr_par_refno               IN   person_search_rules.pesr_par_refno%TYPE,
                                     p_pesr_par_per_alt_ref         IN   person_search_rules.pesr_par_per_alt_ref%TYPE,
                                     p_pesr_par_per_surname         IN   person_search_rules.pesr_par_per_surname%TYPE,
                                     p_pesr_par_per_forename        IN   person_search_rules.pesr_par_per_forename%TYPE,
                                     p_pesr_par_per_date_of_birth   IN   person_search_rules.pesr_par_per_date_of_birth%TYPE)
IS

BEGIN
   MERGE INTO person_search_rules tgt
   USING (SELECT p_pesr_code pesr_code,
                 p_pesr_description pesr_description,
                 p_pesr_current_ind pesr_current_ind,
                 p_pesr_par_refno pesr_par_refno,
                 p_pesr_par_per_alt_ref pesr_par_per_alt_ref,
                 p_pesr_par_per_surname pesr_par_per_surname,
                 p_pesr_par_per_forename pesr_par_per_forename,
                 p_pesr_par_per_date_of_birth pesr_par_per_date_of_birth
            FROM dual) src
      ON (tgt.pesr_code = src.pesr_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.pesr_description = src.pesr_description,
              tgt.pesr_current_ind = src.pesr_current_ind,
              tgt.pesr_par_refno = src.pesr_par_refno,
              tgt.pesr_par_per_alt_ref = src.pesr_par_per_alt_ref,
              tgt.pesr_par_per_surname = src.pesr_par_per_surname,
              tgt.pesr_par_per_forename = src.pesr_par_per_forename,
              tgt.pesr_par_per_date_of_birth = src.pesr_par_per_date_of_birth
    WHEN NOT MATCHED THEN
       INSERT(pesr_code,
              pesr_description,
              pesr_current_ind,
              pesr_par_refno,
              pesr_par_per_alt_ref,
              pesr_par_per_surname,
              pesr_par_per_forename,
              pesr_par_per_date_of_birth)
       VALUES(src.pesr_code,
              src.pesr_description,
              src.pesr_current_ind,
              src.pesr_par_refno,
              src.pesr_par_per_alt_ref,
              src.pesr_par_per_surname,
              src.pesr_par_per_forename,
              src.pesr_par_per_date_of_birth);
END cre_pesr;
/

CREATE OR REPLACE PROCEDURE cre_equ(p_equ_que_refno           IN   elig_questions.equ_que_refno%TYPE,
                                    p_equ_hhts_hrv_els_code   IN   elig_questions.equ_hhts_hrv_els_code%TYPE,
                                    p_equ_hhts_hty_code       IN   elig_questions.equ_hhts_hty_code%TYPE,
                                    p_equ_seqno               IN   elig_questions.equ_seqno%TYPE,
                                    p_equ_answer              IN   elig_questions.equ_answer%TYPE)
IS

BEGIN
   MERGE INTO elig_questions tgt
   USING (SELECT p_equ_que_refno equ_que_refno,
                 p_equ_hhts_hrv_els_code equ_hhts_hrv_els_code,
                 p_equ_hhts_hty_code equ_hhts_hty_code,
                 p_equ_seqno equ_seqno,
                 p_equ_answer equ_answer
            FROM dual) src
      ON (    tgt.equ_que_refno = src.equ_que_refno
          AND tgt.equ_hhts_hrv_els_code = src.equ_hhts_hrv_els_code
          AND tgt.equ_hhts_hty_code = src.equ_hhts_hty_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.equ_seqno = src.equ_seqno,
              tgt.equ_answer = src.equ_answer
    WHEN NOT MATCHED THEN
       INSERT(equ_que_refno,
              equ_hhts_hrv_els_code,
              equ_hhts_hty_code,
              equ_seqno,
              equ_answer)
       VALUES(src.equ_que_refno,
              src.equ_hhts_hrv_els_code,
              src.equ_hhts_hty_code,
              src.equ_seqno,
              src.equ_answer);
END cre_equ;
/

CREATE OR REPLACE PROCEDURE cre_asst(p_asst_code                    IN   assessment_types.asst_code%TYPE,
                                     p_asst_type                    IN   assessment_types.asst_type%TYPE,
                                     p_asst_description             IN   assessment_types.asst_description%TYPE,
                                     p_asst_review_period_unit      IN   assessment_types.asst_review_period_unit%TYPE,
                                     p_asst_review_period_qty       IN   assessment_types.asst_review_period_qty%TYPE,
                                     p_asst_allow_person_ind        IN   assessment_types.asst_allow_person_ind%TYPE,
                                     p_asst_outcome_required_ind    IN   assessment_types.asst_outcome_required_ind%TYPE,
                                     p_asst_approval_required_ind   IN   assessment_types.asst_approval_required_ind%TYPE,
                                     p_asst_generate_outcome_ind    IN   assessment_types.asst_generate_outcome_ind%TYPE,
                                     p_asst_override_allowed_ind    IN   assessment_types.asst_override_allowed_ind%TYPE,
                                     p_asst_display_sequence        IN   assessment_types.asst_display_sequence%TYPE,
                                     p_asst_gque_reference          IN   assessment_types.asst_gque_reference%TYPE)
IS

BEGIN
   MERGE INTO assessment_types tgt
   USING (SELECT p_asst_code asst_code,
                 p_asst_type asst_type,
                 p_asst_description asst_description,
                 p_asst_review_period_unit asst_review_period_unit,
                 p_asst_review_period_qty asst_review_period_qty,
                 p_asst_allow_person_ind asst_allow_person_ind,
                 p_asst_outcome_required_ind asst_outcome_required_ind,
                 p_asst_approval_required_ind asst_approval_required_ind,
                 p_asst_generate_outcome_ind asst_generate_outcome_ind,
                 p_asst_override_allowed_ind asst_override_allowed_ind,
                 p_asst_display_sequence asst_display_sequence,
                 p_asst_gque_reference asst_gque_reference
            FROM dual) src
      ON (tgt.asst_code = src.asst_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.asst_type = src.asst_type,
              tgt.asst_description = src.asst_description,
              tgt.asst_review_period_unit = src.asst_review_period_unit,
              tgt.asst_review_period_qty = src.asst_review_period_qty,
              tgt.asst_allow_person_ind = src.asst_allow_person_ind,
              tgt.asst_outcome_required_ind = src.asst_outcome_required_ind,
              tgt.asst_approval_required_ind = src.asst_approval_required_ind,
              tgt.asst_generate_outcome_ind = src.asst_generate_outcome_ind,
              tgt.asst_override_allowed_ind = src.asst_override_allowed_ind,
              tgt.asst_display_sequence = src.asst_display_sequence,
              tgt.asst_gque_reference = src.asst_gque_reference
    WHEN NOT MATCHED THEN
       INSERT(asst_code,
              asst_type,
              asst_description,
              asst_review_period_unit,
              asst_review_period_qty,
              asst_allow_person_ind,
              asst_outcome_required_ind,
              asst_approval_required_ind,
              asst_generate_outcome_ind,
              asst_override_allowed_ind,
              asst_display_sequence,
              asst_gque_reference)
       VALUES(src.asst_code,
              src.asst_type,
              src.asst_description,
              src.asst_review_period_unit,
              src.asst_review_period_qty,
              src.asst_allow_person_ind,
              src.asst_outcome_required_ind,
              src.asst_approval_required_ind,
              src.asst_generate_outcome_ind,
              src.asst_override_allowed_ind,
              src.asst_display_sequence,
              src.asst_gque_reference);
END cre_asst;
/

CREATE OR REPLACE PROCEDURE cre_gque(p_gque_reference             IN   generic_questions.gque_reference%TYPE,
                                     p_gque_class                 IN   generic_questions.gque_class%TYPE,
                                     p_gque_text                  IN   generic_questions.gque_text%TYPE,
                                     p_gque_current_ind           IN   generic_questions.gque_current_ind%TYPE,
                                     p_gque_show_summary_ind      IN   generic_questions.gque_show_summary_ind%TYPE,
                                     p_gque_summary_display_seq   IN   generic_questions.gque_summary_display_seq%TYPE,
                                     p_gque_data_type             IN   generic_questions.gque_data_type%TYPE,
                                     p_gque_summary_description   IN   generic_questions.gque_summary_description%TYPE,
                                     p_gque_code                  IN   generic_questions.gque_code%TYPE,
                                     p_gque_type                  IN   generic_questions.gque_type%TYPE,
                                     p_gque_obj_name              IN   generic_questions.gque_obj_name%TYPE,
                                     p_gque_display_online_ind    IN   generic_questions.gque_display_online_ind%TYPE,
                                     p_gque_recalculate_ind       IN   generic_questions.gque_recalculate_ind%TYPE)
IS

BEGIN
   MERGE INTO generic_questions tgt
   USING (SELECT p_gque_reference gque_reference,
                 p_gque_class gque_class,
                 p_gque_text gque_text,
                 p_gque_current_ind gque_current_ind,
                 p_gque_show_summary_ind gque_show_summary_ind,
                 p_gque_summary_display_seq gque_summary_display_seq,
                 p_gque_data_type gque_data_type,
                 p_gque_summary_description gque_summary_description,
                 p_gque_code gque_code,
                 p_gque_type gque_type,
                 p_gque_obj_name gque_obj_name,
                 p_gque_display_online_ind gque_display_online_ind,
                 p_gque_recalculate_ind gque_recalculate_ind
            FROM dual) src
      ON (tgt.gque_reference = src.gque_reference)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.gque_class = src.gque_class,
              tgt.gque_text = src.gque_text,
              tgt.gque_current_ind = src.gque_current_ind,
              tgt.gque_show_summary_ind = src.gque_show_summary_ind,
              tgt.gque_summary_display_seq = src.gque_summary_display_seq,
              tgt.gque_data_type = src.gque_data_type,
              tgt.gque_summary_description = src.gque_summary_description,
              tgt.gque_code = src.gque_code,
              tgt.gque_type = src.gque_type,
              tgt.gque_obj_name = src.gque_obj_name,
              tgt.gque_display_online_ind = src.gque_display_online_ind,
              tgt.gque_recalculate_ind = src.gque_recalculate_ind
    WHEN NOT MATCHED THEN
       INSERT(gque_reference,
              gque_class,
              gque_text,
              gque_current_ind,
              gque_show_summary_ind,
              gque_summary_display_seq,
              gque_data_type,
              gque_summary_description,
              gque_code,
              gque_type,
              gque_obj_name,
              gque_display_online_ind,
              gque_recalculate_ind)
       VALUES(src.gque_reference,
              src.gque_class,
              src.gque_text,
              src.gque_current_ind,
              src.gque_show_summary_ind,
              src.gque_summary_display_seq,
              src.gque_data_type,
              src.gque_summary_description,
              src.gque_code,
              src.gque_type,
              src.gque_obj_name,
              src.gque_display_online_ind,
              src.gque_recalculate_ind);
END cre_gque;
/

CREATE OR REPLACE PROCEDURE cre_gqgp(p_gqgp_code          IN   generic_question_groups.gqgp_code%TYPE,
                                     p_gqgp_description   IN   generic_question_groups.gqgp_description%TYPE)
IS

BEGIN
   MERGE INTO generic_question_groups tgt
   USING (SELECT p_gqgp_code gqgp_code,
                 p_gqgp_description gqgp_description
            FROM dual) src
      ON (tgt.gqgp_code = src.gqgp_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.gqgp_description = src.gqgp_description
    WHEN NOT MATCHED THEN
       INSERT(gqgp_code,
              gqgp_description)
       VALUES(src.gqgp_code,
              src.gqgp_description);
END cre_gqgp;
/

CREATE OR REPLACE PROCEDURE cre_gqgu(p_gqgu_refno        IN   generic_question_grp_usages.gqgu_refno%TYPE,
                                     p_gqgu_gqgp_code    IN   generic_question_grp_usages.gqgu_gqgp_code%TYPE,
                                     p_gqgu_start_ind    IN   generic_question_grp_usages.gqgu_start_ind%TYPE,
                                     p_gqgu_asst_code    IN   generic_question_grp_usages.gqgu_asst_code%TYPE,
                                     p_gqgu_gqgu_refno   IN   generic_question_grp_usages.gqgu_gqgu_refno%TYPE)
IS

BEGIN
   MERGE INTO generic_question_grp_usages tgt
   USING (SELECT p_gqgu_refno gqgu_refno,
                 p_gqgu_gqgp_code gqgu_gqgp_code,
                 p_gqgu_start_ind gqgu_start_ind,
                 p_gqgu_asst_code gqgu_asst_code,
                 p_gqgu_gqgu_refno gqgu_gqgu_refno
            FROM dual) src
      ON (tgt.gqgu_refno = src.gqgu_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.gqgu_gqgp_code = src.gqgu_gqgp_code,
              tgt.gqgu_start_ind = src.gqgu_start_ind,
              tgt.gqgu_asst_code = src.gqgu_asst_code,
              tgt.gqgu_gqgu_refno = src.gqgu_gqgu_refno
    WHEN NOT MATCHED THEN
       INSERT(gqgu_refno,
              gqgu_gqgp_code,
              gqgu_start_ind,
              gqgu_asst_code,
              gqgu_gqgu_refno)
       VALUES(src.gqgu_refno,
              src.gqgu_gqgp_code,
              src.gqgu_start_ind,
              src.gqgu_asst_code,
              src.gqgu_gqgu_refno);
END cre_gqgu;
/

CREATE OR REPLACE PROCEDURE cre_gqgm(p_gqgm_gqgp_code        IN   generic_question_grp_members.gqgm_gqgp_code%TYPE,
                                     p_gqgm_gque_reference   IN   generic_question_grp_members.gqgm_gque_reference%TYPE,
                                     p_gqgm_mandatory_ind    IN   generic_question_grp_members.gqgm_mandatory_ind%TYPE,
                                     p_gqgm_display_seq      IN   generic_question_grp_members.gqgm_display_seq%TYPE)
IS

BEGIN
   MERGE INTO generic_question_grp_members tgt
   USING (SELECT p_gqgm_gqgp_code gqgm_gqgp_code,
                 p_gqgm_gque_reference gqgm_gque_reference,
                 p_gqgm_mandatory_ind gqgm_mandatory_ind,
                 p_gqgm_display_seq gqgm_display_seq
            FROM dual) src
      ON (    tgt.gqgm_gqgp_code = src.gqgm_gqgp_code
          AND tgt.gqgm_gque_reference = src.gqgm_gque_reference)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.gqgm_mandatory_ind = src.gqgm_mandatory_ind,
              tgt.gqgm_display_seq = src.gqgm_display_seq
    WHEN NOT MATCHED THEN
       INSERT(gqgm_gqgp_code,
              gqgm_gque_reference,
              gqgm_mandatory_ind,
              gqgm_display_seq)
       VALUES(src.gqgm_gqgp_code,
              src.gqgm_gque_reference,
              src.gqgm_mandatory_ind,
              src.gqgm_display_seq);
END cre_gqgm;
/

CREATE OR REPLACE PROCEDURE cre_gdqu(p_gdqu_refno            IN   generic_derived_question_usage.gdqu_refno%TYPE,
                                     p_gdqu_dqh_refno        IN   generic_derived_question_usage.gdqu_dqh_refno%TYPE,
                                     p_gdqu_gque_reference   IN   generic_derived_question_usage.gdqu_gque_reference%TYPE)
IS

BEGIN
   MERGE INTO generic_derived_question_usage tgt
   USING (SELECT p_gdqu_refno gdqu_refno,
                 p_gdqu_dqh_refno gdqu_dqh_refno,
                 p_gdqu_gque_reference gdqu_gque_reference
            FROM dual) src
      ON (tgt.gdqu_refno = src.gdqu_refno)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.gdqu_dqh_refno = src.gdqu_dqh_refno,
              tgt.gdqu_gque_reference = src.gdqu_gque_reference
    WHEN NOT MATCHED THEN
       INSERT(gdqu_refno,
              gdqu_dqh_refno,
              gdqu_gque_reference)
       VALUES(src.gdqu_refno,
              src.gdqu_dqh_refno,
              src.gdqu_gque_reference);
END cre_gdqu;
/

CREATE OR REPLACE PROCEDURE cre_gcre(p_gcre_gque_reference   IN   generic_coded_responses.gcre_gque_reference%TYPE,
                                     p_gcre_code             IN   generic_coded_responses.gcre_code%TYPE,
                                     p_gcre_description      IN   generic_coded_responses.gcre_description%TYPE,
                                     p_gcre_display_seq      IN   generic_coded_responses.gcre_display_seq%TYPE)
IS

BEGIN
   MERGE INTO generic_coded_responses tgt
   USING (SELECT p_gcre_gque_reference gcre_gque_reference,
                 p_gcre_code gcre_code,
                 p_gcre_description gcre_description,
                 p_gcre_display_seq gcre_display_seq
            FROM dual) src
      ON (    tgt.gcre_gque_reference = src.gcre_gque_reference
          AND tgt.gcre_code = src.gcre_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.gcre_description = src.gcre_description,
              tgt.gcre_display_seq = src.gcre_display_seq
    WHEN NOT MATCHED THEN
       INSERT(gcre_gque_reference,
              gcre_code,
              gcre_description,
              gcre_display_seq)
       VALUES(src.gcre_gque_reference,
              src.gcre_code,
              src.gcre_description,
              src.gcre_display_seq);
END cre_gcre;
/

CREATE OR REPLACE PROCEDURE cre_ato(p_ato_asst_code              IN   assessment_type_outcomes.ato_asst_code%TYPE,
                                    p_ato_code                   IN   assessment_type_outcomes.ato_code%TYPE,
                                    p_ato_description            IN   assessment_type_outcomes.ato_description%TYPE,
                                    p_ato_current_ind            IN   assessment_type_outcomes.ato_current_ind%TYPE,
                                    p_ato_minimum_answer_value   IN   assessment_type_outcomes.ato_minimum_answer_value%TYPE,
                                    p_ato_maximum_answer_value   IN   assessment_type_outcomes.ato_maximum_answer_value%TYPE,
                                    p_ato_display_sequence       IN   assessment_type_outcomes.ato_display_sequence%TYPE)
IS

BEGIN
   MERGE INTO assessment_type_outcomes tgt
   USING (SELECT p_ato_asst_code ato_asst_code,
                 p_ato_code ato_code,
                 p_ato_description ato_description,
                 p_ato_current_ind ato_current_ind,
                 p_ato_minimum_answer_value ato_minimum_answer_value,
                 p_ato_maximum_answer_value ato_maximum_answer_value,
                 p_ato_display_sequence ato_display_sequence
            FROM dual) src
      ON (    tgt.ato_asst_code = src.ato_asst_code
          AND tgt.ato_code = src.ato_code)
    WHEN MATCHED THEN
       UPDATE
          SET tgt.ato_description = src.ato_description,
              tgt.ato_current_ind = src.ato_current_ind,
              tgt.ato_minimum_answer_value = src.ato_minimum_answer_value,
              tgt.ato_maximum_answer_value = src.ato_maximum_answer_value,
              tgt.ato_display_sequence = src.ato_display_sequence
    WHEN NOT MATCHED THEN
       INSERT(ato_asst_code,
              ato_code,
              ato_description,
              ato_current_ind,
              ato_minimum_answer_value,
              ato_maximum_answer_value,
              ato_display_sequence)
       VALUES(src.ato_asst_code,
              src.ato_code,
              src.ato_description,
              src.ato_current_ind,
              src.ato_minimum_answer_value,
              src.ato_maximum_answer_value,
              src.ato_display_sequence);
END cre_ato;
/

SELECT 'Start Time = '||to_char(SYSDATE, 'HH24:MI:SS') 
  FROM dual;
  
UPDATE org_portal_tiles SET opt_current_ind = 'N' WHERE opt_card_title_usr IN ('Referrals','Surveys');
UPDATE rehousing_lists SET rli_current_ind = 'Y' WHERE rli_code IN ('CHP','COOP');

exec insert_fr_values('PRI_SCHEME', 'PSCHP', 'CHP Priority Scheme', 15);
           
UPDATE business_reasons  SET bro_current_ind = 'Y' WHERE bro_code = 'ACCENQCHP';
 
REM INSERTING into QUESTION_GROUPS
MERGE INTO question_groups tgt
USING (SELECT 'FILT' qgr_code,
              'Filters' qgr_description,
              90 qgr_seqno,
              'DQ' qgr_question_category
         FROM dual) src
   ON (    tgt.qgr_code = src.qgr_code)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.qgr_description = src.qgr_description,
           tgt.qgr_seqno = src.qgr_seqno,
           tgt.qgr_question_category = src.qgr_question_category
 WHEN NOT MATCHED THEN
    INSERT(qgr_code,
           qgr_description,
           qgr_seqno,
           qgr_question_category) 
    VALUES(src.qgr_code,
           src.qgr_description,
           src.qgr_seqno,
           src.qgr_question_category);

REM INSERTING into QUESTIONS
exec create_question(351, 'DNQ', 'N', 'DQ', 'DQ', 'N', 'N', 'N', 'N', 'N', 'Y', 'N', 'N', 900, 900, '70', 'Minimum Registrant Age', 'Minimum Registrant Age','N',null);
exec create_question(357,'DNQ','N','DQ','DQ','N','N','N','N','N','Y','N','N',910,910,'70','Maximum Registrant Age','Maximum Registrant Age','N',null);
exec create_question(362,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',920,920,'70','Unity Approved Supports','Unity Approved Supports','N',null);
exec create_question(366,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',930,930,'70','Applicant Gender','Applicant Gender','N',null);
exec create_question(376,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',940,940,'70','Applicant Relationship Status','Applicant relationship  status','N',null);
exec create_question(380,'DNQ','T','DQ','DQ','Y','N','N','N','N','Y','N','N',950,950,'70','Applicants Country of Birth','Applicants country of birth','N',null);
exec create_question(384,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',960,960,'70','Applicant Language','Applicant language','N',null);
exec create_question(392,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',970,970,'70','Is Customer Homeless','Is customer Homeless?','N',null);
exec create_question(396,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',980,980,'70','Registered by PCO','Registered by PCO?','N',null);
exec create_question(402,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1400,1400,'90','Physical Disability','Physical Disability','N',null);
exec create_question(407,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1410,403,'90','Visual Impairment','Visual Impairment','N',null);
exec create_question(413,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1420,1420,'90','Hearing Impairment','Hearing Impairment','N',null);
exec create_question(417,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',1000,413,'70','Eligible for ABCOM','Eligible for ABCOM','N',null);
exec create_question(418,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1430,1430,'90','Mental Health','Mental Health','N',null);
exec create_question(422,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',1010,418,'70','Eligible for CHP','Eligible for CHP','N',null);
exec create_question(423,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1440,1440,'90','Intellectual disability','Intellectual disability','N',null);
exec create_question(427,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',1020,423,'70','Eligible for COOP','Eligible for COOP','N',null);
exec create_question(428,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1450,1450,'90','Acquired brain injury','Acquired brain injury','N',null);
exec create_question(432,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',1030,428,'70','Eligible for MX','Eligible for MX','N',null);
exec create_question(433,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1460,1460,'90','Other Special Need','Other Special Need','N',null);
exec create_question(437,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',1040,433,'70','Eligible for TR','Eligible for TR','N',null);
exec create_question(439,'DNQ','T','FILT','DQ','Y','N','N','N','N','Y','N','N',1470,1470,'90','DSP / TPI (Appl)','DSP / TPI (Applicant)','N',null);
exec create_question(443,'DNQ','T','FILT','DQ','Y','N','N','N','N','Y','N','N',1480,1480,'90','Aged Pension (Appl)','Aged Pension  (Applicant)','N',null);
exec create_question(448,'DNQ','T','FILT','DQ','Y','N','N','N','N','Y','N','N',1490,1490,'90','Parenting Payment / FTB (Appl)','Parenting Payment / FTB (Applicant)','N',null);
exec create_question(452,'DNQ','T','FILT','DQ','Y','N','N','N','N','Y','N','N',1500,1500,'90','Austudy / Abstudy (Appl)','Austudy / Abstudy (Applicant)','N',null);
exec create_question(456,'DNQ','T','FILT','DQ','Y','N','N','N','N','Y','N','N',1510,1510,'90','Youth Allowance (Appl)','Youth Allowance (Applicant)','N',null);
exec create_question(460,'DNQ','T','FILT','DQ','Y','N','N','N','N','Y','N','N',1520,1520,'90','Newstart (Appl)','Newstart (Applicant)','N',null);
exec create_question(464,'DNQ','T','FILT','DQ','Y','N','N','N','N','Y','N','N',1530,1530,'90','Carer''s Payment (Appl)','Carer''s Payment (Applicant)','N',null);
exec create_question(468,'DNQ','T','FILT','DQ','Y','N','N','N','N','Y','N','N',1540,1540,'90','Other Gov Income (Appl)','Other Gov Income (Applicant)','N',null);
exec create_question(472,'DNQ','N','FILT','DQ','N','N','N','N','N','Y','N','N',1550,1550,'90','Household Minimum Age','Household minimum age','N',null);
exec create_question(476,'DNQ','N','FILT','DQ','N','N','N','N','N','Y','N','N',1560,1560,'90','Household Maximum Age','Household maximum age','N',null);
exec create_question(481,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1570,1570,'90','Hmless or Inadequately Housed','Homeless or Inadequately Housed','N',null);
exec create_question(485,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1580,1580,'90','Does not have dogs','Does not have dogs','N',null);
exec create_question(489,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1590,1590,'90','Does not have cats','Does not have cats','N',null);
exec create_question(493,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1600,1600,'90','Does not have birds','Does not have birds','N',null);
exec create_question(497,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1610,1610,'90','Does not have other pets','Does not have other pets','N',null);
exec create_question(501,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1620,1620,'90','Carparking access','Does the customer need carparking access?','N',null);
exec create_question(505,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1630,1630,'90','Co-op Management Skills','Co-op Management Skills','N',null);
exec create_question(509,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1640,1640,'90','Co-op Management Soft Skills','Co-op Management Soft Skills','N',null);
exec create_question(521,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',5000,5000,'1000','**Main Applicant filters**','**Main Applicant filters**','N',null);
exec create_question(526,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1650,1650,'90','Existing Support Provider','Existing Support Provider','N',null);
exec create_question(532,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',1670,1670,'1000','**Special Needs filters**','**Special Needs filters**','N',null);
exec create_question(536,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1680,1680,'90','Any Special Needs','Does any household member have any special needs?','N',null);
exec create_question(542,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',1690,1690,'1000','**Applicant Income filters**','**Applicant Income filters**','N',null);
exec create_question(546,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1710,1710,'90','DVA (Appl)','DVA (Applicant)','N',null);
exec create_question(551,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',1720,1720,'1000','**Household Income filters**','**Household Income filters**','N',null);
exec create_question(555,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1730,1730,'90','DSP / TPI (HH)','DSP / TPI (HH)','N',null);
exec create_question(559,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1740,1740,'90','Aged Pension  (HH)','Aged Pension  (HH)','N',null);
exec create_question(563,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1750,1750,'90','Parenting Payment / FTB (HH)','Parenting Payment / FTB (HH)','N',null);
exec create_question(567,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1760,1760,'90','Austudy / Abstudy (HH)','Austudy / Abstudy (HH)','N',null);
exec create_question(572,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1770,1770,'90','Youth Allowance (HH)','Youth Allowance (HH)','N',null);
exec create_question(576,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1780,1780,'90','Newstart  (HH)','Newstart  (HH)','N',null);
exec create_question(580,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1790,1790,'90','Carer''s Payment (HH)','Carer''s Payment (HH)','N',null);
exec create_question(584,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1800,1800,'90','Other Gov Income (HH)','Other Gov Income (HH)','N',null);
exec create_question(588,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1810,1810,'90','DVA (HH)','DVA (HH)','N',null);
exec create_question(595,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',1820,1820,'1000','** HH composition filters **','** household composition filters **','N',null);
exec create_question(600,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',1830,1830,'1000','** current housing filters **','** current housing filters **','N',null);
exec create_question(604,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1840,1840,'90','Homeless','Is the customer homeless?','N',null);
exec create_question(608,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1850,1850,'90','Homeless or Inad Housed','Homeless or Inadequately Housed','N',null);
exec create_question(613,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',1870,1870,'1000','** Pets filters **','** Pets filters **','N',null);
exec create_question(620,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',1180,1880,'1000','** Housing pref filters **','** housing preferences filters **','N',null);
exec create_question(624,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1920,1920,'90','SDA','Specialist Disability Accommodation','N',null);
exec create_question(630,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',1930,1930,'1000','** skills AND courses **','** skills AND courses filters **','N',null);
exec create_question(634,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1940,1940,'90','Administration / Secretarial','Administration / secretarial','N',null);
exec create_question(638,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1950,1950,'90','Bookkeeping','Bookkeeping?','N',null);
exec create_question(642,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1960,1960,'90','Financial / Accounting','Financial / Accounting','N',null);
exec create_question(646,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1970,1970,'90','Computer / IT','Computer / IT','N',null);
exec create_question(650,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1980,1980,'90','Artist','Artist','N',null);
exec create_question(654,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',1990,1990,'90','Conflict Management','Conflict management','N',null);
exec create_question(661,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',2001,2001,'90','Environmental Awareness','Environmental awareness','N',null);
exec create_question(668,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',2011,2011,'90','Maintenance / Building Trades','Maintenance / building trades','N',null);
exec create_question(676,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',2021,2021,'90','Organisational skills','Organisational skills','N',null);
exec create_question(680,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',2031,2031,'90','Communication / Interpersonal','Communication / interpersonal','N',null);
exec create_question(684,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',2041,2041,'90','Other','Other','N',null);
exec create_question(688,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',2051,2051,'90','Co-op Management Skills','Co-op Management Skills','N',null);
exec create_question(692,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',2061,2061,'90','Co-op Management Soft Skills','Co-op Management Soft Skills','N',null);
exec create_question(696,'PQU','Y','SHR','HR','N','N','N','N','Y','N','N','N',70,2071,'15','Carparking access','Does the customer need housing that has carparking access?','N',null);
exec create_question(700,'PQU','Y','SHR','HR','N','N','N','N','Y','N','N','N',152,697,'15','Disability Modifications','Are mods needed for a disability or medical condition?','N',null);
exec create_question(737,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',800,732,'1000','Applicant Gender','Applicant Gender','N',null);
exec create_question(742,'PQU','C','END','OS','N','N','N','N','N','Y','N','N',810,738,'1000','Applicant relationship status','Applicant relationship status','N',null);
exec create_question(747,'DNQ','N','DQ','DQ','N','N','N','N','N','Y','N','N',1355,743,'70','Category - CHP','Category - CHP','N',null);
exec create_question(778,'DNQ','N','DQ','DQ','N','N','N','N','N','Y','N','N',1356,774,'70','Category COOP','Category - COOP','N',null);
exec create_question(759,'DNQ','D','DQ','DQ','N','N','N','N','N','Y','N','N',222,2155,'70','Category Start Date - COOP','Category Start Date - COOP','N',null);
exec create_question(766,'PQU','Y','CIRC','CC','N','N','N','N','Y','N','N','N',170,760,'7','CHP Reloc or transfer','Is this a relocation or transfer for another provider?','N',null);
exec create_question(786,'DNQ','T','CHPP','DQ','N','N','N','N','N','Y','N','N',430,430,'80','Amelie Housing','Amelie Housing','N',null);
exec create_question(822,'DNQ','T','CHPP','DQ','N','N','N','N','N','Y','N','N',440,440,'80','YourPlace Housing Pty Ltd','YourPlace Housing Pty Ltd','N',null);
exec create_question(823,'DNQ','T','CHPP','DQ','N','N','N','N','N','Y','N','N',450,450,'80','Uniting SA Housing Ltd.','Uniting SA Housing Ltd.','N',null);
exec create_question(824,'DNQ','T','CHPP','DQ','N','N','N','N','N','Y','N','N',460,460,'80','Uniting Country Housing','Uniting Country Housing','N',null);
exec create_question(773,'DNQ','T','CHPP','DQ','N','N','N','N','N','Y','N','N',85,217,'80','COOP - Common Equity Hse SA','COOP - Common Equity Hse SA Ltd','N',null);

exec cre_derived_question_clauses(20000,42,'AND exists (select null from lettings_area_answers WHERE laa_que_refno = 1 AND laa_char_value = ''Y'' AND laa_lar_code in (''COPP'',''PARF'',''PERC'',''PHOE'',''POND'',''PORR'',''SLOL'',''YOCH'',''PEAC'', ''ACRE'', ''ISHA'') AND laa_app_refno = app_refno)');
exec cre_derived_question_clauses(20000,43,'AND exists (select null from lettings_area_answers WHERE laa_que_refno = 1 AND laa_char_value = ''Y'' AND laa_lar_code = ''AMEH'' AND laa_app_refno = app_refno)');
exec cre_derived_question_clauses(20000,44,'AND exists (select null from lettings_area_answers WHERE laa_que_refno = 1 AND laa_char_value = ''Y'' AND laa_lar_code = ''YOUR'' AND laa_app_refno = app_refno)');
exec cre_derived_question_clauses(20000,45,'AND exists (select null from lettings_area_answers WHERE laa_que_refno = 1 AND laa_char_value = ''Y'' AND laa_lar_code = ''UNSA'' AND laa_app_refno = app_refno)');
exec cre_derived_question_clauses(20000,46,'AND exists (select null from lettings_area_answers WHERE laa_que_refno = 1 AND laa_char_value = ''Y'' AND laa_lar_code = ''UNCH'' AND laa_app_refno = app_refno)');

exec cre_derived_question_usage(20000,773,42);
exec cre_derived_question_usage(20000,773,null);
exec cre_derived_question_usage(20000,786,43);
exec cre_derived_question_usage(20000,786,null);
exec cre_derived_question_usage(20000,822,44);
exec cre_derived_question_usage(20000,822,null);
exec cre_derived_question_usage(20000,823,45);
exec cre_derived_question_usage(20000,823,null);
exec cre_derived_question_usage(20000,824,46);
exec cre_derived_question_usage(20000,824,null);

UPDATE questions SET que_question_description = 'Bookkeeping' WHERE que_refno = 257;
 
REM INSERTING into QUESTION_PERMITTED_RESPONSES
exec cre_que_perm_resp('1585','Male',737,'M');
exec cre_que_perm_resp('1586','Female',737,'F');
exec cre_que_perm_resp('1587','Other',737,'O');
exec cre_que_perm_resp('1588','Partnered',742,'P');
exec cre_que_perm_resp('1589','Single',742,'S');

UPDATE question_permitted_responses SET qpr_description = 'SAHT/CHP error' WHERE qpr_code = 'BENEFITERR' AND qpr_que_refno = 965;
   
REM INSERTING into FIRST_REF_VALUES
exec insert_fr_values('ELG_SCHEME','ESTR','TR eligibility scheme',60);
exec insert_fr_values('ELG_SCHEME','ESMX','MX eligibility scheme',50);
exec insert_fr_values('ELG_SCHEME','ESCHP','CHP eligibility scheme',30);
exec insert_fr_values('ELG_SCHEME','ESCOOP','COOP eligibility scheme',40);

MERGE INTO hhold_type_elig_schemes tgt
USING (SELECT hhts_hty_code, 
              frv_code hhts_hrv_els_code, 
              hhts_current_ind, 
              hhts_heg_code
         FROM hhold_type_elig_schemes hhts
        CROSS JOIN first_ref_values frv 
        WHERE hhts_hrv_els_code = 'ESGEN'
          AND frv.frv_frd_domain = 'ELG_SCHEME'
          AND frv.frv_code NOT IN ('ESGEN','ESAB')
        ORDER BY 2,4,1) src
   ON (    src.hhts_hrv_els_code = tgt.hhts_hrv_els_code
       AND src.hhts_hty_code = tgt.hhts_hty_code)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.hhts_current_ind = src.hhts_current_ind,
           tgt.hhts_heg_code = src.hhts_heg_code
 WHEN NOT MATCHED THEN
    INSERT(hhts_hty_code, 
           hhts_hrv_els_code, 
           hhts_current_ind, 
           hhts_heg_code)
    VALUES(src.hhts_hty_code, 
           src.hhts_hrv_els_code, 
           src.hhts_current_ind, 
           src.hhts_heg_code);
           
MERGE INTO elig_criterias tgt
USING (SELECT ecr_apt_code, 
              frv_code ecr_hhts_hrv_els_code, 
              ecr_hhts_hty_code
         FROM elig_criterias 
        CROSS JOIN first_ref_values frv
        WHERE ecr_hhts_hrv_els_code = 'ESGEN'
          AND frv_frd_domain = 'ELG_SCHEME'
          AND frv_code not in ('ESGEN','ESAB')
        ORDER BY 2, 3, 1) src
   ON (    src.ecr_apt_code = tgt.ecr_apt_code
       AND src.ecr_hhts_hrv_els_code = tgt.ecr_hhts_hrv_els_code
       AND src.ecr_hhts_hty_code = tgt.ecr_hhts_hty_code)
 WHEN NOT MATCHED THEN
    INSERT(ecr_apt_code, 
           ecr_hhts_hrv_els_code, 
           ecr_hhts_hty_code)
    VALUES(src.ecr_apt_code, 
           src.ecr_hhts_hrv_els_code, 
           src.ecr_hhts_hty_code);

SPOO op_accg_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * from applic_cat_rule_conditions WHERE accg_acpr_acps_hrv_prs_code IN ('PSCOOP','PSCHP');
DELETE FROM applic_cat_rule_conditions WHERE accg_acpr_acps_hrv_prs_code IN ('PSCOOP','PSCHP');

SPOO op_acpr_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM applic_cat_pri_sch_rules WHERE acpr_acps_hrv_prs_code IN ('PSCOOP','PSCHP');
DELETE FROM applic_cat_pri_sch_rules WHERE acpr_acps_hrv_prs_code IN ('PSCOOP','PSCHP');

SPOO op_acpr_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM applic_cat_pri_sch_rules WHERE acpr_acps_hrv_prs_code IN ('PSCOOP','PSCHP');
DELETE FROM applic_category_pri_schemes WHERE acps_hrv_prs_code IN ('PSCOOP','PSCHP');

SPOO op_config_install.log APPEND
SET LONG 32000
SET LINESIZE 32000
exec cre_acpg('CHPCAT','CHP Category','Y',10);
exec cre_acpg('COOPCAT','COOP Category','Y',20);

exec cre_acrq('CHPCAT',747,20,20);
exec cre_acrq('COOPCAT',778,30,30);

REM INSERTING into APPLIC_CATEGORY_PRI_SCHEMES
exec cre_acps('PSCHP','CAT1','CHPCAT');
exec cre_acps('PSCHP','CAT2','CHPCAT');
exec cre_acps('PSCHP','CAT3','CHPCAT');
exec cre_acps('PSCOOP','CAT1','COOPCAT');
exec cre_acps('PSCOOP','CAT2','COOPCAT');
exec cre_acps('PSCOOP','CAT3','COOPCAT');

REM INSERTING into APPLIC_CAT_PRI_SCH_RULES
exec cre_acpr('PSCHP','CAT1',747,10,10);
exec cre_acpr('PSCHP','CAT2',747,10,10);
exec cre_acpr('PSCHP','CAT3',747,10,10);
exec cre_acpr('PSCOOP','CAT1',778,10,10);
exec cre_acpr('PSCOOP','CAT2',778,10,10);
exec cre_acpr('PSCOOP','CAT3',778,10,10);

REM INSERTING into APPLIC_CAT_RULE_CONDITIONS
exec cre_accg(2,'CNPR',null,null,'PSCOOP','CAT1',778,1,1);
exec cre_accg(3,'CNPR',null,null,'PSCOOP','CAT2',778,2,2);
exec cre_accg(4,'CNPR',null,null,'PSCOOP','CAT3',778,3,3);
exec cre_accg(6,'CNPR',null,null,'PSCHP','CAT1',747,1,1);
exec cre_accg(8,'CNPR',null,null,'PSCHP','CAT2',747,2,2);
exec cre_accg(10,'CNPR',null,null,'PSCHP','CAT3',747,3,3);
exec cre_accg(18,'CNPR','CHPCAT',747,null,null,null,1,5);
exec cre_accg(19,'CNPR','COOPCAT',778,null,null,null,1,5);

REM INSERTING new shortlists
exec cre_slc('AMEH','Amelie Housing',9);
exec cre_slc('UNSA','Uniting SA Housing Ltd.',9);
exec cre_slc('UNCH','Uniting Country Housing',9);
exec cre_slc('YOUR','YourPlace Housing Pty Ltd.',9);
exec cre_slc('CEHC','COOP - Common Equity Hse SA Ltd',9);
exec cre_slc('CEHC','COOP - Common Equity Hse SA Ltd',9);

exec cre_scg('AMEH','CAT1',2);
exec cre_scg('AMEH','CAT2',4);
exec cre_scg('AMEH','CAT3',6);
exec cre_scg('UNCH','CAT1',2);
exec cre_scg('UNCH','CAT2',4);
exec cre_scg('UNCH','CAT3',6);
exec cre_scg('UNSA','CAT1',2);
exec cre_scg('UNSA','CAT2',4);
exec cre_scg('UNSA','CAT3',6);
exec cre_scg('YOUR','CAT1',2);
exec cre_scg('YOUR','CAT2',4);
exec cre_scg('YOUR','CAT3',6);
exec cre_scg('CEHC','CAT1',2);
exec cre_scg('CEHC','CAT2',2);
exec cre_scg('CEHC','CAT3',2);

MERGE INTO slist_alloc_prop_lists tgt
USING (SELECT slc_code spl_slc_code, 
              spl_apt_code, 
              spl_generate_ind 
         FROM slist_alloc_prop_lists s1 
        CROSS JOIN slist_categories
        WHERE spl_slc_code = 'ACAC' 
          AND slc_code IN ('AMEH','UNSA','CEHC','UNCH','YOUR')) src
   ON (    tgt.spl_slc_code = src.spl_slc_code
       AND tgt.spl_apt_code = src.spl_apt_code)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.spl_generate_ind = src.spl_generate_ind
 WHEN NOT MATCHED THEN
    INSERT(spl_slc_code, 
           spl_apt_code, 
           spl_generate_ind)
    VALUES(src.spl_slc_code, 
           src.spl_apt_code, 
           src.spl_generate_ind);

REM INSERTING into DFLT_QUESTION_RESTRICTS
spoo op_dflt_vals.log
SET LONG 32000
SET LINESIZE 32000
SET PAGES 0
SELECT * FROM dflt_question_restricts;
DELETE FROM dflt_question_restricts;

spoo op_config_install.log APPEND
exec cre_dqr(845,5,'Y',203,'ACAC','=',null,'Y');
exec cre_dqr(4550,10,'N',521,'ACAC',null,null,null);
exec cre_dqr(2089,20,'N',351,'ACAC','>=',null,null);
exec cre_dqr(2104,25,'N',357,'ACAC','<=',null,null);
exec cre_dqr(2569,50,'N',526,'ACAC','=',null,'Y');
exec cre_dqr(4747,65,'N',737,'ACAC','=',null,null);
exec cre_dqr(4748,70,'N',742,'ACAC','=',null,null);
exec cre_dqr(2149,80,'N',380,'ACAC','=',null,null);
exec cre_dqr(2164,90,'N',384,'ACAC','=',null,null);
exec cre_dqr(3019,100,'N',975,'ACAC','=',null,'Y');
exec cre_dqr(4549,110,'N',532,'ACAC',null,null,null);
exec cre_dqr(2584,120,'N',536,'ACAC','=',null,'Y');
exec cre_dqr(2224,130,'N',402,'ACAC','=',null,'Y');
exec cre_dqr(2239,140,'N',407,'ACAC','=',null,'Y');
exec cre_dqr(2254,150,'N',413,'ACAC','=',null,'Y');
exec cre_dqr(2269,160,'N',418,'ACAC','=',null,'Y');
exec cre_dqr(2284,170,'N',423,'ACAC','=',null,'Y');
exec cre_dqr(2299,180,'N',428,'ACAC','=',null,'Y');
exec cre_dqr(2314,190,'N',433,'ACAC','=',null,'Y');
exec cre_dqr(4689,200,'N',542,'ACAC',null,null,null);
exec cre_dqr(2329,210,'N',439,'ACAC','=',null,'Y');
exec cre_dqr(2344,220,'N',443,'ACAC','=',null,'Y');
exec cre_dqr(2359,230,'N',448,'ACAC','=',null,'Y');
exec cre_dqr(2374,240,'N',452,'ACAC','=',null,'Y');
exec cre_dqr(2389,250,'N',456,'ACAC','=',null,'Y');
exec cre_dqr(2404,260,'N',460,'ACAC','=',null,'Y');
exec cre_dqr(2419,270,'N',464,'ACAC','=',null,'Y');
exec cre_dqr(2449,280,'N',468,'ACAC','=',null,'Y');
exec cre_dqr(2599,290,'N',546,'ACAC','=',null,'Y');
exec cre_dqr(4548,300,'N',551,'ACAC',null,null,null);
exec cre_dqr(2614,310,'N',555,'ACAC','=',null,'Y');
exec cre_dqr(2629,320,'N',559,'ACAC','=',null,'Y');
exec cre_dqr(2644,330,'N',563,'ACAC','=',null,'Y');
exec cre_dqr(2659,340,'N',567,'ACAC','=',null,'Y');
exec cre_dqr(2674,350,'N',572,'ACAC','=',null,'Y');
exec cre_dqr(2689,360,'N',576,'ACAC','=',null,'Y');
exec cre_dqr(2704,370,'N',580,'ACAC','=',null,'Y');
exec cre_dqr(2719,380,'N',584,'ACAC','=',null,'Y');
exec cre_dqr(2734,390,'N',588,'ACAC','=',null,'Y');
exec cre_dqr(4547,400,'N',595,'ACAC',null,null,null);
exec cre_dqr(2464,410,'N',472,'ACAC','>=',null,null);
exec cre_dqr(2479,420,'N',476,'ACAC','<=',null,null);
exec cre_dqr(4546,430,'N',600,'ACAC',null,null,null);
exec cre_dqr(2749,440,'N',604,'ACAC','=',null,'Y');
exec cre_dqr(2764,450,'N',608,'ACAC','=',null,'Y');
exec cre_dqr(4730,460,'N',404,'ACAC','=',null,null);
exec cre_dqr(4545,470,'N',613,'ACAC',null,null,null);
exec cre_dqr(2494,480,'N',485,'ACAC','=',null,'Y');
exec cre_dqr(2509,490,'N',489,'ACAC','=',null,'Y');
exec cre_dqr(2524,500,'N',493,'ACAC','=',null,'Y');
exec cre_dqr(2539,510,'N',497,'ACAC','=',null,'Y');
exec cre_dqr(4544,520,'N',620,'ACAC',null,null,null);
exec cre_dqr(2059,530,'N',45,'ACAC','=',null,'Y');
exec cre_dqr(2074,540,'N',52,'ACAC','=',null,'Y');
exec cre_dqr(2179,550,'N',387,'ACAC','=',null,'Y');
exec cre_dqr(2194,560,'N',391,'ACAC','=',null,'Y');
exec cre_dqr(3004,570,'N',700,'ACAC','=',null,'Y');
exec cre_dqr(2434,580,'N',465,'ACAC','=',null,'Y');
exec cre_dqr(2554,590,'N',507,'ACAC','=',null,'Y');
exec cre_dqr(2989,600,'N',696,'ACAC','=',null,'Y');
exec cre_dqr(2044,610,'N',37,'ACAC','=',null,'Y');
exec cre_dqr(2779,620,'N',624,'ACAC','=',null,'Y');
exec cre_dqr(4543,630,'N',630,'ACAC',null,null,null);
exec cre_dqr(2794,640,'N',634,'ACAC','=',null,'Y');
exec cre_dqr(2809,650,'N',638,'ACAC','=',null,'Y');
exec cre_dqr(2824,660,'N',642,'ACAC','=',null,'Y');
exec cre_dqr(2839,670,'N',646,'ACAC','=',null,'Y');
exec cre_dqr(2854,680,'N',650,'ACAC','=',null,'Y');
exec cre_dqr(2869,690,'N',654,'ACAC','=',null,'Y');
exec cre_dqr(2884,700,'N',661,'ACAC','=',null,'Y');
exec cre_dqr(2899,710,'N',668,'ACAC','=',null,'Y');
exec cre_dqr(2914,720,'N',676,'ACAC','=',null,'Y');
exec cre_dqr(2929,730,'N',680,'ACAC','=',null,'Y');
exec cre_dqr(2944,740,'N',684,'ACAC','=',null,'Y');
exec cre_dqr(2959,750,'N',688,'ACAC','=',null,'Y');
exec cre_dqr(2974,760,'N',692,'ACAC','=',null,'Y');
exec cre_dqr(846,5,'Y',206,'ACC2','=',null,'Y');
exec cre_dqr(4374,10,'N',521,'ACC2',null,null,null);
exec cre_dqr(3113,20,'N',351,'ACC2','>=',null,null);
exec cre_dqr(3139,25,'N',357,'ACC2','<=',null,null);
exec cre_dqr(3945,50,'N',526,'ACC2','=',null,'Y');
exec cre_dqr(4787,65,'N',737,'ACC2','=',null,null);
exec cre_dqr(4827,70,'N',742,'ACC2','=',null,null);
exec cre_dqr(3217,80,'N',380,'ACC2','=',null,null);
exec cre_dqr(3243,90,'N',384,'ACC2','=',null,null);
exec cre_dqr(4335,100,'N',975,'ACC2','=',null,'Y');
exec cre_dqr(4373,110,'N',532,'ACC2',null,null,null);
exec cre_dqr(3971,120,'N',536,'ACC2','=',null,'Y');
exec cre_dqr(3347,130,'N',402,'ACC2','=',null,'Y');
exec cre_dqr(3373,140,'N',407,'ACC2','=',null,'Y');
exec cre_dqr(3399,150,'N',413,'ACC2','=',null,'Y');
exec cre_dqr(3425,160,'N',418,'ACC2','=',null,'Y');
exec cre_dqr(3451,170,'N',423,'ACC2','=',null,'Y');
exec cre_dqr(3477,180,'N',428,'ACC2','=',null,'Y');
exec cre_dqr(3503,190,'N',433,'ACC2','=',null,'Y');
exec cre_dqr(4664,200,'N',542,'ACC2',null,null,null);
exec cre_dqr(3529,210,'N',439,'ACC2','=',null,'Y');
exec cre_dqr(3555,220,'N',443,'ACC2','=',null,'Y');
exec cre_dqr(3581,230,'N',448,'ACC2','=',null,'Y');
exec cre_dqr(3607,240,'N',452,'ACC2','=',null,'Y');
exec cre_dqr(3633,250,'N',456,'ACC2','=',null,'Y');
exec cre_dqr(3659,260,'N',460,'ACC2','=',null,'Y');
exec cre_dqr(3685,270,'N',464,'ACC2','=',null,'Y');
exec cre_dqr(3737,280,'N',468,'ACC2','=',null,'Y');
exec cre_dqr(3997,290,'N',546,'ACC2','=',null,'Y');
exec cre_dqr(4372,300,'N',551,'ACC2',null,null,null);
exec cre_dqr(4023,310,'N',555,'ACC2','=',null,'Y');
exec cre_dqr(4049,320,'N',559,'ACC2','=',null,'Y');
exec cre_dqr(4075,330,'N',563,'ACC2','=',null,'Y');
exec cre_dqr(4101,340,'N',567,'ACC2','=',null,'Y');
exec cre_dqr(4127,350,'N',572,'ACC2','=',null,'Y');
exec cre_dqr(4153,360,'N',576,'ACC2','=',null,'Y');
exec cre_dqr(4179,370,'N',580,'ACC2','=',null,'Y');
exec cre_dqr(4205,380,'N',584,'ACC2','=',null,'Y');
exec cre_dqr(4371,400,'N',595,'ACC2',null,null,null);
exec cre_dqr(3763,410,'N',472,'ACC2','>=',null,null);
exec cre_dqr(3789,420,'N',476,'ACC2','<=',null,null);
exec cre_dqr(4370,430,'N',600,'ACC2',null,null,null);
exec cre_dqr(4231,450,'N',608,'ACC2','=',null,'Y');
exec cre_dqr(4705,460,'N',404,'ACC2','=',null,null);
exec cre_dqr(4369,470,'N',613,'ACC2',null,null,null);
exec cre_dqr(3815,480,'N',485,'ACC2','=',null,'Y');
exec cre_dqr(3841,490,'N',489,'ACC2','=',null,'Y');
exec cre_dqr(3867,500,'N',493,'ACC2','=',null,'Y');
exec cre_dqr(3893,510,'N',497,'ACC2','=',null,'Y');
exec cre_dqr(4368,520,'N',620,'ACC2',null,null,null);
exec cre_dqr(3061,530,'N',45,'ACC2','=',null,'Y');
exec cre_dqr(3087,540,'N',52,'ACC2','=',null,'Y');
exec cre_dqr(3269,550,'N',387,'ACC2','=',null,'Y');
exec cre_dqr(3295,560,'N',391,'ACC2','=',null,'Y');
exec cre_dqr(4309,570,'N',700,'ACC2','=',null,'Y');
exec cre_dqr(3711,580,'N',465,'ACC2','=',null,'Y');
exec cre_dqr(3919,590,'N',507,'ACC2','=',null,'Y');
exec cre_dqr(4283,600,'N',696,'ACC2','=',null,'Y');
exec cre_dqr(3035,610,'N',37,'ACC2','=',null,'Y');
exec cre_dqr(4257,620,'N',624,'ACC2','=',null,'Y');
exec cre_dqr(848,5,'Y',212,'ANGS','=',null,'Y');
exec cre_dqr(4388,10,'N',521,'ANGS',null,null,null);
exec cre_dqr(3115,20,'N',351,'ANGS','>=',null,null);
exec cre_dqr(3141,25,'N',357,'ANGS','<=',null,null);
exec cre_dqr(3947,50,'N',526,'ANGS','=',null,'Y');
exec cre_dqr(4749,65,'N',737,'ANGS','=',null,null);
exec cre_dqr(4789,70,'N',742,'ANGS','=',null,null);
exec cre_dqr(3219,80,'N',380,'ANGS','=',null,null);
exec cre_dqr(3245,90,'N',384,'ANGS','=',null,null);
exec cre_dqr(4337,100,'N',975,'ANGS','=',null,'Y');
exec cre_dqr(4387,110,'N',532,'ANGS',null,null,null);
exec cre_dqr(3973,120,'N',536,'ANGS','=',null,'Y');
exec cre_dqr(3349,130,'N',402,'ANGS','=',null,'Y');
exec cre_dqr(3375,140,'N',407,'ANGS','=',null,'Y');
exec cre_dqr(3401,150,'N',413,'ANGS','=',null,'Y');
exec cre_dqr(3427,160,'N',418,'ANGS','=',null,'Y');
exec cre_dqr(3453,170,'N',423,'ANGS','=',null,'Y');
exec cre_dqr(3479,180,'N',428,'ANGS','=',null,'Y');
exec cre_dqr(3505,190,'N',433,'ANGS','=',null,'Y');
exec cre_dqr(4666,200,'N',542,'ANGS',null,null,null);
exec cre_dqr(3531,210,'N',439,'ANGS','=',null,'Y');
exec cre_dqr(3557,220,'N',443,'ANGS','=',null,'Y');
exec cre_dqr(3583,230,'N',448,'ANGS','=',null,'Y');
exec cre_dqr(3609,240,'N',452,'ANGS','=',null,'Y');
exec cre_dqr(3635,250,'N',456,'ANGS','=',null,'Y');
exec cre_dqr(3661,260,'N',460,'ANGS','=',null,'Y');
exec cre_dqr(3687,270,'N',464,'ANGS','=',null,'Y');
exec cre_dqr(3739,280,'N',468,'ANGS','=',null,'Y');
exec cre_dqr(3999,290,'N',546,'ANGS','=',null,'Y');
exec cre_dqr(4386,300,'N',551,'ANGS',null,null,null);
exec cre_dqr(4025,310,'N',555,'ANGS','=',null,'Y');
exec cre_dqr(4051,320,'N',559,'ANGS','=',null,'Y');
exec cre_dqr(4077,330,'N',563,'ANGS','=',null,'Y');
exec cre_dqr(4103,340,'N',567,'ANGS','=',null,'Y');
exec cre_dqr(4129,350,'N',572,'ANGS','=',null,'Y');
exec cre_dqr(4155,360,'N',576,'ANGS','=',null,'Y');
exec cre_dqr(4181,370,'N',580,'ANGS','=',null,'Y');
exec cre_dqr(4207,380,'N',584,'ANGS','=',null,'Y');
exec cre_dqr(4385,400,'N',595,'ANGS',null,null,null);
exec cre_dqr(3765,410,'N',472,'ANGS','>=',null,null);
exec cre_dqr(3791,420,'N',476,'ANGS','<=',null,null);
exec cre_dqr(4384,430,'N',600,'ANGS',null,null,null);
exec cre_dqr(4233,450,'N',608,'ANGS','=',null,'Y');
exec cre_dqr(4707,460,'N',404,'ANGS','=',null,null);
exec cre_dqr(4383,470,'N',613,'ANGS',null,null,null);
exec cre_dqr(3817,480,'N',485,'ANGS','=',null,'Y');
exec cre_dqr(3843,490,'N',489,'ANGS','=',null,'Y');
exec cre_dqr(3869,500,'N',493,'ANGS','=',null,'Y');
exec cre_dqr(3895,510,'N',497,'ANGS','=',null,'Y');
exec cre_dqr(4382,520,'N',620,'ANGS',null,null,null);
exec cre_dqr(3063,530,'N',45,'ANGS','=',null,'Y');
exec cre_dqr(3089,540,'N',52,'ANGS','=',null,'Y');
exec cre_dqr(3271,550,'N',387,'ANGS','=',null,'Y');
exec cre_dqr(3297,560,'N',391,'ANGS','=',null,'Y');
exec cre_dqr(4311,570,'N',700,'ANGS','=',null,'Y');
exec cre_dqr(3713,580,'N',465,'ANGS','=',null,'Y');
exec cre_dqr(3921,590,'N',507,'ANGS','=',null,'Y');
exec cre_dqr(4285,600,'N',696,'ANGS','=',null,'Y');
exec cre_dqr(3037,610,'N',37,'ANGS','=',null,'Y');
exec cre_dqr(4259,620,'N',624,'ANGS','=',null,'Y');
exec cre_dqr(4905,5,'Y',773,'CEHC','=',null,'Y');
exec cre_dqr(4840,10,'N',521,'CEHC',null,null,null);
exec cre_dqr(4845,20,'N',351,'CEHC','>=',null,null);
exec cre_dqr(4846,25,'N',357,'CEHC','<=',null,null);
exec cre_dqr(4874,50,'N',526,'CEHC','=',null,'Y');
exec cre_dqr(4841,65,'N',737,'CEHC','=',null,null);
exec cre_dqr(4829,70,'N',742,'CEHC','=',null,null);
exec cre_dqr(4847,80,'N',380,'CEHC','=',null,null);
exec cre_dqr(4848,90,'N',384,'CEHC','=',null,null);
exec cre_dqr(4904,100,'N',975,'CEHC','=',null,'Y');
exec cre_dqr(4839,110,'N',532,'CEHC',null,null,null);
exec cre_dqr(4875,120,'N',536,'CEHC','=',null,'Y');
exec cre_dqr(4851,130,'N',402,'CEHC','=',null,'Y');
exec cre_dqr(4852,140,'N',407,'CEHC','=',null,'Y');
exec cre_dqr(4853,150,'N',413,'CEHC','=',null,'Y');
exec cre_dqr(4854,160,'N',418,'CEHC','=',null,'Y');
exec cre_dqr(4855,170,'N',423,'CEHC','=',null,'Y');
exec cre_dqr(4856,180,'N',428,'CEHC','=',null,'Y');
exec cre_dqr(4857,190,'N',433,'CEHC','=',null,'Y');
exec cre_dqr(4830,200,'N',542,'CEHC',null,null,null);
exec cre_dqr(4858,210,'N',439,'CEHC','=',null,'Y');
exec cre_dqr(4859,220,'N',443,'CEHC','=',null,'Y');
exec cre_dqr(4860,230,'N',448,'CEHC','=',null,'Y');
exec cre_dqr(4861,240,'N',452,'CEHC','=',null,'Y');
exec cre_dqr(4862,250,'N',456,'CEHC','=',null,'Y');
exec cre_dqr(4863,260,'N',460,'CEHC','=',null,'Y');
exec cre_dqr(4864,270,'N',464,'CEHC','=',null,'Y');
exec cre_dqr(4866,280,'N',468,'CEHC','=',null,'Y');
exec cre_dqr(4876,290,'N',546,'CEHC','=',null,'Y');
exec cre_dqr(4838,300,'N',551,'CEHC',null,null,null);
exec cre_dqr(4877,310,'N',555,'CEHC','=',null,'Y');
exec cre_dqr(4878,320,'N',559,'CEHC','=',null,'Y');
exec cre_dqr(4879,330,'N',563,'CEHC','=',null,'Y');
exec cre_dqr(4880,340,'N',567,'CEHC','=',null,'Y');
exec cre_dqr(4881,350,'N',572,'CEHC','=',null,'Y');
exec cre_dqr(4882,360,'N',576,'CEHC','=',null,'Y');
exec cre_dqr(4883,370,'N',580,'CEHC','=',null,'Y');
exec cre_dqr(4884,380,'N',584,'CEHC','=',null,'Y');
exec cre_dqr(4885,390,'N',588,'CEHC','=',null,'Y');
exec cre_dqr(4837,400,'N',595,'CEHC',null,null,null);
exec cre_dqr(4867,410,'N',472,'CEHC','>=',null,null);
exec cre_dqr(4868,420,'N',476,'CEHC','<=',null,null);
exec cre_dqr(4836,430,'N',600,'CEHC',null,null,null);
exec cre_dqr(4886,440,'N',604,'CEHC','=',null,'Y');
exec cre_dqr(4887,450,'N',608,'CEHC','=',null,'Y');
exec cre_dqr(4831,460,'N',404,'CEHC','=',null,null);
exec cre_dqr(4835,470,'N',613,'CEHC',null,null,null);
exec cre_dqr(4869,480,'N',485,'CEHC','=',null,'Y');
exec cre_dqr(4870,490,'N',489,'CEHC','=',null,'Y');
exec cre_dqr(4871,500,'N',493,'CEHC','=',null,'Y');
exec cre_dqr(4872,510,'N',497,'CEHC','=',null,'Y');
exec cre_dqr(4834,520,'N',620,'CEHC',null,null,null);
exec cre_dqr(4843,530,'N',45,'CEHC','=',null,'Y');
exec cre_dqr(4844,540,'N',52,'CEHC','=',null,'Y');
exec cre_dqr(4849,550,'N',387,'CEHC','=',null,'Y');
exec cre_dqr(4850,560,'N',391,'CEHC','=',null,'Y');
exec cre_dqr(4903,570,'N',700,'CEHC','=',null,'Y');
exec cre_dqr(4865,580,'N',465,'CEHC','=',null,'Y');
exec cre_dqr(4873,590,'N',507,'CEHC','=',null,'Y');
exec cre_dqr(4902,600,'N',696,'CEHC','=',null,'Y');
exec cre_dqr(4842,610,'N',37,'CEHC','=',null,'Y');
exec cre_dqr(4888,620,'N',624,'CEHC','=',null,'Y');
exec cre_dqr(4833,630,'N',630,'CEHC',null,null,null);
exec cre_dqr(4889,640,'N',634,'CEHC','=',null,'Y');
exec cre_dqr(4890,650,'N',638,'CEHC','=',null,'Y');
exec cre_dqr(4891,660,'N',642,'CEHC','=',null,'Y');
exec cre_dqr(4892,670,'N',646,'CEHC','=',null,'Y');
exec cre_dqr(4893,680,'N',650,'CEHC','=',null,'Y');
exec cre_dqr(4894,690,'N',654,'CEHC','=',null,'Y');
exec cre_dqr(4895,700,'N',661,'CEHC','=',null,'Y');
exec cre_dqr(4896,710,'N',668,'CEHC','=',null,'Y');
exec cre_dqr(4897,720,'N',676,'CEHC','=',null,'Y');
exec cre_dqr(4898,730,'N',680,'CEHC','=',null,'Y');
exec cre_dqr(4899,740,'N',684,'CEHC','=',null,'Y');
exec cre_dqr(4900,750,'N',688,'CEHC','=',null,'Y');
exec cre_dqr(4901,760,'N',692,'CEHC','=',null,'Y');
exec cre_dqr(850,5,'Y',217,'CEHS','=',null,'Y');
exec cre_dqr(4395,10,'N',521,'CEHS',null,null,null);
exec cre_dqr(3116,20,'N',351,'CEHS','>=',null,null);
exec cre_dqr(3142,25,'N',357,'CEHS','<=',null,null);
exec cre_dqr(3948,50,'N',526,'CEHS','=',null,'Y');
exec cre_dqr(4751,65,'N',737,'CEHS','=',null,null);
exec cre_dqr(4791,70,'N',742,'CEHS','=',null,null);
exec cre_dqr(3220,80,'N',380,'CEHS','=',null,null);
exec cre_dqr(3246,90,'N',384,'CEHS','=',null,null);
exec cre_dqr(4338,100,'N',975,'CEHS','=',null,'Y');
exec cre_dqr(4394,110,'N',532,'CEHS',null,null,null);
exec cre_dqr(3974,120,'N',536,'CEHS','=',null,'Y');
exec cre_dqr(3350,130,'N',402,'CEHS','=',null,'Y');
exec cre_dqr(3376,140,'N',407,'CEHS','=',null,'Y');
exec cre_dqr(3402,150,'N',413,'CEHS','=',null,'Y');
exec cre_dqr(3428,160,'N',418,'CEHS','=',null,'Y');
exec cre_dqr(3454,170,'N',423,'CEHS','=',null,'Y');
exec cre_dqr(3480,180,'N',428,'CEHS','=',null,'Y');
exec cre_dqr(3506,190,'N',433,'CEHS','=',null,'Y');
exec cre_dqr(4667,200,'N',542,'CEHS',null,null,null);
exec cre_dqr(3532,210,'N',439,'CEHS','=',null,'Y');
exec cre_dqr(3558,220,'N',443,'CEHS','=',null,'Y');
exec cre_dqr(3584,230,'N',448,'CEHS','=',null,'Y');
exec cre_dqr(3610,240,'N',452,'CEHS','=',null,'Y');
exec cre_dqr(3636,250,'N',456,'CEHS','=',null,'Y');
exec cre_dqr(3662,260,'N',460,'CEHS','=',null,'Y');
exec cre_dqr(3688,270,'N',464,'CEHS','=',null,'Y');
exec cre_dqr(3740,280,'N',468,'CEHS','=',null,'Y');
exec cre_dqr(4000,290,'N',546,'CEHS','=',null,'Y');
exec cre_dqr(4393,300,'N',551,'CEHS',null,null,null);
exec cre_dqr(4026,310,'N',555,'CEHS','=',null,'Y');
exec cre_dqr(4052,320,'N',559,'CEHS','=',null,'Y');
exec cre_dqr(4078,330,'N',563,'CEHS','=',null,'Y');
exec cre_dqr(4104,340,'N',567,'CEHS','=',null,'Y');
exec cre_dqr(4130,350,'N',572,'CEHS','=',null,'Y');
exec cre_dqr(4156,360,'N',576,'CEHS','=',null,'Y');
exec cre_dqr(4182,370,'N',580,'CEHS','=',null,'Y');
exec cre_dqr(4208,380,'N',584,'CEHS','=',null,'Y');
exec cre_dqr(4392,400,'N',595,'CEHS',null,null,null);
exec cre_dqr(3766,410,'N',472,'CEHS','>=',null,null);
exec cre_dqr(3792,420,'N',476,'CEHS','<=',null,null);
exec cre_dqr(4391,430,'N',600,'CEHS',null,null,null);
exec cre_dqr(4234,450,'N',608,'CEHS','=',null,'Y');
exec cre_dqr(4708,460,'N',404,'CEHS','=',null,null);
exec cre_dqr(4390,470,'N',613,'CEHS',null,null,null);
exec cre_dqr(3818,480,'N',485,'CEHS','=',null,'Y');
exec cre_dqr(3844,490,'N',489,'CEHS','=',null,'Y');
exec cre_dqr(3870,500,'N',493,'CEHS','=',null,'Y');
exec cre_dqr(3896,510,'N',497,'CEHS','=',null,'Y');
exec cre_dqr(4389,520,'N',620,'CEHS',null,null,null);
exec cre_dqr(3064,530,'N',45,'CEHS','=',null,'Y');
exec cre_dqr(3090,540,'N',52,'CEHS','=',null,'Y');
exec cre_dqr(3272,550,'N',387,'CEHS','=',null,'Y');
exec cre_dqr(3298,560,'N',391,'CEHS','=',null,'Y');
exec cre_dqr(4312,570,'N',700,'CEHS','=',null,'Y');
exec cre_dqr(3714,580,'N',465,'CEHS','=',null,'Y');
exec cre_dqr(3922,590,'N',507,'CEHS','=',null,'Y');
exec cre_dqr(4286,600,'N',696,'CEHS','=',null,'Y');
exec cre_dqr(3038,610,'N',37,'CEHS','=',null,'Y');
exec cre_dqr(4260,620,'N',624,'CEHS','=',null,'Y');
exec cre_dqr(1,10,'N',9,'CHP','=',null,'Y');
exec cre_dqr(2,20,'N',45,'CHP','=',null,'Y');
exec cre_dqr(3,30,'N',52,'CHP','=',null,'Y');
exec cre_dqr(4,40,'N',387,'CHP','=',null,'Y');
exec cre_dqr(5,50,'N',391,'CHP','=',null,'Y');
exec cre_dqr(6,60,'N',465,'CHP','=',null,'Y');
exec cre_dqr(7,70,'N',507,'CHP','=',null,'Y');
exec cre_dqr(851,5,'Y',219,'COMH','=',null,'Y');
exec cre_dqr(4402,10,'N',521,'COMH',null,null,null);
exec cre_dqr(3117,20,'N',351,'COMH','>=',null,null);
exec cre_dqr(3143,25,'N',357,'COMH','<=',null,null);
exec cre_dqr(3949,50,'N',526,'COMH','=',null,'Y');
exec cre_dqr(4752,65,'N',737,'COMH','=',null,null);
exec cre_dqr(4792,70,'N',742,'COMH','=',null,null);
exec cre_dqr(3221,80,'N',380,'COMH','=',null,null);
exec cre_dqr(3247,90,'N',384,'COMH','=',null,null);
exec cre_dqr(4339,100,'N',975,'COMH','=',null,'Y');
exec cre_dqr(4401,110,'N',532,'COMH',null,null,null);
exec cre_dqr(3975,120,'N',536,'COMH','=',null,'Y');
exec cre_dqr(3351,130,'N',402,'COMH','=',null,'Y');
exec cre_dqr(3377,140,'N',407,'COMH','=',null,'Y');
exec cre_dqr(3403,150,'N',413,'COMH','=',null,'Y');
exec cre_dqr(3429,160,'N',418,'COMH','=',null,'Y');
exec cre_dqr(3455,170,'N',423,'COMH','=',null,'Y');
exec cre_dqr(3481,180,'N',428,'COMH','=',null,'Y');
exec cre_dqr(3507,190,'N',433,'COMH','=',null,'Y');
exec cre_dqr(4668,200,'N',542,'COMH',null,null,null);
exec cre_dqr(3533,210,'N',439,'COMH','=',null,'Y');
exec cre_dqr(3559,220,'N',443,'COMH','=',null,'Y');
exec cre_dqr(3585,230,'N',448,'COMH','=',null,'Y');
exec cre_dqr(3611,240,'N',452,'COMH','=',null,'Y');
exec cre_dqr(3637,250,'N',456,'COMH','=',null,'Y');
exec cre_dqr(3663,260,'N',460,'COMH','=',null,'Y');
exec cre_dqr(3689,270,'N',464,'COMH','=',null,'Y');
exec cre_dqr(3741,280,'N',468,'COMH','=',null,'Y');
exec cre_dqr(4001,290,'N',546,'COMH','=',null,'Y');
exec cre_dqr(4400,300,'N',551,'COMH',null,null,null);
exec cre_dqr(4027,310,'N',555,'COMH','=',null,'Y');
exec cre_dqr(4053,320,'N',559,'COMH','=',null,'Y');
exec cre_dqr(4079,330,'N',563,'COMH','=',null,'Y');
exec cre_dqr(4105,340,'N',567,'COMH','=',null,'Y');
exec cre_dqr(4131,350,'N',572,'COMH','=',null,'Y');
exec cre_dqr(4157,360,'N',576,'COMH','=',null,'Y');
exec cre_dqr(4183,370,'N',580,'COMH','=',null,'Y');
exec cre_dqr(4209,380,'N',584,'COMH','=',null,'Y');
exec cre_dqr(4399,400,'N',595,'COMH',null,null,null);
exec cre_dqr(3767,410,'N',472,'COMH','>=',null,null);
exec cre_dqr(3793,420,'N',476,'COMH','<=',null,null);
exec cre_dqr(4398,430,'N',600,'COMH',null,null,null);
exec cre_dqr(4235,450,'N',608,'COMH','=',null,'Y');
exec cre_dqr(4709,460,'N',404,'COMH','=',null,null);
exec cre_dqr(4397,470,'N',613,'COMH',null,null,null);
exec cre_dqr(3819,480,'N',485,'COMH','=',null,'Y');
exec cre_dqr(3845,490,'N',489,'COMH','=',null,'Y');
exec cre_dqr(3871,500,'N',493,'COMH','=',null,'Y');
exec cre_dqr(3897,510,'N',497,'COMH','=',null,'Y');
exec cre_dqr(4396,520,'N',620,'COMH',null,null,null);
exec cre_dqr(3065,530,'N',45,'COMH','=',null,'Y');
exec cre_dqr(3091,540,'N',52,'COMH','=',null,'Y');
exec cre_dqr(3273,550,'N',387,'COMH','=',null,'Y');
exec cre_dqr(3299,560,'N',391,'COMH','=',null,'Y');
exec cre_dqr(4313,570,'N',700,'COMH','=',null,'Y');
exec cre_dqr(3715,580,'N',465,'COMH','=',null,'Y');
exec cre_dqr(3923,590,'N',507,'COMH','=',null,'Y');
exec cre_dqr(4287,600,'N',696,'COMH','=',null,'Y');
exec cre_dqr(3039,610,'N',37,'COMH','=',null,'Y');
exec cre_dqr(4261,620,'N',624,'COMH','=',null,'Y');
exec cre_dqr(852,5,'Y',222,'CORS','=',null,'Y');
exec cre_dqr(4409,10,'N',521,'CORS',null,null,null);
exec cre_dqr(3118,20,'N',351,'CORS','>=',null,null);
exec cre_dqr(3144,25,'N',357,'CORS','<=',null,null);
exec cre_dqr(3950,50,'N',526,'CORS','=',null,'Y');
exec cre_dqr(4753,65,'N',737,'CORS','=',null,null);
exec cre_dqr(4793,70,'N',742,'CORS','=',null,null);
exec cre_dqr(3222,80,'N',380,'CORS','=',null,null);
exec cre_dqr(3248,90,'N',384,'CORS','=',null,null);
exec cre_dqr(4340,100,'N',975,'CORS','=',null,'Y');
exec cre_dqr(4408,110,'N',532,'CORS',null,null,null);
exec cre_dqr(3976,120,'N',536,'CORS','=',null,'Y');
exec cre_dqr(3352,130,'N',402,'CORS','=',null,'Y');
exec cre_dqr(3378,140,'N',407,'CORS','=',null,'Y');
exec cre_dqr(3404,150,'N',413,'CORS','=',null,'Y');
exec cre_dqr(3430,160,'N',418,'CORS','=',null,'Y');
exec cre_dqr(3456,170,'N',423,'CORS','=',null,'Y');
exec cre_dqr(3482,180,'N',428,'CORS','=',null,'Y');
exec cre_dqr(3508,190,'N',433,'CORS','=',null,'Y');
exec cre_dqr(4669,200,'N',542,'CORS',null,null,null);
exec cre_dqr(3534,210,'N',439,'CORS','=',null,'Y');
exec cre_dqr(3560,220,'N',443,'CORS','=',null,'Y');
exec cre_dqr(3586,230,'N',448,'CORS','=',null,'Y');
exec cre_dqr(3612,240,'N',452,'CORS','=',null,'Y');
exec cre_dqr(3638,250,'N',456,'CORS','=',null,'Y');
exec cre_dqr(3664,260,'N',460,'CORS','=',null,'Y');
exec cre_dqr(3690,270,'N',464,'CORS','=',null,'Y');
exec cre_dqr(3742,280,'N',468,'CORS','=',null,'Y');
exec cre_dqr(4002,290,'N',546,'CORS','=',null,'Y');
exec cre_dqr(4407,300,'N',551,'CORS',null,null,null);
exec cre_dqr(4028,310,'N',555,'CORS','=',null,'Y');
exec cre_dqr(4054,320,'N',559,'CORS','=',null,'Y');
exec cre_dqr(4080,330,'N',563,'CORS','=',null,'Y');
exec cre_dqr(4106,340,'N',567,'CORS','=',null,'Y');
exec cre_dqr(4132,350,'N',572,'CORS','=',null,'Y');
exec cre_dqr(4158,360,'N',576,'CORS','=',null,'Y');
exec cre_dqr(4184,370,'N',580,'CORS','=',null,'Y');
exec cre_dqr(4210,380,'N',584,'CORS','=',null,'Y');
exec cre_dqr(4406,400,'N',595,'CORS',null,null,null);
exec cre_dqr(3768,410,'N',472,'CORS','>=',null,null);
exec cre_dqr(3794,420,'N',476,'CORS','<=',null,null);
exec cre_dqr(4405,430,'N',600,'CORS',null,null,null);
exec cre_dqr(4236,450,'N',608,'CORS','=',null,'Y');
exec cre_dqr(4710,460,'N',404,'CORS','=',null,null);
exec cre_dqr(4404,470,'N',613,'CORS',null,null,null);
exec cre_dqr(3820,480,'N',485,'CORS','=',null,'Y');
exec cre_dqr(3846,490,'N',489,'CORS','=',null,'Y');
exec cre_dqr(3872,500,'N',493,'CORS','=',null,'Y');
exec cre_dqr(3898,510,'N',497,'CORS','=',null,'Y');
exec cre_dqr(4403,520,'N',620,'CORS',null,null,null);
exec cre_dqr(3066,530,'N',45,'CORS','=',null,'Y');
exec cre_dqr(3092,540,'N',52,'CORS','=',null,'Y');
exec cre_dqr(3274,550,'N',387,'CORS','=',null,'Y');
exec cre_dqr(3300,560,'N',391,'CORS','=',null,'Y');
exec cre_dqr(4314,570,'N',700,'CORS','=',null,'Y');
exec cre_dqr(3716,580,'N',465,'CORS','=',null,'Y');
exec cre_dqr(3924,590,'N',507,'CORS','=',null,'Y');
exec cre_dqr(4288,600,'N',696,'CORS','=',null,'Y');
exec cre_dqr(3040,610,'N',37,'CORS','=',null,'Y');
exec cre_dqr(4262,620,'N',624,'CORS','=',null,'Y');
exec cre_dqr(856,5,'Y',230,'HINH','=',null,'Y');
exec cre_dqr(4566,10,'N',521,'HINH',null,null,null);
exec cre_dqr(2091,20,'N',351,'HINH','>=',null,null);
exec cre_dqr(2106,25,'N',357,'HINH','<=',null,null);
exec cre_dqr(2571,50,'N',526,'HINH','=',null,'Y');
exec cre_dqr(4757,65,'N',737,'HINH','=',null,null);
exec cre_dqr(4797,70,'N',742,'HINH','=',null,null);
exec cre_dqr(2151,80,'N',380,'HINH','=',null,null);
exec cre_dqr(2166,90,'N',384,'HINH','=',null,null);
exec cre_dqr(3021,100,'N',975,'HINH','=',null,'Y');
exec cre_dqr(4565,110,'N',532,'HINH',null,null,null);
exec cre_dqr(2586,120,'N',536,'HINH','=',null,'Y');
exec cre_dqr(2226,130,'N',402,'HINH','=',null,'Y');
exec cre_dqr(2241,140,'N',407,'HINH','=',null,'Y');
exec cre_dqr(2256,150,'N',413,'HINH','=',null,'Y');
exec cre_dqr(2271,160,'N',418,'HINH','=',null,'Y');
exec cre_dqr(2286,170,'N',423,'HINH','=',null,'Y');
exec cre_dqr(2301,180,'N',428,'HINH','=',null,'Y');
exec cre_dqr(2316,190,'N',433,'HINH','=',null,'Y');
exec cre_dqr(4691,200,'N',542,'HINH',null,null,null);
exec cre_dqr(2331,210,'N',439,'HINH','=',null,'Y');
exec cre_dqr(2346,220,'N',443,'HINH','=',null,'Y');
exec cre_dqr(2361,230,'N',448,'HINH','=',null,'Y');
exec cre_dqr(2376,240,'N',452,'HINH','=',null,'Y');
exec cre_dqr(2391,250,'N',456,'HINH','=',null,'Y');
exec cre_dqr(2406,260,'N',460,'HINH','=',null,'Y');
exec cre_dqr(2421,270,'N',464,'HINH','=',null,'Y');
exec cre_dqr(2451,280,'N',468,'HINH','=',null,'Y');
exec cre_dqr(2601,290,'N',546,'HINH','=',null,'Y');
exec cre_dqr(4564,300,'N',551,'HINH',null,null,null);
exec cre_dqr(2616,310,'N',555,'HINH','=',null,'Y');
exec cre_dqr(2631,320,'N',559,'HINH','=',null,'Y');
exec cre_dqr(2646,330,'N',563,'HINH','=',null,'Y');
exec cre_dqr(2661,340,'N',567,'HINH','=',null,'Y');
exec cre_dqr(2676,350,'N',572,'HINH','=',null,'Y');
exec cre_dqr(2691,360,'N',576,'HINH','=',null,'Y');
exec cre_dqr(2706,370,'N',580,'HINH','=',null,'Y');
exec cre_dqr(2721,380,'N',584,'HINH','=',null,'Y');
exec cre_dqr(2736,390,'N',588,'HINH','=',null,'Y');
exec cre_dqr(4563,400,'N',595,'HINH',null,null,null);
exec cre_dqr(2466,410,'N',472,'HINH','>=',null,null);
exec cre_dqr(2481,420,'N',476,'HINH','<=',null,null);
exec cre_dqr(4562,430,'N',600,'HINH',null,null,null);
exec cre_dqr(2751,440,'N',604,'HINH','=',null,'Y');
exec cre_dqr(2766,450,'N',608,'HINH','=',null,'Y');
exec cre_dqr(4732,460,'N',404,'HINH','=',null,null);
exec cre_dqr(4561,470,'N',613,'HINH',null,null,null);
exec cre_dqr(2496,480,'N',485,'HINH','=',null,'Y');
exec cre_dqr(2511,490,'N',489,'HINH','=',null,'Y');
exec cre_dqr(2526,500,'N',493,'HINH','=',null,'Y');
exec cre_dqr(2541,510,'N',497,'HINH','=',null,'Y');
exec cre_dqr(4560,520,'N',620,'HINH',null,null,null);
exec cre_dqr(2061,530,'N',45,'HINH','=',null,'Y');
exec cre_dqr(2076,540,'N',52,'HINH','=',null,'Y');
exec cre_dqr(2181,550,'N',387,'HINH','=',null,'Y');
exec cre_dqr(2196,560,'N',391,'HINH','=',null,'Y');
exec cre_dqr(3006,570,'N',700,'HINH','=',null,'Y');
exec cre_dqr(2436,580,'N',465,'HINH','=',null,'Y');
exec cre_dqr(2556,590,'N',507,'HINH','=',null,'Y');
exec cre_dqr(2991,600,'N',696,'HINH','=',null,'Y');
exec cre_dqr(2046,610,'N',37,'HINH','=',null,'Y');
exec cre_dqr(2781,620,'N',624,'HINH','=',null,'Y');
exec cre_dqr(4559,630,'N',630,'HINH',null,null,null);
exec cre_dqr(2796,640,'N',634,'HINH','=',null,'Y');
exec cre_dqr(2811,650,'N',638,'HINH','=',null,'Y');
exec cre_dqr(2826,660,'N',642,'HINH','=',null,'Y');
exec cre_dqr(2841,670,'N',646,'HINH','=',null,'Y');
exec cre_dqr(2856,680,'N',650,'HINH','=',null,'Y');
exec cre_dqr(2871,690,'N',654,'HINH','=',null,'Y');
exec cre_dqr(2886,700,'N',661,'HINH','=',null,'Y');
exec cre_dqr(2901,710,'N',668,'HINH','=',null,'Y');
exec cre_dqr(2916,720,'N',676,'HINH','=',null,'Y');
exec cre_dqr(2931,730,'N',680,'HINH','=',null,'Y');
exec cre_dqr(2946,740,'N',684,'HINH','=',null,'Y');
exec cre_dqr(2961,750,'N',688,'HINH','=',null,'Y');
exec cre_dqr(2976,760,'N',692,'HINH','=',null,'Y');
exec cre_dqr(857,5,'Y',232,'HOUC','=',null,'Y');
exec cre_dqr(4437,10,'N',521,'HOUC',null,null,null);
exec cre_dqr(3122,20,'N',351,'HOUC','>=',null,null);
exec cre_dqr(3148,25,'N',357,'HOUC','<=',null,null);
exec cre_dqr(3954,50,'N',526,'HOUC','=',null,'Y');
exec cre_dqr(4758,65,'N',737,'HOUC','=',null,null);
exec cre_dqr(4798,70,'N',742,'HOUC','=',null,null);
exec cre_dqr(3226,80,'N',380,'HOUC','=',null,null);
exec cre_dqr(3252,90,'N',384,'HOUC','=',null,null);
exec cre_dqr(4344,100,'N',975,'HOUC','=',null,'Y');
exec cre_dqr(4436,110,'N',532,'HOUC',null,null,null);
exec cre_dqr(3980,120,'N',536,'HOUC','=',null,'Y');
exec cre_dqr(3356,130,'N',402,'HOUC','=',null,'Y');
exec cre_dqr(3382,140,'N',407,'HOUC','=',null,'Y');
exec cre_dqr(3408,150,'N',413,'HOUC','=',null,'Y');
exec cre_dqr(3434,160,'N',418,'HOUC','=',null,'Y');
exec cre_dqr(3460,170,'N',423,'HOUC','=',null,'Y');
exec cre_dqr(3486,180,'N',428,'HOUC','=',null,'Y');
exec cre_dqr(3512,190,'N',433,'HOUC','=',null,'Y');
exec cre_dqr(4673,200,'N',542,'HOUC',null,null,null);
exec cre_dqr(3538,210,'N',439,'HOUC','=',null,'Y');
exec cre_dqr(3564,220,'N',443,'HOUC','=',null,'Y');
exec cre_dqr(3590,230,'N',448,'HOUC','=',null,'Y');
exec cre_dqr(3616,240,'N',452,'HOUC','=',null,'Y');
exec cre_dqr(3642,250,'N',456,'HOUC','=',null,'Y');
exec cre_dqr(3668,260,'N',460,'HOUC','=',null,'Y');
exec cre_dqr(3694,270,'N',464,'HOUC','=',null,'Y');
exec cre_dqr(3746,280,'N',468,'HOUC','=',null,'Y');
exec cre_dqr(4006,290,'N',546,'HOUC','=',null,'Y');
exec cre_dqr(4435,300,'N',551,'HOUC',null,null,null);
exec cre_dqr(4032,310,'N',555,'HOUC','=',null,'Y');
exec cre_dqr(4058,320,'N',559,'HOUC','=',null,'Y');
exec cre_dqr(4084,330,'N',563,'HOUC','=',null,'Y');
exec cre_dqr(4110,340,'N',567,'HOUC','=',null,'Y');
exec cre_dqr(4136,350,'N',572,'HOUC','=',null,'Y');
exec cre_dqr(4162,360,'N',576,'HOUC','=',null,'Y');
exec cre_dqr(4188,370,'N',580,'HOUC','=',null,'Y');
exec cre_dqr(4214,380,'N',584,'HOUC','=',null,'Y');
exec cre_dqr(4434,400,'N',595,'HOUC',null,null,null);
exec cre_dqr(3772,410,'N',472,'HOUC','>=',null,null);
exec cre_dqr(3798,420,'N',476,'HOUC','<=',null,null);
exec cre_dqr(4433,430,'N',600,'HOUC',null,null,null);
exec cre_dqr(4240,450,'N',608,'HOUC','=',null,'Y');
exec cre_dqr(4714,460,'N',404,'HOUC','=',null,null);
exec cre_dqr(4432,470,'N',613,'HOUC',null,null,null);
exec cre_dqr(3824,480,'N',485,'HOUC','=',null,'Y');
exec cre_dqr(3850,490,'N',489,'HOUC','=',null,'Y');
exec cre_dqr(3876,500,'N',493,'HOUC','=',null,'Y');
exec cre_dqr(3902,510,'N',497,'HOUC','=',null,'Y');
exec cre_dqr(4431,520,'N',620,'HOUC',null,null,null);
exec cre_dqr(3070,530,'N',45,'HOUC','=',null,'Y');
exec cre_dqr(3096,540,'N',52,'HOUC','=',null,'Y');
exec cre_dqr(3278,550,'N',387,'HOUC','=',null,'Y');
exec cre_dqr(3304,560,'N',391,'HOUC','=',null,'Y');
exec cre_dqr(4318,570,'N',700,'HOUC','=',null,'Y');
exec cre_dqr(3720,580,'N',465,'HOUC','=',null,'Y');
exec cre_dqr(3928,590,'N',507,'HOUC','=',null,'Y');
exec cre_dqr(4292,600,'N',696,'HOUC','=',null,'Y');
exec cre_dqr(3044,610,'N',37,'HOUC','=',null,'Y');
exec cre_dqr(4266,620,'N',624,'HOUC','=',null,'Y');
exec cre_dqr(858,5,'Y',234,'HOUP','=',null,'Y');
exec cre_dqr(4574,10,'N',521,'HOUP',null,null,null);
exec cre_dqr(2092,20,'N',351,'HOUP','>=',null,null);
exec cre_dqr(2107,25,'N',357,'HOUP','<=',null,null);
exec cre_dqr(2572,50,'N',526,'HOUP','=',null,'Y');
exec cre_dqr(4759,65,'N',737,'HOUP','=',null,null);
exec cre_dqr(4799,70,'N',742,'HOUP','=',null,null);
exec cre_dqr(2152,80,'N',380,'HOUP','=',null,null);
exec cre_dqr(2167,90,'N',384,'HOUP','=',null,null);
exec cre_dqr(3022,100,'N',975,'HOUP','=',null,'Y');
exec cre_dqr(4573,110,'N',532,'HOUP',null,null,null);
exec cre_dqr(2587,120,'N',536,'HOUP','=',null,'Y');
exec cre_dqr(2227,130,'N',402,'HOUP','=',null,'Y');
exec cre_dqr(2242,140,'N',407,'HOUP','=',null,'Y');
exec cre_dqr(2257,150,'N',413,'HOUP','=',null,'Y');
exec cre_dqr(2272,160,'N',418,'HOUP','=',null,'Y');
exec cre_dqr(2287,170,'N',423,'HOUP','=',null,'Y');
exec cre_dqr(2302,180,'N',428,'HOUP','=',null,'Y');
exec cre_dqr(2317,190,'N',433,'HOUP','=',null,'Y');
exec cre_dqr(4692,200,'N',542,'HOUP',null,null,null);
exec cre_dqr(2332,210,'N',439,'HOUP','=',null,'Y');
exec cre_dqr(2347,220,'N',443,'HOUP','=',null,'Y');
exec cre_dqr(2362,230,'N',448,'HOUP','=',null,'Y');
exec cre_dqr(2377,240,'N',452,'HOUP','=',null,'Y');
exec cre_dqr(2392,250,'N',456,'HOUP','=',null,'Y');
exec cre_dqr(2407,260,'N',460,'HOUP','=',null,'Y');
exec cre_dqr(2422,270,'N',464,'HOUP','=',null,'Y');
exec cre_dqr(2452,280,'N',468,'HOUP','=',null,'Y');
exec cre_dqr(2602,290,'N',546,'HOUP','=',null,'Y');
exec cre_dqr(4572,300,'N',551,'HOUP',null,null,null);
exec cre_dqr(2617,310,'N',555,'HOUP','=',null,'Y');
exec cre_dqr(2632,320,'N',559,'HOUP','=',null,'Y');
exec cre_dqr(2647,330,'N',563,'HOUP','=',null,'Y');
exec cre_dqr(2662,340,'N',567,'HOUP','=',null,'Y');
exec cre_dqr(2677,350,'N',572,'HOUP','=',null,'Y');
exec cre_dqr(2692,360,'N',576,'HOUP','=',null,'Y');
exec cre_dqr(2707,370,'N',580,'HOUP','=',null,'Y');
exec cre_dqr(2722,380,'N',584,'HOUP','=',null,'Y');
exec cre_dqr(2737,390,'N',588,'HOUP','=',null,'Y');
exec cre_dqr(4571,400,'N',595,'HOUP',null,null,null);
exec cre_dqr(2467,410,'N',472,'HOUP','>=',null,null);
exec cre_dqr(2482,420,'N',476,'HOUP','<=',null,null);
exec cre_dqr(4570,430,'N',600,'HOUP',null,null,null);
exec cre_dqr(2752,440,'N',604,'HOUP','=',null,'Y');
exec cre_dqr(2767,450,'N',608,'HOUP','=',null,'Y');
exec cre_dqr(4733,460,'N',404,'HOUP','=',null,null);
exec cre_dqr(4569,470,'N',613,'HOUP',null,null,null);
exec cre_dqr(2497,480,'N',485,'HOUP','=',null,'Y');
exec cre_dqr(2512,490,'N',489,'HOUP','=',null,'Y');
exec cre_dqr(2527,500,'N',493,'HOUP','=',null,'Y');
exec cre_dqr(2542,510,'N',497,'HOUP','=',null,'Y');
exec cre_dqr(4568,520,'N',620,'HOUP',null,null,null);
exec cre_dqr(2062,530,'N',45,'HOUP','=',null,'Y');
exec cre_dqr(2077,540,'N',52,'HOUP','=',null,'Y');
exec cre_dqr(2182,550,'N',387,'HOUP','=',null,'Y');
exec cre_dqr(2197,560,'N',391,'HOUP','=',null,'Y');
exec cre_dqr(3007,570,'N',700,'HOUP','=',null,'Y');
exec cre_dqr(2437,580,'N',465,'HOUP','=',null,'Y');
exec cre_dqr(2557,590,'N',507,'HOUP','=',null,'Y');
exec cre_dqr(2992,600,'N',696,'HOUP','=',null,'Y');
exec cre_dqr(2047,610,'N',37,'HOUP','=',null,'Y');
exec cre_dqr(2782,620,'N',624,'HOUP','=',null,'Y');
exec cre_dqr(4567,630,'N',630,'HOUP',null,null,null);
exec cre_dqr(2797,640,'N',634,'HOUP','=',null,'Y');
exec cre_dqr(2812,650,'N',638,'HOUP','=',null,'Y');
exec cre_dqr(2827,660,'N',642,'HOUP','=',null,'Y');
exec cre_dqr(2842,670,'N',646,'HOUP','=',null,'Y');
exec cre_dqr(2857,680,'N',650,'HOUP','=',null,'Y');
exec cre_dqr(2872,690,'N',654,'HOUP','=',null,'Y');
exec cre_dqr(2887,700,'N',661,'HOUP','=',null,'Y');
exec cre_dqr(2902,710,'N',668,'HOUP','=',null,'Y');
exec cre_dqr(2917,720,'N',676,'HOUP','=',null,'Y');
exec cre_dqr(2932,730,'N',680,'HOUP','=',null,'Y');
exec cre_dqr(2947,740,'N',684,'HOUP','=',null,'Y');
exec cre_dqr(2962,750,'N',688,'HOUP','=',null,'Y');
exec cre_dqr(2977,760,'N',692,'HOUP','=',null,'Y');
exec cre_dqr(861,5,'Y',240,'JULF','=',null,'Y');
exec cre_dqr(4451,10,'N',521,'JULF',null,null,null);
exec cre_dqr(3124,20,'N',351,'JULF','>=',null,null);
exec cre_dqr(3150,25,'N',357,'JULF','<=',null,null);
exec cre_dqr(3956,50,'N',526,'JULF','=',null,'Y');
exec cre_dqr(4762,65,'N',737,'JULF','=',null,null);
exec cre_dqr(4802,70,'N',742,'JULF','=',null,null);
exec cre_dqr(3228,80,'N',380,'JULF','=',null,null);
exec cre_dqr(3254,90,'N',384,'JULF','=',null,null);
exec cre_dqr(4346,100,'N',975,'JULF','=',null,'Y');
exec cre_dqr(4450,110,'N',532,'JULF',null,null,null);
exec cre_dqr(3982,120,'N',536,'JULF','=',null,'Y');
exec cre_dqr(3358,130,'N',402,'JULF','=',null,'Y');
exec cre_dqr(3384,140,'N',407,'JULF','=',null,'Y');
exec cre_dqr(3410,150,'N',413,'JULF','=',null,'Y');
exec cre_dqr(3436,160,'N',418,'JULF','=',null,'Y');
exec cre_dqr(3462,170,'N',423,'JULF','=',null,'Y');
exec cre_dqr(3488,180,'N',428,'JULF','=',null,'Y');
exec cre_dqr(3514,190,'N',433,'JULF','=',null,'Y');
exec cre_dqr(4675,200,'N',542,'JULF',null,null,null);
exec cre_dqr(3540,210,'N',439,'JULF','=',null,'Y');
exec cre_dqr(3566,220,'N',443,'JULF','=',null,'Y');
exec cre_dqr(3592,230,'N',448,'JULF','=',null,'Y');
exec cre_dqr(3618,240,'N',452,'JULF','=',null,'Y');
exec cre_dqr(3644,250,'N',456,'JULF','=',null,'Y');
exec cre_dqr(3670,260,'N',460,'JULF','=',null,'Y');
exec cre_dqr(3696,270,'N',464,'JULF','=',null,'Y');
exec cre_dqr(3748,280,'N',468,'JULF','=',null,'Y');
exec cre_dqr(4008,290,'N',546,'JULF','=',null,'Y');
exec cre_dqr(4449,300,'N',551,'JULF',null,null,null);
exec cre_dqr(4034,310,'N',555,'JULF','=',null,'Y');
exec cre_dqr(4060,320,'N',559,'JULF','=',null,'Y');
exec cre_dqr(4086,330,'N',563,'JULF','=',null,'Y');
exec cre_dqr(4112,340,'N',567,'JULF','=',null,'Y');
exec cre_dqr(4138,350,'N',572,'JULF','=',null,'Y');
exec cre_dqr(4164,360,'N',576,'JULF','=',null,'Y');
exec cre_dqr(4190,370,'N',580,'JULF','=',null,'Y');
exec cre_dqr(4216,380,'N',584,'JULF','=',null,'Y');
exec cre_dqr(4448,400,'N',595,'JULF',null,null,null);
exec cre_dqr(3774,410,'N',472,'JULF','>=',null,null);
exec cre_dqr(3800,420,'N',476,'JULF','<=',null,null);
exec cre_dqr(4447,430,'N',600,'JULF',null,null,null);
exec cre_dqr(4242,450,'N',608,'JULF','=',null,'Y');
exec cre_dqr(4716,460,'N',404,'JULF','=',null,null);
exec cre_dqr(4446,470,'N',613,'JULF',null,null,null);
exec cre_dqr(3826,480,'N',485,'JULF','=',null,'Y');
exec cre_dqr(3852,490,'N',489,'JULF','=',null,'Y');
exec cre_dqr(3878,500,'N',493,'JULF','=',null,'Y');
exec cre_dqr(3904,510,'N',497,'JULF','=',null,'Y');
exec cre_dqr(4445,520,'N',620,'JULF',null,null,null);
exec cre_dqr(3072,530,'N',45,'JULF','=',null,'Y');
exec cre_dqr(3098,540,'N',52,'JULF','=',null,'Y');
exec cre_dqr(3280,550,'N',387,'JULF','=',null,'Y');
exec cre_dqr(3306,560,'N',391,'JULF','=',null,'Y');
exec cre_dqr(4320,570,'N',700,'JULF','=',null,'Y');
exec cre_dqr(3722,580,'N',465,'JULF','=',null,'Y');
exec cre_dqr(3930,590,'N',507,'JULF','=',null,'Y');
exec cre_dqr(4294,600,'N',696,'JULF','=',null,'Y');
exec cre_dqr(3046,610,'N',37,'JULF','=',null,'Y');
exec cre_dqr(4268,620,'N',624,'JULF','=',null,'Y');
exec cre_dqr(862,5,'Y',243,'JUNW','=',null,'Y');
exec cre_dqr(4458,10,'N',521,'JUNW',null,null,null);
exec cre_dqr(3125,20,'N',351,'JUNW','>=',null,null);
exec cre_dqr(3151,25,'N',357,'JUNW','<=',null,null);
exec cre_dqr(3957,50,'N',526,'JUNW','=',null,'Y');
exec cre_dqr(4763,65,'N',737,'JUNW','=',null,null);
exec cre_dqr(4803,70,'N',742,'JUNW','=',null,null);
exec cre_dqr(3229,80,'N',380,'JUNW','=',null,null);
exec cre_dqr(3255,90,'N',384,'JUNW','=',null,null);
exec cre_dqr(4347,100,'N',975,'JUNW','=',null,'Y');
exec cre_dqr(4457,110,'N',532,'JUNW',null,null,null);
exec cre_dqr(3983,120,'N',536,'JUNW','=',null,'Y');
exec cre_dqr(3359,130,'N',402,'JUNW','=',null,'Y');
exec cre_dqr(3385,140,'N',407,'JUNW','=',null,'Y');
exec cre_dqr(3411,150,'N',413,'JUNW','=',null,'Y');
exec cre_dqr(3437,160,'N',418,'JUNW','=',null,'Y');
exec cre_dqr(3463,170,'N',423,'JUNW','=',null,'Y');
exec cre_dqr(3489,180,'N',428,'JUNW','=',null,'Y');
exec cre_dqr(3515,190,'N',433,'JUNW','=',null,'Y');
exec cre_dqr(4676,200,'N',542,'JUNW',null,null,null);
exec cre_dqr(3541,210,'N',439,'JUNW','=',null,'Y');
exec cre_dqr(3567,220,'N',443,'JUNW','=',null,'Y');
exec cre_dqr(3593,230,'N',448,'JUNW','=',null,'Y');
exec cre_dqr(3619,240,'N',452,'JUNW','=',null,'Y');
exec cre_dqr(3645,250,'N',456,'JUNW','=',null,'Y');
exec cre_dqr(3671,260,'N',460,'JUNW','=',null,'Y');
exec cre_dqr(3697,270,'N',464,'JUNW','=',null,'Y');
exec cre_dqr(3749,280,'N',468,'JUNW','=',null,'Y');
exec cre_dqr(4009,290,'N',546,'JUNW','=',null,'Y');
exec cre_dqr(4456,300,'N',551,'JUNW',null,null,null);
exec cre_dqr(4035,310,'N',555,'JUNW','=',null,'Y');
exec cre_dqr(4061,320,'N',559,'JUNW','=',null,'Y');
exec cre_dqr(4087,330,'N',563,'JUNW','=',null,'Y');
exec cre_dqr(4113,340,'N',567,'JUNW','=',null,'Y');
exec cre_dqr(4139,350,'N',572,'JUNW','=',null,'Y');
exec cre_dqr(4165,360,'N',576,'JUNW','=',null,'Y');
exec cre_dqr(4191,370,'N',580,'JUNW','=',null,'Y');
exec cre_dqr(4217,380,'N',584,'JUNW','=',null,'Y');
exec cre_dqr(4455,400,'N',595,'JUNW',null,null,null);
exec cre_dqr(3775,410,'N',472,'JUNW','>=',null,null);
exec cre_dqr(3801,420,'N',476,'JUNW','<=',null,null);
exec cre_dqr(4454,430,'N',600,'JUNW',null,null,null);
exec cre_dqr(4243,450,'N',608,'JUNW','=',null,'Y');
exec cre_dqr(4717,460,'N',404,'JUNW','=',null,null);
exec cre_dqr(4453,470,'N',613,'JUNW',null,null,null);
exec cre_dqr(3827,480,'N',485,'JUNW','=',null,'Y');
exec cre_dqr(3853,490,'N',489,'JUNW','=',null,'Y');
exec cre_dqr(3879,500,'N',493,'JUNW','=',null,'Y');
exec cre_dqr(3905,510,'N',497,'JUNW','=',null,'Y');
exec cre_dqr(4452,520,'N',620,'JUNW',null,null,null);
exec cre_dqr(3073,530,'N',45,'JUNW','=',null,'Y');
exec cre_dqr(3099,540,'N',52,'JUNW','=',null,'Y');
exec cre_dqr(3281,550,'N',387,'JUNW','=',null,'Y');
exec cre_dqr(3307,560,'N',391,'JUNW','=',null,'Y');
exec cre_dqr(4321,570,'N',700,'JUNW','=',null,'Y');
exec cre_dqr(3723,580,'N',465,'JUNW','=',null,'Y');
exec cre_dqr(3931,590,'N',507,'JUNW','=',null,'Y');
exec cre_dqr(4295,600,'N',696,'JUNW','=',null,'Y');
exec cre_dqr(3047,610,'N',37,'JUNW','=',null,'Y');
exec cre_dqr(4269,620,'N',624,'JUNW','=',null,'Y');
exec cre_dqr(864,5,'Y',248,'LANV','=',null,'Y');
exec cre_dqr(4590,10,'N',521,'LANV',null,null,null);
exec cre_dqr(2094,20,'N',351,'LANV','>=',null,null);
exec cre_dqr(2109,25,'N',357,'LANV','<=',null,null);
exec cre_dqr(2574,50,'N',526,'LANV','=',null,'Y');
exec cre_dqr(4765,65,'N',737,'LANV','=',null,null);
exec cre_dqr(4805,70,'N',742,'LANV','=',null,null);
exec cre_dqr(2154,80,'N',380,'LANV','=',null,null);
exec cre_dqr(2169,90,'N',384,'LANV','=',null,null);
exec cre_dqr(3024,100,'N',975,'LANV','=',null,'Y');
exec cre_dqr(4589,110,'N',532,'LANV',null,null,null);
exec cre_dqr(2589,120,'N',536,'LANV','=',null,'Y');
exec cre_dqr(2229,130,'N',402,'LANV','=',null,'Y');
exec cre_dqr(2244,140,'N',407,'LANV','=',null,'Y');
exec cre_dqr(2259,150,'N',413,'LANV','=',null,'Y');
exec cre_dqr(2274,160,'N',418,'LANV','=',null,'Y');
exec cre_dqr(2289,170,'N',423,'LANV','=',null,'Y');
exec cre_dqr(2304,180,'N',428,'LANV','=',null,'Y');
exec cre_dqr(2319,190,'N',433,'LANV','=',null,'Y');
exec cre_dqr(4694,200,'N',542,'LANV',null,null,null);
exec cre_dqr(2334,210,'N',439,'LANV','=',null,'Y');
exec cre_dqr(2349,220,'N',443,'LANV','=',null,'Y');
exec cre_dqr(2364,230,'N',448,'LANV','=',null,'Y');
exec cre_dqr(2379,240,'N',452,'LANV','=',null,'Y');
exec cre_dqr(2394,250,'N',456,'LANV','=',null,'Y');
exec cre_dqr(2409,260,'N',460,'LANV','=',null,'Y');
exec cre_dqr(2424,270,'N',464,'LANV','=',null,'Y');
exec cre_dqr(2454,280,'N',468,'LANV','=',null,'Y');
exec cre_dqr(2604,290,'N',546,'LANV','=',null,'Y');
exec cre_dqr(4588,300,'N',551,'LANV',null,null,null);
exec cre_dqr(2619,310,'N',555,'LANV','=',null,'Y');
exec cre_dqr(2634,320,'N',559,'LANV','=',null,'Y');
exec cre_dqr(2649,330,'N',563,'LANV','=',null,'Y');
exec cre_dqr(2664,340,'N',567,'LANV','=',null,'Y');
exec cre_dqr(2679,350,'N',572,'LANV','=',null,'Y');
exec cre_dqr(2694,360,'N',576,'LANV','=',null,'Y');
exec cre_dqr(2709,370,'N',580,'LANV','=',null,'Y');
exec cre_dqr(2724,380,'N',584,'LANV','=',null,'Y');
exec cre_dqr(2739,390,'N',588,'LANV','=',null,'Y');
exec cre_dqr(4587,400,'N',595,'LANV',null,null,null);
exec cre_dqr(2469,410,'N',472,'LANV','>=',null,null);
exec cre_dqr(2484,420,'N',476,'LANV','<=',null,null);
exec cre_dqr(4586,430,'N',600,'LANV',null,null,null);
exec cre_dqr(2754,440,'N',604,'LANV','=',null,'Y');
exec cre_dqr(2769,450,'N',608,'LANV','=',null,'Y');
exec cre_dqr(4735,460,'N',404,'LANV','=',null,null);
exec cre_dqr(4585,470,'N',613,'LANV',null,null,null);
exec cre_dqr(2499,480,'N',485,'LANV','=',null,'Y');
exec cre_dqr(2514,490,'N',489,'LANV','=',null,'Y');
exec cre_dqr(2529,500,'N',493,'LANV','=',null,'Y');
exec cre_dqr(2544,510,'N',497,'LANV','=',null,'Y');
exec cre_dqr(4584,520,'N',620,'LANV',null,null,null);
exec cre_dqr(2064,530,'N',45,'LANV','=',null,'Y');
exec cre_dqr(2079,540,'N',52,'LANV','=',null,'Y');
exec cre_dqr(2184,550,'N',387,'LANV','=',null,'Y');
exec cre_dqr(2199,560,'N',391,'LANV','=',null,'Y');
exec cre_dqr(3009,570,'N',700,'LANV','=',null,'Y');
exec cre_dqr(2439,580,'N',465,'LANV','=',null,'Y');
exec cre_dqr(2559,590,'N',507,'LANV','=',null,'Y');
exec cre_dqr(2994,600,'N',696,'LANV','=',null,'Y');
exec cre_dqr(2049,610,'N',37,'LANV','=',null,'Y');
exec cre_dqr(2784,620,'N',624,'LANV','=',null,'Y');
exec cre_dqr(4583,630,'N',630,'LANV',null,null,null);
exec cre_dqr(2799,640,'N',634,'LANV','=',null,'Y');
exec cre_dqr(2814,650,'N',638,'LANV','=',null,'Y');
exec cre_dqr(2829,660,'N',642,'LANV','=',null,'Y');
exec cre_dqr(2844,670,'N',646,'LANV','=',null,'Y');
exec cre_dqr(2859,680,'N',650,'LANV','=',null,'Y');
exec cre_dqr(2874,690,'N',654,'LANV','=',null,'Y');
exec cre_dqr(2889,700,'N',661,'LANV','=',null,'Y');
exec cre_dqr(2904,710,'N',668,'LANV','=',null,'Y');
exec cre_dqr(2919,720,'N',676,'LANV','=',null,'Y');
exec cre_dqr(2934,730,'N',680,'LANV','=',null,'Y');
exec cre_dqr(2949,740,'N',684,'LANV','=',null,'Y');
exec cre_dqr(2964,750,'N',688,'LANV','=',null,'Y');
exec cre_dqr(2979,760,'N',692,'LANV','=',null,'Y');
exec cre_dqr(4598,10,'N',521,'MERZ',null,null,null);
exec cre_dqr(2096,20,'N',351,'MERZ','>=',null,null);
exec cre_dqr(2111,25,'N',357,'MERZ','<=',null,null);
exec cre_dqr(2576,50,'N',526,'MERZ','=',null,'Y');
exec cre_dqr(4766,65,'N',737,'MERZ','=',null,null);
exec cre_dqr(4806,70,'N',742,'MERZ','=',null,null);
exec cre_dqr(2156,80,'N',380,'MERZ','=',null,null);
exec cre_dqr(2171,90,'N',384,'MERZ','=',null,null);
exec cre_dqr(3026,100,'N',975,'MERZ','=',null,'Y');
exec cre_dqr(4597,110,'N',532,'MERZ',null,null,null);
exec cre_dqr(2591,120,'N',536,'MERZ','=',null,'Y');
exec cre_dqr(2231,130,'N',402,'MERZ','=',null,'Y');
exec cre_dqr(2246,140,'N',407,'MERZ','=',null,'Y');
exec cre_dqr(2261,150,'N',413,'MERZ','=',null,'Y');
exec cre_dqr(2276,160,'N',418,'MERZ','=',null,'Y');
exec cre_dqr(2291,170,'N',423,'MERZ','=',null,'Y');
exec cre_dqr(2306,180,'N',428,'MERZ','=',null,'Y');
exec cre_dqr(2321,190,'N',433,'MERZ','=',null,'Y');
exec cre_dqr(4695,200,'N',542,'MERZ',null,null,null);
exec cre_dqr(2336,210,'N',439,'MERZ','=',null,'Y');
exec cre_dqr(2351,220,'N',443,'MERZ','=',null,'Y');
exec cre_dqr(2366,230,'N',448,'MERZ','=',null,'Y');
exec cre_dqr(2381,240,'N',452,'MERZ','=',null,'Y');
exec cre_dqr(2396,250,'N',456,'MERZ','=',null,'Y');
exec cre_dqr(2411,260,'N',460,'MERZ','=',null,'Y');
exec cre_dqr(2426,270,'N',464,'MERZ','=',null,'Y');
exec cre_dqr(2456,280,'N',468,'MERZ','=',null,'Y');
exec cre_dqr(2606,290,'N',546,'MERZ','=',null,'Y');
exec cre_dqr(4596,300,'N',551,'MERZ',null,null,null);
exec cre_dqr(2621,310,'N',555,'MERZ','=',null,'Y');
exec cre_dqr(2636,320,'N',559,'MERZ','=',null,'Y');
exec cre_dqr(2651,330,'N',563,'MERZ','=',null,'Y');
exec cre_dqr(2666,340,'N',567,'MERZ','=',null,'Y');
exec cre_dqr(2681,350,'N',572,'MERZ','=',null,'Y');
exec cre_dqr(2696,360,'N',576,'MERZ','=',null,'Y');
exec cre_dqr(2711,370,'N',580,'MERZ','=',null,'Y');
exec cre_dqr(2726,380,'N',584,'MERZ','=',null,'Y');
exec cre_dqr(2741,390,'N',588,'MERZ','=',null,'Y');
exec cre_dqr(4595,400,'N',595,'MERZ',null,null,null);
exec cre_dqr(2471,410,'N',472,'MERZ','>=',null,null);
exec cre_dqr(2486,420,'N',476,'MERZ','<=',null,null);
exec cre_dqr(4594,430,'N',600,'MERZ',null,null,null);
exec cre_dqr(2756,440,'N',604,'MERZ','=',null,'Y');
exec cre_dqr(2771,450,'N',608,'MERZ','=',null,'Y');
exec cre_dqr(4737,460,'N',404,'MERZ','=',null,null);
exec cre_dqr(4593,470,'N',613,'MERZ',null,null,null);
exec cre_dqr(2501,480,'N',485,'MERZ','=',null,'Y');
exec cre_dqr(2516,490,'N',489,'MERZ','=',null,'Y');
exec cre_dqr(2531,500,'N',493,'MERZ','=',null,'Y');
exec cre_dqr(2546,510,'N',497,'MERZ','=',null,'Y');
exec cre_dqr(4592,520,'N',620,'MERZ',null,null,null);
exec cre_dqr(2066,530,'N',45,'MERZ','=',null,'Y');
exec cre_dqr(2081,540,'N',52,'MERZ','=',null,'Y');
exec cre_dqr(2186,550,'N',387,'MERZ','=',null,'Y');
exec cre_dqr(2201,560,'N',391,'MERZ','=',null,'Y');
exec cre_dqr(3011,570,'N',700,'MERZ','=',null,'Y');
exec cre_dqr(2441,580,'N',465,'MERZ','=',null,'Y');
exec cre_dqr(2561,590,'N',507,'MERZ','=',null,'Y');
exec cre_dqr(2996,600,'N',696,'MERZ','=',null,'Y');
exec cre_dqr(2051,610,'N',37,'MERZ','=',null,'Y');
exec cre_dqr(2786,620,'N',624,'MERZ','=',null,'Y');
exec cre_dqr(4591,630,'N',630,'MERZ',null,null,null);
exec cre_dqr(2801,640,'N',634,'MERZ','=',null,'Y');
exec cre_dqr(2816,650,'N',638,'MERZ','=',null,'Y');
exec cre_dqr(2831,660,'N',642,'MERZ','=',null,'Y');
exec cre_dqr(2846,670,'N',646,'MERZ','=',null,'Y');
exec cre_dqr(2861,680,'N',650,'MERZ','=',null,'Y');
exec cre_dqr(2876,690,'N',654,'MERZ','=',null,'Y');
exec cre_dqr(2891,700,'N',661,'MERZ','=',null,'Y');
exec cre_dqr(2906,710,'N',668,'MERZ','=',null,'Y');
exec cre_dqr(2921,720,'N',676,'MERZ','=',null,'Y');
exec cre_dqr(2936,730,'N',680,'MERZ','=',null,'Y');
exec cre_dqr(2951,740,'N',684,'MERZ','=',null,'Y');
exec cre_dqr(2966,750,'N',688,'MERZ','=',null,'Y');
exec cre_dqr(2981,760,'N',692,'MERZ','=',null,'Y');
exec cre_dqr(867,5,'Y',264,'MINI','=',null,'Y');
exec cre_dqr(4472,10,'N',521,'MINI',null,null,null);
exec cre_dqr(3127,20,'N',351,'MINI','>=',null,null);
exec cre_dqr(3153,25,'N',357,'MINI','<=',null,null);
exec cre_dqr(3959,50,'N',526,'MINI','=',null,'Y');
exec cre_dqr(4768,65,'N',737,'MINI','=',null,null);
exec cre_dqr(4808,70,'N',742,'MINI','=',null,null);
exec cre_dqr(3231,80,'N',380,'MINI','=',null,null);
exec cre_dqr(3257,90,'N',384,'MINI','=',null,null);
exec cre_dqr(4349,100,'N',975,'MINI','=',null,'Y');
exec cre_dqr(4471,110,'N',532,'MINI',null,null,null);
exec cre_dqr(3985,120,'N',536,'MINI','=',null,'Y');
exec cre_dqr(3361,130,'N',402,'MINI','=',null,'Y');
exec cre_dqr(3387,140,'N',407,'MINI','=',null,'Y');
exec cre_dqr(3413,150,'N',413,'MINI','=',null,'Y');
exec cre_dqr(3439,160,'N',418,'MINI','=',null,'Y');
exec cre_dqr(3465,170,'N',423,'MINI','=',null,'Y');
exec cre_dqr(3491,180,'N',428,'MINI','=',null,'Y');
exec cre_dqr(3517,190,'N',433,'MINI','=',null,'Y');
exec cre_dqr(4678,200,'N',542,'MINI',null,null,null);
exec cre_dqr(3543,210,'N',439,'MINI','=',null,'Y');
exec cre_dqr(3569,220,'N',443,'MINI','=',null,'Y');
exec cre_dqr(3595,230,'N',448,'MINI','=',null,'Y');
exec cre_dqr(3621,240,'N',452,'MINI','=',null,'Y');
exec cre_dqr(3647,250,'N',456,'MINI','=',null,'Y');
exec cre_dqr(3673,260,'N',460,'MINI','=',null,'Y');
exec cre_dqr(3699,270,'N',464,'MINI','=',null,'Y');
exec cre_dqr(3751,280,'N',468,'MINI','=',null,'Y');
exec cre_dqr(4011,290,'N',546,'MINI','=',null,'Y');
exec cre_dqr(4470,300,'N',551,'MINI',null,null,null);
exec cre_dqr(4037,310,'N',555,'MINI','=',null,'Y');
exec cre_dqr(4063,320,'N',559,'MINI','=',null,'Y');
exec cre_dqr(4089,330,'N',563,'MINI','=',null,'Y');
exec cre_dqr(4115,340,'N',567,'MINI','=',null,'Y');
exec cre_dqr(4141,350,'N',572,'MINI','=',null,'Y');
exec cre_dqr(4167,360,'N',576,'MINI','=',null,'Y');
exec cre_dqr(4193,370,'N',580,'MINI','=',null,'Y');
exec cre_dqr(4219,380,'N',584,'MINI','=',null,'Y');
exec cre_dqr(4469,400,'N',595,'MINI',null,null,null);
exec cre_dqr(3777,410,'N',472,'MINI','>=',null,null);
exec cre_dqr(3803,420,'N',476,'MINI','<=',null,null);
exec cre_dqr(4468,430,'N',600,'MINI',null,null,null);
exec cre_dqr(4245,450,'N',608,'MINI','=',null,'Y');
exec cre_dqr(4719,460,'N',404,'MINI','=',null,null);
exec cre_dqr(4467,470,'N',613,'MINI',null,null,null);
exec cre_dqr(3829,480,'N',485,'MINI','=',null,'Y');
exec cre_dqr(3855,490,'N',489,'MINI','=',null,'Y');
exec cre_dqr(3881,500,'N',493,'MINI','=',null,'Y');
exec cre_dqr(3907,510,'N',497,'MINI','=',null,'Y');
exec cre_dqr(4466,520,'N',620,'MINI',null,null,null);
exec cre_dqr(3075,530,'N',45,'MINI','=',null,'Y');
exec cre_dqr(3101,540,'N',52,'MINI','=',null,'Y');
exec cre_dqr(3283,550,'N',387,'MINI','=',null,'Y');
exec cre_dqr(3309,560,'N',391,'MINI','=',null,'Y');
exec cre_dqr(4323,570,'N',700,'MINI','=',null,'Y');
exec cre_dqr(3725,580,'N',465,'MINI','=',null,'Y');
exec cre_dqr(3933,590,'N',507,'MINI','=',null,'Y');
exec cre_dqr(4297,600,'N',696,'MINI','=',null,'Y');
exec cre_dqr(3049,610,'N',37,'MINI','=',null,'Y');
exec cre_dqr(4271,620,'N',624,'MINI','=',null,'Y');
exec cre_dqr(868,5,'Y',266,'NEHC','=',null,'Y');
exec cre_dqr(4614,10,'N',521,'NEHC',null,null,null);
exec cre_dqr(2097,20,'N',351,'NEHC','>=',null,null);
exec cre_dqr(2112,25,'N',357,'NEHC','<=',null,null);
exec cre_dqr(2577,50,'N',526,'NEHC','=',null,'Y');
exec cre_dqr(4769,65,'N',737,'NEHC','=',null,null);
exec cre_dqr(4809,70,'N',742,'NEHC','=',null,null);
exec cre_dqr(2157,80,'N',380,'NEHC','=',null,null);
exec cre_dqr(2172,90,'N',384,'NEHC','=',null,null);
exec cre_dqr(3027,100,'N',975,'NEHC','=',null,'Y');
exec cre_dqr(4613,110,'N',532,'NEHC',null,null,null);
exec cre_dqr(2592,120,'N',536,'NEHC','=',null,'Y');
exec cre_dqr(2232,130,'N',402,'NEHC','=',null,'Y');
exec cre_dqr(2247,140,'N',407,'NEHC','=',null,'Y');
exec cre_dqr(2262,150,'N',413,'NEHC','=',null,'Y');
exec cre_dqr(2277,160,'N',418,'NEHC','=',null,'Y');
exec cre_dqr(2292,170,'N',423,'NEHC','=',null,'Y');
exec cre_dqr(2307,180,'N',428,'NEHC','=',null,'Y');
exec cre_dqr(2322,190,'N',433,'NEHC','=',null,'Y');
exec cre_dqr(4697,200,'N',542,'NEHC',null,null,null);
exec cre_dqr(2337,210,'N',439,'NEHC','=',null,'Y');
exec cre_dqr(2352,220,'N',443,'NEHC','=',null,'Y');
exec cre_dqr(2367,230,'N',448,'NEHC','=',null,'Y');
exec cre_dqr(2382,240,'N',452,'NEHC','=',null,'Y');
exec cre_dqr(2397,250,'N',456,'NEHC','=',null,'Y');
exec cre_dqr(2412,260,'N',460,'NEHC','=',null,'Y');
exec cre_dqr(2427,270,'N',464,'NEHC','=',null,'Y');
exec cre_dqr(2457,280,'N',468,'NEHC','=',null,'Y');
exec cre_dqr(2607,290,'N',546,'NEHC','=',null,'Y');
exec cre_dqr(4612,300,'N',551,'NEHC',null,null,null);
exec cre_dqr(2622,310,'N',555,'NEHC','=',null,'Y');
exec cre_dqr(2637,320,'N',559,'NEHC','=',null,'Y');
exec cre_dqr(2652,330,'N',563,'NEHC','=',null,'Y');
exec cre_dqr(2667,340,'N',567,'NEHC','=',null,'Y');
exec cre_dqr(2682,350,'N',572,'NEHC','=',null,'Y');
exec cre_dqr(2697,360,'N',576,'NEHC','=',null,'Y');
exec cre_dqr(2712,370,'N',580,'NEHC','=',null,'Y');
exec cre_dqr(2727,380,'N',584,'NEHC','=',null,'Y');
exec cre_dqr(2742,390,'N',588,'NEHC','=',null,'Y');
exec cre_dqr(4611,400,'N',595,'NEHC',null,null,null);
exec cre_dqr(2472,410,'N',472,'NEHC','>=',null,null);
exec cre_dqr(2487,420,'N',476,'NEHC','<=',null,null);
exec cre_dqr(4610,430,'N',600,'NEHC',null,null,null);
exec cre_dqr(2757,440,'N',604,'NEHC','=',null,'Y');
exec cre_dqr(2772,450,'N',608,'NEHC','=',null,'Y');
exec cre_dqr(4738,460,'N',404,'NEHC','=',null,null);
exec cre_dqr(4609,470,'N',613,'NEHC',null,null,null);
exec cre_dqr(2502,480,'N',485,'NEHC','=',null,'Y');
exec cre_dqr(2517,490,'N',489,'NEHC','=',null,'Y');
exec cre_dqr(2532,500,'N',493,'NEHC','=',null,'Y');
exec cre_dqr(2547,510,'N',497,'NEHC','=',null,'Y');
exec cre_dqr(4608,520,'N',620,'NEHC',null,null,null);
exec cre_dqr(2067,530,'N',45,'NEHC','=',null,'Y');
exec cre_dqr(2082,540,'N',52,'NEHC','=',null,'Y');
exec cre_dqr(2187,550,'N',387,'NEHC','=',null,'Y');
exec cre_dqr(2202,560,'N',391,'NEHC','=',null,'Y');
exec cre_dqr(3012,570,'N',700,'NEHC','=',null,'Y');
exec cre_dqr(2442,580,'N',465,'NEHC','=',null,'Y');
exec cre_dqr(2562,590,'N',507,'NEHC','=',null,'Y');
exec cre_dqr(2997,600,'N',696,'NEHC','=',null,'Y');
exec cre_dqr(2052,610,'N',37,'NEHC','=',null,'Y');
exec cre_dqr(2787,620,'N',624,'NEHC','=',null,'Y');
exec cre_dqr(4607,630,'N',630,'NEHC',null,null,null);
exec cre_dqr(2802,640,'N',634,'NEHC','=',null,'Y');
exec cre_dqr(2817,650,'N',638,'NEHC','=',null,'Y');
exec cre_dqr(2832,660,'N',642,'NEHC','=',null,'Y');
exec cre_dqr(2847,670,'N',646,'NEHC','=',null,'Y');
exec cre_dqr(2862,680,'N',650,'NEHC','=',null,'Y');
exec cre_dqr(2877,690,'N',654,'NEHC','=',null,'Y');
exec cre_dqr(2892,700,'N',661,'NEHC','=',null,'Y');
exec cre_dqr(2907,710,'N',668,'NEHC','=',null,'Y');
exec cre_dqr(2922,720,'N',676,'NEHC','=',null,'Y');
exec cre_dqr(2937,730,'N',680,'NEHC','=',null,'Y');
exec cre_dqr(2952,740,'N',684,'NEHC','=',null,'Y');
exec cre_dqr(2967,750,'N',688,'NEHC','=',null,'Y');
exec cre_dqr(2982,760,'N',692,'NEHC','=',null,'Y');
exec cre_dqr(871,5,'Y',274,'PENL','=',null,'Y');
exec cre_dqr(4630,10,'N',521,'PENL',null,null,null);
exec cre_dqr(2099,20,'N',351,'PENL','>=',null,null);
exec cre_dqr(2114,25,'N',357,'PENL','<=',null,null);
exec cre_dqr(2579,50,'N',526,'PENL','=',null,'Y');
exec cre_dqr(4772,65,'N',737,'PENL','=',null,null);
exec cre_dqr(4812,70,'N',742,'PENL','=',null,null);
exec cre_dqr(2159,80,'N',380,'PENL','=',null,null);
exec cre_dqr(2174,90,'N',384,'PENL','=',null,null);
exec cre_dqr(3029,100,'N',975,'PENL','=',null,'Y');
exec cre_dqr(4629,110,'N',532,'PENL',null,null,null);
exec cre_dqr(2594,120,'N',536,'PENL','=',null,'Y');
exec cre_dqr(2234,130,'N',402,'PENL','=',null,'Y');
exec cre_dqr(2249,140,'N',407,'PENL','=',null,'Y');
exec cre_dqr(2264,150,'N',413,'PENL','=',null,'Y');
exec cre_dqr(2279,160,'N',418,'PENL','=',null,'Y');
exec cre_dqr(2294,170,'N',423,'PENL','=',null,'Y');
exec cre_dqr(2309,180,'N',428,'PENL','=',null,'Y');
exec cre_dqr(2324,190,'N',433,'PENL','=',null,'Y');
exec cre_dqr(4699,200,'N',542,'PENL',null,null,null);
exec cre_dqr(2339,210,'N',439,'PENL','=',null,'Y');
exec cre_dqr(2354,220,'N',443,'PENL','=',null,'Y');
exec cre_dqr(2369,230,'N',448,'PENL','=',null,'Y');
exec cre_dqr(2384,240,'N',452,'PENL','=',null,'Y');
exec cre_dqr(2399,250,'N',456,'PENL','=',null,'Y');
exec cre_dqr(2414,260,'N',460,'PENL','=',null,'Y');
exec cre_dqr(2429,270,'N',464,'PENL','=',null,'Y');
exec cre_dqr(2459,280,'N',468,'PENL','=',null,'Y');
exec cre_dqr(2609,290,'N',546,'PENL','=',null,'Y');
exec cre_dqr(4628,300,'N',551,'PENL',null,null,null);
exec cre_dqr(2624,310,'N',555,'PENL','=',null,'Y');
exec cre_dqr(2639,320,'N',559,'PENL','=',null,'Y');
exec cre_dqr(2654,330,'N',563,'PENL','=',null,'Y');
exec cre_dqr(2669,340,'N',567,'PENL','=',null,'Y');
exec cre_dqr(2684,350,'N',572,'PENL','=',null,'Y');
exec cre_dqr(2699,360,'N',576,'PENL','=',null,'Y');
exec cre_dqr(2714,370,'N',580,'PENL','=',null,'Y');
exec cre_dqr(2729,380,'N',584,'PENL','=',null,'Y');
exec cre_dqr(2744,390,'N',588,'PENL','=',null,'Y');
exec cre_dqr(4627,400,'N',595,'PENL',null,null,null);
exec cre_dqr(2474,410,'N',472,'PENL','>=',null,null);
exec cre_dqr(2489,420,'N',476,'PENL','<=',null,null);
exec cre_dqr(4626,430,'N',600,'PENL',null,null,null);
exec cre_dqr(2759,440,'N',604,'PENL','=',null,'Y');
exec cre_dqr(2774,450,'N',608,'PENL','=',null,'Y');
exec cre_dqr(4740,460,'N',404,'PENL','=',null,null);
exec cre_dqr(4625,470,'N',613,'PENL',null,null,null);
exec cre_dqr(2504,480,'N',485,'PENL','=',null,'Y');
exec cre_dqr(2519,490,'N',489,'PENL','=',null,'Y');
exec cre_dqr(2534,500,'N',493,'PENL','=',null,'Y');
exec cre_dqr(2549,510,'N',497,'PENL','=',null,'Y');
exec cre_dqr(4624,520,'N',620,'PENL',null,null,null);
exec cre_dqr(2069,530,'N',45,'PENL','=',null,'Y');
exec cre_dqr(2084,540,'N',52,'PENL','=',null,'Y');
exec cre_dqr(2189,550,'N',387,'PENL','=',null,'Y');
exec cre_dqr(2204,560,'N',391,'PENL','=',null,'Y');
exec cre_dqr(3014,570,'N',700,'PENL','=',null,'Y');
exec cre_dqr(2444,580,'N',465,'PENL','=',null,'Y');
exec cre_dqr(2564,590,'N',507,'PENL','=',null,'Y');
exec cre_dqr(2999,600,'N',696,'PENL','=',null,'Y');
exec cre_dqr(2054,610,'N',37,'PENL','=',null,'Y');
exec cre_dqr(2789,620,'N',624,'PENL','=',null,'Y');
exec cre_dqr(4623,630,'N',630,'PENL',null,null,null);
exec cre_dqr(2804,640,'N',634,'PENL','=',null,'Y');
exec cre_dqr(2819,650,'N',638,'PENL','=',null,'Y');
exec cre_dqr(2834,660,'N',642,'PENL','=',null,'Y');
exec cre_dqr(2849,670,'N',646,'PENL','=',null,'Y');
exec cre_dqr(2864,680,'N',650,'PENL','=',null,'Y');
exec cre_dqr(2879,690,'N',654,'PENL','=',null,'Y');
exec cre_dqr(2894,700,'N',661,'PENL','=',null,'Y');
exec cre_dqr(2909,710,'N',668,'PENL','=',null,'Y');
exec cre_dqr(2924,720,'N',676,'PENL','=',null,'Y');
exec cre_dqr(2939,730,'N',680,'PENL','=',null,'Y');
exec cre_dqr(2954,740,'N',684,'PENL','=',null,'Y');
exec cre_dqr(2969,750,'N',688,'PENL','=',null,'Y');
exec cre_dqr(2984,760,'N',692,'PENL','=',null,'Y');
exec cre_dqr(873,5,'Y',278,'SALH','=',null,'Y');
exec cre_dqr(4638,10,'N',521,'SALH',null,null,null);
exec cre_dqr(2100,20,'N',351,'SALH','>=',null,null);
exec cre_dqr(2115,25,'N',357,'SALH','<=',null,null);
exec cre_dqr(2580,50,'N',526,'SALH','=',null,'Y');
exec cre_dqr(4774,65,'N',737,'SALH','=',null,null);
exec cre_dqr(4814,70,'N',742,'SALH','=',null,null);
exec cre_dqr(2160,80,'N',380,'SALH','=',null,null);
exec cre_dqr(2175,90,'N',384,'SALH','=',null,null);
exec cre_dqr(3030,100,'N',975,'SALH','=',null,'Y');
exec cre_dqr(4637,110,'N',532,'SALH',null,null,null);
exec cre_dqr(2595,120,'N',536,'SALH','=',null,'Y');
exec cre_dqr(2235,130,'N',402,'SALH','=',null,'Y');
exec cre_dqr(2250,140,'N',407,'SALH','=',null,'Y');
exec cre_dqr(2265,150,'N',413,'SALH','=',null,'Y');
exec cre_dqr(2280,160,'N',418,'SALH','=',null,'Y');
exec cre_dqr(2295,170,'N',423,'SALH','=',null,'Y');
exec cre_dqr(2310,180,'N',428,'SALH','=',null,'Y');
exec cre_dqr(2325,190,'N',433,'SALH','=',null,'Y');
exec cre_dqr(4700,200,'N',542,'SALH',null,null,null);
exec cre_dqr(2340,210,'N',439,'SALH','=',null,'Y');
exec cre_dqr(2355,220,'N',443,'SALH','=',null,'Y');
exec cre_dqr(2370,230,'N',448,'SALH','=',null,'Y');
exec cre_dqr(2385,240,'N',452,'SALH','=',null,'Y');
exec cre_dqr(2400,250,'N',456,'SALH','=',null,'Y');
exec cre_dqr(2415,260,'N',460,'SALH','=',null,'Y');
exec cre_dqr(2430,270,'N',464,'SALH','=',null,'Y');
exec cre_dqr(2460,280,'N',468,'SALH','=',null,'Y');
exec cre_dqr(2610,290,'N',546,'SALH','=',null,'Y');
exec cre_dqr(4636,300,'N',551,'SALH',null,null,null);
exec cre_dqr(2625,310,'N',555,'SALH','=',null,'Y');
exec cre_dqr(2640,320,'N',559,'SALH','=',null,'Y');
exec cre_dqr(2655,330,'N',563,'SALH','=',null,'Y');
exec cre_dqr(2670,340,'N',567,'SALH','=',null,'Y');
exec cre_dqr(2685,350,'N',572,'SALH','=',null,'Y');
exec cre_dqr(2700,360,'N',576,'SALH','=',null,'Y');
exec cre_dqr(2715,370,'N',580,'SALH','=',null,'Y');
exec cre_dqr(2730,380,'N',584,'SALH','=',null,'Y');
exec cre_dqr(2745,390,'N',588,'SALH','=',null,'Y');
exec cre_dqr(4635,400,'N',595,'SALH',null,null,null);
exec cre_dqr(2475,410,'N',472,'SALH','>=',null,null);
exec cre_dqr(2490,420,'N',476,'SALH','<=',null,null);
exec cre_dqr(4634,430,'N',600,'SALH',null,null,null);
exec cre_dqr(2760,440,'N',604,'SALH','=',null,'Y');
exec cre_dqr(2775,450,'N',608,'SALH','=',null,'Y');
exec cre_dqr(4741,460,'N',404,'SALH','=',null,null);
exec cre_dqr(4633,470,'N',613,'SALH',null,null,null);
exec cre_dqr(2505,480,'N',485,'SALH','=',null,'Y');
exec cre_dqr(2520,490,'N',489,'SALH','=',null,'Y');
exec cre_dqr(2535,500,'N',493,'SALH','=',null,'Y');
exec cre_dqr(2550,510,'N',497,'SALH','=',null,'Y');
exec cre_dqr(4632,520,'N',620,'SALH',null,null,null);
exec cre_dqr(2070,530,'N',45,'SALH','=',null,'Y');
exec cre_dqr(2085,540,'N',52,'SALH','=',null,'Y');
exec cre_dqr(2190,550,'N',387,'SALH','=',null,'Y');
exec cre_dqr(2205,560,'N',391,'SALH','=',null,'Y');
exec cre_dqr(3015,570,'N',700,'SALH','=',null,'Y');
exec cre_dqr(2445,580,'N',465,'SALH','=',null,'Y');
exec cre_dqr(2565,590,'N',507,'SALH','=',null,'Y');
exec cre_dqr(3000,600,'N',696,'SALH','=',null,'Y');
exec cre_dqr(2055,610,'N',37,'SALH','=',null,'Y');
exec cre_dqr(2790,620,'N',624,'SALH','=',null,'Y');
exec cre_dqr(4631,630,'N',630,'SALH',null,null,null);
exec cre_dqr(2805,640,'N',634,'SALH','=',null,'Y');
exec cre_dqr(2820,650,'N',638,'SALH','=',null,'Y');
exec cre_dqr(2835,660,'N',642,'SALH','=',null,'Y');
exec cre_dqr(2850,670,'N',646,'SALH','=',null,'Y');
exec cre_dqr(2865,680,'N',650,'SALH','=',null,'Y');
exec cre_dqr(2880,690,'N',654,'SALH','=',null,'Y');
exec cre_dqr(2895,700,'N',661,'SALH','=',null,'Y');
exec cre_dqr(2910,710,'N',668,'SALH','=',null,'Y');
exec cre_dqr(2925,720,'N',676,'SALH','=',null,'Y');
exec cre_dqr(2940,730,'N',680,'SALH','=',null,'Y');
exec cre_dqr(2955,740,'N',684,'SALH','=',null,'Y');
exec cre_dqr(2970,750,'N',688,'SALH','=',null,'Y');
exec cre_dqr(2985,760,'N',692,'SALH','=',null,'Y');
exec cre_dqr(874,5,'Y',280,'SALV','=',null,'Y');
exec cre_dqr(4493,10,'N',521,'SALV',null,null,null);
exec cre_dqr(3130,20,'N',351,'SALV','>=',null,null);
exec cre_dqr(3156,25,'N',357,'SALV','<=',null,null);
exec cre_dqr(3962,50,'N',526,'SALV','=',null,'Y');
exec cre_dqr(4775,65,'N',737,'SALV','=',null,null);
exec cre_dqr(4815,70,'N',742,'SALV','=',null,null);
exec cre_dqr(3234,80,'N',380,'SALV','=',null,null);
exec cre_dqr(3260,90,'N',384,'SALV','=',null,null);
exec cre_dqr(4352,100,'N',975,'SALV','=',null,'Y');
exec cre_dqr(4492,110,'N',532,'SALV',null,null,null);
exec cre_dqr(3988,120,'N',536,'SALV','=',null,'Y');
exec cre_dqr(3364,130,'N',402,'SALV','=',null,'Y');
exec cre_dqr(3390,140,'N',407,'SALV','=',null,'Y');
exec cre_dqr(3416,150,'N',413,'SALV','=',null,'Y');
exec cre_dqr(3442,160,'N',418,'SALV','=',null,'Y');
exec cre_dqr(3468,170,'N',423,'SALV','=',null,'Y');
exec cre_dqr(3494,180,'N',428,'SALV','=',null,'Y');
exec cre_dqr(3520,190,'N',433,'SALV','=',null,'Y');
exec cre_dqr(4681,200,'N',542,'SALV',null,null,null);
exec cre_dqr(3546,210,'N',439,'SALV','=',null,'Y');
exec cre_dqr(3572,220,'N',443,'SALV','=',null,'Y');
exec cre_dqr(3598,230,'N',448,'SALV','=',null,'Y');
exec cre_dqr(3624,240,'N',452,'SALV','=',null,'Y');
exec cre_dqr(3650,250,'N',456,'SALV','=',null,'Y');
exec cre_dqr(3676,260,'N',460,'SALV','=',null,'Y');
exec cre_dqr(3702,270,'N',464,'SALV','=',null,'Y');
exec cre_dqr(3754,280,'N',468,'SALV','=',null,'Y');
exec cre_dqr(4014,290,'N',546,'SALV','=',null,'Y');
exec cre_dqr(4491,300,'N',551,'SALV',null,null,null);
exec cre_dqr(4040,310,'N',555,'SALV','=',null,'Y');
exec cre_dqr(4066,320,'N',559,'SALV','=',null,'Y');
exec cre_dqr(4092,330,'N',563,'SALV','=',null,'Y');
exec cre_dqr(4118,340,'N',567,'SALV','=',null,'Y');
exec cre_dqr(4144,350,'N',572,'SALV','=',null,'Y');
exec cre_dqr(4170,360,'N',576,'SALV','=',null,'Y');
exec cre_dqr(4196,370,'N',580,'SALV','=',null,'Y');
exec cre_dqr(4222,380,'N',584,'SALV','=',null,'Y');
exec cre_dqr(4490,400,'N',595,'SALV',null,null,null);
exec cre_dqr(3780,410,'N',472,'SALV','>=',null,null);
exec cre_dqr(3806,420,'N',476,'SALV','<=',null,null);
exec cre_dqr(4489,430,'N',600,'SALV',null,null,null);
exec cre_dqr(4248,450,'N',608,'SALV','=',null,'Y');
exec cre_dqr(4722,460,'N',404,'SALV','=',null,null);
exec cre_dqr(4488,470,'N',613,'SALV',null,null,null);
exec cre_dqr(3832,480,'N',485,'SALV','=',null,'Y');
exec cre_dqr(3858,490,'N',489,'SALV','=',null,'Y');
exec cre_dqr(3884,500,'N',493,'SALV','=',null,'Y');
exec cre_dqr(3910,510,'N',497,'SALV','=',null,'Y');
exec cre_dqr(4487,520,'N',620,'SALV',null,null,null);
exec cre_dqr(3078,530,'N',45,'SALV','=',null,'Y');
exec cre_dqr(3104,540,'N',52,'SALV','=',null,'Y');
exec cre_dqr(3286,550,'N',387,'SALV','=',null,'Y');
exec cre_dqr(3312,560,'N',391,'SALV','=',null,'Y');
exec cre_dqr(4326,570,'N',700,'SALV','=',null,'Y');
exec cre_dqr(3728,580,'N',465,'SALV','=',null,'Y');
exec cre_dqr(3936,590,'N',507,'SALV','=',null,'Y');
exec cre_dqr(4300,600,'N',696,'SALV','=',null,'Y');
exec cre_dqr(3052,610,'N',37,'SALV','=',null,'Y');
exec cre_dqr(4274,620,'N',624,'SALV','=',null,'Y');
exec cre_dqr(4646,10,'N',521,'SOUS',null,null,null);
exec cre_dqr(2102,20,'N',351,'SOUS','>=',null,null);
exec cre_dqr(2117,25,'N',357,'SOUS','<=',null,null);
exec cre_dqr(2582,50,'N',526,'SOUS','=',null,'Y');
exec cre_dqr(4776,65,'N',737,'SOUS','=',null,null);
exec cre_dqr(4816,70,'N',742,'SOUS','=',null,null);
exec cre_dqr(2162,80,'N',380,'SOUS','=',null,null);
exec cre_dqr(2177,90,'N',384,'SOUS','=',null,null);
exec cre_dqr(3032,100,'N',975,'SOUS','=',null,'Y');
exec cre_dqr(4645,110,'N',532,'SOUS',null,null,null);
exec cre_dqr(2597,120,'N',536,'SOUS','=',null,'Y');
exec cre_dqr(2237,130,'N',402,'SOUS','=',null,'Y');
exec cre_dqr(2252,140,'N',407,'SOUS','=',null,'Y');
exec cre_dqr(2267,150,'N',413,'SOUS','=',null,'Y');
exec cre_dqr(2282,160,'N',418,'SOUS','=',null,'Y');
exec cre_dqr(2297,170,'N',423,'SOUS','=',null,'Y');
exec cre_dqr(2312,180,'N',428,'SOUS','=',null,'Y');
exec cre_dqr(2327,190,'N',433,'SOUS','=',null,'Y');
exec cre_dqr(4701,200,'N',542,'SOUS',null,null,null);
exec cre_dqr(2342,210,'N',439,'SOUS','=',null,'Y');
exec cre_dqr(2357,220,'N',443,'SOUS','=',null,'Y');
exec cre_dqr(2372,230,'N',448,'SOUS','=',null,'Y');
exec cre_dqr(2387,240,'N',452,'SOUS','=',null,'Y');
exec cre_dqr(2402,250,'N',456,'SOUS','=',null,'Y');
exec cre_dqr(2417,260,'N',460,'SOUS','=',null,'Y');
exec cre_dqr(2432,270,'N',464,'SOUS','=',null,'Y');
exec cre_dqr(2462,280,'N',468,'SOUS','=',null,'Y');
exec cre_dqr(2612,290,'N',546,'SOUS','=',null,'Y');
exec cre_dqr(4644,300,'N',551,'SOUS',null,null,null);
exec cre_dqr(2627,310,'N',555,'SOUS','=',null,'Y');
exec cre_dqr(2642,320,'N',559,'SOUS','=',null,'Y');
exec cre_dqr(2657,330,'N',563,'SOUS','=',null,'Y');
exec cre_dqr(2672,340,'N',567,'SOUS','=',null,'Y');
exec cre_dqr(2687,350,'N',572,'SOUS','=',null,'Y');
exec cre_dqr(2702,360,'N',576,'SOUS','=',null,'Y');
exec cre_dqr(2717,370,'N',580,'SOUS','=',null,'Y');
exec cre_dqr(2732,380,'N',584,'SOUS','=',null,'Y');
exec cre_dqr(2747,390,'N',588,'SOUS','=',null,'Y');
exec cre_dqr(4643,400,'N',595,'SOUS',null,null,null);
exec cre_dqr(2477,410,'N',472,'SOUS','>=',null,null);
exec cre_dqr(2492,420,'N',476,'SOUS','<=',null,null);
exec cre_dqr(4642,430,'N',600,'SOUS',null,null,null);
exec cre_dqr(2762,440,'N',604,'SOUS','=',null,'Y');
exec cre_dqr(2777,450,'N',608,'SOUS','=',null,'Y');
exec cre_dqr(4743,460,'N',404,'SOUS','=',null,null);
exec cre_dqr(4641,470,'N',613,'SOUS',null,null,null);
exec cre_dqr(2507,480,'N',485,'SOUS','=',null,'Y');
exec cre_dqr(2522,490,'N',489,'SOUS','=',null,'Y');
exec cre_dqr(2537,500,'N',493,'SOUS','=',null,'Y');
exec cre_dqr(2552,510,'N',497,'SOUS','=',null,'Y');
exec cre_dqr(4640,520,'N',620,'SOUS',null,null,null);
exec cre_dqr(2072,530,'N',45,'SOUS','=',null,'Y');
exec cre_dqr(2087,540,'N',52,'SOUS','=',null,'Y');
exec cre_dqr(2192,550,'N',387,'SOUS','=',null,'Y');
exec cre_dqr(2207,560,'N',391,'SOUS','=',null,'Y');
exec cre_dqr(3017,570,'N',700,'SOUS','=',null,'Y');
exec cre_dqr(2447,580,'N',465,'SOUS','=',null,'Y');
exec cre_dqr(2567,590,'N',507,'SOUS','=',null,'Y');
exec cre_dqr(3002,600,'N',696,'SOUS','=',null,'Y');
exec cre_dqr(2057,610,'N',37,'SOUS','=',null,'Y');
exec cre_dqr(2792,620,'N',624,'SOUS','=',null,'Y');
exec cre_dqr(4639,630,'N',630,'SOUS',null,null,null);
exec cre_dqr(2807,640,'N',634,'SOUS','=',null,'Y');
exec cre_dqr(2822,650,'N',638,'SOUS','=',null,'Y');
exec cre_dqr(2837,660,'N',642,'SOUS','=',null,'Y');
exec cre_dqr(2852,670,'N',646,'SOUS','=',null,'Y');
exec cre_dqr(2867,680,'N',650,'SOUS','=',null,'Y');
exec cre_dqr(2882,690,'N',654,'SOUS','=',null,'Y');
exec cre_dqr(2897,700,'N',661,'SOUS','=',null,'Y');
exec cre_dqr(2912,710,'N',668,'SOUS','=',null,'Y');
exec cre_dqr(2927,720,'N',676,'SOUS','=',null,'Y');
exec cre_dqr(2942,730,'N',680,'SOUS','=',null,'Y');
exec cre_dqr(2957,740,'N',684,'SOUS','=',null,'Y');
exec cre_dqr(2972,750,'N',688,'SOUS','=',null,'Y');
exec cre_dqr(2987,760,'N',692,'SOUS','=',null,'Y');
exec cre_dqr(877,5,'Y',287,'SPLC','=',null,'Y');
exec cre_dqr(4500,10,'N',521,'SPLC',null,null,null);
exec cre_dqr(3131,20,'N',351,'SPLC','>=',null,null);
exec cre_dqr(3157,25,'N',357,'SPLC','<=',null,null);
exec cre_dqr(3963,50,'N',526,'SPLC','=',null,'Y');
exec cre_dqr(4778,65,'N',737,'SPLC','=',null,null);
exec cre_dqr(4818,70,'N',742,'SPLC','=',null,null);
exec cre_dqr(3235,80,'N',380,'SPLC','=',null,null);
exec cre_dqr(3261,90,'N',384,'SPLC','=',null,null);
exec cre_dqr(4353,100,'N',975,'SPLC','=',null,'Y');
exec cre_dqr(4499,110,'N',532,'SPLC',null,null,null);
exec cre_dqr(3989,120,'N',536,'SPLC','=',null,'Y');
exec cre_dqr(3365,130,'N',402,'SPLC','=',null,'Y');
exec cre_dqr(3391,140,'N',407,'SPLC','=',null,'Y');
exec cre_dqr(3417,150,'N',413,'SPLC','=',null,'Y');
exec cre_dqr(3443,160,'N',418,'SPLC','=',null,'Y');
exec cre_dqr(3469,170,'N',423,'SPLC','=',null,'Y');
exec cre_dqr(3495,180,'N',428,'SPLC','=',null,'Y');
exec cre_dqr(3521,190,'N',433,'SPLC','=',null,'Y');
exec cre_dqr(4682,200,'N',542,'SPLC',null,null,null);
exec cre_dqr(3547,210,'N',439,'SPLC','=',null,'Y');
exec cre_dqr(3573,220,'N',443,'SPLC','=',null,'Y');
exec cre_dqr(3599,230,'N',448,'SPLC','=',null,'Y');
exec cre_dqr(3625,240,'N',452,'SPLC','=',null,'Y');
exec cre_dqr(3651,250,'N',456,'SPLC','=',null,'Y');
exec cre_dqr(3677,260,'N',460,'SPLC','=',null,'Y');
exec cre_dqr(3703,270,'N',464,'SPLC','=',null,'Y');
exec cre_dqr(3755,280,'N',468,'SPLC','=',null,'Y');
exec cre_dqr(4015,290,'N',546,'SPLC','=',null,'Y');
exec cre_dqr(4498,300,'N',551,'SPLC',null,null,null);
exec cre_dqr(4041,310,'N',555,'SPLC','=',null,'Y');
exec cre_dqr(4067,320,'N',559,'SPLC','=',null,'Y');
exec cre_dqr(4093,330,'N',563,'SPLC','=',null,'Y');
exec cre_dqr(4119,340,'N',567,'SPLC','=',null,'Y');
exec cre_dqr(4145,350,'N',572,'SPLC','=',null,'Y');
exec cre_dqr(4171,360,'N',576,'SPLC','=',null,'Y');
exec cre_dqr(4197,370,'N',580,'SPLC','=',null,'Y');
exec cre_dqr(4223,380,'N',584,'SPLC','=',null,'Y');
exec cre_dqr(4497,400,'N',595,'SPLC',null,null,null);
exec cre_dqr(3781,410,'N',472,'SPLC','>=',null,null);
exec cre_dqr(3807,420,'N',476,'SPLC','<=',null,null);
exec cre_dqr(4496,430,'N',600,'SPLC',null,null,null);
exec cre_dqr(4249,450,'N',608,'SPLC','=',null,'Y');
exec cre_dqr(4723,460,'N',404,'SPLC','=',null,null);
exec cre_dqr(4495,470,'N',613,'SPLC',null,null,null);
exec cre_dqr(3833,480,'N',485,'SPLC','=',null,'Y');
exec cre_dqr(3859,490,'N',489,'SPLC','=',null,'Y');
exec cre_dqr(3885,500,'N',493,'SPLC','=',null,'Y');
exec cre_dqr(3911,510,'N',497,'SPLC','=',null,'Y');
exec cre_dqr(4494,520,'N',620,'SPLC',null,null,null);
exec cre_dqr(3079,530,'N',45,'SPLC','=',null,'Y');
exec cre_dqr(3105,540,'N',52,'SPLC','=',null,'Y');
exec cre_dqr(3287,550,'N',387,'SPLC','=',null,'Y');
exec cre_dqr(3313,560,'N',391,'SPLC','=',null,'Y');
exec cre_dqr(4327,570,'N',700,'SPLC','=',null,'Y');
exec cre_dqr(3729,580,'N',465,'SPLC','=',null,'Y');
exec cre_dqr(3937,590,'N',507,'SPLC','=',null,'Y');
exec cre_dqr(4301,600,'N',696,'SPLC','=',null,'Y');
exec cre_dqr(3053,610,'N',37,'SPLC','=',null,'Y');
exec cre_dqr(4275,620,'N',624,'SPLC','=',null,'Y');
exec cre_dqr(878,5,'Y',290,'SYPC','=',null,'Y');
exec cre_dqr(4507,10,'N',521,'SYPC',null,null,null);
exec cre_dqr(3132,20,'N',351,'SYPC','>=',null,null);
exec cre_dqr(3158,25,'N',357,'SYPC','<=',null,null);
exec cre_dqr(3964,50,'N',526,'SYPC','=',null,'Y');
exec cre_dqr(4779,65,'N',737,'SYPC','=',null,null);
exec cre_dqr(4819,70,'N',742,'SYPC','=',null,null);
exec cre_dqr(3236,80,'N',380,'SYPC','=',null,null);
exec cre_dqr(3262,90,'N',384,'SYPC','=',null,null);
exec cre_dqr(4354,100,'N',975,'SYPC','=',null,'Y');
exec cre_dqr(4506,110,'N',532,'SYPC',null,null,null);
exec cre_dqr(3990,120,'N',536,'SYPC','=',null,'Y');
exec cre_dqr(3366,130,'N',402,'SYPC','=',null,'Y');
exec cre_dqr(3392,140,'N',407,'SYPC','=',null,'Y');
exec cre_dqr(3418,150,'N',413,'SYPC','=',null,'Y');
exec cre_dqr(3444,160,'N',418,'SYPC','=',null,'Y');
exec cre_dqr(3470,170,'N',423,'SYPC','=',null,'Y');
exec cre_dqr(3496,180,'N',428,'SYPC','=',null,'Y');
exec cre_dqr(3522,190,'N',433,'SYPC','=',null,'Y');
exec cre_dqr(4683,200,'N',542,'SYPC',null,null,null);
exec cre_dqr(3548,210,'N',439,'SYPC','=',null,'Y');
exec cre_dqr(3574,220,'N',443,'SYPC','=',null,'Y');
exec cre_dqr(3600,230,'N',448,'SYPC','=',null,'Y');
exec cre_dqr(3626,240,'N',452,'SYPC','=',null,'Y');
exec cre_dqr(3652,250,'N',456,'SYPC','=',null,'Y');
exec cre_dqr(3678,260,'N',460,'SYPC','=',null,'Y');
exec cre_dqr(3704,270,'N',464,'SYPC','=',null,'Y');
exec cre_dqr(3756,280,'N',468,'SYPC','=',null,'Y');
exec cre_dqr(4016,290,'N',546,'SYPC','=',null,'Y');
exec cre_dqr(4505,300,'N',551,'SYPC',null,null,null);
exec cre_dqr(4042,310,'N',555,'SYPC','=',null,'Y');
exec cre_dqr(4068,320,'N',559,'SYPC','=',null,'Y');
exec cre_dqr(4094,330,'N',563,'SYPC','=',null,'Y');
exec cre_dqr(4120,340,'N',567,'SYPC','=',null,'Y');
exec cre_dqr(4146,350,'N',572,'SYPC','=',null,'Y');
exec cre_dqr(4172,360,'N',576,'SYPC','=',null,'Y');
exec cre_dqr(4198,370,'N',580,'SYPC','=',null,'Y');
exec cre_dqr(4224,380,'N',584,'SYPC','=',null,'Y');
exec cre_dqr(4504,400,'N',595,'SYPC',null,null,null);
exec cre_dqr(3782,410,'N',472,'SYPC','>=',null,null);
exec cre_dqr(3808,420,'N',476,'SYPC','<=',null,null);
exec cre_dqr(4503,430,'N',600,'SYPC',null,null,null);
exec cre_dqr(4250,450,'N',608,'SYPC','=',null,'Y');
exec cre_dqr(4724,460,'N',404,'SYPC','=',null,null);
exec cre_dqr(4502,470,'N',613,'SYPC',null,null,null);
exec cre_dqr(3834,480,'N',485,'SYPC','=',null,'Y');
exec cre_dqr(3860,490,'N',489,'SYPC','=',null,'Y');
exec cre_dqr(3886,500,'N',493,'SYPC','=',null,'Y');
exec cre_dqr(3912,510,'N',497,'SYPC','=',null,'Y');
exec cre_dqr(4501,520,'N',620,'SYPC',null,null,null);
exec cre_dqr(3080,530,'N',45,'SYPC','=',null,'Y');
exec cre_dqr(3106,540,'N',52,'SYPC','=',null,'Y');
exec cre_dqr(3288,550,'N',387,'SYPC','=',null,'Y');
exec cre_dqr(3314,560,'N',391,'SYPC','=',null,'Y');
exec cre_dqr(4328,570,'N',700,'SYPC','=',null,'Y');
exec cre_dqr(3730,580,'N',465,'SYPC','=',null,'Y');
exec cre_dqr(3938,590,'N',507,'SYPC','=',null,'Y');
exec cre_dqr(4302,600,'N',696,'SYPC','=',null,'Y');
exec cre_dqr(3054,610,'N',37,'SYPC','=',null,'Y');
exec cre_dqr(4276,620,'N',624,'SYPC','=',null,'Y');
exec cre_dqr(879,5,'Y',292,'TOWC','=',null,'Y');
exec cre_dqr(4662,10,'N',521,'TOWC',null,null,null);
exec cre_dqr(2103,20,'N',351,'TOWC','>=',null,null);
exec cre_dqr(2118,25,'N',357,'TOWC','<=',null,null);
exec cre_dqr(2583,50,'N',526,'TOWC','=',null,'Y');
exec cre_dqr(4780,65,'N',737,'TOWC','=',null,null);
exec cre_dqr(4820,70,'N',742,'TOWC','=',null,null);
exec cre_dqr(2163,80,'N',380,'TOWC','=',null,null);
exec cre_dqr(2178,90,'N',384,'TOWC','=',null,null);
exec cre_dqr(3033,100,'N',975,'TOWC','=',null,'Y');
exec cre_dqr(4661,110,'N',532,'TOWC',null,null,null);
exec cre_dqr(2598,120,'N',536,'TOWC','=',null,'Y');
exec cre_dqr(2238,130,'N',402,'TOWC','=',null,'Y');
exec cre_dqr(2253,140,'N',407,'TOWC','=',null,'Y');
exec cre_dqr(2268,150,'N',413,'TOWC','=',null,'Y');
exec cre_dqr(2283,160,'N',418,'TOWC','=',null,'Y');
exec cre_dqr(2298,170,'N',423,'TOWC','=',null,'Y');
exec cre_dqr(2313,180,'N',428,'TOWC','=',null,'Y');
exec cre_dqr(2328,190,'N',433,'TOWC','=',null,'Y');
exec cre_dqr(4703,200,'N',542,'TOWC',null,null,null);
exec cre_dqr(2343,210,'N',439,'TOWC','=',null,'Y');
exec cre_dqr(2358,220,'N',443,'TOWC','=',null,'Y');
exec cre_dqr(2373,230,'N',448,'TOWC','=',null,'Y');
exec cre_dqr(2388,240,'N',452,'TOWC','=',null,'Y');
exec cre_dqr(2403,250,'N',456,'TOWC','=',null,'Y');
exec cre_dqr(2418,260,'N',460,'TOWC','=',null,'Y');
exec cre_dqr(2433,270,'N',464,'TOWC','=',null,'Y');
exec cre_dqr(2463,280,'N',468,'TOWC','=',null,'Y');
exec cre_dqr(2613,290,'N',546,'TOWC','=',null,'Y');
exec cre_dqr(4660,300,'N',551,'TOWC',null,null,null);
exec cre_dqr(2628,310,'N',555,'TOWC','=',null,'Y');
exec cre_dqr(2643,320,'N',559,'TOWC','=',null,'Y');
exec cre_dqr(2658,330,'N',563,'TOWC','=',null,'Y');
exec cre_dqr(2673,340,'N',567,'TOWC','=',null,'Y');
exec cre_dqr(2688,350,'N',572,'TOWC','=',null,'Y');
exec cre_dqr(2703,360,'N',576,'TOWC','=',null,'Y');
exec cre_dqr(2718,370,'N',580,'TOWC','=',null,'Y');
exec cre_dqr(2733,380,'N',584,'TOWC','=',null,'Y');
exec cre_dqr(2748,390,'N',588,'TOWC','=',null,'Y');
exec cre_dqr(4659,400,'N',595,'TOWC',null,null,null);
exec cre_dqr(2478,410,'N',472,'TOWC','>=',null,null);
exec cre_dqr(2493,420,'N',476,'TOWC','<=',null,null);
exec cre_dqr(4658,430,'N',600,'TOWC',null,null,null);
exec cre_dqr(2763,440,'N',604,'TOWC','=',null,'Y');
exec cre_dqr(2778,450,'N',608,'TOWC','=',null,'Y');
exec cre_dqr(4744,460,'N',404,'TOWC','=',null,null);
exec cre_dqr(4657,470,'N',613,'TOWC',null,null,null);
exec cre_dqr(2508,480,'N',485,'TOWC','=',null,'Y');
exec cre_dqr(2523,490,'N',489,'TOWC','=',null,'Y');
exec cre_dqr(2538,500,'N',493,'TOWC','=',null,'Y');
exec cre_dqr(2553,510,'N',497,'TOWC','=',null,'Y');
exec cre_dqr(4656,520,'N',620,'TOWC',null,null,null);
exec cre_dqr(2073,530,'N',45,'TOWC','=',null,'Y');
exec cre_dqr(2088,540,'N',52,'TOWC','=',null,'Y');
exec cre_dqr(2193,550,'N',387,'TOWC','=',null,'Y');
exec cre_dqr(2208,560,'N',391,'TOWC','=',null,'Y');
exec cre_dqr(3018,570,'N',700,'TOWC','=',null,'Y');
exec cre_dqr(2448,580,'N',465,'TOWC','=',null,'Y');
exec cre_dqr(2568,590,'N',507,'TOWC','=',null,'Y');
exec cre_dqr(3003,600,'N',696,'TOWC','=',null,'Y');
exec cre_dqr(2058,610,'N',37,'TOWC','=',null,'Y');
exec cre_dqr(2793,620,'N',624,'TOWC','=',null,'Y');
exec cre_dqr(4655,630,'N',630,'TOWC',null,null,null);
exec cre_dqr(2808,640,'N',634,'TOWC','=',null,'Y');
exec cre_dqr(2823,650,'N',638,'TOWC','=',null,'Y');
exec cre_dqr(2838,660,'N',642,'TOWC','=',null,'Y');
exec cre_dqr(2853,670,'N',646,'TOWC','=',null,'Y');
exec cre_dqr(2868,680,'N',650,'TOWC','=',null,'Y');
exec cre_dqr(2883,690,'N',654,'TOWC','=',null,'Y');
exec cre_dqr(2898,700,'N',661,'TOWC','=',null,'Y');
exec cre_dqr(2913,710,'N',668,'TOWC','=',null,'Y');
exec cre_dqr(2928,720,'N',676,'TOWC','=',null,'Y');
exec cre_dqr(2943,730,'N',680,'TOWC','=',null,'Y');
exec cre_dqr(2958,740,'N',684,'TOWC','=',null,'Y');
exec cre_dqr(2973,750,'N',688,'TOWC','=',null,'Y');
exec cre_dqr(2988,760,'N',692,'TOWC','=',null,'Y');
exec cre_dqr(881,5,'Y',297,'UNIT','=',null,'Y');
exec cre_dqr(4521,10,'N',521,'UNIT',null,null,null);
exec cre_dqr(3134,20,'N',351,'UNIT','>=',null,null);
exec cre_dqr(3160,25,'N',357,'UNIT','<=',null,null);
exec cre_dqr(3966,50,'N',362,'UNIT','=',null,'Y');
exec cre_dqr(4782,65,'N',737,'UNIT','=',null,null);
exec cre_dqr(4822,70,'N',742,'UNIT','=',null,null);
exec cre_dqr(3238,80,'N',380,'UNIT','=',null,null);
exec cre_dqr(3264,90,'N',384,'UNIT','=',null,null);
exec cre_dqr(4356,100,'N',975,'UNIT','=',null,'Y');
exec cre_dqr(4520,110,'N',532,'UNIT',null,null,null);
exec cre_dqr(3992,120,'N',536,'UNIT','=',null,'Y');
exec cre_dqr(3368,130,'N',402,'UNIT','=',null,'Y');
exec cre_dqr(3394,140,'N',407,'UNIT','=',null,'Y');
exec cre_dqr(3420,150,'N',413,'UNIT','=',null,'Y');
exec cre_dqr(3446,160,'N',418,'UNIT','=',null,'Y');
exec cre_dqr(3472,170,'N',423,'UNIT','=',null,'Y');
exec cre_dqr(3498,180,'N',428,'UNIT','=',null,'Y');
exec cre_dqr(3524,190,'N',433,'UNIT','=',null,'Y');
exec cre_dqr(4685,200,'N',542,'UNIT',null,null,null);
exec cre_dqr(3550,210,'N',439,'UNIT','=',null,'Y');
exec cre_dqr(3576,220,'N',443,'UNIT','=',null,'Y');
exec cre_dqr(3602,230,'N',448,'UNIT','=',null,'Y');
exec cre_dqr(3628,240,'N',452,'UNIT','=',null,'Y');
exec cre_dqr(3654,250,'N',456,'UNIT','=',null,'Y');
exec cre_dqr(3680,260,'N',460,'UNIT','=',null,'Y');
exec cre_dqr(3706,270,'N',464,'UNIT','=',null,'Y');
exec cre_dqr(3758,280,'N',468,'UNIT','=',null,'Y');
exec cre_dqr(4018,290,'N',546,'UNIT','=',null,'Y');
exec cre_dqr(4519,300,'N',551,'UNIT',null,null,null);
exec cre_dqr(4044,310,'N',555,'UNIT','=',null,'Y');
exec cre_dqr(4070,320,'N',559,'UNIT','=',null,'Y');
exec cre_dqr(4096,330,'N',563,'UNIT','=',null,'Y');
exec cre_dqr(4122,340,'N',567,'UNIT','=',null,'Y');
exec cre_dqr(4148,350,'N',572,'UNIT','=',null,'Y');
exec cre_dqr(4174,360,'N',576,'UNIT','=',null,'Y');
exec cre_dqr(4200,370,'N',580,'UNIT','=',null,'Y');
exec cre_dqr(4226,380,'N',584,'UNIT','=',null,'Y');
exec cre_dqr(4518,400,'N',595,'UNIT',null,null,null);
exec cre_dqr(3784,410,'N',472,'UNIT','>=',null,null);
exec cre_dqr(3810,420,'N',476,'UNIT','<=',null,null);
exec cre_dqr(4517,430,'N',600,'UNIT',null,null,null);
exec cre_dqr(4252,450,'N',608,'UNIT','=',null,'Y');
exec cre_dqr(4726,460,'N',404,'UNIT','=',null,null);
exec cre_dqr(4516,470,'N',613,'UNIT',null,null,null);
exec cre_dqr(3836,480,'N',485,'UNIT','=',null,'Y');
exec cre_dqr(3862,490,'N',489,'UNIT','=',null,'Y');
exec cre_dqr(3888,500,'N',493,'UNIT','=',null,'Y');
exec cre_dqr(3914,510,'N',497,'UNIT','=',null,'Y');
exec cre_dqr(4515,520,'N',620,'UNIT',null,null,null);
exec cre_dqr(3082,530,'N',45,'UNIT','=',null,'Y');
exec cre_dqr(3108,540,'N',52,'UNIT','=',null,'Y');
exec cre_dqr(3290,550,'N',387,'UNIT','=',null,'Y');
exec cre_dqr(3316,560,'N',391,'UNIT','=',null,'Y');
exec cre_dqr(4330,570,'N',700,'UNIT','=',null,'Y');
exec cre_dqr(3732,580,'N',465,'UNIT','=',null,'Y');
exec cre_dqr(3940,590,'N',507,'UNIT','=',null,'Y');
exec cre_dqr(4304,600,'N',696,'UNIT','=',null,'Y');
exec cre_dqr(3056,610,'N',37,'UNIT','=',null,'Y');
exec cre_dqr(4278,620,'N',624,'UNIT','=',null,'Y');
exec cre_dqr(884,5,'Y',305,'WESH','=',null,'Y');
exec cre_dqr(4542,10,'N',521,'WESH',null,null,null);
exec cre_dqr(3137,20,'N',351,'WESH','>=',null,null);
exec cre_dqr(3163,25,'N',357,'WESH','<=',null,null);
exec cre_dqr(3969,50,'N',526,'WESH','=',null,'Y');
exec cre_dqr(4785,65,'N',737,'WESH','=',null,null);
exec cre_dqr(4825,70,'N',742,'WESH','=',null,null);
exec cre_dqr(3241,80,'N',380,'WESH','=',null,null);
exec cre_dqr(3267,90,'N',384,'WESH','=',null,null);
exec cre_dqr(4359,100,'N',975,'WESH','=',null,'Y');
exec cre_dqr(4541,110,'N',532,'WESH',null,null,null);
exec cre_dqr(3995,120,'N',536,'WESH','=',null,'Y');
exec cre_dqr(3371,130,'N',402,'WESH','=',null,'Y');
exec cre_dqr(3397,140,'N',407,'WESH','=',null,'Y');
exec cre_dqr(3423,150,'N',413,'WESH','=',null,'Y');
exec cre_dqr(3449,160,'N',418,'WESH','=',null,'Y');
exec cre_dqr(3475,170,'N',423,'WESH','=',null,'Y');
exec cre_dqr(3501,180,'N',428,'WESH','=',null,'Y');
exec cre_dqr(3527,190,'N',433,'WESH','=',null,'Y');
exec cre_dqr(4688,200,'N',542,'WESH',null,null,null);
exec cre_dqr(3553,210,'N',439,'WESH','=',null,'Y');
exec cre_dqr(3579,220,'N',443,'WESH','=',null,'Y');
exec cre_dqr(3605,230,'N',448,'WESH','=',null,'Y');
exec cre_dqr(3631,240,'N',452,'WESH','=',null,'Y');
exec cre_dqr(3657,250,'N',456,'WESH','=',null,'Y');
exec cre_dqr(3683,260,'N',460,'WESH','=',null,'Y');
exec cre_dqr(3709,270,'N',464,'WESH','=',null,'Y');
exec cre_dqr(3761,280,'N',468,'WESH','=',null,'Y');
exec cre_dqr(4021,290,'N',546,'WESH','=',null,'Y');
exec cre_dqr(4540,300,'N',551,'WESH',null,null,null);
exec cre_dqr(4047,310,'N',555,'WESH','=',null,'Y');
exec cre_dqr(4073,320,'N',559,'WESH','=',null,'Y');
exec cre_dqr(4099,330,'N',563,'WESH','=',null,'Y');
exec cre_dqr(4125,340,'N',567,'WESH','=',null,'Y');
exec cre_dqr(4151,350,'N',572,'WESH','=',null,'Y');
exec cre_dqr(4177,360,'N',576,'WESH','=',null,'Y');
exec cre_dqr(4203,370,'N',580,'WESH','=',null,'Y');
exec cre_dqr(4229,380,'N',584,'WESH','=',null,'Y');
exec cre_dqr(4539,400,'N',595,'WESH',null,null,null);
exec cre_dqr(3787,410,'N',472,'WESH','>=',null,null);
exec cre_dqr(3813,420,'N',476,'WESH','<=',null,null);
exec cre_dqr(4538,430,'N',600,'WESH',null,null,null);
exec cre_dqr(4255,450,'N',608,'WESH','=',null,'Y');
exec cre_dqr(4729,460,'N',404,'WESH','=',null,null);
exec cre_dqr(4537,470,'N',613,'WESH',null,null,null);
exec cre_dqr(3839,480,'N',485,'WESH','=',null,'Y');
exec cre_dqr(3865,490,'N',489,'WESH','=',null,'Y');
exec cre_dqr(3891,500,'N',493,'WESH','=',null,'Y');
exec cre_dqr(3917,510,'N',497,'WESH','=',null,'Y');
exec cre_dqr(4536,520,'N',620,'WESH',null,null,null);
exec cre_dqr(3085,530,'N',45,'WESH','=',null,'Y');
exec cre_dqr(3111,540,'N',52,'WESH','=',null,'Y');
exec cre_dqr(3293,550,'N',387,'WESH','=',null,'Y');
exec cre_dqr(3319,560,'N',391,'WESH','=',null,'Y');
exec cre_dqr(4333,570,'N',700,'WESH','=',null,'Y');
exec cre_dqr(3735,580,'N',465,'WESH','=',null,'Y');
exec cre_dqr(3943,590,'N',507,'WESH','=',null,'Y');
exec cre_dqr(4307,600,'N',696,'WESH','=',null,'Y');
exec cre_dqr(3059,610,'N',37,'WESH','=',null,'Y');
exec cre_dqr(4281,620,'N',624,'WESH','=',null,'Y');
exec cre_dqr(4966,5,'Y',786,'AMEH','=',null,'Y');
exec cre_dqr(4947,10,'N',521,'AMEH',null,null,null);
exec cre_dqr(4951,20,'N',351,'AMEH','>=',null,null);
exec cre_dqr(4952,25,'N',357,'AMEH','<=',null,null);
exec cre_dqr(4920,50,'N',526,'AMEH','=',null,'Y');
exec cre_dqr(4931,65,'N',737,'AMEH','=',null,null);
exec cre_dqr(4932,70,'N',742,'AMEH','=',null,null);
exec cre_dqr(4953,80,'N',380,'AMEH','=',null,null);
exec cre_dqr(4954,90,'N',384,'AMEH','=',null,null);
exec cre_dqr(4937,100,'N',975,'AMEH','=',null,'Y');
exec cre_dqr(4946,110,'N',532,'AMEH',null,null,null);
exec cre_dqr(4921,120,'N',536,'AMEH','=',null,'Y');
exec cre_dqr(4957,130,'N',402,'AMEH','=',null,'Y');
exec cre_dqr(4958,140,'N',407,'AMEH','=',null,'Y');
exec cre_dqr(4959,150,'N',413,'AMEH','=',null,'Y');
exec cre_dqr(4960,160,'N',418,'AMEH','=',null,'Y');
exec cre_dqr(4961,170,'N',423,'AMEH','=',null,'Y');
exec cre_dqr(4962,180,'N',428,'AMEH','=',null,'Y');
exec cre_dqr(4963,190,'N',433,'AMEH','=',null,'Y');
exec cre_dqr(4938,200,'N',542,'AMEH',null,null,null);
exec cre_dqr(4964,210,'N',439,'AMEH','=',null,'Y');
exec cre_dqr(4965,220,'N',443,'AMEH','=',null,'Y');
exec cre_dqr(4906,230,'N',448,'AMEH','=',null,'Y');
exec cre_dqr(4907,240,'N',452,'AMEH','=',null,'Y');
exec cre_dqr(4908,250,'N',456,'AMEH','=',null,'Y');
exec cre_dqr(4909,260,'N',460,'AMEH','=',null,'Y');
exec cre_dqr(4910,270,'N',464,'AMEH','=',null,'Y');
exec cre_dqr(4912,280,'N',468,'AMEH','=',null,'Y');
exec cre_dqr(4922,290,'N',546,'AMEH','=',null,'Y');
exec cre_dqr(4945,300,'N',551,'AMEH',null,null,null);
exec cre_dqr(4923,310,'N',555,'AMEH','=',null,'Y');
exec cre_dqr(4924,320,'N',559,'AMEH','=',null,'Y');
exec cre_dqr(4925,330,'N',563,'AMEH','=',null,'Y');
exec cre_dqr(4926,340,'N',567,'AMEH','=',null,'Y');
exec cre_dqr(4927,350,'N',572,'AMEH','=',null,'Y');
exec cre_dqr(4928,360,'N',576,'AMEH','=',null,'Y');
exec cre_dqr(4929,370,'N',580,'AMEH','=',null,'Y');
exec cre_dqr(4930,380,'N',584,'AMEH','=',null,'Y');
exec cre_dqr(4944,400,'N',595,'AMEH',null,null,null);
exec cre_dqr(4913,410,'N',472,'AMEH','>=',null,null);
exec cre_dqr(4914,420,'N',476,'AMEH','<=',null,null);
exec cre_dqr(4943,430,'N',600,'AMEH',null,null,null);
exec cre_dqr(4933,450,'N',608,'AMEH','=',null,'Y');
exec cre_dqr(4939,460,'N',404,'AMEH','=',null,null);
exec cre_dqr(4942,470,'N',613,'AMEH',null,null,null);
exec cre_dqr(4915,480,'N',485,'AMEH','=',null,'Y');
exec cre_dqr(4916,490,'N',489,'AMEH','=',null,'Y');
exec cre_dqr(4917,500,'N',493,'AMEH','=',null,'Y');
exec cre_dqr(4918,510,'N',497,'AMEH','=',null,'Y');
exec cre_dqr(4941,520,'N',620,'AMEH',null,null,null);
exec cre_dqr(4949,530,'N',45,'AMEH','=',null,'Y');
exec cre_dqr(4950,540,'N',52,'AMEH','=',null,'Y');
exec cre_dqr(4955,550,'N',387,'AMEH','=',null,'Y');
exec cre_dqr(4956,560,'N',391,'AMEH','=',null,'Y');
exec cre_dqr(4936,570,'N',700,'AMEH','=',null,'Y');
exec cre_dqr(4911,580,'N',465,'AMEH','=',null,'Y');
exec cre_dqr(4919,590,'N',507,'AMEH','=',null,'Y');
exec cre_dqr(4935,600,'N',696,'AMEH','=',null,'Y');
exec cre_dqr(4948,610,'N',37,'AMEH','=',null,'Y');
exec cre_dqr(4934,620,'N',624,'AMEH','=',null,'Y');
exec cre_dqr(5027,5,'Y',824,'UNCH','=',null,'Y');
exec cre_dqr(5008,10,'N',521,'UNCH',null,null,null);
exec cre_dqr(5012,20,'N',351,'UNCH','>=',null,null);
exec cre_dqr(5013,25,'N',357,'UNCH','<=',null,null);
exec cre_dqr(4982,50,'N',526,'UNCH','=',null,'Y');
exec cre_dqr(4993,65,'N',737,'UNCH','=',null,null);
exec cre_dqr(4994,70,'N',742,'UNCH','=',null,null);
exec cre_dqr(5014,80,'N',380,'UNCH','=',null,null);
exec cre_dqr(5015,90,'N',384,'UNCH','=',null,null);
exec cre_dqr(4999,100,'N',975,'UNCH','=',null,'Y');
exec cre_dqr(5007,110,'N',532,'UNCH',null,null,null);
exec cre_dqr(4983,120,'N',536,'UNCH','=',null,'Y');
exec cre_dqr(5018,130,'N',402,'UNCH','=',null,'Y');
exec cre_dqr(5019,140,'N',407,'UNCH','=',null,'Y');
exec cre_dqr(5020,150,'N',413,'UNCH','=',null,'Y');
exec cre_dqr(5021,160,'N',418,'UNCH','=',null,'Y');
exec cre_dqr(5022,170,'N',423,'UNCH','=',null,'Y');
exec cre_dqr(5023,180,'N',428,'UNCH','=',null,'Y');
exec cre_dqr(5024,190,'N',433,'UNCH','=',null,'Y');
exec cre_dqr(5000,200,'N',542,'UNCH',null,null,null);
exec cre_dqr(5025,210,'N',439,'UNCH','=',null,'Y');
exec cre_dqr(5026,220,'N',443,'UNCH','=',null,'Y');
exec cre_dqr(4968,230,'N',448,'UNCH','=',null,'Y');
exec cre_dqr(4969,240,'N',452,'UNCH','=',null,'Y');
exec cre_dqr(4970,250,'N',456,'UNCH','=',null,'Y');
exec cre_dqr(4971,260,'N',460,'UNCH','=',null,'Y');
exec cre_dqr(4972,270,'N',464,'UNCH','=',null,'Y');
exec cre_dqr(4974,280,'N',468,'UNCH','=',null,'Y');
exec cre_dqr(4984,290,'N',546,'UNCH','=',null,'Y');
exec cre_dqr(5006,300,'N',551,'UNCH',null,null,null);
exec cre_dqr(4985,310,'N',555,'UNCH','=',null,'Y');
exec cre_dqr(4986,320,'N',559,'UNCH','=',null,'Y');
exec cre_dqr(4987,330,'N',563,'UNCH','=',null,'Y');
exec cre_dqr(4988,340,'N',567,'UNCH','=',null,'Y');
exec cre_dqr(4989,350,'N',572,'UNCH','=',null,'Y');
exec cre_dqr(4990,360,'N',576,'UNCH','=',null,'Y');
exec cre_dqr(4991,370,'N',580,'UNCH','=',null,'Y');
exec cre_dqr(4992,380,'N',584,'UNCH','=',null,'Y');
exec cre_dqr(5005,400,'N',595,'UNCH',null,null,null);
exec cre_dqr(4975,410,'N',472,'UNCH','>=',null,null);
exec cre_dqr(4976,420,'N',476,'UNCH','<=',null,null);
exec cre_dqr(5004,430,'N',600,'UNCH',null,null,null);
exec cre_dqr(4995,450,'N',608,'UNCH','=',null,'Y');
exec cre_dqr(5001,460,'N',404,'UNCH','=',null,null);
exec cre_dqr(5003,470,'N',613,'UNCH',null,null,null);
exec cre_dqr(4977,480,'N',485,'UNCH','=',null,'Y');
exec cre_dqr(4978,490,'N',489,'UNCH','=',null,'Y');
exec cre_dqr(4979,500,'N',493,'UNCH','=',null,'Y');
exec cre_dqr(4980,510,'N',497,'UNCH','=',null,'Y');
exec cre_dqr(5002,520,'N',620,'UNCH',null,null,null);
exec cre_dqr(5010,530,'N',45,'UNCH','=',null,'Y');
exec cre_dqr(5011,540,'N',52,'UNCH','=',null,'Y');
exec cre_dqr(5016,550,'N',387,'UNCH','=',null,'Y');
exec cre_dqr(5017,560,'N',391,'UNCH','=',null,'Y');
exec cre_dqr(4998,570,'N',700,'UNCH','=',null,'Y');
exec cre_dqr(4973,580,'N',465,'UNCH','=',null,'Y');
exec cre_dqr(4981,590,'N',507,'UNCH','=',null,'Y');
exec cre_dqr(4997,600,'N',696,'UNCH','=',null,'Y');
exec cre_dqr(5009,610,'N',37,'UNCH','=',null,'Y');
exec cre_dqr(4996,620,'N',624,'UNCH','=',null,'Y');
exec cre_dqr(5088,5,'Y',823,'UNSA','=',null,'Y');
exec cre_dqr(5069,10,'N',521,'UNSA',null,null,null);
exec cre_dqr(5073,20,'N',351,'UNSA','>=',null,null);
exec cre_dqr(5074,25,'N',357,'UNSA','<=',null,null);
exec cre_dqr(5043,50,'N',526,'UNSA','=',null,'Y');
exec cre_dqr(5054,65,'N',737,'UNSA','=',null,null);
exec cre_dqr(5055,70,'N',742,'UNSA','=',null,null);
exec cre_dqr(5075,80,'N',380,'UNSA','=',null,null);
exec cre_dqr(5076,90,'N',384,'UNSA','=',null,null);
exec cre_dqr(5060,100,'N',975,'UNSA','=',null,'Y');
exec cre_dqr(5068,110,'N',532,'UNSA',null,null,null);
exec cre_dqr(5044,120,'N',536,'UNSA','=',null,'Y');
exec cre_dqr(5079,130,'N',402,'UNSA','=',null,'Y');
exec cre_dqr(5080,140,'N',407,'UNSA','=',null,'Y');
exec cre_dqr(5081,150,'N',413,'UNSA','=',null,'Y');
exec cre_dqr(5082,160,'N',418,'UNSA','=',null,'Y');
exec cre_dqr(5083,170,'N',423,'UNSA','=',null,'Y');
exec cre_dqr(5084,180,'N',428,'UNSA','=',null,'Y');
exec cre_dqr(5085,190,'N',433,'UNSA','=',null,'Y');
exec cre_dqr(5061,200,'N',542,'UNSA',null,null,null);
exec cre_dqr(5086,210,'N',439,'UNSA','=',null,'Y');
exec cre_dqr(5087,220,'N',443,'UNSA','=',null,'Y');
exec cre_dqr(5029,230,'N',448,'UNSA','=',null,'Y');
exec cre_dqr(5030,240,'N',452,'UNSA','=',null,'Y');
exec cre_dqr(5031,250,'N',456,'UNSA','=',null,'Y');
exec cre_dqr(5032,260,'N',460,'UNSA','=',null,'Y');
exec cre_dqr(5033,270,'N',464,'UNSA','=',null,'Y');
exec cre_dqr(5035,280,'N',468,'UNSA','=',null,'Y');
exec cre_dqr(5045,290,'N',546,'UNSA','=',null,'Y');
exec cre_dqr(5067,300,'N',551,'UNSA',null,null,null);
exec cre_dqr(5046,310,'N',555,'UNSA','=',null,'Y');
exec cre_dqr(5047,320,'N',559,'UNSA','=',null,'Y');
exec cre_dqr(5048,330,'N',563,'UNSA','=',null,'Y');
exec cre_dqr(5049,340,'N',567,'UNSA','=',null,'Y');
exec cre_dqr(5050,350,'N',572,'UNSA','=',null,'Y');
exec cre_dqr(5051,360,'N',576,'UNSA','=',null,'Y');
exec cre_dqr(5052,370,'N',580,'UNSA','=',null,'Y');
exec cre_dqr(5053,380,'N',584,'UNSA','=',null,'Y');
exec cre_dqr(5066,400,'N',595,'UNSA',null,null,null);
exec cre_dqr(5036,410,'N',472,'UNSA','>=',null,null);
exec cre_dqr(5037,420,'N',476,'UNSA','<=',null,null);
exec cre_dqr(5065,430,'N',600,'UNSA',null,null,null);
exec cre_dqr(5056,450,'N',608,'UNSA','=',null,'Y');
exec cre_dqr(5062,460,'N',404,'UNSA','=',null,null);
exec cre_dqr(5064,470,'N',613,'UNSA',null,null,null);
exec cre_dqr(5038,480,'N',485,'UNSA','=',null,'Y');
exec cre_dqr(5039,490,'N',489,'UNSA','=',null,'Y');
exec cre_dqr(5040,500,'N',493,'UNSA','=',null,'Y');
exec cre_dqr(5041,510,'N',497,'UNSA','=',null,'Y');
exec cre_dqr(5063,520,'N',620,'UNSA',null,null,null);
exec cre_dqr(5071,530,'N',45,'UNSA','=',null,'Y');
exec cre_dqr(5072,540,'N',52,'UNSA','=',null,'Y');
exec cre_dqr(5077,550,'N',387,'UNSA','=',null,'Y');
exec cre_dqr(5078,560,'N',391,'UNSA','=',null,'Y');
exec cre_dqr(5059,570,'N',700,'UNSA','=',null,'Y');
exec cre_dqr(5034,580,'N',465,'UNSA','=',null,'Y');
exec cre_dqr(5042,590,'N',507,'UNSA','=',null,'Y');
exec cre_dqr(5058,600,'N',696,'UNSA','=',null,'Y');
exec cre_dqr(5070,610,'N',37,'UNSA','=',null,'Y');
exec cre_dqr(5057,620,'N',624,'UNSA','=',null,'Y');
exec cre_dqr(5149,5,'Y',822,'YOUR','=',null,'Y');
exec cre_dqr(5130,10,'N',521,'YOUR',null,null,null);
exec cre_dqr(5134,20,'N',351,'YOUR','>=',null,null);
exec cre_dqr(5135,25,'N',357,'YOUR','<=',null,null);
exec cre_dqr(5104,50,'N',526,'YOUR','=',null,'Y');
exec cre_dqr(5115,65,'N',737,'YOUR','=',null,null);
exec cre_dqr(5116,70,'N',742,'YOUR','=',null,null);
exec cre_dqr(5136,80,'N',380,'YOUR','=',null,null);
exec cre_dqr(5137,90,'N',384,'YOUR','=',null,null);
exec cre_dqr(5121,100,'N',975,'YOUR','=',null,'Y');
exec cre_dqr(5129,110,'N',532,'YOUR',null,null,null);
exec cre_dqr(5105,120,'N',536,'YOUR','=',null,'Y');
exec cre_dqr(5140,130,'N',402,'YOUR','=',null,'Y');
exec cre_dqr(5141,140,'N',407,'YOUR','=',null,'Y');
exec cre_dqr(5142,150,'N',413,'YOUR','=',null,'Y');
exec cre_dqr(5143,160,'N',418,'YOUR','=',null,'Y');
exec cre_dqr(5144,170,'N',423,'YOUR','=',null,'Y');
exec cre_dqr(5145,180,'N',428,'YOUR','=',null,'Y');
exec cre_dqr(5146,190,'N',433,'YOUR','=',null,'Y');
exec cre_dqr(5122,200,'N',542,'YOUR',null,null,null);
exec cre_dqr(5147,210,'N',439,'YOUR','=',null,'Y');
exec cre_dqr(5148,220,'N',443,'YOUR','=',null,'Y');
exec cre_dqr(5090,230,'N',448,'YOUR','=',null,'Y');
exec cre_dqr(5091,240,'N',452,'YOUR','=',null,'Y');
exec cre_dqr(5092,250,'N',456,'YOUR','=',null,'Y');
exec cre_dqr(5093,260,'N',460,'YOUR','=',null,'Y');
exec cre_dqr(5094,270,'N',464,'YOUR','=',null,'Y');
exec cre_dqr(5096,280,'N',468,'YOUR','=',null,'Y');
exec cre_dqr(5106,290,'N',546,'YOUR','=',null,'Y');
exec cre_dqr(5128,300,'N',551,'YOUR',null,null,null);
exec cre_dqr(5107,310,'N',555,'YOUR','=',null,'Y');
exec cre_dqr(5108,320,'N',559,'YOUR','=',null,'Y');
exec cre_dqr(5109,330,'N',563,'YOUR','=',null,'Y');
exec cre_dqr(5110,340,'N',567,'YOUR','=',null,'Y');
exec cre_dqr(5111,350,'N',572,'YOUR','=',null,'Y');
exec cre_dqr(5112,360,'N',576,'YOUR','=',null,'Y');
exec cre_dqr(5113,370,'N',580,'YOUR','=',null,'Y');
exec cre_dqr(5114,380,'N',584,'YOUR','=',null,'Y');
exec cre_dqr(5127,400,'N',595,'YOUR',null,null,null);
exec cre_dqr(5097,410,'N',472,'YOUR','>=',null,null);
exec cre_dqr(5098,420,'N',476,'YOUR','<=',null,null);
exec cre_dqr(5126,430,'N',600,'YOUR',null,null,null);
exec cre_dqr(5117,450,'N',608,'YOUR','=',null,'Y');
exec cre_dqr(5123,460,'N',404,'YOUR','=',null,null);
exec cre_dqr(5125,470,'N',613,'YOUR',null,null,null);
exec cre_dqr(5099,480,'N',485,'YOUR','=',null,'Y');
exec cre_dqr(5100,490,'N',489,'YOUR','=',null,'Y');
exec cre_dqr(5101,500,'N',493,'YOUR','=',null,'Y');
exec cre_dqr(5102,510,'N',497,'YOUR','=',null,'Y');
exec cre_dqr(5124,520,'N',620,'YOUR',null,null,null);
exec cre_dqr(5132,530,'N',45,'YOUR','=',null,'Y');
exec cre_dqr(5133,540,'N',52,'YOUR','=',null,'Y');
exec cre_dqr(5138,550,'N',387,'YOUR','=',null,'Y');
exec cre_dqr(5139,560,'N',391,'YOUR','=',null,'Y');
exec cre_dqr(5120,570,'N',700,'YOUR','=',null,'Y');
exec cre_dqr(5095,580,'N',465,'YOUR','=',null,'Y');
exec cre_dqr(5103,590,'N',507,'YOUR','=',null,'Y');
exec cre_dqr(5119,600,'N',696,'YOUR','=',null,'Y');
exec cre_dqr(5131,610,'N',37,'YOUR','=',null,'Y');
exec cre_dqr(5118,620,'N',624,'YOUR','=',null,'Y');
/*exec cre_dqr(860,5,'Y',238,'ISHA','=',null,'Y');
exec cre_dqr(4582,10,'N',521,'ISHA',null,null,null);
exec cre_dqr(2093,20,'N',351,'ISHA','>=',null,null);
exec cre_dqr(2108,25,'N',357,'ISHA','<=',null,null);
exec cre_dqr(2573,50,'N',526,'ISHA','=',null,'Y');
exec cre_dqr(4761,65,'N',737,'ISHA','=',null,null);
exec cre_dqr(4801,70,'N',742,'ISHA','=',null,null);
exec cre_dqr(2153,80,'N',380,'ISHA','=',null,null);
exec cre_dqr(2168,90,'N',384,'ISHA','=',null,null);
exec cre_dqr(3023,100,'N',975,'ISHA','=',null,'Y');
exec cre_dqr(4581,110,'N',532,'ISHA',null,null,null);
exec cre_dqr(2588,120,'N',536,'ISHA','=',null,'Y');
exec cre_dqr(2228,130,'N',402,'ISHA','=',null,'Y');
exec cre_dqr(2243,140,'N',407,'ISHA','=',null,'Y');
exec cre_dqr(2258,150,'N',413,'ISHA','=',null,'Y');
exec cre_dqr(2273,160,'N',418,'ISHA','=',null,'Y');
exec cre_dqr(2288,170,'N',423,'ISHA','=',null,'Y');
exec cre_dqr(2303,180,'N',428,'ISHA','=',null,'Y');
exec cre_dqr(2318,190,'N',433,'ISHA','=',null,'Y');
exec cre_dqr(4693,200,'N',542,'ISHA',null,null,null);
exec cre_dqr(2333,210,'N',439,'ISHA','=',null,'Y');
exec cre_dqr(2348,220,'N',443,'ISHA','=',null,'Y');
exec cre_dqr(2363,230,'N',448,'ISHA','=',null,'Y');
exec cre_dqr(2378,240,'N',452,'ISHA','=',null,'Y');
exec cre_dqr(2393,250,'N',456,'ISHA','=',null,'Y');
exec cre_dqr(2408,260,'N',460,'ISHA','=',null,'Y');
exec cre_dqr(2423,270,'N',464,'ISHA','=',null,'Y');
exec cre_dqr(2453,280,'N',468,'ISHA','=',null,'Y');
exec cre_dqr(2603,290,'N',546,'ISHA','=',null,'Y');
exec cre_dqr(4580,300,'N',551,'ISHA',null,null,null);
exec cre_dqr(2618,310,'N',555,'ISHA','=',null,'Y');
exec cre_dqr(2633,320,'N',559,'ISHA','=',null,'Y');
exec cre_dqr(2648,330,'N',563,'ISHA','=',null,'Y');
exec cre_dqr(2663,340,'N',567,'ISHA','=',null,'Y');
exec cre_dqr(2678,350,'N',572,'ISHA','=',null,'Y');
exec cre_dqr(2693,360,'N',576,'ISHA','=',null,'Y');
exec cre_dqr(2708,370,'N',580,'ISHA','=',null,'Y');
exec cre_dqr(2723,380,'N',584,'ISHA','=',null,'Y');
exec cre_dqr(2738,390,'N',588,'ISHA','=',null,'Y');
exec cre_dqr(4579,400,'N',595,'ISHA',null,null,null);
exec cre_dqr(2468,410,'N',472,'ISHA','>=',null,null);
exec cre_dqr(2483,420,'N',476,'ISHA','<=',null,null);
exec cre_dqr(4578,430,'N',600,'ISHA',null,null,null);
exec cre_dqr(2753,440,'N',604,'ISHA','=',null,'Y');
exec cre_dqr(2768,450,'N',608,'ISHA','=',null,'Y');
exec cre_dqr(4734,460,'N',404,'ISHA','=',null,null);
exec cre_dqr(4577,470,'N',613,'ISHA',null,null,null);
exec cre_dqr(2498,480,'N',485,'ISHA','=',null,'Y');
exec cre_dqr(2513,490,'N',489,'ISHA','=',null,'Y');
exec cre_dqr(2528,500,'N',493,'ISHA','=',null,'Y');
exec cre_dqr(2543,510,'N',497,'ISHA','=',null,'Y');
exec cre_dqr(4576,520,'N',620,'ISHA',null,null,null);
exec cre_dqr(2063,530,'N',45,'ISHA','=',null,'Y');
exec cre_dqr(2078,540,'N',52,'ISHA','=',null,'Y');
exec cre_dqr(2183,550,'N',387,'ISHA','=',null,'Y');
exec cre_dqr(2198,560,'N',391,'ISHA','=',null,'Y');
exec cre_dqr(3008,570,'N',700,'ISHA','=',null,'Y');
exec cre_dqr(2438,580,'N',465,'ISHA','=',null,'Y');
exec cre_dqr(2558,590,'N',507,'ISHA','=',null,'Y');
exec cre_dqr(2993,600,'N',696,'ISHA','=',null,'Y');
exec cre_dqr(2048,610,'N',37,'ISHA','=',null,'Y');
exec cre_dqr(2783,620,'N',624,'ISHA','=',null,'Y');
exec cre_dqr(4575,630,'N',630,'ISHA',null,null,null);
exec cre_dqr(2798,640,'N',634,'ISHA','=',null,'Y');
exec cre_dqr(2813,650,'N',638,'ISHA','=',null,'Y');
exec cre_dqr(2828,660,'N',642,'ISHA','=',null,'Y');
exec cre_dqr(2843,670,'N',646,'ISHA','=',null,'Y');
exec cre_dqr(2858,680,'N',650,'ISHA','=',null,'Y');
exec cre_dqr(2873,690,'N',654,'ISHA','=',null,'Y');
exec cre_dqr(2888,700,'N',661,'ISHA','=',null,'Y');
exec cre_dqr(2903,710,'N',668,'ISHA','=',null,'Y');
exec cre_dqr(2918,720,'N',676,'ISHA','=',null,'Y');
exec cre_dqr(2933,730,'N',680,'ISHA','=',null,'Y');
exec cre_dqr(2948,740,'N',684,'ISHA','=',null,'Y');
exec cre_dqr(2963,750,'N',688,'ISHA','=',null,'Y');
exec cre_dqr(2978,760,'N',692,'ISHA','=',null,'Y');
exec cre_dqr(870,5,'Y',271,'PEAC','=',null,'Y');
exec cre_dqr(4622,10,'N',521,'PEAC',null,null,null);
exec cre_dqr(2098,20,'N',351,'PEAC','>=',null,null);
exec cre_dqr(2113,25,'N',357,'PEAC','<=',null,null);
exec cre_dqr(2578,50,'N',526,'PEAC','=',null,'Y');
exec cre_dqr(4771,65,'N',737,'PEAC','=',null,null);
exec cre_dqr(4811,70,'N',742,'PEAC','=',null,null);
exec cre_dqr(2158,80,'N',380,'PEAC','=',null,null);
exec cre_dqr(2173,90,'N',384,'PEAC','=',null,null);
exec cre_dqr(3028,100,'N',975,'PEAC','=',null,'Y');
exec cre_dqr(4621,110,'N',532,'PEAC',null,null,null);
exec cre_dqr(2593,120,'N',536,'PEAC','=',null,'Y');
exec cre_dqr(2233,130,'N',402,'PEAC','=',null,'Y');
exec cre_dqr(2248,140,'N',407,'PEAC','=',null,'Y');
exec cre_dqr(2263,150,'N',413,'PEAC','=',null,'Y');
exec cre_dqr(2278,160,'N',418,'PEAC','=',null,'Y');
exec cre_dqr(2293,170,'N',423,'PEAC','=',null,'Y');
exec cre_dqr(2308,180,'N',428,'PEAC','=',null,'Y');
exec cre_dqr(2323,190,'N',433,'PEAC','=',null,'Y');
exec cre_dqr(4698,200,'N',542,'PEAC',null,null,null);
exec cre_dqr(2338,210,'N',439,'PEAC','=',null,'Y');
exec cre_dqr(2353,220,'N',443,'PEAC','=',null,'Y');
exec cre_dqr(2368,230,'N',448,'PEAC','=',null,'Y');
exec cre_dqr(2383,240,'N',452,'PEAC','=',null,'Y');
exec cre_dqr(2398,250,'N',456,'PEAC','=',null,'Y');
exec cre_dqr(2413,260,'N',460,'PEAC','=',null,'Y');
exec cre_dqr(2428,270,'N',464,'PEAC','=',null,'Y');
exec cre_dqr(2458,280,'N',468,'PEAC','=',null,'Y');
exec cre_dqr(2608,290,'N',546,'PEAC','=',null,'Y');
exec cre_dqr(4620,300,'N',551,'PEAC',null,null,null);
exec cre_dqr(2623,310,'N',555,'PEAC','=',null,'Y');
exec cre_dqr(2638,320,'N',559,'PEAC','=',null,'Y');
exec cre_dqr(2653,330,'N',563,'PEAC','=',null,'Y');
exec cre_dqr(2668,340,'N',567,'PEAC','=',null,'Y');
exec cre_dqr(2683,350,'N',572,'PEAC','=',null,'Y');
exec cre_dqr(2698,360,'N',576,'PEAC','=',null,'Y');
exec cre_dqr(2713,370,'N',580,'PEAC','=',null,'Y');
exec cre_dqr(2728,380,'N',584,'PEAC','=',null,'Y');
exec cre_dqr(2743,390,'N',588,'PEAC','=',null,'Y');
exec cre_dqr(4619,400,'N',595,'PEAC',null,null,null);
exec cre_dqr(2473,410,'N',472,'PEAC','>=',null,null);
exec cre_dqr(2488,420,'N',476,'PEAC','<=',null,null);
exec cre_dqr(4618,430,'N',600,'PEAC',null,null,null);
exec cre_dqr(2758,440,'N',604,'PEAC','=',null,'Y');
exec cre_dqr(2773,450,'N',608,'PEAC','=',null,'Y');
exec cre_dqr(4739,460,'N',404,'PEAC','=',null,null);
exec cre_dqr(4617,470,'N',613,'PEAC',null,null,null);
exec cre_dqr(2503,480,'N',485,'PEAC','=',null,'Y');
exec cre_dqr(2518,490,'N',489,'PEAC','=',null,'Y');
exec cre_dqr(2533,500,'N',493,'PEAC','=',null,'Y');
exec cre_dqr(2548,510,'N',497,'PEAC','=',null,'Y');
exec cre_dqr(4616,520,'N',620,'PEAC',null,null,null);
exec cre_dqr(2068,530,'N',45,'PEAC','=',null,'Y');
exec cre_dqr(2083,540,'N',52,'PEAC','=',null,'Y');
exec cre_dqr(2188,550,'N',387,'PEAC','=',null,'Y');
exec cre_dqr(2203,560,'N',391,'PEAC','=',null,'Y');
exec cre_dqr(3013,570,'N',700,'PEAC','=',null,'Y');
exec cre_dqr(2443,580,'N',465,'PEAC','=',null,'Y');
exec cre_dqr(2563,590,'N',507,'PEAC','=',null,'Y');
exec cre_dqr(2998,600,'N',696,'PEAC','=',null,'Y');
exec cre_dqr(2053,610,'N',37,'PEAC','=',null,'Y');
exec cre_dqr(2788,620,'N',624,'PEAC','=',null,'Y');
exec cre_dqr(4615,630,'N',630,'PEAC',null,null,null);
exec cre_dqr(2803,640,'N',634,'PEAC','=',null,'Y');
exec cre_dqr(2818,650,'N',638,'PEAC','=',null,'Y');
exec cre_dqr(2833,660,'N',642,'PEAC','=',null,'Y');
exec cre_dqr(2848,670,'N',646,'PEAC','=',null,'Y');
exec cre_dqr(2863,680,'N',650,'PEAC','=',null,'Y');
exec cre_dqr(2878,690,'N',654,'PEAC','=',null,'Y');
exec cre_dqr(2893,700,'N',661,'PEAC','=',null,'Y');
exec cre_dqr(2908,710,'N',668,'PEAC','=',null,'Y');
exec cre_dqr(2923,720,'N',676,'PEAC','=',null,'Y');
exec cre_dqr(2938,730,'N',680,'PEAC','=',null,'Y');
exec cre_dqr(2953,740,'N',684,'PEAC','=',null,'Y');
exec cre_dqr(2968,750,'N',688,'PEAC','=',null,'Y');
exec cre_dqr(2983,760,'N',692,'PEAC','=',null,'Y');
exec cre_dqr(880,5,'Y',295,'UNIS','=',null,'Y');
exec cre_dqr(4514,10,'N',521,'UNIS',null,null,null);
exec cre_dqr(3133,20,'N',351,'UNIS','>=',null,null);
exec cre_dqr(3159,25,'N',357,'UNIS','<=',null,null);
exec cre_dqr(3965,50,'N',526,'UNIS','=',null,'Y');
exec cre_dqr(4781,65,'N',737,'UNIS','=',null,null);
exec cre_dqr(4821,70,'N',742,'UNIS','=',null,null);
exec cre_dqr(3237,80,'N',380,'UNIS','=',null,null);
exec cre_dqr(3263,90,'N',384,'UNIS','=',null,null);
exec cre_dqr(4355,100,'N',975,'UNIS','=',null,'Y',null);
exec cre_dqr(4513,110,'N',532,'UNIS',null,null,null);
exec cre_dqr(3991,120,'N',536,'UNIS','=',null,'Y');
exec cre_dqr(3367,130,'N',402,'UNIS','=',null,'Y');
exec cre_dqr(3393,140,'N',407,'UNIS','=',null,'Y');
exec cre_dqr(3419,150,'N',413,'UNIS','=',null,'Y');
exec cre_dqr(3445,160,'N',418,'UNIS','=',null,'Y');
exec cre_dqr(3471,170,'N',423,'UNIS','=',null,'Y');
exec cre_dqr(3497,180,'N',428,'UNIS','=',null,'Y');
exec cre_dqr(3523,190,'N',433,'UNIS','=',null,'Y');
exec cre_dqr(4684,200,'N',542,'UNIS',null,null,null);
exec cre_dqr(3549,210,'N',439,'UNIS','=',null,'Y');
exec cre_dqr(3575,220,'N',443,'UNIS','=',null,'Y');
exec cre_dqr(3601,230,'N',448,'UNIS','=',null,'Y');
exec cre_dqr(3627,240,'N',452,'UNIS','=',null,'Y');
exec cre_dqr(3653,250,'N',456,'UNIS','=',null,'Y');
exec cre_dqr(3679,260,'N',460,'UNIS','=',null,'Y');
exec cre_dqr(3705,270,'N',464,'UNIS','=',null,'Y');
exec cre_dqr(3757,280,'N',468,'UNIS','=',null,'Y');
exec cre_dqr(4017,290,'N',546,'UNIS','=',null,'Y');
exec cre_dqr(4512,300,'N',551,'UNIS',null,null,null);
exec cre_dqr(4043,310,'N',555,'UNIS','=',null,'Y');
exec cre_dqr(4069,320,'N',559,'UNIS','=',null,'Y');
exec cre_dqr(4095,330,'N',563,'UNIS','=',null,'Y');
exec cre_dqr(4121,340,'N',567,'UNIS','=',null,'Y');
exec cre_dqr(4147,350,'N',572,'UNIS','=',null,'Y');
exec cre_dqr(4173,360,'N',576,'UNIS','=',null,'Y');
exec cre_dqr(4199,370,'N',580,'UNIS','=',null,'Y');
exec cre_dqr(4225,380,'N',584,'UNIS','=',null,'Y');
exec cre_dqr(4511,400,'N',595,'UNIS',null,null,null);
exec cre_dqr(3783,410,'N',472,'UNIS','>=',null,null);
exec cre_dqr(3809,420,'N',476,'UNIS','<=',null,null);
exec cre_dqr(4510,430,'N',600,'UNIS',null,null,null);
exec cre_dqr(4251,450,'N',608,'UNIS','=',null,'Y');
exec cre_dqr(4725,460,'N',404,'UNIS','=',null,null);
exec cre_dqr(4509,470,'N',613,'UNIS',null,null,null);
exec cre_dqr(3835,480,'N',485,'UNIS','=',null,'Y');
exec cre_dqr(3861,490,'N',489,'UNIS','=',null,'Y');
exec cre_dqr(3887,500,'N',493,'UNIS','=',null,'Y');
exec cre_dqr(3913,510,'N',497,'UNIS','=',null,'Y');
exec cre_dqr(4508,520,'N',620,'UNIS',null,null,null);
exec cre_dqr(3081,530,'N',45,'UNIS','=',null,'Y');
exec cre_dqr(3107,540,'N',52,'UNIS','=',null,'Y');
exec cre_dqr(3289,550,'N',387,'UNIS','=',null,'Y');
exec cre_dqr(3315,560,'N',391,'UNIS','=',null,'Y');
exec cre_dqr(4329,570,'N',700,'UNIS','=',null,'Y');
exec cre_dqr(3731,580,'N',465,'UNIS','=',null,'Y');
exec cre_dqr(3939,590,'N',507,'UNIS','=',null,'Y');
exec cre_dqr(4303,600,'N',696,'UNIS','=',null,'Y');
exec cre_dqr(3055,610,'N',37,'UNIS','=',null,'Y');
exec cre_dqr(4277,620,'N',624,'UNIS','=',null,'Y');*/


MERGE INTO elig_questions tgt
USING (SELECT equ_que_refno, 
              frv_code equ_hhts_hrv_els_code, 
              equ_hhts_hty_code, 
              equ_seqno, 
              equ_min_value, 
              equ_max_value, 
              equ_answer, 
              equ_qpr_refno
         FROM elig_questions
        CROSS JOIN first_ref_values frv
        WHERE equ_hhts_hrv_els_code = 'ESGEN'
          AND equ_que_refno != 619
          AND frv_frd_domain = 'ELG_SCHEME'
          AND frv_code not in ('ESGEN','ESAB')
        ORDER BY 2,3,1) src
   ON (    tgt.equ_hhts_hrv_els_code = src.equ_hhts_hrv_els_code
       AND tgt.equ_hhts_hty_code = src.equ_hhts_hty_code
       AND tgt.equ_que_refno = src.equ_que_refno)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.equ_seqno = src.equ_seqno,
           tgt.equ_min_value = src.equ_min_value,
           tgt.equ_max_value = src.equ_max_value,
           tgt.equ_answer = src.equ_answer,
           tgt.equ_qpr_refno = src.equ_qpr_refno
 WHEN NOT MATCHED THEN
    INSERT(equ_que_refno, 
           equ_hhts_hrv_els_code, 
           equ_hhts_hty_code, 
           equ_seqno, 
           equ_min_value, 
           equ_max_value, 
           equ_answer, 
           equ_qpr_refno)
    VALUES(src.equ_que_refno, 
           src.equ_hhts_hrv_els_code, 
           src.equ_hhts_hty_code, 
           src.equ_seqno, 
           src.equ_min_value, 
           src.equ_max_value, 
           src.equ_answer, 
           src.equ_qpr_refno);

MERGE INTO elig_questions tgt
USING (SELECT DECODE(frv_code, 'ESGEN',619,'ESTR',437,'ESMX',432,'ESAB',417,'ESCHP',422,'ESCOOP',427) equ_que_refno, 
              frv_code equ_hhts_hrv_els_code, 
              hty_code equ_hhts_hty_code, 
              50 equ_seqno, 
              'Y' equ_answer
         FROM hhold_types
        CROSS JOIN first_ref_values frv
        WHERE frv_frd_domain = 'ELG_SCHEME'
          AND hty_code NOT IN ('UNDERNSA','NOCONNECT','INELIGIBLE','UNCL')
        ORDER BY 3,2) src
   ON (    tgt.equ_hhts_hrv_els_code = src.equ_hhts_hrv_els_code
       AND tgt.equ_hhts_hty_code = src.equ_hhts_hty_code
       AND tgt.equ_que_refno = src.equ_que_refno)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.equ_seqno = src.equ_seqno,
           tgt.equ_min_value = NULL,
           tgt.equ_max_value = NULL,
           tgt.equ_answer = src.equ_answer,
           tgt.equ_qpr_refno = NULL
 WHEN NOT MATCHED THEN
    INSERT(equ_que_refno, 
           equ_hhts_hrv_els_code, 
           equ_hhts_hty_code, 
           equ_seqno, 
           equ_answer)
    VALUES(src.equ_que_refno, 
           src.equ_hhts_hrv_els_code, 
           src.equ_hhts_hty_code, 
           src.equ_seqno,
           src.equ_answer);

UPDATE hhold_type_elig_schemes
   SET hhts_heg_code = 'SINGLES'
 WHERE hhts_heg_code = 'SINGLESAB';

UPDATE hhold_type_elig_schemes
   SET hhts_heg_code = 'COUPLES'
 WHERE hhts_heg_code = 'COUPLESAB';

REM INSERTING into ELIG_GROUP_RULE_QUESTIONS
SPOO op_egr_vals.log
SET LONG 32000
SET LINESIZE 32000
SET PAGES 0

SELECT * FROM elig_group_rule_questions;
DELETE FROM elig_group_rule_questions;

SPOO op_config_install.log APPEND
exec cre_egr(313,'COUPLES',10,2,2,null);
exec cre_egr(227,'COUPLESAB',30,null,null,'Y');
exec cre_egr(313,'COUPLESAB',10,2,2,null);
exec cre_egr(371,'COUPLESAB',20,null,null,'Y');
exec cre_egr(371,'INELAB',10,null,null,'N');
exec cre_egr(227,'NOCONNECT',10,null,null,'N');
exec cre_egr(417,'NOCONNECT',20,null,null,'Y');
exec cre_egr(313,'SINGLES',10,1,1,null);
exec cre_egr(227,'SINGLESAB',30,null,null,'Y');
exec cre_egr(313,'SINGLESAB',10,1,1,null);
exec cre_egr(371,'SINGLESAB',20,null,null,'Y');
exec cre_egr(313,'INEL',10,1,2,null);

REM INSERTING into MATCHING_RULES
exec cre_mru(2,'LAR','DMC','PSCHP','LAR','Y',20,1);
exec cre_mru(5,'LAR','DMC','PSCOOP','LAR','Y',20,1);

REM INSERTING into SCHEME_QUESTIONS
exec cre_squ('PSCOOP',773,'CHPP','DQ',80,'N',85,217,'N');
exec cre_squ('PSAB',417,'DQ','DQ',70,'N',1000,413,'N');
exec cre_squ('PSAB',696,'SHR','HR',15,'N',70,2071,'N');
exec cre_squ('PSAB',700,'SHR','HR',15,'N',152,697,'N');
exec cre_squ('PSCHP',1,'SSA','AP',10,'N',1,1,'N');
exec cre_squ('PSCHP',9,'GHR','HR',10,'Y',10,4,'N');
exec cre_squ('PSCHP',19,'GHR','HR',10,'Y',20,16,'N');
exec cre_squ('PSCHP',27,'GHR','HR',10,'N',25,21,'N');
exec cre_squ('PSCHP',37,'GHR','HR',10,'Y',40,33,'N');
exec cre_squ('PSCHP',41,'STCR','SG',10,'Y',80,38,'N');
exec cre_squ('PSCHP',45,'SHR','HR',15,'Y',10,39,'N');
exec cre_squ('PSCHP',46,'DQ','DQ',70,'N',40,41,'N');
exec cre_squ('PSCHP',52,'SHR','HR',15,'Y',20,47,'N');
exec cre_squ('PSCHP',54,'STLV','SG',20,'N',210,51,'N');
exec cre_squ('PSCHP',58,'STLV','SG',20,'N',240,55,'N');
exec cre_squ('PSCHP',61,'SHR','HR',15,'N',210,58,'N');
exec cre_squ('PSCHP',66,'STLV','SG',20,'N',220,63,'N');
exec cre_squ('PSCHP',68,'SHR','HR',15,'N',211,64,'N');
exec cre_squ('PSCHP',70,'STLV','SG',20,'N',250,67,'N');
exec cre_squ('PSCHP',72,'DQ','DQ',70,'N',90,69,'N');
exec cre_squ('PSCHP',74,'STLV','SG',20,'N',230,71,'N');
exec cre_squ('PSCHP',82,'STLV','SG',20,'N',270,79,'N');
exec cre_squ('PSCHP',86,'STLV','SG',20,'N',300,83,'N');
exec cre_squ('PSCHP',87,'STHO','SG',50,'Y',100,82,'N');
exec cre_squ('PSCHP',90,'STLV','SG',20,'N',280,87,'N');
exec cre_squ('PSCHP',91,'AGCY','SG',5,'Y',10,88,'N');
exec cre_squ('PSCHP',94,'STLV','SG',20,'N',310,91,'N');
exec cre_squ('PSCHP',96,'AGCY','SG',5,'Y',20,92,'N');
exec cre_squ('PSCHP',98,'STLV','SG',20,'N',290,95,'N');
exec cre_squ('PSCHP',101,'AGCY','SG',5,'Y',30,97,'N');
exec cre_squ('PSCHP',106,'STLV','SG',20,'N',312,103,'N');
exec cre_squ('PSCHP',107,'AGCY','SG',5,'N',40,102,'N');
exec cre_squ('PSCHP',110,'STLV','SG',20,'N',325,107,'N');
exec cre_squ('PSCHP',114,'STLV','SG',20,'N',315,111,'N');
exec cre_squ('PSCHP',118,'STLV','SG',20,'N',328,115,'N');
exec cre_squ('PSCHP',123,'STLV','SG',20,'N',320,119,'N');
exec cre_squ('PSCHP',136,'STLV','SG',20,'Y',260,130,'N');
exec cre_squ('PSCHP',140,'STLV','SG',20,'Y',330,137,'N');
exec cre_squ('PSCHP',143,'AGCY','SG',5,'Y',50,108,'N');
exec cre_squ('PSCHP',148,'STHS','SG',30,'Y',10,145,'N');
exec cre_squ('PSCHP',149,'AGCY','SG',5,'N',60,144,'N');
exec cre_squ('PSCHP',153,'AGCY','SG',5,'N',70,150,'N');
exec cre_squ('PSCHP',157,'DQ','DQ',70,'N',420,154,'N');
exec cre_squ('PSCHP',172,'STBA','SG',35,'Y',80,166,'N');
exec cre_squ('PSCHP',176,'GE','CC',8,'N',30,173,'N');
exec cre_squ('PSCHP',179,'STHO','SG',50,'N',20,176,'N');
exec cre_squ('PSCHP',183,'STHO','SG',50,'N',30,180,'N');
exec cre_squ('PSCHP',187,'STHO','SG',50,'N',40,184,'N');
exec cre_squ('PSCHP',194,'STHO','SG',50,'N',70,188,'N');
exec cre_squ('PSCHP',199,'CHPP','DQ',80,'N',20,197,'N');
exec cre_squ('PSCHP',202,'STHO','SG',50,'N',75,195,'N');
exec cre_squ('PSCHP',203,'CHPP','DQ',80,'N',30,200,'N');
exec cre_squ('PSCHP',206,'CHPP','DQ',80,'N',40,204,'N');
exec cre_squ('PSCHP',209,'CHPP','DQ',80,'N',50,207,'N');
exec cre_squ('PSCHP',212,'CHPP','DQ',80,'N',60,210,'N');
exec cre_squ('PSCHP',215,'CHPP','DQ',80,'N',70,213,'N');
exec cre_squ('PSCHP',217,'CHPP','DQ',80,'N',80,216,'N');
exec cre_squ('PSCHP',219,'CHPP','DQ',80,'N',90,218,'N');
exec cre_squ('PSCHP',222,'CHPP','DQ',80,'N',100,220,'N');
exec cre_squ('PSCHP',224,'CHPP','DQ',80,'N',110,223,'N');
exec cre_squ('PSCHP',226,'CHPP','DQ',80,'N',120,225,'N');
exec cre_squ('PSCHP',228,'CHPP','DQ',80,'N',130,227,'N');
exec cre_squ('PSCHP',230,'CHPP','DQ',80,'N',140,229,'N');
exec cre_squ('PSCHP',232,'CHPP','DQ',80,'N',150,231,'N');
exec cre_squ('PSCHP',234,'CHPP','DQ',80,'N',160,233,'N');
exec cre_squ('PSCHP',236,'CHPP','DQ',80,'N',170,235,'N');
exec cre_squ('PSCHP',238,'CHPP','DQ',80,'N',180,237,'N');
exec cre_squ('PSCHP',240,'CHPP','DQ',80,'N',190,239,'N');
exec cre_squ('PSCHP',243,'CHPP','DQ',80,'N',200,241,'N');
exec cre_squ('PSCHP',246,'CHPP','DQ',80,'N',210,244,'N');
exec cre_squ('PSCHP',248,'CHPP','DQ',80,'N',220,247,'N');
exec cre_squ('PSCHP',250,'CHPP','DQ',80,'N',230,249,'N');
exec cre_squ('PSCHP',251,'GE','CC',8,'Y',10,248,'N');
exec cre_squ('PSCHP',252,'CHPP','DQ',80,'N',240,251,'N');
exec cre_squ('PSCHP',264,'CHPP','DQ',80,'N',250,262,'N');
exec cre_squ('PSCHP',266,'CHPP','DQ',80,'N',260,265,'N');
exec cre_squ('PSCHP',269,'CHPP','DQ',80,'N',270,267,'N');
exec cre_squ('PSCHP',271,'CHPP','DQ',80,'N',280,270,'N');
exec cre_squ('PSCHP',274,'CHPP','DQ',80,'N',290,272,'N');
exec cre_squ('PSCHP',276,'CHPP','DQ',80,'N',300,275,'N');
exec cre_squ('PSCHP',278,'CHPP','DQ',80,'N',310,277,'N');
exec cre_squ('PSCHP',280,'CHPP','DQ',80,'N',320,279,'N');
exec cre_squ('PSCHP',282,'CHPP','DQ',80,'N',330,281,'N');
exec cre_squ('PSCHP',285,'CHPP','DQ',80,'N',340,283,'N');
exec cre_squ('PSCHP',287,'CHPP','DQ',80,'N',350,286,'N');
exec cre_squ('PSCHP',290,'CHPP','DQ',80,'N',360,288,'N');
exec cre_squ('PSCHP',292,'CHPP','DQ',80,'N',370,291,'N');
exec cre_squ('PSCHP',295,'CHPP','DQ',80,'N',380,293,'N');
exec cre_squ('PSCHP',297,'CHPP','DQ',80,'N',390,296,'N');
exec cre_squ('PSCHP',300,'CHPP','DQ',80,'N',400,298,'N');
exec cre_squ('PSCHP',302,'CHPP','DQ',80,'N',410,301,'N');
exec cre_squ('PSCHP',305,'CHPP','DQ',80,'N',420,303,'N');
exec cre_squ('PSCHP',310,'END','OS',1000,'N',10,306,'N');
exec cre_squ('PSCHP',313,'DQ','DQ',70,'N',60,309,'N');
exec cre_squ('PSCHP',319,'DQ','DQ',70,'N',70,315,'N');
exec cre_squ('PSCHP',322,'GHR','HR',10,'N',5,318,'N');
exec cre_squ('PSCHP',328,'DQ','DQ',70,'N',100,320,'N');
exec cre_squ('PSCHP',329,'SHR','HR',15,'N',155,323,'N');
exec cre_squ('PSCHP',334,'SHR','HR',15,'N',157,330,'N');
exec cre_squ('PSCHP',351,'DQ','DQ',70,'N',900,340,'N');
exec cre_squ('PSCHP',356,'SHR','HR',15,'N',212,351,'N');
exec cre_squ('PSCHP',357,'DQ','DQ',70,'N',910,910,'N');
exec cre_squ('PSCHP',362,'DQ','DQ',70,'N',920,920,'N');
exec cre_squ('PSCHP',364,'SHR','HR',15,'N',213,358,'N');
exec cre_squ('PSCHP',366,'DQ','DQ',70,'N',930,930,'N');
exec cre_squ('PSCHP',368,'SHR','HR',15,'N',214,365,'N');
exec cre_squ('PSCHP',371,'DQ','DQ',70,'N',1100,367,'N');
exec cre_squ('PSCHP',373,'SHR','HR',15,'N',215,369,'N');
exec cre_squ('PSCHP',376,'DQ','DQ',70,'N',940,940,'N');
exec cre_squ('PSCHP',377,'SHR','HR',15,'N',216,374,'N');
exec cre_squ('PSCHP',380,'DQ','DQ',70,'N',950,950,'N');
exec cre_squ('PSCHP',384,'DQ','DQ',70,'N',960,960,'N');
exec cre_squ('PSCHP',387,'SHR','HR',15,'Y',51,383,'N');
exec cre_squ('PSCHP',391,'SHR','HR',15,'Y',41,388,'N');
exec cre_squ('PSCHP',392,'DQ','DQ',70,'N',970,970,'N');
exec cre_squ('PSCHP',396,'DQ','DQ',70,'N',980,980,'N');
exec cre_squ('PSCHP',402,'FILT','DQ',90,'N',1400,1400,'N');
exec cre_squ('PSCHP',404,'STCR','SG',10,'Y',20,392,'N');
exec cre_squ('PSCHP',407,'FILT','DQ',90,'N',1410,403,'N');
exec cre_squ('PSCHP',411,'STCR','SG',10,'Y',90,405,'N');
exec cre_squ('PSCHP',412,'DQ','DQ',70,'N',10,407,'N');
exec cre_squ('PSCHP',413,'FILT','DQ',90,'N',1420,1420,'N');
exec cre_squ('PSCHP',416,'STLV','SG',20,'Y',340,412,'N');
exec cre_squ('PSCHP',418,'FILT','DQ',90,'N',1430,1430,'N');
exec cre_squ('PSCHP',422,'DQ','DQ',70,'N',1010,418,'N');
exec cre_squ('PSCHP',423,'FILT','DQ',90,'N',1440,1440,'N');
exec cre_squ('PSCHP',425,'STBA','SG',35,'N',20,422,'N');
exec cre_squ('PSCHP',428,'FILT','DQ',90,'N',1450,1450,'N');
exec cre_squ('PSCHP',430,'STBA','SG',35,'N',50,426,'N');
exec cre_squ('PSCHP',433,'FILT','DQ',90,'N',1460,1460,'N');
exec cre_squ('PSCHP',434,'STBA','SG',35,'N',30,431,'N');
exec cre_squ('PSCHP',438,'STBA','SG',35,'N',60,435,'N');
exec cre_squ('PSCHP',439,'FILT','DQ',90,'N',1470,1470,'N');
exec cre_squ('PSCHP',443,'FILT','DQ',90,'N',1480,1480,'N');
exec cre_squ('PSCHP',447,'STBA','SG',35,'N',40,439,'N');
exec cre_squ('PSCHP',448,'FILT','DQ',90,'N',1490,1490,'N');
exec cre_squ('PSCHP',451,'STBA','SG',35,'N',70,448,'N');
exec cre_squ('PSCHP',452,'FILT','DQ',90,'N',1500,1500,'N');
exec cre_squ('PSCHP',456,'FILT','DQ',90,'N',1510,1510,'N');
exec cre_squ('PSCHP',460,'FILT','DQ',90,'N',1520,1520,'N');
exec cre_squ('PSCHP',464,'FILT','DQ',90,'N',1530,1530,'N');
exec cre_squ('PSCHP',465,'SHR','HR',15,'Y',60,462,'N');
exec cre_squ('PSCHP',468,'FILT','DQ',90,'N',1540,1540,'N');
exec cre_squ('PSCHP',472,'FILT','DQ',90,'N',1550,1550,'N');
exec cre_squ('PSCHP',476,'FILT','DQ',90,'N',1560,1560,'N');
exec cre_squ('PSCHP',481,'FILT','DQ',90,'N',1570,1570,'N');
exec cre_squ('PSCHP',485,'FILT','DQ',90,'N',1580,1580,'N');
exec cre_squ('PSCHP',489,'FILT','DQ',90,'N',1590,1590,'N');
exec cre_squ('PSCHP',493,'FILT','DQ',90,'N',1600,1600,'N');
exec cre_squ('PSCHP',497,'FILT','DQ',90,'N',1610,1610,'N');
exec cre_squ('PSCHP',501,'FILT','DQ',90,'N',1620,1620,'N');
exec cre_squ('PSCHP',507,'SHR','HR',15,'Y',150,504,'N');
exec cre_squ('PSCHP',526,'FILT','DQ',90,'N',1650,1650,'N');
exec cre_squ('PSCHP',536,'FILT','DQ',90,'N',1680,1680,'N');
exec cre_squ('PSCHP',546,'FILT','DQ',90,'N',1710,1710,'N');
exec cre_squ('PSCHP',555,'FILT','DQ',90,'N',1730,1730,'N');
exec cre_squ('PSCHP',559,'FILT','DQ',90,'N',1740,1740,'N');
exec cre_squ('PSCHP',563,'FILT','DQ',90,'N',1750,1750,'N');
exec cre_squ('PSCHP',567,'FILT','DQ',90,'N',1760,1760,'N');
exec cre_squ('PSCHP',572,'FILT','DQ',90,'N',1770,1770,'N');
exec cre_squ('PSCHP',576,'FILT','DQ',90,'N',1780,1780,'N');
exec cre_squ('PSCHP',580,'FILT','DQ',90,'N',1790,1790,'N');
exec cre_squ('PSCHP',584,'FILT','DQ',90,'N',1800,1800,'N');
exec cre_squ('PSCHP',608,'FILT','DQ',90,'N',1850,1850,'N');
exec cre_squ('PSCHP',619,'DQ','DQ',70,'N',1300,615,'N');
exec cre_squ('PSCHP',624,'FILT','DQ',90,'N',1920,1920,'N');
exec cre_squ('PSCHP',696,'SHR','HR',15,'N',70,2071,'N');
exec cre_squ('PSCHP',700,'SHR','HR',15,'N',152,697,'N');
exec cre_squ('PSCHP',808,'STLV','SG',20,'N',10,805,'N');
exec cre_squ('PSCHP',812,'STBA','SG',35,'N',5,809,'N');
exec cre_squ('PSCHP',816,'STHO','SG',50,'N',10,813,'N');
exec cre_squ('PSCHP',821,'STHO','SG',50,'N',80,817,'N');
exec cre_squ('PSCHP',826,'STHO','SG',50,'N',90,822,'N');
exec cre_squ('PSCHP',876,'DQ','DQ',70,'N',1350,872,'N');
exec cre_squ('PSCHP',890,'DQ','DQ',70,'N',220,886,'N');
exec cre_squ('PSCHP',895,'DQ','DQ',70,'N',150,891,'N');
exec cre_squ('PSCHP',901,'DQ','DQ',70,'N',155,897,'N');
exec cre_squ('PSCHP',906,'DQ','DQ',70,'N',160,902,'N');
exec cre_squ('PSCHP',911,'DQ','DQ',70,'N',170,907,'N');
exec cre_squ('PSCHP',916,'DQ','DQ',70,'N',175,912,'N');
exec cre_squ('PSCHP',921,'DQ','DQ',70,'N',180,917,'N');
exec cre_squ('PSCHP',926,'DQ','DQ',70,'N',75,922,'N');
exec cre_squ('PSCHP',931,'DQ','DQ',70,'N',77,927,'N');
exec cre_squ('PSCHP',956,'SHR','HR',15,'N',170,953,'N');
exec cre_squ('PSCHP',960,'SHR','HR',15,'Y',180,957,'N');
exec cre_squ('PSCHP',965,'SHR','HR',15,'Y',190,961,'N');
exec cre_squ('PSCHP',969,'SHR','HR',15,'N',200,966,'N');
exec cre_squ('PSCHP',975,'DQ','DQ',70,'N',72,971,'N');
exec cre_squ('PSCHP',981,'GE','CC',8,'N',5,976,'N');
exec cre_squ('PSCHP',988,'CIRC','CC',7,'N',10,983,'N');
exec cre_squ('PSCHP',992,'CIRC','CC',7,'N',20,989,'N');
exec cre_squ('PSCHP',998,'CIRC','CC',7,'N',30,993,'N');
exec cre_squ('PSCHP',1002,'CIRC','CC',7,'N',40,999,'N');
exec cre_squ('PSCHP',1007,'CIRC','CC',7,'N',50,1003,'N');
exec cre_squ('PSCHP',1013,'CIRC','CC',7,'N',60,1008,'N');
exec cre_squ('PSCHP',1018,'CIRC','CC',7,'N',70,1014,'N');
exec cre_squ('PSCHP',1023,'CIRC','CC',7,'N',80,1019,'N');
exec cre_squ('PSCHP',1030,'CIRC','CC',7,'N',90,1024,'N');
exec cre_squ('PSCHP',1039,'CIRC','CC',7,'N',100,1031,'N');
exec cre_squ('PSCHP',1047,'CIRC','CC',7,'N',110,1040,'N');
exec cre_squ('PSCHP',1053,'CIRC','CC',7,'N',120,1048,'N');
exec cre_squ('PSCHP',1060,'CIRC','CC',7,'N',130,1054,'N');
exec cre_squ('PSCHP',1071,'GHR','HR',10,'N',50,1067,'N');
exec cre_squ('PSCHP',1076,'SHR','HR',15,'N',160,1073,'N');
exec cre_squ('PSCHP',1091,'GE','CC',8,'N',20,1087,'N');
exec cre_squ('PSCHP',1095,'GE','CC',8,'Y',8,1092,'N');
exec cre_squ('PSCHP',1137,'DQ','DQ',70,'N',210,1134,'N');
exec cre_squ('PSCHP',1148,'DQ','DQ',70,'N',225,1138,'N');
exec cre_squ('PSCHP',1152,'DQ','DQ',70,'N',230,1149,'N');
exec cre_squ('PSCHP',1156,'DQ','DQ',70,'N',240,1153,'N');
exec cre_squ('PSCHP',1160,'DQ','DQ',70,'N',250,1157,'N');
exec cre_squ('PSCHP',1165,'DQ','DQ',70,'N',260,1161,'N');
exec cre_squ('PSCOOP',41,'STCR','SG',10,'Y',80,38,'N');
exec cre_squ('PSCOOP',54,'STLV','SG',20,'N',210,51,'N');
exec cre_squ('PSCOOP',58,'STLV','SG',20,'N',240,55,'N');
exec cre_squ('PSCOOP',66,'STLV','SG',20,'N',220,63,'N');
exec cre_squ('PSCOOP',70,'STLV','SG',20,'N',250,67,'N');
exec cre_squ('PSCOOP',74,'STLV','SG',20,'N',230,71,'N');
exec cre_squ('PSCOOP',82,'STLV','SG',20,'N',270,79,'N');
exec cre_squ('PSCOOP',86,'STLV','SG',20,'N',300,83,'N');
exec cre_squ('PSCOOP',87,'STHO','SG',50,'Y',100,82,'N');
exec cre_squ('PSCOOP',90,'STLV','SG',20,'N',280,87,'N');
exec cre_squ('PSCOOP',91,'AGCY','SG',5,'Y',10,88,'N');
exec cre_squ('PSCOOP',94,'STLV','SG',20,'N',310,91,'N');
exec cre_squ('PSCOOP',96,'AGCY','SG',5,'Y',20,92,'N');
exec cre_squ('PSCOOP',98,'STLV','SG',20,'N',290,95,'N');
exec cre_squ('PSCOOP',101,'AGCY','SG',5,'Y',30,97,'N');
exec cre_squ('PSCOOP',106,'STLV','SG',20,'N',312,103,'N');
exec cre_squ('PSCOOP',107,'AGCY','SG',5,'N',40,102,'N');
exec cre_squ('PSCOOP',110,'STLV','SG',20,'N',325,107,'N');
exec cre_squ('PSCOOP',114,'STLV','SG',20,'N',315,111,'N');
exec cre_squ('PSCOOP',118,'STLV','SG',20,'N',328,115,'N');
exec cre_squ('PSCOOP',123,'STLV','SG',20,'N',320,119,'N');
exec cre_squ('PSCOOP',136,'STLV','SG',20,'Y',260,130,'N');
exec cre_squ('PSCOOP',140,'STLV','SG',20,'Y',330,137,'N');
exec cre_squ('PSCOOP',143,'AGCY','SG',5,'Y',50,108,'N');
exec cre_squ('PSCOOP',148,'STHS','SG',30,'Y',10,145,'N');
exec cre_squ('PSCOOP',149,'AGCY','SG',5,'N',60,144,'N');
exec cre_squ('PSCOOP',153,'AGCY','SG',5,'N',70,150,'N');
exec cre_squ('PSCOOP',172,'STBA','SG',35,'Y',80,166,'N');
exec cre_squ('PSCOOP',179,'STHO','SG',50,'N',20,176,'N');
exec cre_squ('PSCOOP',183,'STHO','SG',50,'N',30,180,'N');
exec cre_squ('PSCOOP',187,'STHO','SG',50,'N',40,184,'N');
exec cre_squ('PSCOOP',194,'STHO','SG',50,'N',70,188,'N');
exec cre_squ('PSCOOP',202,'STHO','SG',50,'N',75,195,'N');
exec cre_squ('PSCOOP',351,'DQ','DQ',70,'N',900,340,'N');
exec cre_squ('PSCOOP',357,'DQ','DQ',70,'N',910,910,'N');
exec cre_squ('PSCOOP',362,'DQ','DQ',70,'N',920,920,'N');
exec cre_squ('PSCOOP',366,'DQ','DQ',70,'N',930,930,'N');
exec cre_squ('PSCOOP',376,'DQ','DQ',70,'N',940,940,'N');
exec cre_squ('PSCOOP',380,'DQ','DQ',70,'N',950,950,'N');
exec cre_squ('PSCOOP',384,'DQ','DQ',70,'N',960,960,'N');
exec cre_squ('PSCOOP',392,'DQ','DQ',70,'N',970,970,'N');
exec cre_squ('PSCOOP',396,'DQ','DQ',70,'N',980,980,'N');
exec cre_squ('PSCOOP',402,'FILT','DQ',90,'N',1400,1400,'N');
exec cre_squ('PSCOOP',404,'STCR','SG',10,'Y',20,392,'N');
exec cre_squ('PSCOOP',407,'FILT','DQ',90,'N',1410,403,'N');
exec cre_squ('PSCOOP',411,'STCR','SG',10,'Y',90,405,'N');
exec cre_squ('PSCOOP',413,'FILT','DQ',90,'N',1420,1420,'N');
exec cre_squ('PSCOOP',416,'STLV','SG',20,'Y',340,412,'N');
exec cre_squ('PSCOOP',418,'FILT','DQ',90,'N',1430,1430,'N');
exec cre_squ('PSCOOP',423,'FILT','DQ',90,'N',1440,1440,'N');
exec cre_squ('PSCOOP',425,'STBA','SG',35,'N',20,422,'N');
exec cre_squ('PSCOOP',427,'DQ','DQ',70,'N',1020,423,'N');
exec cre_squ('PSCOOP',428,'FILT','DQ',90,'N',1450,1450,'N');
exec cre_squ('PSCOOP',430,'STBA','SG',35,'N',50,426,'N');
exec cre_squ('PSCOOP',433,'FILT','DQ',90,'N',1460,1460,'N');
exec cre_squ('PSCOOP',434,'STBA','SG',35,'N',30,431,'N');
exec cre_squ('PSCOOP',438,'STBA','SG',35,'N',60,435,'N');
exec cre_squ('PSCOOP',439,'FILT','DQ',90,'N',1470,1470,'N');
exec cre_squ('PSCOOP',443,'FILT','DQ',90,'N',1480,1480,'N');
exec cre_squ('PSCOOP',447,'STBA','SG',35,'N',40,439,'N');
exec cre_squ('PSCOOP',448,'FILT','DQ',90,'N',1490,1490,'N');
exec cre_squ('PSCOOP',451,'STBA','SG',35,'N',70,448,'N');
exec cre_squ('PSCOOP',452,'FILT','DQ',90,'N',1500,1500,'N');
exec cre_squ('PSCOOP',456,'FILT','DQ',90,'N',1510,1510,'N');
exec cre_squ('PSCOOP',460,'FILT','DQ',90,'N',1520,1520,'N');
exec cre_squ('PSCOOP',464,'FILT','DQ',90,'N',1530,1530,'N');
exec cre_squ('PSCOOP',468,'FILT','DQ',90,'N',1540,1540,'N');
exec cre_squ('PSCOOP',472,'FILT','DQ',90,'N',1550,1550,'N');
exec cre_squ('PSCOOP',476,'FILT','DQ',90,'N',1560,1560,'N');
exec cre_squ('PSCOOP',481,'FILT','DQ',90,'N',1570,1570,'N');
exec cre_squ('PSCOOP',485,'FILT','DQ',90,'N',1580,1580,'N');
exec cre_squ('PSCOOP',489,'FILT','DQ',90,'N',1590,1590,'N');
exec cre_squ('PSCOOP',493,'FILT','DQ',90,'N',1600,1600,'N');
exec cre_squ('PSCOOP',497,'FILT','DQ',90,'N',1610,1610,'N');
exec cre_squ('PSCOOP',501,'FILT','DQ',90,'N',1620,1620,'N');
exec cre_squ('PSCOOP',505,'FILT','DQ',90,'N',1630,1630,'N');
exec cre_squ('PSCOOP',509,'FILT','DQ',90,'N',1640,1640,'N');
exec cre_squ('PSCOOP',526,'FILT','DQ',90,'N',1650,1650,'N');
exec cre_squ('PSCOOP',536,'FILT','DQ',90,'N',1680,1680,'N');
exec cre_squ('PSCOOP',546,'FILT','DQ',90,'N',1710,1710,'N');
exec cre_squ('PSCOOP',555,'FILT','DQ',90,'N',1730,1730,'N');
exec cre_squ('PSCOOP',559,'FILT','DQ',90,'N',1740,1740,'N');
exec cre_squ('PSCOOP',563,'FILT','DQ',90,'N',1750,1750,'N');
exec cre_squ('PSCOOP',567,'FILT','DQ',90,'N',1760,1760,'N');
exec cre_squ('PSCOOP',572,'FILT','DQ',90,'N',1770,1770,'N');
exec cre_squ('PSCOOP',576,'FILT','DQ',90,'N',1780,1780,'N');
exec cre_squ('PSCOOP',580,'FILT','DQ',90,'N',1790,1790,'N');
exec cre_squ('PSCOOP',584,'FILT','DQ',90,'N',1800,1800,'N');
exec cre_squ('PSCOOP',588,'FILT','DQ',90,'N',1810,1810,'N');
exec cre_squ('PSCOOP',604,'FILT','DQ',90,'N',1840,1840,'N');
exec cre_squ('PSCOOP',608,'FILT','DQ',90,'N',1850,1850,'N');
exec cre_squ('PSCOOP',624,'FILT','DQ',90,'N',1920,1920,'N');
exec cre_squ('PSCOOP',634,'FILT','DQ',90,'N',1940,1940,'N');
exec cre_squ('PSCOOP',638,'FILT','DQ',90,'N',1950,1950,'N');
exec cre_squ('PSCOOP',642,'FILT','DQ',90,'N',1960,1960,'N');
exec cre_squ('PSCOOP',646,'FILT','DQ',90,'N',1970,1970,'N');
exec cre_squ('PSCOOP',650,'FILT','DQ',90,'N',1980,1980,'N');
exec cre_squ('PSCOOP',654,'FILT','DQ',90,'N',1990,1990,'N');
exec cre_squ('PSCOOP',661,'FILT','DQ',90,'N',2001,2001,'N');
exec cre_squ('PSCOOP',668,'FILT','DQ',90,'N',2011,2011,'N');
exec cre_squ('PSCOOP',676,'FILT','DQ',90,'N',2021,2021,'N');
exec cre_squ('PSCOOP',680,'FILT','DQ',90,'N',2031,2031,'N');
exec cre_squ('PSCOOP',684,'FILT','DQ',90,'N',2041,2041,'N');
exec cre_squ('PSCOOP',688,'FILT','DQ',90,'N',2051,2051,'N');
exec cre_squ('PSCOOP',692,'FILT','DQ',90,'N',2061,2061,'N');
exec cre_squ('PSCOOP',696,'SHR','HR',15,'N',70,2071,'N');
exec cre_squ('PSCOOP',700,'SHR','HR',15,'N',152,697,'N');
exec cre_squ('PSCOOP',808,'STLV','SG',20,'N',10,805,'N');
exec cre_squ('PSCOOP',812,'STBA','SG',35,'N',5,809,'N');
exec cre_squ('PSCOOP',816,'STHO','SG',50,'N',10,813,'N');
exec cre_squ('PSCOOP',821,'STHO','SG',50,'N',80,817,'N');
exec cre_squ('PSCOOP',826,'STHO','SG',50,'N',90,822,'N');
exec cre_squ('PSGEN',588,'FILT','DQ',90,'N',1810,1810,'N');
exec cre_squ('PSGEN',604,'FILT','DQ',90,'N',1840,1840,'N');
exec cre_squ('PSGEN',696,'SHR','HR',15,'N',70,2071,'N');
exec cre_squ('PSGEN',700,'SHR','HR',15,'N',152,697,'N');
exec cre_squ('PSMX',432,'DQ','DQ',70,'N',1030,428,'N');
exec cre_squ('PSTR',437,'DQ','DQ',70,'N',1040,433,'N');
exec cre_squ('PSCOOP',742,'END','OS',1000,'N',810,738,'N');
exec cre_squ('PSCHP',737,'END','OS',1000,'N',800,732,'N');
exec cre_squ('PSCOOP',737,'END','OS',1000,'N',800,732,'N');
exec cre_squ('PSCHP',742,'END','OS',1000,'N',810,738,'N');
exec cre_squ('PSCHP',747,'DQ','DQ',70,'N',1355,743,'N');
exec cre_squ('PSCOOP',778,'DQ','DQ',70,'N',1356,774,'N');
exec cre_squ('PSCOOP',759,'DQ','DQ',70,'N',222,2155,'N');
exec cre_squ('PSCHP',345,'CIRC','CC',7,'N',140,341,'N');
exec cre_squ('PSCHP',350,'CIRC','CC',7,'N',150,346,'N');
exec cre_squ('PSCHP',361,'CIRC','CC',7,'N',160,352,'N');
exec cre_squ('PSCHP',766,'CIRC','CC',7,'N',170,760,'N');
exec cre_squ('PSCOOP',345,'CIRC','CC',7,'N',140,341,'N');
exec cre_squ('PSCOOP',350,'CIRC','CC',7,'N',150,346,'N');
exec cre_squ('PSCOOP',361,'CIRC','CC',7,'N',160,352,'N');
exec cre_squ('PSCHP',786,'CHPP','DQ',80,'N',430,430,'N');
exec cre_squ('PSCHP',822,'CHPP','DQ',80,'N',440,440,'N');
exec cre_squ('PSCHP',823,'CHPP','DQ',80,'N',450,450,'N');
exec cre_squ('PSCHP',824,'CHPP','DQ',80,'N',460,460,'N');

UPDATE rehousing_policies SET RPL_HRV_ESC_CODE = 'ESCHP', RPL_HRV_PRS_CODE='PSCHP' WHERE RPL_RLI_CODE = 'CHP';
UPDATE rehousing_policies SET RPL_HRV_ESC_CODE = 'ESMX' WHERE RPL_RLI_CODE = 'MX';
UPDATE rehousing_policies SET RPL_HRV_ESC_CODE = 'ESCOOP' WHERE RPL_RLI_CODE = 'COOP';
UPDATE rehousing_policies SET RPL_HRV_ESC_CODE = 'ESTR' WHERE RPL_RLI_CODE = 'TR';
UPDATE rehousing_lists SET RLI_QUE_REFNO = 759 WHERE rli_code = 'COOP';

REM INSERTING into ATTRIBUTES
exec cre_att('LAR','AMEH',3055,'Amelie Housing');
exec cre_att('LAR','UNCH',3365,'Uniting Country Housing');
exec cre_att('LAR','UNSA',3395,'UNITINGSA housing Ltd');
exec cre_att('LAR','YOUR',3430,'YourPlace');
exec cre_att('LAR','ACRE',3460,'ACRE Housing Co-op');
exec cre_att('LAR','COPP',3470,'Copper Triangle Housing Co-op');
exec cre_att('LAR','PARF',3480,'Paris Flat Housing Co-op');
exec cre_att('LAR','PERC',3490,'PERCH');
exec cre_att('LAR','PHOE',3500,'Phoenix Housing Co-op');
exec cre_att('LAR','POND',3510,'Ponderosa Housing Co-op');
exec cre_att('LAR','PORR',3520,'Porridge Bowl Housing Co-op');
exec cre_att('LAR','SLOL',3530,'Slovanic Life Housing Co-op');
exec cre_att('LAR','YOCH',3540,'YOCHI Inc');

REM INSERTING into LETTINGS_AREAS
exec cre_lar('AMEH','Amelie Housing','Y',3055,'C','COMMPREF','LAR','AMEH');
exec cre_lar('UNCH','Uniting Country Housing','Y',3365,'C','COMMPREF','LAR','UNCH');
exec cre_lar('UNSA','UNITINGSA housing Ltd','Y',3395,'C','COMMPREF','LAR','UNSA');
exec cre_lar('YOUR','YourPlace','Y',3430,'C','COMMPREF','LAR','YOUR');
exec cre_lar('MET - NTH','Metro North','Y',12,'P','METRO',null,null);
exec cre_lar('MET - STH','Metro South','Y',14,'P','METRO',null,null);
exec cre_lar('MET - WEST','Metro West','Y',18,'P','METRO',null,null);
exec cre_lar('MET - EAST','Metro East','Y',16,'P','METRO',null,null);
exec cre_lar('ADH','Adelaide Hills Area','Y',2120,'P','COUNTRY',null,null);
exec cre_lar('FA','Fleurieu Area','Y',2091,'P','COUNTRY',null,null);
exec cre_att('LAR','LAR178',2130,'MONARTO');
exec cre_lar('LAR178','MONARTO','Y',2130,'C','MBA','LAR','LAR178');
exec cre_lar('KIA','Kangaroo IslAND Area','Y',2110,'P','COUNTRY',null,null);
exec cre_lar('ACRE','ACRE Housing Co-op','Y',3460,'C','COOPPREF','LAR','ACRE');
exec cre_lar('COPP','Copper Triangle Housing Co-op','Y',3470,'C','COOPPREF','LAR','COPP');
exec cre_lar('PARF','Paris Flat Housing Co-op','Y',3480,'C','COOPPREF','LAR','PARF');
exec cre_lar('PERC','PERCH','Y',3490,'C','COOPPREF','LAR','PERC');
exec cre_lar('PHOE','Phoenix Housing Co-op','Y',3500,'C','COOPPREF','LAR','PHOE');
exec cre_lar('POND','Ponderosa Housing Co-op','Y',3510,'C','COOPPREF','LAR','POND');
exec cre_lar('PORR','Porridge Bowl Housing Co-op','Y',3520,'C','COOPPREF','LAR','PORR');
exec cre_lar('SLOL','Slovanic Life Housing Co-op','Y',3530,'C','COOPPREF','LAR','SLOL');
exec cre_lar('YOCH','YOCHI Inc','Y',3540,'C','COOPPREF','LAR','YOCH');

UPDATE lettings_areas SET lar_lar_code = 'FA' WHERE lar_lar_code = 'NA';

SPOO op_lar_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM lettings_areas WHERE lar_code = 'NA';
DELETE FROM lettings_areas WHERE lar_code = 'NA';

SPOO op_config_install.log APPEND
SET LONG 32000
SET LINESIZE 32000
UPDATE lettings_areas SET lar_seqno = 2090 WHERE lar_code = 'FA';
UPDATE lettings_areas SET lar_description = 'MCCRACKEN' WHERE lar_code = 'LAR179';
UPDATE lettings_areas SET lar_lar_code = 'GA' WHERE lar_code = 'LAR558';
UPDATE lettings_areas SET lar_lar_code = 'GA' WHERE lar_code = 'LAR158';
UPDATE lettings_areas SET lar_lar_code = 'PPA' WHERE lar_code = 'LAR162';
UPDATE lettings_areas SET lar_lar_code = 'PPA' WHERE lar_code = 'LAR209';
UPDATE lettings_areas SET lar_lar_code = 'BA' WHERE lar_code = 'LAR237';
UPDATE lettings_areas SET lar_lar_code = 'MGA' WHERE lar_code = 'LAR240';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR241';
UPDATE lettings_areas SET lar_lar_code = 'MBA' WHERE lar_code = 'LAR246';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR551';
UPDATE lettings_areas SET lar_lar_code = 'KIA' WHERE lar_code = 'LAR262';
UPDATE lettings_areas SET lar_lar_code = 'KIA' WHERE lar_code = 'LAR314';
UPDATE lettings_areas SET lar_lar_code = 'KIA' WHERE lar_code = 'LAR461';
UPDATE lettings_areas SET lar_lar_code = 'KIA' WHERE lar_code = 'LAR146';
UPDATE lettings_areas SET lar_lar_code = 'FA' WHERE lar_code = 'LAR426';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR032';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR106';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR112';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR126';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR134';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR151';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR152';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR161';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR184';
UPDATE lettings_areas SET lar_lar_code = 'ADH' WHERE lar_code = 'LAR233';

UPDATE lettings_areas SET lar_current_ind = 'Y' WHERE lar_code IN ('ORGPREF');
UPDATE lettings_areas SET lar_current_ind = 'Y' WHERE lar_lar_code IN ('ORGPREF');
UPDATE lettings_areas SET lar_current_ind = 'Y' WHERE lar_lar_code IN ('COOPPREF','COMMPREF');

UPDATE lettings_areas SET lar_lar_code = 'MET - STH' WHERE lar_code = 'LAR001';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR002';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR003';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR004';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR005';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR006';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR007';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR008';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR009';
UPDATE lettings_areas SET lar_lar_code = 'MET - WEST' WHERE lar_code = 'LAR010';
UPDATE lettings_areas SET lar_lar_code = 'MET - STH' WHERE lar_code = 'LAR011';
UPDATE lettings_areas SET lar_lar_code = 'MET - STH' WHERE lar_code = 'LAR012';
UPDATE lettings_areas SET lar_lar_code = 'MET - EAST' WHERE lar_code = 'LAR013';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR014';
UPDATE lettings_areas SET lar_lar_code = 'MET - EAST' WHERE lar_code = 'LAR015';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR016';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR017';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR018';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR019';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR020';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR021';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR022';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR023';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR024';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR025';
UPDATE lettings_areas SET lar_lar_code = 'MET - NTH' WHERE lar_code = 'LAR026';
UPDATE lettings_areas SET lar_lar_code = 'MET - STH' WHERE lar_code = 'LAR027';
UPDATE lettings_areas SET lar_lar_code = 'MET - STH' WHERE lar_code = 'LAR028';
UPDATE lettings_areas SET lar_lar_code = 'MET - STH' WHERE lar_code = 'LAR029';
UPDATE lettings_areas SET lar_lar_code = 'MET - STH' WHERE lar_code = 'LAR030';
UPDATE lettings_areas SET lar_lar_code = 'MET - STH' WHERE lar_code = 'LAR031';
UPDATE lettings_areas SET lar_lar_code = 'FA' WHERE lar_code = 'LAR187';
/*

    Remove the bits AND pieces from shortlists which are being retired

*/

SPOO op_gan_values.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM general_answers where gan_que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 238, 271, 295);
DELETE FROM general_answers WHERE gan_que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 238, 271, 295);

SPOO op_laa_values.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM lettings_area_answers WHERE laa_lar_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','UNIS');
DELETE FROM lettings_area_answers WHERE laa_lar_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','UNIS');

SPOO op_lar_values.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM lettings_areas WHERE lar_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','UNIS');
DELETE FROM lettings_areas WHERE lar_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','UNIS');

SPOO op_att_values.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM attributes WHERE att_ele_code = 'LAR' AND att_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','UNIS');
DELETE FROM attributes WHERE att_ele_code = 'LAR' AND att_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','UNIS');

SPOO op_dqu_values.log APPEND
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM derived_question_usages WHERE dqu_dqh_refno = 20000 AND dqu_dqc_seqno IN (1, 4, 6, 10, 11, 12, 16, 20, 23, 26, 29, 33, 39, 40, 37);
DELETE FROM derived_question_usages WHERE dqu_dqh_refno = 20000 AND dqu_dqc_seqno IN (1, 4, 6, 10, 11, 12, 16, 20, 23, 26, 29, 33, 39, 40, 37);

SELECT * FROM derived_question_usages WHERE dqu_que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 238, 271, 295);
DELETE FROM derived_question_usages WHERE dqu_que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 238, 271, 295);

SPOO op_dqc_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM derived_question_clauses WHERE dqc_dqh_refno = 20000 AND dqc_seqno IN (1, 4, 6, 10, 11, 12, 16, 20, 23, 26, 29, 33, 39, 40, 17, 27, 37);
DELETE FROM derived_question_clauses WHERE dqc_dqh_refno = 20000 AND dqc_seqno IN (1, 4, 6, 10, 11, 12, 16, 20, 23, 26, 29, 33, 39, 40, 17, 27, 37);

SPOO op_dqr_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM dflt_question_restricts WHERE dqr_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS');
DELETE FROM dflt_question_restricts WHERE dqr_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS');
SELECT * FROM dflt_question_restricts WHERE dqr_que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 295);
DELETE FROM dflt_question_restricts WHERE dqr_que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 295);

SPOO op_squ_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM scheme_questions WHERE squ_que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 238, 271, 295);
DELETE FROM scheme_questions WHERE squ_que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 238, 271, 295);

SPOO op_que_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM questions WHERE que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 238, 271, 295);
DELETE FROM questions WHERE que_refno IN (199, 209, 215, 224, 226, 228, 236, 246, 252, 269, 276, 285, 300, 302, 238, 271, 295);

SPOO op_slc_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM short_lists WHERE sli_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS');
DELETE FROM short_lists WHERE sli_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS');

SPOO op_spl_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM slist_alloc_prop_lists WHERE spl_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS') ;
DELETE FROM slist_alloc_prop_lists WHERE spl_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS') ;

SPOO op_dqr_vals.log APPEND
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM dflt_question_restricts WHERE dqr_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS');
DELETE FROM dflt_question_restricts WHERE dqr_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS');

SPOO op_scg_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM slist_cat_appl_cat WHERE scg_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS');
DELETE FROM slist_cat_appl_cat WHERE scg_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA', 'UNIS');

SPOO op_srp_vals.log
SET LONG 32000
SET LINESIZE 32000
DELETE FROM slist_cat_rehousing_policies WHERE srp_slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA','UNIS');

SPOO op_slc_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM slist_categories WHERE slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA','UNIS');
DELETE FROM slist_categories WHERE slc_code IN ('AACC','ACCH','ARKH','DASH','FRED','HELP','IDAA','KICH','MEHC','NORS','PORT','SOUH','WAIS','WESC','PEAC','ISHA','UNIS');

SPOO op_config_install.log APPEND
SET LONG 32000
SET LINESIZE 32000

REM INSERTING into DERIVED_QUESTION_HEADERS
exec cre_dqh(75,'Minimum Registrant Age','N');
exec cre_dqh(76,'Maximum Registrant Age','N');
exec cre_dqh(77,'Unity Approved Supports','T');
exec cre_dqh(78,'Applicant Gender','T');
exec cre_dqh(79,'Applicant Relationship Status','T');
exec cre_dqh(80,'Applicant''s Country of Birth','T');
exec cre_dqh(81,'Applicant''s Language','T');
exec cre_dqh(84,'HAT Physical disability','T');
exec cre_dqh(85,'HAT disability - visual','T');
exec cre_dqh(86,'HAT Disability - Hearing','T');
exec cre_dqh(87,'HAT - Disability - Mental','T');
exec cre_dqh(88,'HAT Disability - Intellect','T');
exec cre_dqh(89,'HAT Disability - ABI','T');
exec cre_dqh(90,'HAT Disability - Other','T');
exec cre_dqh(91,'HAT Income - DSP','T');
exec cre_dqh(92,'HAT Income - AGed','T');
exec cre_dqh(93,'HAT - Income Parenting','T');
exec cre_dqh(94,'HAT Income Austudy','T');
exec cre_dqh(95,'HAT Income - Youth Allowance','T');
exec cre_dqh(96,'HAT Income - Newstart','T');
exec cre_dqh(97,'HAT Income - Carer','T');
exec cre_dqh(98,'HAT ANY disabilities','T');
exec cre_dqh(100,'HAT Inc DSP (HH)','T');
exec cre_dqh(101,'HAT Inc AGE (HH)','T');
exec cre_dqh(102,'HAT Inc Parenting (HH)','T');
exec cre_dqh(103,'HAT Inc Austudy (HH)','T');
exec cre_dqh(104,'HAT Inc YAL (HH)','T');
exec cre_dqh(105,'HAT Inc Newstart (HH)','T');
exec cre_dqh(106,'HAT Inc Carer (HH)','T');
exec cre_dqh(107,'HAT Customer Homesless','T');
exec cre_dqh(108,'HAT Inad Housed','T');
exec cre_dqh(109,'COOP Admin','T');
exec cre_dqh(110,'COOP Book keeping','T');
exec cre_dqh(111,'COOP Financial','T');
exec cre_dqh(112,'COOP IT','T');
exec cre_dqh(113,'COOP Artist','T');
exec cre_dqh(114,'COOP Conflict mgt','T');
exec cre_dqh(115,'COOP Environ','T');
exec cre_dqh(116,'COOP Maint','T');
exec cre_dqh(117,'COOP Organisation','T');
exec cre_dqh(118,'COOP Communication','T');
exec cre_dqh(119,'COOP Other','T');
exec cre_dqh(120,'HAT SDA','T');
exec cre_dqh(121,'HAT Dogs','T');
exec cre_dqh(122,'HAT Cats','T');
exec cre_dqh(123,'HAT Bird','T');
exec cre_dqh(124,'HAT Other Pet','T');
exec cre_dqh(125,'HAT DVA (Appl)','T');
exec cre_dqh(126,'HAT DVA (HH)','T');
exec cre_dqh(127,'COOP Mgt skills','T');
exec cre_dqh(128,'COOP Soft Skills','T');
exec cre_dqh(129,'HAT Other Gov inc (appl)','T');
exec cre_dqh(130,'HAT Other Gov Inc (HH)','T');
exec cre_dqh(131,'HAT Support in place','T');
exec cre_dqh(133,'Final Approved Category - CHP','N');
exec cre_dqh(134,'Category Start Date - COOP','D');
exec cre_dqh(135,'Eligible for CHP / COOP','T');
exec cre_dqh(136,'Final Approved Category - COOP','N');

REM INSERTING into DERIVED_QUESTION_DETAILS
exec cre_dqd(75,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''MINREGAGE'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(76,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''MAXREGAGE'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(77,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''UNITAPP'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(78,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''APPGENDER'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(79,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''APPRELATN'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(80,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''APPBIRTH'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(81,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''APPLANG'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(84,1,'select s_usr_dqs_hat.hat_dq_sql(''ATTEXIST'', app_refno, ''~PHYSICAL~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(85,1,'select s_usr_dqs_hat.hat_dq_sql(''ATTEXIST'', app_refno, ''~VISION~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(86,1,'select s_usr_dqs_hat.hat_dq_sql(''ATTEXIST'', app_refno, ''~HEARING~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(87,1,'select s_usr_dqs_hat.hat_dq_sql(''ATTEXIST'', app_refno, ''~MENTALHTH~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(88,1,'select s_usr_dqs_hat.hat_dq_sql(''ATTEXIST'', app_refno, ''~INTELLECTL~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(89,1,'select s_usr_dqs_hat.hat_dq_sql(''ATTEXIST'', app_refno, ''~ACQUIRED~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(90,1,'select s_usr_dqs_hat.hat_dq_sql(''ATTEXIST'', app_refno, ''~OTHER~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(91,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~DSP~TPI~'',''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(92,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~AGE~'',''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(93,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~PPP~PPS~FTBA~'',''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(94,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~AUS~ABY~'',''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(95,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~YAL~'',''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(96,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~NSA~NMA~JSP~'',''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(97,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~CAR~'',''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(98,1,'select s_usr_dqs_hat.hat_dq_sql(''ATTEXIST'', app_refno, ''~PHYSICAL~INTELLECTL~ACQUIRED~VISION~HEARING~MENTALHTH~OTHER~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(100,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~DSP~TPI~'',''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(101,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~AGE~'',''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(102,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~PPP~PPS~FTBA~'',''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(103,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~AUS~ABY~'',''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(104,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~YAL~'',''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(105,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~NSA~NMA~JSP~'',''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(106,1,'select s_usr_dqs_hat.hat_dq_sql(''INCEXIST'', app_refno, ''~CAR~'',''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(107,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 404, ''~NCON~ROUGH~STEA~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(108,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 404, ''~SUPPORT~BOARD~CARAVAN~HOSPIT~INSTIT~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(109,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 247, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(110,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 257, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(111,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 263, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(112,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 268, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(113,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 299, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(114,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 273, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(115,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 279, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(116,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 284, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(117,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 289, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(118,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 294, ''~FORMAL~EXPONLY~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(119,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 304, ''~COMMENT~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(120,1,'select s_usr_dqs_hat.hat_dq_sql(''SDAEXIST'', app_refno) from applications WHERE app_refno = &app_refno');
exec cre_dqd(121,1,'select s_usr_dqs_hat.hat_dq_sql(''HASPETS'', app_refno, ''DOG'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(122,1,'select s_usr_dqs_hat.hat_dq_sql(''HASPETS'', app_refno, ''CAT'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(123,1,'select s_usr_dqs_hat.hat_dq_sql(''HASPETS'', app_refno, ''BIRD'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(124,1,'select s_usr_dqs_hat.hat_dq_sql(''HASPETS'', app_refno, ''OTHER'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(125,1,'select s_usr_dqs_hat.hat_dq_sql(''DVAEXIST'', app_refno, ''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(126,1,'select s_usr_dqs_hat.hat_dq_sql(''DVAEXIST'', app_refno, ''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(127,1,'select s_usr_dqs_hat.hat_dq_sql(''COOPMGT'', app_refno) from applications WHERE app_refno = &app_refno');
exec cre_dqd(128,1,'select s_usr_dqs_hat.hat_dq_sql(''COOPSFT'', app_refno) from applications WHERE app_refno = &app_refno');
exec cre_dqd(129,1,'select s_usr_dqs_hat.hat_dq_sql(''OGVEXIST'', app_refno, ''MAIN'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(130,1,'select s_usr_dqs_hat.hat_dq_sql(''OGVEXIST'', app_refno, ''HH'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(131,1,'select s_usr_dqs_hat.hat_dq_sql(''SUPAPP'', app_refno) from applications WHERE app_refno = &app_refno');
exec cre_dqd(133,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''APPCATEGORYCHP'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(134,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''CATSTARTDATECOOP'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(135,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''ELIGCHP'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(136,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''APPCATEGORYCOOP'', app_refno) FROM applications WHERE app_refno = &app_refno');
REM INSERTING into DERIVED_QUESTION_CLAUSES
UPDATE derived_question_clauses
SET dqc_sql_text = replace ( replace (dqc_sql_text,'!=','='), 'AND not exists', 'AND exists')
WHERE dqc_dqh_refno = 20000;
REM INSERTING into DERIVED_QUESTION_USAGES
exec cre_derived_question_usage(75,351,null);
exec cre_derived_question_usage(76,357,null);
exec cre_derived_question_usage(77,362,null);
exec cre_derived_question_usage(78,366,null);
exec cre_derived_question_usage(79,376,null);
exec cre_derived_question_usage(80,380,null);
exec cre_derived_question_usage(81,384,null);
exec cre_derived_question_usage(84,402,null);
exec cre_derived_question_usage(85,407,null);
exec cre_derived_question_usage(86,413,null);
exec cre_derived_question_usage(87,418,null);
exec cre_derived_question_usage(88,423,null);
exec cre_derived_question_usage(89,428,null);
exec cre_derived_question_usage(90,433,null);
exec cre_derived_question_usage(91,439,null);
exec cre_derived_question_usage(92,443,null);
exec cre_derived_question_usage(93,448,null);
exec cre_derived_question_usage(94,452,null);
exec cre_derived_question_usage(95,456,null);
exec cre_derived_question_usage(96,460,null);
exec cre_derived_question_usage(97,464,null);
exec cre_derived_question_usage(98,536,null);
exec cre_derived_question_usage(100,555,null);
exec cre_derived_question_usage(101,559,null);
exec cre_derived_question_usage(102,563,null);
exec cre_derived_question_usage(103,567,null);
exec cre_derived_question_usage(104,572,null);
exec cre_derived_question_usage(105,576,null);
exec cre_derived_question_usage(106,580,null);
exec cre_derived_question_usage(107,604,null);
exec cre_derived_question_usage(108,608,null);
exec cre_derived_question_usage(109,634,null);
exec cre_derived_question_usage(110,638,null);
exec cre_derived_question_usage(111,642,null);
exec cre_derived_question_usage(112,646,null);
exec cre_derived_question_usage(113,650,null);
exec cre_derived_question_usage(114,654,null);
exec cre_derived_question_usage(115,661,null);
exec cre_derived_question_usage(116,668,null);
exec cre_derived_question_usage(117,676,null);
exec cre_derived_question_usage(118,680,null);
exec cre_derived_question_usage(119,684,null);
exec cre_derived_question_usage(120,624,null);
exec cre_derived_question_usage(121,485,null);
exec cre_derived_question_usage(122,489,null);
exec cre_derived_question_usage(123,493,null);
exec cre_derived_question_usage(124,497,null);
exec cre_derived_question_usage(125,546,null);
exec cre_derived_question_usage(126,588,null);
exec cre_derived_question_usage(127,505,null);
exec cre_derived_question_usage(127,688,null);
exec cre_derived_question_usage(128,509,null);
exec cre_derived_question_usage(128,692,null);
exec cre_derived_question_usage(129,468,null);
exec cre_derived_question_usage(130,584,null);
exec cre_derived_question_usage(131,526,null);
exec cre_derived_question_usage(133,747,null);
exec cre_derived_question_usage(134,759,null);
exec cre_derived_question_usage(20000,417,null);
exec cre_derived_question_usage(135,422,null);
exec cre_derived_question_usage(135,427,null);
exec cre_derived_question_usage(20000,432,null);
exec cre_derived_question_usage(20000,437,null);

UPDATE applications 
   SET app_aun_code = (SELECT MAX(ale_aun_code)
                         FROM applic_list_entries 
                        WHERE ale_rli_code NOT LIKE '%HNA%'
                          AND app_refno = ale_app_refno)
 WHERE app_aun_code IS NULL
   AND app_sco_code = 'CUR';
   
UPDATE parameter_values SET pva_char_value = 'TABLES' WHERE pva_pdu_pdf_name = 'ORG_PORTAL_DEF_TABLESPACE';
UPDATE parameter_values SET pva_char_value = 'TEMP' WHERE pva_pdu_pdf_name = 'ORG_PORTAL_TMP_TABLESPACE';
UPDATE parameter_values SET pva_char_value = 'FIRST_DEFAULT' WHERE pva_pdu_pdf_name = 'ORG_PORTAL_USR_PROFILE';

exec cre_rra('PLU','Property location unsuitable','Y',20);
exec cre_rra('PNLA','Property no longer available','Y',30);
exec cre_rra('UNO','Unsuitable offer','Y',40);
exec cre_rra('CIC','Change in circumstances','Y',50);
exec cre_rra('NR','No response','Y',60);
exec cre_rra('IO','Incorrect offer','Y',70);
exec cre_rra('AOO','Registrant accepted another offer','Y',80);
exec cre_rra('OTH','Other','Y',90);
exec cre_rra('PCU','Property condition unsuitable','Y',100);
exec cre_rra('NOMATCH','Property does not match prop preferences','Y',110);

UPDATE header_text SET het_text_value = REPLACE(het_text_value, 'Received Date','Vacancy Date of Previous Tenancy') WHERE HET_TEXT_VALUE_SYS LIKE '%Received Date%' AND HET_REGION_ID LIKE 'HAT-NOM-%';
UPDATE header_text SET het_text_value = REPLACE(het_text_value, 'Target Date','Target Completion Date of Nomination') WHERE HET_TEXT_VALUE_SYS LIKE '%Target Date%' AND HET_REGION_ID LIKE 'HAT-NOM-%';
UPDATE header_text SET het_text_value = 'Single Housing Register' WHERE HET_TEXT_VALUE_SYS = 'Organisation Portal';
UPDATE header_text SET het_text_value = 'AAT Score' WHERE HET_ITEM_POS = 9 AND HET_REGION_ID = 'HAT-APP-ASS-PT1';

exec cre_hid('HAT-NOF-MAN',12,'FIELD_HIDE');
exec cre_hid('HAT-NOF-SLO',12,'FIELD_HIDE');
exec cre_hid('HAT-APP-AS',51,'FIELD_HIDE');
exec cre_hid('HAT-APP-AS',52,'FIELD_HIDE');
exec cre_hid('HSS-CLI-APPLS',12,'FIELD_HIDE');
exec cre_hid('HSS-CLI-APPLS',18,'FIELD_HIDE');
exec cre_hid('HSS-CLI-APPLS',19,'FIELD_HIDE');
exec cre_hid('HAT-APP-ALE',18,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-AS',51,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-AS',52,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-DP',8,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-DP',10,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-DP',11,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-DP',13,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-DP',16,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-HIST-DP',8,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-HIST-DP',10,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-HIST-DP',11,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-HIST-DP',13,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-HIST-DP',16,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-SP',12,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-SP',14,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-SP',17,'ORG_FIELDHIDE');
exec cre_hid('HAT-APP-U',5,'ORG_FIELDHIDE');
exec cre_hid('HSS-CLI-APPLS',12,'ORG_FIELDHIDE');
exec cre_hid('HSS-CLI-APPLS',18,'ORG_FIELDHIDE');
exec cre_hid('HSS-CLI-APPLS',19,'ORG_FIELDHIDE');

UPDATE refusal_reasons SET rfr_sequence = 60 WHERE rfr_code = 'REFPAY';
UPDATE job_role_object_rows SET JRB_READ_WRITE_IND = 'Y' WHERE JRB_PK_CODE1 = 'HSD' AND JRB_JRO_CODE = 'ORG_GENERAL' AND JRB_OBJ_NAME = 'LIST_STATUSES';
UPDATE admin_units SET aun_current_ind = 'Y' WHERE aun_current_ind = 'N' AND aun_auy_code in ('OFF','OFC');
UPDATE questions SET QUE_SUMMARY_DESCRIPTION = REPLACE(QUE_SUMMARY_DESCRIPTION,'?','') WHERE que_refno IN (SELECT dqr_que_refno FROM dflt_question_restricts);
UPDATE tenancy_types SET TTY_CURRENT_IND = 'Y' WHERE TTY_CODE IN ('GCH','ARCH','THP','DHP','COOP');

exec insert_fr_values('ORG_CONTACT_ROLE','VOI','Verification of Identity',70);
exec insert_fr_values('ORG_CONTACT_ROLE','ASSETS','ASSETS',80);
exec insert_fr_values('ORG_CONTACT_ROLE','CPC','Corrections Protocol Contact',140);
exec insert_fr_values('ORG_CONTACT_ROLE','IO','Intervention Order',120);
exec insert_fr_values('ORG_CONTACT_ROLE','MAINT','Maintenance',90);
exec insert_fr_values('ORG_CONTACT_ROLE','KEYSHPCON','Key SHP Contact',130);
exec insert_fr_values('ORG_CONTACT_ROLE','SERVPROV','Service Provider',100);
exec insert_fr_values('ORG_CONTACT_ROLE','AUDITOR','Auditor',110);

exec cre_tty('THP','Transitional Housing Program','D','N','USR','N',170,'N','N','N','N','N');
exec cre_tty('DHP','Disability Housing Program','D','N','USR','N',175,'N','N','N','N','N');
UPDATE tenancy_types SET tty_description = 'Aged Homelessness Assistance Program' WHERE tty_code = 'AGEDHLSS';

exec cre_ntt('PROJECT','Project','Y','N','N');
exec cre_ntt('SHP','Supported AND Supportive Housing Program','Y','N','N');

exec cre_nto('PRO','PROJECT');
exec cre_nto('APP','SHP');

REM INSERTING into SLIST_CAT_REHOUSING_POLICIES
SPOO op_srp_vals.log APPEND

SELECT * FROM slist_cat_rehousing_policies;
DELETE FROM slist_cat_rehousing_policies;

SPOO op_config_install.log APPEND
exec cre_srp(2,'ACC2');
exec cre_srp(2,'ANGS');
exec cre_srp(2,'CEHS');
exec cre_srp(2,'COMH');
exec cre_srp(2,'CORS');
exec cre_srp(2,'HOUC');
exec cre_srp(2,'JULF');
exec cre_srp(2,'JUNW');
exec cre_srp(2,'MINI');
exec cre_srp(2,'SALV');
exec cre_srp(2,'SPLC');
exec cre_srp(2,'UNIT');
exec cre_srp(2,'WESH');
exec cre_srp(2,'SYPC');
exec cre_srp(7,'ACAC');
exec cre_srp(7,'CEHC');
exec cre_srp(7,'HINH');
exec cre_srp(7,'HOUP');
exec cre_srp(7,'LANV');
exec cre_srp(7,'MERZ');
exec cre_srp(7,'NEHC');
exec cre_srp(7,'PENL');
exec cre_srp(7,'SALH');
exec cre_srp(7,'SOUS');
exec cre_srp(7,'TOWC');
exec cre_srp(1,'SAHT');
exec cre_srp(6,'SAHT');
exec cre_srp(9,'SAHT');
exec cre_srp(2,'AMEH');
exec cre_srp(2,'UNSA');
exec cre_srp(2,'UNCH');
exec cre_srp(2,'YOUR');

UPDATE slist_cat_appl_cat SET SCG_PRIORITY=2 WHERE SCG_SLC_CODE IN ('ACAC', 'CEHC','HINH','HOUP','ISHA','LANV','MERZ','NEHC','PEAC','PENL','SALH','SOUS','TOWC');
UPDATE list_statuses SET LST_UPDATE_ALL_LISTS_IND = 'Y' WHERE lst_code = 'DEF';
UPDATE parameter_values SET pva_char_value = 'LISTGROUP' WHERE PVA_PDU_PDF_NAME = 'UPD_ALL_LISTS';
/*

    Insert ALLOTMENT address usage

*/

exec insert_fr_values('ADDRESS_ROLES','ALLOTMENT','Allotment',null);

MERGE INTO address_usage_types tgt
USING (SELECT 'PRO' aut_fao_code,
              'ALLOTMENT' aut_far_code,
              'USR' aut_usage,
              'Y' aut_current_ind,
              'SIA' aut_format_ind,
              'N' aut_allow_end_ind,
              'N' aut_allow_contact_name_ind
         FROM dual) src
   ON (    tgt.aut_far_code = src.aut_far_code
       AND tgt.aut_fao_code = src.aut_fao_code)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.aut_usage = src.aut_usage,
           tgt.aut_current_ind = src.aut_current_ind,
           tgt.aut_format_ind = src.aut_format_ind,
           tgt.aut_allow_end_ind = src.aut_allow_end_ind,
           tgt.aut_allow_contact_name_ind = src.aut_allow_contact_name_ind
 WHEN NOT MATCHED THEN
    INSERT(aut_fao_code,
           aut_far_code,
           aut_usage,
           aut_current_ind,
           aut_format_ind,
           aut_allow_end_ind,
           aut_allow_contact_name_ind)
    VALUES(src.aut_fao_code,
           src.aut_far_code,
           src.aut_usage,
           src.aut_current_ind,
           src.aut_format_ind,
           src.aut_allow_end_ind,
           src.aut_allow_contact_name_ind);
           
MERGE INTO admin_unit_types tgt
USING (SELECT 'HR' auy_code,
              'Single housing register' auy_name,
              'HOU' auy_type,
              'Y' auy_current_ind,
              'USR' auy_usage
         FROM dual) src
   ON (tgt.auy_code = src.auy_code)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.auy_name = src.auy_name,
           tgt.auy_type = src.auy_type,
           tgt.auy_current_ind = src.auy_current_ind,
           tgt.auy_usage = src.auy_usage
 WHEN NOT MATCHED THEN
    INSERT(auy_code,
           auy_name,
           auy_type,
           auy_current_ind,
           auy_usage) 
    VALUES(src.auy_code,
           src.auy_name,
           src.auy_type,
           src.auy_current_ind,
           src.auy_usage);
/*

    Missing person search rule

*/
REM INSERTING into PERSON_SEARCH_RULES

exec cre_pesr('RULEB','Rule B','Y',null,'M','W','W','M');
exec cre_pesr('RULEA','Rule A','Y','M',null,'W','W','M');
/*

    Turn on object security

*/
UPDATE parameter_values SET pva_char_value = 'Y' WHERE PVA_PDU_PDF_NAME = 'AU_SEC_FOR_PAR' AND PVA_PDU_PDF_PARAM_TYPE = 'SYSTEM';
/*

    Problem with INELIGIBLE 

*/
REM INSERTING into ELIG_QUESTIONS
exec cre_equ(422,'ESCHP','INELIGIBLE',10,'N');
exec cre_equ(427,'ESCOOP','INELIGIBLE',10,'N');
exec cre_equ(417,'ESAB','INELIGIBLE',10,'N');
exec cre_equ(432,'ESMX','INELIGIBLE',10,'N');
exec cre_equ(437,'ESTR','INELIGIBLE',10,'N');

/*

    VSTS 45456 Missing shortlist filters

*/
MERGE INTO dflt_question_restricts tgt
USING (SELECT dqr_seqno, 
              dqr_auto_apply_ind, 
              dqr_que_refno, 
              srp_slc_code dqr_slc_code, 
              dqr_operand, 
              dqr_qpr_refno, 
              dqr_default_answer
         FROM dflt_question_restricts
        CROSS JOIN slist_cat_rehousing_policies 
        WHERE dqr_que_refno IN (588,604)
          AND dqr_slc_code = 'ACAC'
          AND srp_rpl_refno = 2) src
   ON (    tgt.dqr_que_refno = src.dqr_que_refno
       AND tgt.dqr_slc_code = src.dqr_slc_code)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.dqr_seqno = src.dqr_seqno,
           tgt.dqr_auto_apply_ind = src.dqr_auto_apply_ind,
           tgt.dqr_operand = src.dqr_operand,
           tgt.dqr_qpr_refno = src.dqr_qpr_refno,
           tgt.dqr_default_answer = src.dqr_default_answer
 WHEN NOT MATCHED THEN
    INSERT(dqr_refno, 
           dqr_seqno, 
           dqr_auto_apply_ind, 
           dqr_que_refno, 
           dqr_slc_code, 
           dqr_operand, 
           dqr_qpr_refno, 
           dqr_default_answer)
    VALUES(dqr_refno_seq.nextval, 
           src.dqr_seqno, 
           src.dqr_auto_apply_ind, 
           src.dqr_que_refno, 
           src.dqr_slc_code, 
           src.dqr_operand, 
           src.dqr_qpr_refno, 
           src.dqr_default_answer);
/*

    VSTS 45466 REJ Rejection reason should be retired

*/

SPOO op_rra_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM rejection_reasons WHERE rra_code = 'REJ';
DELETE FROM rejection_reasons WHERE rra_code = 'REJ';

SPOO op_config_install.log APPEND
SET LONG 32000
SET LINESIZE 32000
/*

    VSTS 45501 Category on COOP list entry is incorrect

*/
exec cre_derived_question_usage(136,778,null);
/*

    VSTS 43650 New tenancy types not displaying in Nominations

*/
UPDATE tenancy_types
   SET tty_current_ind = 'Y'
 WHERE tty_code IN ('THP','DHP');
/*

    VSTS 45512 Incorrect order for derived questions

*/
UPDATE questions SET que_seqno = 1310 WHERE que_refno = 422;
UPDATE questions SET que_seqno = 1320 WHERE que_refno = 427;
UPDATE scheme_questions SET SQU_SCHEME_ALT_SEQNO = 1310 WHERE squ_que_refno = 422;
UPDATE scheme_questions SET SQU_SCHEME_ALT_SEQNO = 1320 WHERE squ_que_refno = 427;
/*

    VSTS 45495 SCreate person wizard cannot add notepad - shoudln't need the JR security

*/
UPDATE hidden_field_job_roles
   SET hid_jro_code = 'BASIC'
 WHERE hid_region_id = 'HSS-CLI-C'
   AND hid_item_pos IN (47,49);
/*

    VSTS 45606 NGO PORTAL - Missing Application questions

*/

SPOO op_squ_vals.log APPEND
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM scheme_questions WHERE squ_que_refno = 373 AND SQU_HRV_PRS_CODE = 'PSCHP';
DELETE FROM scheme_questions WHERE squ_que_refno = 373 AND SQU_HRV_PRS_CODE = 'PSCHP';
exec cre_squ('PSCOOP',960,'SHR','HR',15,'Y',180,957,'N');
exec cre_squ('PSCOOP',965,'SHR','HR',15,'Y',190,961,'N');
exec cre_squ('PSCOOP',969,'SHR','HR',15,'N',200,966,'N');

SPOO op_config_install.log APPEND
SET LONG 32000
SET LINESIZE 32000
/*

    VSTS 45635 Unable to create contact ACCENQCHP 

*/
UPDATE business_reasons
   SET bro_contact_auy_code = null, 
       bro_contact_au_req_ind = 'N'
 WHERE bro_code = 'ACCENQCHP';
/*

    VSTS 45676 Update to more application details order

*/
UPDATE questions SET que_summary_seqno = 2410 WHERE que_refno = 619;
UPDATE questions SET que_summary_seqno = 2420 WHERE que_refno = 422;
UPDATE questions SET que_summary_seqno = 2430 WHERE que_refno = 427;
UPDATE questions SET que_summary_seqno = 2440 WHERE que_refno = 371;
UPDATE questions SET que_summary_seqno = 2450 WHERE que_refno = 895;
UPDATE questions SET que_summary_seqno = 2460 WHERE que_refno = 906;
UPDATE questions SET que_summary_seqno = 2470 WHERE que_refno = 901;
UPDATE questions SET que_summary_seqno = 2480 WHERE que_refno = 911;
UPDATE questions SET que_summary_seqno = 2490 WHERE que_refno = 921;
UPDATE questions SET que_summary_seqno = 2500 WHERE que_refno = 916;
UPDATE questions SET que_summary_seqno = 2510 WHERE que_refno = 1148;
UPDATE questions SET que_summary_seqno = 2520 WHERE que_refno = 1152;
UPDATE questions SET que_summary_seqno = 2530 WHERE que_refno = 72;
UPDATE questions SET que_summary_seqno = 2540 WHERE que_refno = 1137;
UPDATE questions SET que_summary_seqno = 2550 WHERE que_refno = 926;
UPDATE questions SET que_summary_seqno = 2560 WHERE que_refno = 931;
UPDATE questions SET que_summary_seqno = 2570 WHERE que_refno = 876;
UPDATE questions SET que_summary_seqno = 2580 WHERE que_refno = 747;
UPDATE questions SET que_summary_seqno = 2590 WHERE que_refno = 778;
UPDATE questions SET que_summary_seqno = 2600 WHERE que_refno = 890;
UPDATE questions SET que_summary_seqno = 2610 WHERE que_refno = 759;
UPDATE questions SET que_summary_seqno = 2620 WHERE que_refno = 956;
UPDATE questions SET que_summary_seqno = 2630 WHERE que_refno = 960;
UPDATE questions SET que_summary_seqno = 2640 WHERE que_refno = 969;
UPDATE questions SET que_summary_seqno = 2650 WHERE que_refno = 965;
UPDATE questions SET que_summary_seqno = 2660 WHERE que_refno = 344;
UPDATE questions SET que_summary_seqno = 2670 WHERE que_refno = 412;
UPDATE questions SET que_summary_seqno = 2680 WHERE que_refno = 975;
UPDATE questions SET que_summary_seqno = 2690 WHERE que_refno = 313;
UPDATE questions SET que_summary_seqno = 2700 WHERE que_refno = 319;
UPDATE questions SET que_summary_seqno = 2710 WHERE que_refno = 328;
UPDATE questions SET que_summary_seqno = 2720 WHERE que_refno = 1071;
UPDATE questions SET que_summary_seqno = 2730 WHERE que_refno = 46;
UPDATE questions SET que_summary_seqno = 2740 WHERE que_refno = 157;
UPDATE questions SET que_summary_seqno = 2750 WHERE que_refno = 1156;
UPDATE questions SET que_summary_seqno = 2760 WHERE que_refno = 1160;
UPDATE questions SET que_summary_seqno = 2770 WHERE que_refno = 1165;
UPDATE questions SET que_summary_seqno = 2780 WHERE que_refno = 624;
UPDATE questions SET que_summary_seqno = 2790 WHERE que_refno = 176;
UPDATE questions SET que_summary_seqno = 2800 WHERE que_refno = 351;
UPDATE questions SET que_summary_seqno = 2810 WHERE que_refno = 357;
UPDATE questions SET que_summary_seqno = 2820 WHERE que_refno = 366;
UPDATE questions SET que_summary_seqno = 2830 WHERE que_refno = 376;
UPDATE questions SET que_summary_seqno = 2840 WHERE que_refno = 380;
UPDATE questions SET que_summary_seqno = 2850 WHERE que_refno = 384;
UPDATE questions SET que_summary_seqno = 2860 WHERE que_refno = 392;
UPDATE questions SET que_summary_seqno = 2870 WHERE que_refno = 396;
UPDATE questions SET que_summary_seqno = 2880 WHERE que_refno = 526;
UPDATE questions SET que_summary_seqno = 2890 WHERE que_refno = 362;
UPDATE questions SET que_summary_seqno = 2900 WHERE que_refno = 536;
UPDATE questions SET que_summary_seqno = 2910 WHERE que_refno = 402;
UPDATE questions SET que_summary_seqno = 2920 WHERE que_refno = 407;
UPDATE questions SET que_summary_seqno = 2930 WHERE que_refno = 413;
UPDATE questions SET que_summary_seqno = 2940 WHERE que_refno = 418;
UPDATE questions SET que_summary_seqno = 2950 WHERE que_refno = 423;
UPDATE questions SET que_summary_seqno = 2960 WHERE que_refno = 428;
UPDATE questions SET que_summary_seqno = 2970 WHERE que_refno = 433;
UPDATE questions SET que_summary_seqno = 2980 WHERE que_refno = 439;
UPDATE questions SET que_summary_seqno = 2990 WHERE que_refno = 443;
UPDATE questions SET que_summary_seqno = 3000 WHERE que_refno = 448;
UPDATE questions SET que_summary_seqno = 3010 WHERE que_refno = 452;
UPDATE questions SET que_summary_seqno = 3020 WHERE que_refno = 456;
UPDATE questions SET que_summary_seqno = 3030 WHERE que_refno = 460;
UPDATE questions SET que_summary_seqno = 3040 WHERE que_refno = 464;
UPDATE questions SET que_summary_seqno = 3050 WHERE que_refno = 468;
UPDATE questions SET que_summary_seqno = 3060 WHERE que_refno = 546;
UPDATE questions SET que_summary_seqno = 3070 WHERE que_refno = 555;
UPDATE questions SET que_summary_seqno = 3080 WHERE que_refno = 559;
UPDATE questions SET que_summary_seqno = 3090 WHERE que_refno = 563;
UPDATE questions SET que_summary_seqno = 3100 WHERE que_refno = 567;
UPDATE questions SET que_summary_seqno = 3110 WHERE que_refno = 572;
UPDATE questions SET que_summary_seqno = 3120 WHERE que_refno = 576;
UPDATE questions SET que_summary_seqno = 3130 WHERE que_refno = 580;
UPDATE questions SET que_summary_seqno = 3140 WHERE que_refno = 584;
UPDATE questions SET que_summary_seqno = 3150 WHERE que_refno = 588;
UPDATE questions SET que_summary_seqno = 3160 WHERE que_refno = 604;
UPDATE questions SET que_summary_seqno = 3170 WHERE que_refno = 608;
UPDATE questions SET que_summary_seqno = 3180 WHERE que_refno = 485;
UPDATE questions SET que_summary_seqno = 3190 WHERE que_refno = 489;
UPDATE questions SET que_summary_seqno = 3200 WHERE que_refno = 493;
UPDATE questions SET que_summary_seqno = 3210 WHERE que_refno = 497;
UPDATE questions SET que_summary_seqno = 3220 WHERE que_refno = 634;
UPDATE questions SET que_summary_seqno = 3230 WHERE que_refno = 638;
UPDATE questions SET que_summary_seqno = 3240 WHERE que_refno = 642;
UPDATE questions SET que_summary_seqno = 3250 WHERE que_refno = 646;
UPDATE questions SET que_summary_seqno = 3260 WHERE que_refno = 650;
UPDATE questions SET que_summary_seqno = 3270 WHERE que_refno = 654;
UPDATE questions SET que_summary_seqno = 3280 WHERE que_refno = 661;
UPDATE questions SET que_summary_seqno = 3290 WHERE que_refno = 668;
UPDATE questions SET que_summary_seqno = 3300 WHERE que_refno = 676;
UPDATE questions SET que_summary_seqno = 3310 WHERE que_refno = 680;
UPDATE questions SET que_summary_seqno = 3320 WHERE que_refno = 684;
UPDATE questions SET que_summary_seqno = 3330 WHERE que_refno = 688;
UPDATE questions SET que_summary_seqno = 3340 WHERE que_refno = 692;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2410 WHERE squ_que_refno = 619;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2420 WHERE squ_que_refno = 422;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2430 WHERE squ_que_refno = 427;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2440 WHERE squ_que_refno = 371;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2450 WHERE squ_que_refno = 895;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2460 WHERE squ_que_refno = 906;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2470 WHERE squ_que_refno = 901;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2480 WHERE squ_que_refno = 911;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2490 WHERE squ_que_refno = 921;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2500 WHERE squ_que_refno = 916;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2510 WHERE squ_que_refno = 1148;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2520 WHERE squ_que_refno = 1152;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2530 WHERE squ_que_refno = 72;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2540 WHERE squ_que_refno = 1137;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2550 WHERE squ_que_refno = 926;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2560 WHERE squ_que_refno = 931;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2570 WHERE squ_que_refno = 876;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2580 WHERE squ_que_refno = 747;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2590 WHERE squ_que_refno = 778;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2600 WHERE squ_que_refno = 890;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2610 WHERE squ_que_refno = 759;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2620 WHERE squ_que_refno = 956;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2630 WHERE squ_que_refno = 960;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2640 WHERE squ_que_refno = 969;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2650 WHERE squ_que_refno = 965;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2660 WHERE squ_que_refno = 344;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2670 WHERE squ_que_refno = 412;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2680 WHERE squ_que_refno = 975;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2690 WHERE squ_que_refno = 313;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2700 WHERE squ_que_refno = 319;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2710 WHERE squ_que_refno = 328;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2720 WHERE squ_que_refno = 1071;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2730 WHERE squ_que_refno = 46;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2740 WHERE squ_que_refno = 157;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2750 WHERE squ_que_refno = 1156;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2760 WHERE squ_que_refno = 1160;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2770 WHERE squ_que_refno = 1165;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2780 WHERE squ_que_refno = 624;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2790 WHERE squ_que_refno = 176;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2800 WHERE squ_que_refno = 351;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2810 WHERE squ_que_refno = 357;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2820 WHERE squ_que_refno = 366;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2830 WHERE squ_que_refno = 376;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2840 WHERE squ_que_refno = 380;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2850 WHERE squ_que_refno = 384;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2860 WHERE squ_que_refno = 392;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2870 WHERE squ_que_refno = 396;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2880 WHERE squ_que_refno = 526;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2890 WHERE squ_que_refno = 362;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2900 WHERE squ_que_refno = 536;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2910 WHERE squ_que_refno = 402;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2920 WHERE squ_que_refno = 407;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2930 WHERE squ_que_refno = 413;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2940 WHERE squ_que_refno = 418;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2950 WHERE squ_que_refno = 423;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2960 WHERE squ_que_refno = 428;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2970 WHERE squ_que_refno = 433;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2980 WHERE squ_que_refno = 439;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2990 WHERE squ_que_refno = 443;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3000 WHERE squ_que_refno = 448;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3010 WHERE squ_que_refno = 452;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3020 WHERE squ_que_refno = 456;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3030 WHERE squ_que_refno = 460;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3040 WHERE squ_que_refno = 464;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3050 WHERE squ_que_refno = 468;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3060 WHERE squ_que_refno = 546;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3070 WHERE squ_que_refno = 555;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3080 WHERE squ_que_refno = 559;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3090 WHERE squ_que_refno = 563;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3100 WHERE squ_que_refno = 567;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3110 WHERE squ_que_refno = 572;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3120 WHERE squ_que_refno = 576;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3130 WHERE squ_que_refno = 580;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3140 WHERE squ_que_refno = 584;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3150 WHERE squ_que_refno = 588;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3160 WHERE squ_que_refno = 604;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3170 WHERE squ_que_refno = 608;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3180 WHERE squ_que_refno = 485;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3190 WHERE squ_que_refno = 489;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3200 WHERE squ_que_refno = 493;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3210 WHERE squ_que_refno = 497;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3220 WHERE squ_que_refno = 634;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3230 WHERE squ_que_refno = 638;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3240 WHERE squ_que_refno = 642;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3250 WHERE squ_que_refno = 646;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3260 WHERE squ_que_refno = 650;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3270 WHERE squ_que_refno = 654;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3280 WHERE squ_que_refno = 661;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3290 WHERE squ_que_refno = 668;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3300 WHERE squ_que_refno = 676;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3310 WHERE squ_que_refno = 680;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3320 WHERE squ_que_refno = 684;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3330 WHERE squ_que_refno = 688;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 3340 WHERE squ_que_refno = 692;
UPDATE questions SET QUE_SHOW_SUMMARY_IND = 'N' WHERE que_refno in (786, 822, 823, 824, 737, 742, 217, 472, 476, 481, 505, 509, 501);
UPDATE questions SET QUE_SUMMARY_DESCRIPTION ='Eligible for GEN', QUE_QUESTION_DESCRIPTION='Eligible for GEN' WHERE que_refno in (619);
UPDATE questions SET QUE_SUMMARY_DESCRIPTION ='Category - GEN', QUE_QUESTION_DESCRIPTION='Category - GEN' WHERE que_refno in (876);
UPDATE questions SET QUE_SUMMARY_DESCRIPTION ='Category - COOP', QUE_QUESTION_DESCRIPTION='Category - COOP' WHERE que_refno in (778);


SPOO op_squ_vals.log APPEND
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM scheme_questions WHERE squ_que_refno = 588 AND SQU_HRV_PRS_CODE in ('PSGEN');
DELETE FROM scheme_questions WHERE squ_que_refno = 588 AND SQU_HRV_PRS_CODE in ('PSGEN');

SELECT * FROM scheme_questions WHERE squ_que_refno in (619,1152,876,157,1156,1160,1165) AND SQU_HRV_PRS_CODE in ('PSCHP');
DELETE FROM scheme_questions WHERE squ_que_refno in (619,1152,876,157,1156,1160,1165) AND SQU_HRV_PRS_CODE in ('PSCHP');

SELECT * FROM scheme_questions WHERE squ_que_refno in (773, 619, 876, 890, 1156, 1160, 1165) AND SQU_HRV_PRS_CODE in ('PSCOOP');
DELETE FROM scheme_questions WHERE squ_que_refno in (773, 619, 876, 890, 1156, 1160, 1165) AND SQU_HRV_PRS_CODE in ('PSCOOP');

SELECT * FROM scheme_questions WHERE squ_que_refno in (901,916);
DELETE FROM scheme_questions WHERE squ_que_refno in (901,916);

SPOO op_config_install.log APPEND
exec cre_squ('PSCHP',588,'FILT','DQ',90,'N',1810,3150,'N');
exec cre_squ('PSCHP',604,'FILT','DQ',90,'N',1840,3160,'N');
/*

    VSTS 43599 Cosmetic changes for the shortlist filters

*/
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Minimum Applicant Age', QUE_QUESTION_DESCRIPTION = 'Minimum Applicant Age' WHERE que_refno = 351;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Maximum Applicant Age', QUE_QUESTION_DESCRIPTION = 'Maximum Applicant Age' WHERE que_refno = 357;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Applicant Country of Birth', QUE_QUESTION_DESCRIPTION = 'Applicant Country of Birth' WHERE que_refno = 380;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Intellectual Disability', QUE_QUESTION_DESCRIPTION = 'Intellectual Disability' WHERE que_refno = 423;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Acquired Brain Injury', QUE_QUESTION_DESCRIPTION = 'Acquired Brain Injury' WHERE que_refno = 428;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Wheelchair Access', QUE_QUESTION_DESCRIPTION = 'Wheelchair Access' WHERE que_refno = 507;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**Main Applicant Filters**', QUE_QUESTION_DESCRIPTION = '**Main Applicant Filters**' WHERE que_refno = 521;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**Special Needs Filters**', QUE_QUESTION_DESCRIPTION = '**Special Needs Filters**' WHERE que_refno = 532;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**Applicant Income Filters**', QUE_QUESTION_DESCRIPTION = '**Applicant Income Filters**' WHERE que_refno = 542;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**Household Income Filters**', QUE_QUESTION_DESCRIPTION = '**Household Income Filters**' WHERE que_refno = 551;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**HH composition filters**', QUE_QUESTION_DESCRIPTION = '**HH composition filters**' WHERE que_refno = 595;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**Current Housing Filters**', QUE_QUESTION_DESCRIPTION = '**Current Housing Filters**' WHERE que_refno = 600;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**Pets Filters**', QUE_QUESTION_DESCRIPTION = '**Pets Filters**' WHERE que_refno = 613;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**Housing Pref Filters**', QUE_QUESTION_DESCRIPTION = '**Housing Pref Filters**' WHERE que_refno = 620;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = '**Skills AND Courses**', QUE_QUESTION_DESCRIPTION = '**Skills AND Courses**' WHERE que_refno = 630;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Organisational Skills', QUE_QUESTION_DESCRIPTION = 'Organisational Skills' WHERE que_refno = 676;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Carparking Access', QUE_QUESTION_DESCRIPTION = 'Carparking Access' WHERE que_refno = 696;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'Applicant Relationship Status', QUE_QUESTION_DESCRIPTION = 'Applicant Relationship Status' WHERE que_refno = 742;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION  = 'ATSI Descent', QUE_QUESTION_DESCRIPTION = 'ATSI Descent' WHERE que_refno = 975;
/*


    VSTS 45799 NGO Portal create contact from client - admin unit is invalid

*/
UPDATE business_reasons
SET BRO_CONTACT_AUY_CODE = null, BRO_CONTACT_AU_REQ_IND='N'
WHERE bro_code = 'UPDETAIL';
/*

    VSTS 46020 Various issues with shortlisting AND filters

*/
exec cre_dqr(922,5,'Y',250,'MERZ','=',null,'Y');
exec cre_dqr(921,5,'Y',282,'SOUS','=',null,'Y');
/*

    VSTS 46100 Typo in nomination status drop down

*/
Update nomination_statuses
SET NST_DESCRIPTION = 'Received'
WHERE nst_code = 'RECD';
/*

    VSTS 46115 - 

*/
UPDATE parameter_values
SET PVA_CHAR_VALUE = 'APPLICTN'
WHERE PVA_PDU_PDF_NAME = 'UPD_ALL_LISTS';
/*

    VSTS 46123 - UNITYSUP is required as interested party role

*/
Insert into PARAMETER_GROUPS (PGP_REFNO) values (pgp_refno_seq.nextval);
Insert into INTERESTED_PARTY_ROLE_TYPES (IPRT_CODE,IPRT_DESCRIPTION,IPRT_PGP_REFNO,IPRT_CURRENT_IND,IPRT_DISPLAY_SEQ) values ('UNITYSUP','MOU with Unity Housing Company Ltd',pgp_refno_seq.currval,'Y',30);
/*

    VSTS 45815 Change REF indicator to '-'

*/
UPDATE header_text SET HET_TEXT_VALUE = '-' WHERE HET_TEXT_VALUE_SYS = 'Ref' AND het_region_id = 'HOP-CLI-SRS';
/*

    VSTS 46141 Wording changes to income related filters AND its according DQs on application

*/
UPDATE questions SET QUE_SUMMARY_DESCRIPTION = 'JobSeeker (Appl)', QUE_QUESTION_DESCRIPTION = 'JobSeeker (Appl)' WHERE que_refno = 460;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION = 'Other Income (Appl)', QUE_QUESTION_DESCRIPTION = 'Other Income (Appl)' WHERE que_refno = 468;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION = 'JobSeeker (HH)', QUE_QUESTION_DESCRIPTION = 'JobSeeker (HH)' WHERE que_refno = 576;
UPDATE questions SET QUE_SUMMARY_DESCRIPTION = 'Other Income (HH)', QUE_QUESTION_DESCRIPTION = 'Other Income (HH)' WHERE que_refno = 584;
/*

    VSTS 45514 - NGO Portal - Changes to list entry questions, shortlist filters AND eligibility assessment

*/
UPDATE questions SET QUE_PARENT_CHILD_IND = 'P', QUE_SUMMARY_DESCRIPTION = 'Current Public or CHP?', QUE_QUESTION_DESCRIPTION='Does customer currently live in public / community housing?' WHERE que_refno = 766;
exec create_question(346,'PQU','C','CIRC','CC','N','C','N','N','Y','Y','N','N',180,340,'7','The customer is','The customer is','Y',766);
exec create_question(353,'PQU','C','CIRC','CC','N','C','N','N','Y','Y','N','N',190,347,'7','Reason for leaving','The customer needs to leave because','N',766);
exec create_question(358,'PQU','Y','SHR','HR','N','C','N','N','Y','Y','N','N',218,354,'15','O/Ride Ten or Ptr must leave','Override Reason - Current tenant or partner needs to leave','N',61);
exec create_question(363,'DNQ','T','FILT','DQ','N','N','N','N','N','Y','N','N',232,359,'90','Transfer or Relocation?','Transfer or Relocation?','N',null);
exec create_question(369,'DNQ','T','DQ','DQ','N','N','N','N','N','Y','N','N',975,364,'70','Current tcy eligible','Current tenant/partner/household eligible?','N',null);
exec cre_squ('PSGEN',346,'CIRC','CC',7,'Y',180,340,'N');
exec cre_squ('PSCHP',346,'CIRC','CC',7,'Y',180,340,'N');
exec cre_squ('PSCOOP',346,'CIRC','CC',7,'Y',180,340,'N');
exec cre_squ('PSGEN',353,'CIRC','CC',7,'N',190,347,'N');
exec cre_squ('PSCHP',353,'CIRC','CC',7,'N',190,347,'N');
exec cre_squ('PSCOOP',353,'CIRC','CC',7,'N',190,347,'N');
exec cre_squ('PSGEN',358,'SHR','HR',15,'N',218,354,'N');
exec cre_squ('PSCHP',358,'SHR','HR',15,'N',218,354,'N');
exec cre_squ('PSCOOP',358,'SHR','HR',15,'N',218,354,'N');
exec cre_squ('PSGEN',363,'FILT','DQ',90,'N',232,359,'N');
exec cre_squ('PSCHP',363,'FILT','DQ',90,'N',232,359,'N');
exec cre_squ('PSCOOP',363,'FILT','DQ',90,'N',232,359,'N');
exec cre_squ('PSGEN',369,'DQ','DQ',70,'N',975,364,'N');
exec cre_squ('PSCHP',369,'DQ','DQ',70,'N',975,364,'N');
exec cre_squ('PSCOOP',369,'DQ','DQ',70,'N',975,364,'N');
exec cre_squ('PSGEN',766,'CIRC','CC',7,'N',170,760,'N');
exec cre_squ('PSCOOP',766,'CIRC','CC',7,'N',170,760,'N');
exec cre_dqh(152,'HAT customer needs to leave reason','T');
exec cre_dqh(153,'HAT Current tenant/part/hhld elig','T');
exec cre_dqd(152,1,'select s_usr_dqs_hat.hat_dq_sql(''ANSMATCH'', app_refno, 353, ''~TR~RELOC~'') from applications WHERE app_refno = &app_refno');
exec cre_dqd(153,1,'select s_usr_dqs_hat.hat_dq_sql(''ELIGTRANS'',app_refno) from applications WHERE app_refno = &app_refno');
exec cre_derived_question_usage(152,363,null);
exec cre_derived_question_usage(153,369,null);
exec cre_dqr(964,7,'N',363,'ACAC','=',null,'Y');
exec cre_dqr(965,7,'N',363,'ACC2','=',null,'Y');
exec cre_dqr(975,7,'N',363,'AMEH','=',null,'Y');
exec cre_dqr(966,7,'N',363,'ANGS','=',null,'Y');
exec cre_dqr(967,7,'N',363,'CEHC','=',null,'Y');
exec cre_dqr(5187,7,'N',363,'CEHS','=',null,'Y');
exec cre_dqr(5188,7,'N',363,'COMH','=',null,'Y');
exec cre_dqr(923,7,'N',363,'CORS','=',null,'Y');
exec cre_dqr(924,7,'N',363,'HINH','=',null,'Y');
exec cre_dqr(925,7,'N',363,'JULF','=',null,'Y');
exec cre_dqr(957,7,'N',363,'HOUC','=',null,'Y');
exec cre_dqr(958,7,'N',363,'HOUP','=',null,'Y');
exec cre_dqr(959,7,'N',363,'JUNW','=',null,'Y');
exec cre_dqr(960,7,'N',363,'LANV','=',null,'Y');
exec cre_dqr(980,7,'N',363,'MERZ','=',null,'Y');
exec cre_dqr(961,7,'N',363,'MINI','=',null,'Y');
exec cre_dqr(962,7,'N',363,'NEHC','=',null,'Y');
exec cre_dqr(963,7,'N',363,'PENL','=',null,'Y');
exec cre_dqr(968,7,'N',363,'SALH','=',null,'Y');
exec cre_dqr(969,7,'N',363,'SALV','=',null,'Y');
exec cre_dqr(979,7,'N',363,'SOUS','=',null,'Y');
exec cre_dqr(970,7,'N',363,'SPLC','=',null,'Y');
exec cre_dqr(971,7,'N',363,'SYPC','=',null,'Y');
exec cre_dqr(972,7,'N',363,'TOWC','=',null,'Y');
exec cre_dqr(976,7,'N',363,'UNCH','=',null,'Y');
exec cre_dqr(973,7,'N',363,'UNIT','=',null,'Y');
exec cre_dqr(977,7,'N',363,'UNSA','=',null,'Y');
exec cre_dqr(974,7,'N',363,'WESH','=',null,'Y');
exec cre_dqr(978,7,'N',363,'YOUR','=',null,'Y');
UPDATE questions SET QUE_SHOW_SUMMARY_IND = 'N' WHERE que_refno in (346, 353, 358);
UPDATE questions SET QUE_SUMMARY_SEQNO = 2502 WHERE que_refno = 363;
UPDATE questions SET QUE_SUMMARY_SEQNO = 2504 WHERE que_refno = 369;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2502 WHERE squ_que_refno = 363;
UPDATE scheme_questions SET SQU_SUMMARY_ALT_SEQNO = 2504 WHERE squ_que_refno = 369;
/*

    VSTS 46171 - Shortlist filters on SAHT shortlist

*/
REM INSERTING into DFLT_QUESTION_RESTRICTS
exec cre_dqr(5150,10,'N',521,'SAHT',null,null,null);
exec cre_dqr(5151,20,'N',351,'SAHT','>=',null,null);
exec cre_dqr(5152,25,'N',357,'SAHT','<=',null,null);
exec cre_dqr(5153,50,'N',526,'SAHT','=',null,'Y');
exec cre_dqr(5154,65,'N',737,'SAHT','=',null,null);
exec cre_dqr(5155,70,'N',742,'SAHT','=',null,null);
exec cre_dqr(5156,80,'N',380,'SAHT','=',null,null);
exec cre_dqr(5157,90,'N',384,'SAHT','=',null,null);
exec cre_dqr(5158,100,'N',975,'SAHT','=',null,'Y');
exec cre_dqr(5159,110,'N',532,'SAHT',null,null,null);
exec cre_dqr(5160,120,'N',536,'SAHT','=',null,'Y');
exec cre_dqr(5161,130,'N',402,'SAHT','=',null,'Y');
exec cre_dqr(5162,140,'N',407,'SAHT','=',null,'Y');
exec cre_dqr(5163,150,'N',413,'SAHT','=',null,'Y');
exec cre_dqr(5164,160,'N',418,'SAHT','=',null,'Y');
exec cre_dqr(5165,170,'N',423,'SAHT','=',null,'Y');
exec cre_dqr(5166,180,'N',428,'SAHT','=',null,'Y');
exec cre_dqr(5167,190,'N',433,'SAHT','=',null,'Y');
exec cre_dqr(5168,400,'N',595,'SAHT',null,null,null);
exec cre_dqr(5169,410,'N',472,'SAHT','>=',null,null);
exec cre_dqr(5170,420,'N',476,'SAHT','<=',null,null);
exec cre_dqr(5171,470,'N',613,'SAHT',null,null,null);
exec cre_dqr(5172,480,'N',485,'SAHT','=',null,'Y');
exec cre_dqr(5173,490,'N',489,'SAHT','=',null,'Y');
exec cre_dqr(5174,500,'N',493,'SAHT','=',null,'Y');
exec cre_dqr(5175,510,'N',497,'SAHT','=',null,'Y');
exec cre_dqr(5176,520,'N',620,'SAHT',null,null,null);
exec cre_dqr(5177,530,'N',45,'SAHT','=',null,'Y');
exec cre_dqr(5178,540,'N',52,'SAHT','=',null,'Y');
exec cre_dqr(5179,550,'N',387,'SAHT','=',null,'Y');
exec cre_dqr(5180,560,'N',391,'SAHT','=',null,'Y');
exec cre_dqr(5181,570,'N',700,'SAHT','=',null,'Y');
exec cre_dqr(5182,580,'N',465,'SAHT','=',null,'Y');
exec cre_dqr(5183,590,'N',507,'SAHT','=',null,'Y');
exec cre_dqr(5184,600,'N',696,'SAHT','=',null,'Y');
exec cre_dqr(5185,610,'N',37,'SAHT','=',null,'Y');
exec cre_dqr(5186,620,'N',624,'SAHT','=',null,'Y');

MERGE INTO scheme_questions tgt
USING (SELECT 'PSGEN' squ_hrv_prs_code, 
              squ_que_refno, 
              squ_qgr_code, 
              squ_qgr_question_category, 
              squ_group_alt_seqno, 
              squ_required_ind, 
              squ_scheme_alt_seqno, 
              squ_summary_alt_seqno, 
              squ_nomination_que_ind, 
              squ_nomination_operator, 
              squ_nomination_response
         FROM scheme_questions
        WHERE squ_que_refno in (521, 351, 357, 526, 737, 742, 380, 384, 975, 532, 536, 402, 407, 413, 418, 423, 428, 433, 595, 472, 476, 613, 485, 489, 493, 497, 620, 45, 52, 387, 391, 700, 465, 507, 696, 37, 624)
          AND squ_hrv_prs_code = 'PSCHP') src
   ON (    tgt.squ_que_refno = src.squ_que_refno
       AND tgt.squ_hrv_prs_code = src.squ_hrv_prs_code)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.squ_qgr_code = src.squ_qgr_code,
           tgt.squ_qgr_question_category = src.squ_qgr_question_category,
           tgt.squ_group_alt_seqno = src.squ_group_alt_seqno,
           tgt.squ_required_ind = src.squ_required_ind,
           tgt.squ_scheme_alt_seqno = src.squ_scheme_alt_seqno,
           tgt.squ_summary_alt_seqno = src.squ_summary_alt_seqno,
           tgt.squ_nomination_que_ind = src.squ_nomination_que_ind,
           tgt.squ_nomination_operator = src.squ_nomination_operator,
           tgt.squ_nomination_response = src.squ_nomination_response
 WHEN NOT MATCHED THEN
    INSERT(squ_hrv_prs_code, 
           squ_que_refno, 
           squ_qgr_code, 
           squ_qgr_question_category, 
           squ_group_alt_seqno, 
           squ_required_ind, 
           squ_scheme_alt_seqno, 
           squ_summary_alt_seqno, 
           squ_nomination_que_ind, 
           squ_nomination_operator, 
           squ_nomination_response)
    VALUES(src.squ_hrv_prs_code, 
           src.squ_que_refno, 
           src.squ_qgr_code, 
           src.squ_qgr_question_category, 
           src.squ_group_alt_seqno, 
           src.squ_required_ind, 
           src.squ_scheme_alt_seqno, 
           src.squ_summary_alt_seqno, 
           src.squ_nomination_que_ind, 
           src.squ_nomination_operator, 
           src.squ_nomination_response);
/*

    VSTS 46081 - Two new doc types for NGO reports

*/
REM INSERTING into FIRST_REF_VALUES
exec insert_fr_values('ORG_DOC_TYPES','REPORT','Standard Report',60);
exec insert_fr_values('ORG_DOC_TYPES','ADHOCR','Ad Hoc Report',70);
/*

    VSTS 46984 - Shortlist filters not working

*/
REM INSERTING into DERIVED_QUESTION_HEADERS
exec cre_dqh(138,'Minimum HHld Age','N');
exec cre_dqh(139,'Maximum HHld Age','N');

REM INSERTING into DERIVED_QUESTION_DETAILS
exec cre_dqd(138,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''MINHHLDAGE'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(139,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''MAXHHLDAGE'', app_refno) FROM applications WHERE app_refno = &app_refno');

REM INSERTING into DERIVED_QUESTION_USAGES
exec cre_derived_question_usage(138,472,null);
exec cre_derived_question_usage(139,476,null);
UPDATE questions SET QUE_SHOW_SUMMARY_IND = 'Y' WHERE que_refno in (472,476);
/*

    VSTS 47014 - Hide end date AND end reason from person window    

*/
REM INSERTING into HIDDEN_FIELD_JOB_ROLES
exec cre_hid('HEM-PAR-U',8,'ORG_FIELDHIDE');
exec cre_hid('HEM-PAR-U',15,'ORG_FIELDHIDE');

/*

    VSTS 47313 Change REF column to "-"

*/
UPDATE header_text
   SET HET_TEXT_VALUE = '-'
 WHERE het_region_id = 'HAT-APP-SP'
   AND HET_TEXT_VALUE_SYS = 'Rfs';
/*

    VSTS 47254 hiding fields on create contact

*/
exec cre_hid('HCS-CCT-C',6,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',24,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',12,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',35,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',14,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',15,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',16,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',18,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',19,'ORG_FIELDHIDE');
exec cre_hid('HCS-CCT-C',20,'ORG_FIELDHIDE');
/*

    VSTS 47379 - Org pref letting area hierarchy - descriptions to be UPDATEd

*/
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Housing Plus SA Inc' WHERE lar_code = 'HOUP';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Lansones Village Housing Co-op Inc' WHERE lar_code = 'LANV';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'MERZ Housing Inc' WHERE lar_code = 'MERZ';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'North East Housing Co-op Inc' WHERE lar_code = 'NEHC';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Penny Lane Housing Co-op' WHERE lar_code = 'PENL';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Salisbury Housing Co-op Inc' WHERE lar_code = 'SALH';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Southern Housing Support Co-op Inc' WHERE lar_code = 'SOUS';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Town & Country Housing Inc' WHERE lar_code = 'TOWC';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Access 2 Place Housing Ltd' WHERE lar_code = 'ACC2';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Amelie Housing' WHERE lar_code = 'AMEH';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'AnglicareSA' WHERE lar_code = 'ANGS';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Common Equity Housing SA' WHERE lar_code = 'CEHS';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Community Housing Ltd' WHERE lar_code = 'COMH';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Cornerstone Housing Ltd' WHERE lar_code = 'CORS';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Housing Choices SA Ltd' WHERE lar_code = 'HOUC';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Inhousing SA' WHERE lar_code = 'JULF';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Junction AND Women''s Housing Ltd' WHERE lar_code = 'JUNW';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Minda Housing' WHERE lar_code = 'MINI';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Salvation Army Housing' WHERE lar_code = 'SALV';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'SYP Community Housing Assoc Inc' WHERE lar_code = 'SYPC';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Uniting Country Housing Ltd' WHERE lar_code = 'UNCH';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'UnitingSA Housing Ltd' WHERE lar_code = 'UNSA';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Unity Housing Company Ltd' WHERE lar_code = 'UNIT';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'Westside Housing Company' WHERE lar_code = 'WESH';
UPDATE lettings_areas SET LAR_DESCRIPTION = 'YourPlace Housing Ltd' WHERE lar_code = 'YOUR';
/*

    VSTS 47393 - Empty dropdown for questinos 346 / 353

*/
exec cre_que_perm_resp('1575','The tenant',346,'TENANT');
exec cre_que_perm_resp('1576','The tenant''s partner',346,'PARTNER');
exec cre_que_perm_resp('1577','Another household member',346,'HH');
exec cre_que_perm_resp('1578','CH tenant registering for a transfer',353,'TR');
exec cre_que_perm_resp('1579','CH tenant being relocated',353,'RELOC');
exec cre_que_perm_resp('1580','Partner leaving - relationship breakdown',353,'BD');
exec cre_que_perm_resp('1581','Partner leaving - domestic abuse',353,'DV');
/*

    VSTS 45635 / 45799 Unable to create Contact ACCENQCHP - Access Service Enquiry - CHP.

*/
UPDATE business_reasons
   SET BRO_CONTACT_AUY_CODE = null, BRO_CONTACT_AU_REQ_IND = 'N'
 WHERE bro_code in ('UPDETAILS','ACCENQCHP','CATREVIEW','CHANGECIRC','HNASUPPORT','HNAREQ','REGENQ','VERIFYHNA','ACCENQ');

/*

    VSTS 47452 NGO Portal: Issues in Q346 & Q353 in application form

*/
UPDATE scheme_questions SET squ_required_ind = 'N' WHERE squ_que_refno = 353;
UPDATE questions SET QUE_REQUIRED_IND = 'N' WHERE que_refno = 353;
/*

	Install the Assessments config for HNA AND TRHNA

*/
UPDATE org_portal_tiles
   SET OPT_CARD_SEQNO = 45
 WHERE OPT_CARD_TITLE = 'Assessments';

REM INSERTING into PARTIES - CHP Team member
DECLARE
   l_par_refno   parties.par_refno%TYPE;
   l_exists      VARCHAR2(1);
   
   CURSOR C_PAR_EXISTS IS
      SELECT 'Y'
        FROM parties
       WHERE par_type = 'ORG'
         AND par_org_name = 'COMMUNITY HOUSING PROVIDERS'
         AND par_org_short_name = 'CHP';
BEGIN
   OPEN C_PAR_EXISTS;
   FETCH C_PAR_EXISTS INTO l_exists;
   CLOSE C_PAR_EXISTS;
   
   IF NVL(l_exists, 'N') = 'N'
   THEN
      l_par_refno := par_refno_seq.NEXTVAL;
   END IF;
   
   MERGE INTO parties tgt
   USING (SELECT 'ORG' par_type,
                 'COMMUNITY HOUSING PROVIDERS' par_org_name,
                 'CHP' par_org_short_name,
                 'TEAMMEMBER' par_org_frv_oty_code,
                 'Y' par_org_current_ind
            FROM dual) src
      ON (    tgt.par_type = src.par_type
          AND tgt.par_org_name = src.par_org_name
          AND tgt.par_org_frv_oty_code = src.par_org_frv_oty_code)
    WHEN NOT MATCHED THEN
       INSERT(par_refno,
              par_type,
              par_org_name,
              par_org_short_name,
              par_org_frv_oty_code,
              par_org_current_ind) 
       VALUES(l_par_refno,
              src.par_type,
              src.par_org_name,
              src.par_org_short_name,
              src.par_org_frv_oty_code,
              src.par_org_current_ind);
   
   IF l_par_refno IS NOT NULL
   THEN
      MERGE INTO interested_parties tgt
      USING (SELECT 'CHP' ipp_shortname,
                    'TEAMMEMBER' ipp_ipt_code,
                    l_par_refno ipp_par_refno
               FROM dual) src
         ON (    tgt.ipp_shortname = src.ipp_shortname
             AND tgt.ipp_ipt_code = src.ipp_ipt_code)
       WHEN NOT MATCHED THEN
          INSERT(ipp_shortname,
                 ipp_ipt_code,
                 ipp_par_refno)
          VALUES(src.ipp_shortname,
                 src.ipp_ipt_code,
                 src.ipp_par_refno);
   END IF;
END;
/

UPDATE rehousing_lists SET RLI_CURRENT_IND = 'N' WHERE rli_code in ('HNA','TRHNA');
UPDATE rehousing_lists SET RLI_CURRENT_IND = 'N' WHERE rli_code in ('HNA','TRHNA');
UPDATE rehousing_list_stages SET RLS_CURRENT_IND = 'N' WHERE rls_code in ('HNA','HNATR','APPRTR','APPROVAL');
UPDATE first_ref_values SET FRV_CURRENT_IND = 'N' WHERE FRV_FRD_DOMAIN = 'STAGE_DEC';

REM INSERTING into ASSESSMENT_TYPES
MERGE INTO generic_questions tgt
USING (SELECT 194 gque_reference,
              'N' gque_class,
              'HNA outcome' gque_text,
              'Y' gque_current_ind,
              'Y' gque_show_summary_ind,
              1000 gque_summary_display_seq,
              'HNA Outcome' gque_summary_description,
              'DNQ' gque_type,
              'ASSESSMENTS' gque_obj_name,
              'Y' gque_display_online_ind,
              'Y' gque_recalculate_ind
         FROM dual) src
   ON (tgt.gque_reference = src.gque_reference)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.gque_class = src.gque_class,
           tgt.gque_text = src.gque_text,
           tgt.gque_current_ind = src.gque_current_ind,
           tgt.gque_show_summary_ind = src.gque_show_summary_ind,
           tgt.gque_summary_display_seq = src.gque_summary_display_seq,
           tgt.gque_summary_description = src.gque_summary_description,
           tgt.gque_type = src.gque_type,
           tgt.gque_obj_name = src.gque_obj_name,
           tgt.gque_display_online_ind = src.gque_display_online_ind,
           tgt.gque_recalculate_ind = src.gque_recalculate_ind
 WHEN NOT MATCHED THEN
    INSERT(gque_reference,
           gque_class,
           gque_text,
           gque_current_ind,
           gque_show_summary_ind,
           gque_summary_display_seq,
           gque_summary_description,
           gque_type,
           gque_obj_name,
           gque_display_online_ind,
           gque_recalculate_ind) 
    VALUES(src.gque_reference,
           src.gque_class,
           src.gque_text,
           src.gque_current_ind,
           src.gque_show_summary_ind,
           src.gque_summary_display_seq,
           src.gque_summary_description,
           src.gque_type,
           src.gque_obj_name,
           src.gque_display_online_ind,
           src.gque_recalculate_ind);
           
exec cre_asst('HNA','O','Housing Needs Assessments','M',12,'Y','M','N','Y','Y',30,194);
exec cre_asst('TRHNA','O','Transfer HNA','M',12,'Y','M','N','Y','N',null,194);

REM INSERTING into GENERIC_QUESTIONS
exec cre_gque(241,'Y','Is this an Agency HNA?','Y','Y',10670,null,'Agency HNA?','10670','PQU','ASSESSMENTS','Y','N');
exec cre_gque(270,'T','Details of living situation','Y','Y',10680,null,'Details of living situation','10680','PQU','ASSESSMENTS','Y','N');
exec cre_gque(246,'Y','Customer needs to leave due to domestic/family violence?','Y','Y',10610,null,'Domestic / Family Violence','10610','PQU','ASSESSMENTS','Y','N');
exec cre_gque(248,'Y','Customer needs to leave due to being victim of major crime?','Y','Y',10630,null,'Victim of major crime','10630','PQU','ASSESSMENTS','Y','N');
exec cre_gque(250,'Y','Does the customer need to leave due to unsuitable location?','Y','Y',10650,null,'Location unsuitable','10650','PQU','ASSESSMENTS','Y','N');
exec cre_gque(247,'Y','Does the customer need to leave due to natural disaster?','Y','Y',10760,null,'Natural disaster','10760','PQU','ASSESSMENTS','Y','N');
exec cre_gque(249,'Y','Need to leave due to unsafe/unhealthy dwelling condition?','Y','Y',10640,null,'Dwelling unsafe / unhealthy','10640','PQU','ASSESSMENTS','Y','N');
exec cre_gque(261,'Y','Housing situation poses an imminent threat to health/safety?','Y','Y',10710,null,'At risk: Imminent threat','10710','PQU','ASSESSMENTS','Y','N');
exec cre_gque(256,'Y','Is the customer experiencing persistent harassment?','Y','Y',10620,null,'Persistent harassment','10620','PQU','ASSESSMENTS','Y','N');
exec cre_gque(258,'Y','Does the customer have health/medical conditions?','Y','Y',10740,null,'Health / medical reasons','10740','PQU','ASSESSMENTS','Y','N');
exec cre_gque(260,'Y','Does the customer need to be closer to supports?','Y','Y',10720,null,'Needs to be closer to supports','10720','PQU','ASSESSMENTS','Y','N');
exec cre_gque(257,'Y','Is there an unresolved neighbour dispute?','Y','Y',10750,null,'Unresolved neighbour dispute','10750','PQU','ASSESSMENTS','Y','N');
exec cre_gque(259,'Y','Are they experiencing overcrowding?','Y','Y',10730,null,'Overcrowding','10730','PQU','ASSESSMENTS','Y','N');
exec cre_gque(262,'T','When does the customer need to leave?','Y','Y',10700,null,'When do they need to leave?','10700','PQU','ASSESSMENTS','Y','N');
exec cre_gque(271,'T','Details of why the customer needs to leave','Y','Y',10660,null,'Why they need to leave','10660','PQU','ASSESSMENTS','Y','N');
exec cre_gque(263,'T','Accommodation history for at least the last three years','Y','Y',10690,null,'Address History?','10690','PQU','ASSESSMENTS','Y','N');
exec cre_gque(285,'T','Name of Agency','Y','Y',10790,null,'Name of Agency','10790','PQU','ASSESSMENTS','Y','N');
exec cre_gque(243,'T','Agency file number','Y','Y',10810,null,'Agency file number','10810','PQU','ASSESSMENTS','Y','N');
exec cre_gque(284,'T','Contact officer','Y','Y',10800,null,'Contact person','10800','PQU',null,'Y','N');
exec cre_gque(286,'T','Address','Y','Y',10780,null,'Agency Address','10780','PQU',null,'Y','N');
exec cre_gque(242,'T','Phone','Y','Y',10820,null,'Phone number','10820','PQU',null,'Y','N');
exec cre_gque(289,'T','Email','Y','N',10770,null,'Email','10770','PQU',null,'N','N');
exec cre_gque(269,'C','Where is the customer living?','Y','Y',10830,null,'Where are they living','10830','PQU',null,'N','N');
exec cre_gque(245,'T','How long has the customer been living there?','Y','Y',10840,null,'How long have they been there?','10840','PQU','ASSESSMENTS','Y','N');
exec cre_gque(251,'Y','Has the customer''s lease expired?','Y','Y',10850,null,'Lease has expired','10850','PQU','ASSESSMENTS','Y','N');
exec cre_gque(253,'Y','Is the customer experiencing a relationship breakdown?','Y','Y',10880,null,'Relationship breakdown','10880','PQU','ASSESSMENTS','Y','N');
exec cre_gque(255,'Y','Have they been asked to leave?','Y','Y',10860,null,'Asked to leave','10860','PQU','ASSESSMENTS','Y','N');
exec cre_gque(252,'Y','Is the rent unaffordable?','Y','Y',10890,null,'Rent unaffordable','10890','PQU','ASSESSMENTS','Y','N');
exec cre_gque(254,'Y','Has an eviction notice been received?','Y','Y',10870,null,'Eviction notice','10870','PQU','ASSESSMENTS','Y','N');
exec cre_gque(272,'Y','Are there long-term health issues?','Y','Y',10910,null,'Long-term health issues','10910','PQU','ASSESSMENTS','Y','N');
exec cre_gque(274,'Y','Are there long term disability issues?','Y','Y',10900,null,'Long term disability issues','10900','PQU','ASSESSMENTS','Y','N');
exec cre_gque(276,'Y','Has the customer experienced private rental discrimination?','Y','Y',10950,null,'Discrimination','10950','PQU','ASSESSMENTS','Y','N');
exec cre_gque(273,'Y','Is the customer experiencing long term financial hardship?','Y','Y',10920,null,'Long term financial hardship','10920','PQU','ASSESSMENTS','Y','N');
exec cre_gque(275,'Y','Is the customer exiting institutional care?','Y','Y',10960,null,'Exiting institutional care','10960','PQU','ASSESSMENTS','Y','N');
exec cre_gque(277,'Y','Customer presents as having a chronic lack of social skills?','Y','Y',10940,null,'Chronic lack of social skills?','10940','PQU','ASSESSMENTS','Y','N');
exec cre_gque(287,'T','Details about barriers to accessing accommodation','Y','Y',10930,null,'Private housing barriers dets','10930','PQU','ASSESSMENTS','Y','N');
exec cre_gque(264,'T','Is buying their own home an option?','Y','Y',10970,null,'Home Ownership','10970','PQU','ASSESSMENTS','Y','N');
exec cre_gque(265,'T','Is private housing an option?','Y','Y',10980,null,'Private Housing','10980','PQU','ASSESSMENTS','Y','N');
exec cre_gque(266,'T','Is support to maintain current accommodation an option?','Y','Y',10990,null,'Tenancy Support','10990','PQU','ASSESSMENTS','Y','N');
exec cre_gque(268,'T','Is supported or transitional housing an option?','Y','Y',11010,null,'Supported or transitional','11010','PQU','ASSESSMENTS','Y','N');
exec cre_gque(267,'T','Are other housing options available?','Y','Y',11000,null,'Other housing options','11000','PQU','ASSESSMENTS','Y','N');
exec cre_gque(281,'T','Are there any existing supports?','Y','Y',11020,null,'Existing Supports','11020','PQU','ASSESSMENTS','Y','N');
exec cre_gque(282,'T','Are there any additional supports needed?','Y','Y',11030,null,'Additional supports required','11030','PQU','ASSESSMENTS','Y','N');
exec cre_gque(283,'T','Have needs been verified?','Y','Y',11040,null,'Verification','11040','PQU','ASSESSMENTS','Y','N');
exec cre_gque(290,'T','Current category ','Y','N',11100,null,'Current category ','11100','DNQ','ASSESSMENTS','Y','Y');
exec cre_gque(293,'T','Auto category 2?','Y','N',11050,null,'Auto category 2?','11050','DNQ','ASSESSMENTS','Y','Y');
exec cre_gque(196,'C','Assessment reason','Y','Y',11080,null,'Assessment reason','11080','PQU',null,'Y','N');
exec cre_gque(195,'C','Category recommendation','Y','Y',11070,null,'Category recommendation','11070','PQU',null,'Y','N');
exec cre_gque(197,'T','Reasons for your recommendation','Y','Y',11090,null,'Reasons for your recommendation','11090','PQU',null,'Y','N');
exec cre_gque(294,'T','Ready for approval?','Y','N',11060,null,'Ready for approval?','11060','PQU',null,'N','N');
exec cre_gque(311,'C','Category recommendation','Y','N',11110,null,'Category recommendation','11110','PQU',null,'N','N');
exec cre_gque(198,'C','Category approved','Y','Y',11140,null,'Category approved','11140','PQU',null,'Y','N');
exec cre_gque(201,'D','Category approved date','Y','Y',11120,null,'Category approved date','11120','PQU',null,'Y','N');
exec cre_gque(200,'T','Reasons for approval','Y','Y',11130,null,'Reasons for approval','11130','PQU',null,'Y','N');
exec cre_gque(295,'C','Category approved','Y','N',null,null,'Category approved',null,'PQU',null,'N','N');
exec cre_gque(202,'T','HNA outcome approved by manager','Y','Y',10110,null,'HNA outcome approved by manager','10110','DNQ','ASSESSMENTS','Y','Y');

REM INSERTING into GENERIC_QUESTION_GROUPS
exec cre_gqgp('HNA01','Agency Questions');
exec cre_gqgp('HNA01A','Agency details');
exec cre_gqgp('HNA02','Current Circumstances');
exec cre_gqgp('HNA03','Why do the need to leave');
exec cre_gqgp('HNA05','Barriers');
exec cre_gqgp('HNA06','Housing Options');
exec cre_gqgp('HNAAPP','HNA Approval');
exec cre_gqgp('HNAREC','HNA Recommendation');
exec cre_gqgp('TRHNA01','Transfer HNA screen 1');
exec cre_gqgp('TRHNA02','Transfer HNA screen 2');
exec cre_gqgp('TRHNAAPP','Transfer HNA approval');
exec cre_gqgp('TRHNADQ','Transfer HNA DQs');
exec cre_gqgp('TRHNAREC','Transfer HNA recommendation');
exec cre_gqgp('HNADQ','HNA Derived questions');

REM INSERTING into GENERIC_QUESTION_GRP_USAGES
exec cre_gqgu(38,'HNAAPP','N','HNA',null);
exec cre_gqgu(39,'HNAREC','N','HNA',38);
exec cre_gqgu(29,'HNA06','N','HNA',39);
exec cre_gqgu(30,'HNA05','N','HNA',29);
exec cre_gqgu(32,'HNA03','N','HNA',30);
exec cre_gqgu(33,'HNA02','N','HNA',32);
exec cre_gqgu(35,'HNA01A','N','HNA',33);
exec cre_gqgu(34,'HNA01','Y','HNA',33);
exec cre_gqgu(36,'HNADQ','N','HNA',null);
exec cre_gqgu(40,'TRHNAAPP','N','TRHNA',null);
exec cre_gqgu(41,'TRHNAREC','N','TRHNA',40);
exec cre_gqgu(42,'TRHNA02','N','TRHNA',41);
exec cre_gqgu(43,'TRHNA01','Y','TRHNA',42);
exec cre_gqgu(37,'TRHNADQ','N','TRHNA',null);

REM INSERTING into GENERIC_QUESTION_GRP_MEMBERS
exec cre_gqgm('HNA01',241,'N',100);
exec cre_gqgm('HNA01A',285,'N',200);
exec cre_gqgm('HNA01A',243,'N',210);
exec cre_gqgm('HNA01A',284,'N',220);
exec cre_gqgm('HNA01A',286,'N',230);
exec cre_gqgm('HNA01A',242,'N',240);
exec cre_gqgm('HNA01A',289,'N',250);
exec cre_gqgm('HNA02',269,'Y',310);
exec cre_gqgm('HNA02',245,'N',320);
exec cre_gqgm('HNA02',270,'N',330);
exec cre_gqgm('HNA03',246,'N',400);
exec cre_gqgm('HNA03',248,'N',405);
exec cre_gqgm('HNA03',250,'N',410);
exec cre_gqgm('HNA03',247,'N',415);
exec cre_gqgm('HNA03',249,'N',420);
exec cre_gqgm('HNA03',261,'N',425);
exec cre_gqgm('HNA03',251,'N',430);
exec cre_gqgm('HNA03',253,'N',435);
exec cre_gqgm('HNA03',255,'N',440);
exec cre_gqgm('HNA03',252,'N',445);
exec cre_gqgm('HNA03',254,'N',450);
exec cre_gqgm('HNA03',262,'N',455);
exec cre_gqgm('HNA03',271,'N',460);
exec cre_gqgm('HNA03',263,'N',465);
exec cre_gqgm('HNA05',272,'N',500);
exec cre_gqgm('HNA05',274,'N',510);
exec cre_gqgm('HNA05',276,'N',520);
exec cre_gqgm('HNA05',273,'N',530);
exec cre_gqgm('HNA05',275,'N',540);
exec cre_gqgm('HNA05',277,'N',550);
exec cre_gqgm('HNA05',287,'N',560);
exec cre_gqgm('HNA06',264,'N',600);
exec cre_gqgm('HNA06',265,'N',610);
exec cre_gqgm('HNA06',266,'N',620);
exec cre_gqgm('HNA06',268,'N',630);
exec cre_gqgm('HNA06',267,'N',640);
exec cre_gqgm('HNA06',281,'N',650);
exec cre_gqgm('HNA06',282,'N',660);
exec cre_gqgm('HNA06',283,'N',670);
exec cre_gqgm('HNAAPP',198,'N',800);
exec cre_gqgm('HNAAPP',201,'N',810);
exec cre_gqgm('HNAAPP',200,'N',820);
exec cre_gqgm('HNADQ',202,'N',900);
exec cre_gqgm('HNADQ',194,'N',910);
exec cre_gqgm('HNAREC',290,'N',700);
exec cre_gqgm('HNAREC',293,'N',710);
exec cre_gqgm('HNAREC',196,'N',720);
exec cre_gqgm('HNAREC',195,'N',730);
exec cre_gqgm('HNAREC',197,'N',740);
exec cre_gqgm('HNAREC',294,'N',750);
exec cre_gqgm('TRHNA01',270,'N',100);
exec cre_gqgm('TRHNA01',246,'N',105);
exec cre_gqgm('TRHNA01',248,'N',110);
exec cre_gqgm('TRHNA01',250,'N',115);
exec cre_gqgm('TRHNA01',247,'N',120);
exec cre_gqgm('TRHNA01',249,'N',125);
exec cre_gqgm('TRHNA01',261,'N',130);
exec cre_gqgm('TRHNA01',256,'N',135);
exec cre_gqgm('TRHNA01',258,'N',140);
exec cre_gqgm('TRHNA01',260,'N',145);
exec cre_gqgm('TRHNA01',257,'N',150);
exec cre_gqgm('TRHNA01',259,'N',155);
exec cre_gqgm('TRHNA01',262,'N',160);
exec cre_gqgm('TRHNA01',271,'N',165);
exec cre_gqgm('TRHNA01',263,'N',170);
exec cre_gqgm('TRHNA02',264,'N',210);
exec cre_gqgm('TRHNA02',265,'N',220);
exec cre_gqgm('TRHNA02',266,'N',230);
exec cre_gqgm('TRHNA02',268,'N',240);
exec cre_gqgm('TRHNA02',267,'N',250);
exec cre_gqgm('TRHNA02',281,'N',260);
exec cre_gqgm('TRHNA02',282,'N',270);
exec cre_gqgm('TRHNA02',283,'N',280);
exec cre_gqgm('TRHNAAPP',295,'N',410);
exec cre_gqgm('TRHNAAPP',201,'N',420);
exec cre_gqgm('TRHNAAPP',200,'N',430);
exec cre_gqgm('TRHNADQ',202,'N',510);
exec cre_gqgm('TRHNADQ',194,'N',520);
exec cre_gqgm('TRHNAREC',290,'N',310);
exec cre_gqgm('TRHNAREC',293,'N',320);
exec cre_gqgm('TRHNAREC',196,'N',330);
exec cre_gqgm('TRHNAREC',311,'N',340);
exec cre_gqgm('TRHNAREC',197,'N',350);
exec cre_gqgm('TRHNAREC',294,'N',360);

REM INSERTING into GENERIC_QUESTION_FOLLOW_ONS
MERGE INTO generic_question_follow_ons tgt
USING (SELECT 1 gqfo_refno,
              34 gqfo_gqgu_refno,
              'Y' gqfo_type,
              241 gqfo_gque_reference,
              35 gqfo_to_gqgu_refno,
              null gqfo_gcre_code,
              'Y' gqfo_yesno_response_ind
         FROM dual) src
   ON (tgt.gqfo_refno = src.gqfo_refno)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.gqfo_gqgu_refno = src.gqfo_gqgu_refno,
           tgt.gqfo_type = src.gqfo_type,
           tgt.gqfo_gque_reference = src.gqfo_gque_reference,
           tgt.gqfo_to_gqgu_refno = src.gqfo_to_gqgu_refno,
           tgt.gqfo_gcre_code = src.gqfo_gcre_code,
           tgt.gqfo_yesno_response_ind = src.gqfo_yesno_response_ind
 WHEN NOT MATCHED THEN
    INSERT(gqfo_refno,
           gqfo_gqgu_refno,
           gqfo_type,
           gqfo_gque_reference,
           gqfo_to_gqgu_refno,
           gqfo_gcre_code,
           gqfo_yesno_response_ind)
    VALUES(src.gqfo_refno,
           src.gqfo_gqgu_refno,
           src.gqfo_type,
           src.gqfo_gque_reference,
           src.gqfo_to_gqgu_refno,
           src.gqfo_gcre_code,
           src.gqfo_yesno_response_ind);

REM INSERTING into DERIVED_QUESTION_HEADERS
exec cre_dqh(140,'HNA Current Category','T');
exec cre_dqh(141,'HNA Auto Cat 2','T');
exec cre_dqh(148,'HNA Outcome','N');
exec cre_dqh(149,'HNA approved by manager','T');

REM INSERTING into DERIVED_QUESTION_DETAILS
exec cre_dqd(140,1,'SELECT S_USR_DQS_HSS.HSS_DQ_SQL(''CURRCAT'', assm_refno) FROM assessments WHERE assm_refno = &assm_refno');
exec cre_dqd(141,1,'SELECT S_USR_DQS_HSS.HSS_DQ_SQL(''AUTOCAT2'', assm_refno) FROM assessments WHERE assm_refno = &assm_refno');
exec cre_dqd(148,1,'SELECT S_USR_DQS_HSS.HSS_DQ_SQL(''HNAOUTCOME'', assm_refno) FROM assessments WHERE assm_refno = &assm_refno');
exec cre_dqd(149,1,'SELECT S_USR_DQS_HSS.HSS_DQ_SQL(''HNAAPPR'', assm_refno) FROM assessments WHERE assm_refno = &assm_refno');

REM INSERTING into GENERIC_DERIVED_QUESTION_USAGE
exec cre_gdqu(86,140,290);
exec cre_gdqu(88,149,202);
exec cre_gdqu(89,141,293);
exec cre_gdqu(87,148,194);

REM INSERTING into GENERIC_CODED_RESPONSES
exec cre_gcre(195,'CAT1','Category 1',10);
exec cre_gcre(195,'CAT2','Category 2',20);
exec cre_gcre(195,'CAT3','Category 3',30);
exec cre_gcre(196,'HNACAT','HNA Category',10);
exec cre_gcre(196,'CATREVIEW','Category Review',20);
exec cre_gcre(196,'APPEAL','Appeal',30);
exec cre_gcre(198,'CAT1','Category 1',10);
exec cre_gcre(198,'CAT2','Category 2',20);
exec cre_gcre(198,'CAT3','Category 3',30);
exec cre_gcre(269,'BOARD','Boarding house',10);
exec cre_gcre(269,'CARAVAN','Caravan Park (long stay)',20);
exec cre_gcre(269,'CH','Community Housing',30);
exec cre_gcre(269,'HOME','Own home',40);
exec cre_gcre(269,'HOSPIT','Hospital / nursing home',50);
exec cre_gcre(269,'INSTIT','Institutional care',60);
exec cre_gcre(269,'OTHER','Other (please specify)',80);
exec cre_gcre(269,'PRIVATE','Renting privately',90);
exec cre_gcre(269,'PUBLIC','Public or Aboriginal Housing',100);
exec cre_gcre(269,'ROUGH','Sleeping rough/non-conventional',110);
exec cre_gcre(269,'SHARE','Share Housing',120);
exec cre_gcre(269,'STEA','Short-term / Emergency accommodation',130);
exec cre_gcre(269,'SUPPORT','Supported accommodation',140);
exec cre_gcre(295,'CAT1','CAT1',10);
exec cre_gcre(295,'CAT2','CAT2',20);
exec cre_gcre(295,'CAT4','CAT4',40);
exec cre_gcre(311,'CAT1','CAT1',10);
exec cre_gcre(311,'CAT2','CAT2',20);
exec cre_gcre(311,'CAT4','CAT4',40);

REM INSERTING into ASSESSMENT_TYPE_OUTCOMES
exec cre_ato('HNA','CAT1','CAT1','Y',1,1,null);
exec cre_ato('HNA','CAT2','CAT2','Y',2,2,null);
exec cre_ato('HNA','CAT3','CAT3','Y',3,3,null);
exec cre_ato('TRHNA','CAT1','CAT1','Y',1,1,10);
exec cre_ato('TRHNA','CAT2','CAT2','Y',2,2,20);
exec cre_ato('TRHNA','CAT4','CAT4','Y',4,4,40);

REM INSERTING into HIDDEN_FIELD_JOB_ROLES
exec cre_hid('HSS-ASM-ASS',14,'FIELD_HIDE');
exec cre_hid('HSS-ASM-DP',13,'FIELD_HIDE');
exec cre_hid('HSS-ASM-DP',23,'ORG_FIELDHIDE');
exec cre_hid('HSS-ASM-DP',24,'ORG_FIELDHIDE');
exec cre_hid('HSS-CLI-ASM',13,'FIELD_HIDE');
exec cre_hid('HSS-CLI-ASM',16,'FIELD_HIDE');
exec cre_hid('HSS-CLI-ASM',17,'FIELD_HIDE');
exec cre_hid('HSS-CLI-ASM',18,'ORG_FIELDHIDE');
exec cre_hid('HSS-CLI-ASM',19,'ORG_FIELDHIDE');
exec cre_hid('HSS-CLI-ASM',20,'FIELD_HIDE');
exec cre_hid('HSS-CLI-ASM',29,'FIELD_HIDE');
exec cre_hid('HSS-CLI-ASM',30,'FIELD_HIDE');
exec cre_hid('HSS-CLI-ASM',31,'FIELD_HIDE');
exec cre_hid('HSS-CLI-ASM',34,'FIELD_HIDE');
exec cre_hid('HSS-ASM-C',7,'FIELD_HIDE');
exec cre_hid('HSS-ASM-SP',13,'FIELD_HIDE');
exec cre_hid('HSS-ASM-SP',16,'FIELD_HIDE');
exec cre_hid('HSS-ASM-SP',17,'FIELD_HIDE');
exec cre_hid('HSS-ASM-SP',18,'ORG_FIELDHIDE');
exec cre_hid('HSS-ASM-SP',19,'ORG_FIELDHIDE');
exec cre_hid('HSS-ASM-SP',20,'FIELD_HIDE');
exec cre_hid('HSS-ASM-SP',29,'FIELD_HIDE');
exec cre_hid('HSS-ASM-SP',30,'FIELD_HIDE');
exec cre_hid('HSS-ASM-SP',31,'FIELD_HIDE');
exec cre_hid('HSS-ASM-SP',38,'FIELD_HIDE');
/*

    Fix problem with eligibility schemes 

*/
UPDATE HHOLD_TYPE_ELIG_SCHEMES SET HHTS_HEG_CODE = 'INEL' WHERE hhts_hty_code = 'INELIGIBLE' AND HHTS_HRV_ELS_CODE = 'ESAB';

SPOO op_egr_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM ELIG_GROUP_RULE_QUESTIONS WHERE egr_heg_code like '%AB%';
DELETE FROM ELIG_GROUP_RULE_QUESTIONS WHERE egr_heg_code like '%AB%';

SPOO op_heg_vals.log
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM HHOLD_ELIG_GROUP_RULES WHERE heg_code like '%AB';
DELETE FROM HHOLD_ELIG_GROUP_RULES WHERE heg_code like '%AB';

SPOO op_config_install.log APPEND
SET LONG 32000
SET LINESIZE 32000
/*

    VSTS 47052 - SAHT profile user can log in NGO portal

*/
MERGE INTO org_port_tile_job_roles tgt
USING (SELECT opt_refno optj_opt_refno,
              'ORG_BASIC' optj_jro_code
         FROM org_portal_tiles
        WHERE NOT EXISTS (SELECT NULL 
                            FROM org_port_tile_job_roles 
                           WHERE optj_opt_refno = opt_refno 
                             AND optj_jro_code = 'ORG_BASIC')) src
   ON (    tgt.optj_opt_refno = src.optj_opt_refno
       AND tgt.optj_jro_code = src.optj_jro_code)
 WHEN NOT MATCHED THEN
    INSERT(optj_opt_refno, 
           optj_jro_code)
    VALUES(src.optj_opt_refno, 
           src.optj_jro_code);
/*

    47596 - calculating category strat date (for CHP)

*/
exec create_question(347,'DNQ','D','DQ','DQ','N','N','N','N','N','Y','N','N',221,2601,'70','Category Start Date - CHP','Category Start Date - CHP','N',null);
exec cre_squ('PSCHP',347,'DQ','DQ',70,'N',221,2601,'N');

SPOO op_squ_vals.log APPEND
SET LONG 32000
SET LINESIZE 32000
DELETE FROM scheme_questions WHERE SQU_HRV_PRS_CODE = 'PSCHP' AND SQU_QUE_REFNO = 890;

SPOO op_config_install.log APPEND
exec cre_dqh(167,'Category Start Date - CHP','D');
exec cre_dqd(167,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''CATSTARTDATECHP'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_derived_question_usage(167,347,null);
UPDATE rehousing_lists SET RLI_QUE_REFNO = 347 WHERE RLI_CODE = 'CHP';
/*

    48261 - Hiding contact AND assessment module from the menu AND home page

*/
UPDATE org_portal_tiles SET OPT_CURRENT_IND = 'N' WHERE upper(OPT_CARD_TITLE) = 'CONTACTS';
UPDATE org_portal_tiles SET OPT_CURRENT_IND = 'N' WHERE upper(OPT_CARD_TITLE) = 'ASSESSMENTS';
/*

    48302 - derived question on GEN AND MX to align with produciton

*/
REM INSERTING into DERIVED_QUESTION_HEADERS
exec cre_dqh(168,'Eligible for ABCOM','T');
exec cre_dqh(169,'Eligible for MX','T');
exec cre_dqh(170,'Eligible for TR','T');

REM INSERTING into DERIVED_QUESTION_DETAILS
exec cre_dqd(168,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''ELIGABCOM'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(169,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''ELIGMX'', app_refno) FROM applications WHERE app_refno = &app_refno');
exec cre_dqd(170,1,'SELECT S_USR_DQS_HAT.HAT_DQ_SQL(''ELIGTR'', app_refno) FROM applications WHERE app_refno = &app_refno');

REM INSERTING into DERIVED_QUESTION_USGES

SPOO op_dqu_vals.log APPEND
SET LONG 32000
SET LINESIZE 32000
SELECT * FROM derived_question_usages WHERE dqu_que_refno in (417,432,437);
DELETE FROM derived_question_usages WHERE dqu_que_refno in (417,432,437);

SPOO op_config_install.log APPEND
exec cre_derived_question_usage(168,417,null);
exec cre_derived_question_usage(169,432,null);
exec cre_derived_question_usage(170,437,null);
/*

    Fix up organisation admin units

*/

SPOO op_orau_vals.log
SET LONG 32000
SET LINESIZE 32000

SELECT *
  FROM organisation_admin_units
 WHERE orau_aun_code = 'ACCHOU'
   AND orau_par_refno = (SELECT par_refno 
                           FROM organisations 
                          WHERE PAR_ORG_SHORT_NAME = 'YOURPL');

DELETE FROM organisation_admin_units
 WHERE orau_aun_code = 'ACCHOU'
   AND orau_par_refno = (SELECT par_refno 
                           FROM organisations 
                          WHERE PAR_ORG_SHORT_NAME = 'YOURPL');

SPOO op_config_install.log APPEND

select 'End Time = '||to_char(sysdate, 'hh24:mi:ss') from dual;

DROP PROCEDURE create_question;
DROP PROCEDURE cre_derived_question_clauses;
DROP PROCEDURE cre_derived_question_usage;
DROP PROCEDURE cre_que_perm_resp;
DROP PROCEDURE insert_fr_values;
DROP PROCEDURE cre_acpg;
DROP PROCEDURE cre_acrq;
DROP PROCEDURE cre_accg;
DROP PROCEDURE cre_acps;
DROP PROCEDURE cre_acpr;
DROP PROCEDURE cre_slc;
DROP PROCEDURE cre_scg;
DROP PROCEDURE cre_dqr;
DROP PROCEDURE cre_egr;
DROP PROCEDURE cre_mru;
DROP PROCEDURE cre_squ;
DROP PROCEDURE cre_att;
DROP PROCEDURE cre_lar;
DROP PROCEDURE cre_dqh;
DROP PROCEDURE cre_dqd;
DROP PROCEDURE cre_rra;
DROP PROCEDURE cre_hid;
DROP PROCEDURE cre_tty;
DROP PROCEDURE cre_ntt;
DROP PROCEDURE cre_nto;
DROP PROCEDURE cre_srp;
DROP PROCEDURE cre_pesr;
DROP PROCEDURE cre_equ;
DROP PROCEDURE cre_asst;
DROP PROCEDURE cre_gque;
DROP PROCEDURE cre_gqgp;
DROP PROCEDURE cre_gqgu;
DROP PROCEDURE cre_gqgm;
DROP PROCEDURE cre_gdqu;
DROP PROCEDURE cre_gcre;
DROP PROCEDURE cre_ato;


--commit;
--rollback;


SPOO OFF