CREATE OR REPLACE PROCEDURE create_assessment(p_assm_refno             IN   assessments.assm_refno%TYPE,
                                              p_assm_asst_code         IN   assessments.assm_asst_code%TYPE,
                                              p_assm_sequence          IN   assessments.assm_sequence%TYPE,
                                              p_assm_assessment_date   IN   assessments.assm_assessment_date%TYPE,
                                              p_assm_sco_code          IN   assessments.assm_sco_code%TYPE,
                                              p_assm_ato_code          IN   assessments.assm_ato_code%TYPE,
                                              p_assm_ipp_refno         IN   assessments.assm_ipp_refno%TYPE,
                                              p_assm_par_refno         IN   assessments.assm_par_refno%TYPE,
                                              p_assm_aun_code          IN   assessments.assm_aun_code%TYPE,
                                              p_assm_status_date       IN   assessments.assm_status_date%TYPE,
                                              p_assm_created_by        IN   assessments.assm_created_by%TYPE,
                                              p_assm_created_date      IN   assessments.assm_created_date%TYPE,
                                              p_assm_modified_by       IN   assessments.assm_modified_by%TYPE,
                                              p_assm_modified_date     IN   assessments.assm_modified_date%TYPE,
                                              p_src_app_refno          IN   applications.app_refno%TYPE)
IS

BEGIN
   INSERT INTO assessments(assm_refno,
                           assm_asst_code,
                           assm_sequence,
                           assm_type,
                           assm_sco_code,
                           assm_ato_code,
                           assm_assessment_date,
                           assm_ipp_refno,
                           assm_par_refno,
                           assm_aun_code,
                           assm_status_date,
                           assm_created_by,
                           assm_created_date,
                           assm_modified_by,
                           assm_modified_date,
                           assm_comments)
                    VALUES(p_assm_refno,
                           p_assm_asst_code,
                           p_assm_sequence,
                           'O',
                           p_assm_sco_code,
                           p_assm_ato_code,
                           p_assm_assessment_date,
                           p_assm_ipp_refno,
                           p_assm_par_refno,
                           p_assm_aun_code,
                           p_assm_status_date,
                           p_assm_created_by,
                           p_assm_created_date,
                           p_assm_modified_by,
                           p_assm_modified_date,
                           'Source application: ' || p_src_app_refno);
END create_assessment;
/

CREATE OR REPLACE PROCEDURE create_gqre(p_gqre_assm_refno       IN   generic_question_responses.gqre_assm_refno%TYPE     DEFAULT NULL,
                                        p_gqre_gque_reference   IN   generic_question_responses.gqre_gque_reference%TYPE DEFAULT NULL, 
                                        p_gqre_boolean_value    IN   generic_question_responses.gqre_boolean_value%TYPE  DEFAULT NULL,
                                        p_gqre_date_value       IN   generic_question_responses.gqre_date_value%TYPE     DEFAULT NULL, 
                                        p_gqre_numeric_value    IN   generic_question_responses.gqre_numeric_value%TYPE  DEFAULT NULL, 
                                        p_gqre_text_value       IN   generic_question_responses.gqre_text_value%TYPE     DEFAULT NULL,
                                        p_gqre_gcre_code        IN   generic_question_responses.gqre_gcre_code%TYPE      DEFAULT NULL,
                                        p_gqre_created_by       IN   generic_question_responses.gqre_created_by%TYPE     DEFAULT NULL,
                                        p_gqre_created_date     IN   generic_question_responses.gqre_created_date%TYPE   DEFAULT NULL)
IS

BEGIN
   INSERT INTO generic_question_responses(gqre_class,
                                          gqre_assm_refno,
                                          gqre_arc_object,
                                          gqre_gque_reference,
                                          gqre_boolean_value,
                                          gqre_date_value,
                                          gqre_numeric_value,
                                          gqre_text_value,
                                          gqre_gcre_code,
                                          gqre_created_by,
                                          gqre_created_date)
                                          SELECT gque_class,
                                                 p_gqre_assm_refno,
                                                 'ASSM',
                                                 gque_reference,
                                                 p_gqre_boolean_value,
                                                 p_gqre_date_value,
                                                 p_gqre_numeric_value,
                                                 SUBSTR(p_gqre_text_value, 1, 2000),
                                                 p_gqre_gcre_code,
                                                 p_gqre_created_by,
                                                 p_gqre_created_date
                                            FROM generic_questions
                                           WHERE gque_reference = p_gqre_gque_reference;
END create_gqre;
/

CREATE OR REPLACE PROCEDURE generate_answer_comments(p_app_refno             IN   applications.app_refno%TYPE                         DEFAULT NULL,
                                                     p_que_refno             IN   questions.que_refno%TYPE                            DEFAULT NULL,
                                                     p_gqre_assm_refno       IN   generic_question_responses.gqre_assm_refno%TYPE     DEFAULT NULL,
                                                     p_gqre_gque_reference   IN   generic_question_responses.gqre_gque_reference%TYPE DEFAULT NULL, 
                                                     p_gqre_created_by       IN   generic_question_responses.gqre_created_by%TYPE     DEFAULT NULL,
                                                     p_gqre_created_date     IN   generic_question_responses.gqre_created_date%TYPE   DEFAULT NULL)
IS
   l_answer   generic_question_responses.gqre_text_value%TYPE;
   
   CURSOR C_ANSWER_VALUE IS
      SELECT gan.gan_char_value || DECODE(gan.gan_comments, NULL, '', ' ') || gan.gan_comments
        FROM general_answers gan
       INNER JOIN questions que ON que.que_refno = gan.gan_que_refno
       WHERE gan.gan_app_refno = p_app_refno
         AND gan.gan_que_refno = p_que_refno;
BEGIN
   OPEN C_ANSWER_VALUE;
   FETCH C_ANSWER_VALUE INTO l_answer;
   CLOSE C_ANSWER_VALUE;
   
   create_gqre(p_gqre_assm_refno => p_gqre_assm_refno,
               p_gqre_gque_reference => p_gqre_gque_reference,
               p_gqre_text_value => l_answer,
               p_gqre_created_by => p_gqre_created_by,
               p_gqre_created_date => p_gqre_created_date);
END generate_answer_comments;
/

CREATE OR REPLACE PROCEDURE create_gqre_from_app(p_app_refno             IN   applications.app_refno%TYPE                         DEFAULT NULL,
                                                 p_que_refno             IN   questions.que_refno%TYPE                            DEFAULT NULL,
                                                 p_gqre_assm_refno       IN   generic_question_responses.gqre_assm_refno%TYPE     DEFAULT NULL,
                                                 p_gqre_gque_reference   IN   generic_question_responses.gqre_gque_reference%TYPE DEFAULT NULL, 
                                                 p_gqre_created_by       IN   generic_question_responses.gqre_created_by%TYPE     DEFAULT NULL,
                                                 p_gqre_created_date     IN   generic_question_responses.gqre_created_date%TYPE   DEFAULT NULL)
IS
   CURSOR C_ANSWER_INFO IS
      SELECT que.que_datatype,
             gan.gan_date_value,
             gan.gan_number_value,
             gan.gan_char_value,
             gan.gan_comments,
             gan.gan_que_refno
        FROM general_answers gan
       INNER JOIN questions que ON que.que_refno = gan.gan_que_refno
       WHERE gan.gan_app_refno = p_app_refno
         AND gan.gan_que_refno = p_que_refno;
   
   l_answer_info   C_ANSWER_INFO%ROWTYPE;
BEGIN
   OPEN C_ANSWER_INFO;
   FETCH C_ANSWER_INFO INTO l_answer_info;
   CLOSE C_ANSWER_INFO;
   
   create_gqre(p_gqre_assm_refno => p_gqre_assm_refno,
               p_gqre_gque_reference => p_gqre_gque_reference,
               p_gqre_boolean_value => CASE l_answer_info.que_datatype
                                          WHEN 'Y' THEN l_answer_info.gan_char_value
                                          ELSE NULL
                                       END,
               p_gqre_date_value => CASE l_answer_info.que_datatype
                                       WHEN 'D' THEN l_answer_info.gan_date_value
                                       ELSE NULL
                                    END,
               p_gqre_numeric_value => CASE l_answer_info.que_datatype
                                          WHEN 'N' THEN l_answer_info.gan_number_value
                                          ELSE NULL
                                       END,
               p_gqre_text_value => CASE 
                                       WHEN l_answer_info.que_datatype = 'C' AND l_answer_info.gan_char_value = 'COMMENT' THEN l_answer_info.gan_comments
                                       ELSE NULL
                                    END,
               p_gqre_gcre_code => CASE 
                                      WHEN l_answer_info.que_datatype = 'C' AND l_answer_info.gan_char_value != 'COMMENT' THEN l_answer_info.gan_char_value
                                      ELSE NULL
                                   END,
               p_gqre_created_by => p_gqre_created_by,
               p_gqre_created_date => p_gqre_created_date);
END create_gqre_from_app;
/

CREATE OR REPLACE PROCEDURE process_approval_questions(p_assm_refno               IN   assessments.assm_refno%TYPE,
                                                       p_als_sco_code             IN   applic_list_stage_decisions.als_sco_code%TYPE,
                                                       p_als_hrv_sdr_code         IN   applic_list_stage_decisions.als_hrv_sdr_code%TYPE,
                                                       p_als_rsd_hrv_lsd_code     IN   applic_list_stage_decisions.als_rsd_hrv_lsd_code%TYPE,
                                                       p_als_comments             IN   applic_list_stage_decisions.als_comments%TYPE,
                                                       p_als_decision_date        IN   applic_list_stage_decisions.als_decision_date%TYPE,
                                                       p_als_created_by           IN   applic_list_stage_decisions.als_created_by%TYPE,
                                                       p_als_created_date         IN   applic_list_stage_decisions.als_created_date%TYPE,
                                                       p_als_auth_by              IN   applic_list_stage_decisions.als_authorised_by%TYPE,
                                                       p_als_auth_date            IN   applic_list_stage_decisions.als_authorised_date%TYPE)
IS

BEGIN
   IF p_als_sco_code != 'AUT'
   THEN                                 
      create_gqre(p_gqre_assm_refno => p_assm_refno,
                  p_gqre_gque_reference => 294, 
                  p_gqre_boolean_value => 'Y',
                  p_gqre_created_by => p_als_created_by,
                  p_gqre_created_date => p_als_created_date);
   END IF;
   
   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 196, 
               p_gqre_gcre_code => p_als_hrv_sdr_code,
               p_gqre_created_by => NVL(p_als_auth_by, p_als_created_by),
               p_gqre_created_date => NVL(p_als_auth_date, p_als_created_date));
               
   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 198, 
               p_gqre_gcre_code => p_als_rsd_hrv_lsd_code,
               p_gqre_created_by => NVL(p_als_auth_by, p_als_created_by),
               p_gqre_created_date => NVL(p_als_auth_date, p_als_created_date));
   
   IF p_als_comments IS NOT NULL
   THEN
      create_gqre(p_gqre_assm_refno => p_assm_refno,
                  p_gqre_gque_reference => 200, 
                  p_gqre_text_value => p_als_comments,
                  p_gqre_created_by => NVL(p_als_auth_by, p_als_created_by),
                  p_gqre_created_date => NVL(p_als_auth_date, p_als_created_date));
   END IF;

   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 201, 
               p_gqre_date_value => p_als_decision_date,
               p_gqre_created_by => NVL(p_als_auth_by, p_als_created_by),
               p_gqre_created_date => NVL(p_als_auth_date, p_als_created_date));
END process_approval_questions;
/

CREATE OR REPLACE PROCEDURE process_apprtr_questions(p_assm_refno               IN   assessments.assm_refno%TYPE,
                                                     p_als_sco_code             IN   applic_list_stage_decisions.als_sco_code%TYPE,
                                                     p_als_hrv_sdr_code         IN   applic_list_stage_decisions.als_hrv_sdr_code%TYPE,
                                                     p_als_rsd_hrv_lsd_code     IN   applic_list_stage_decisions.als_rsd_hrv_lsd_code%TYPE,
                                                     p_als_comments             IN   applic_list_stage_decisions.als_comments%TYPE,
                                                     p_als_decision_date        IN   applic_list_stage_decisions.als_decision_date%TYPE,
                                                     p_als_created_by           IN   applic_list_stage_decisions.als_created_by%TYPE,
                                                     p_als_created_date         IN   applic_list_stage_decisions.als_created_date%TYPE,
                                                     p_als_auth_by              IN   applic_list_stage_decisions.als_authorised_by%TYPE,
                                                     p_als_auth_date            IN   applic_list_stage_decisions.als_authorised_date%TYPE)
IS

BEGIN
   IF p_als_sco_code != 'AUT'
   THEN                                 
      create_gqre(p_gqre_assm_refno => p_assm_refno,
                  p_gqre_gque_reference => 294, 
                  p_gqre_boolean_value => 'Y',
                  p_gqre_created_by => p_als_created_by,
                  p_gqre_created_date => p_als_created_date);
   END IF;

   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 196, 
               p_gqre_gcre_code => p_als_hrv_sdr_code,
               p_gqre_created_by => NVL(p_als_auth_by, p_als_created_by),
               p_gqre_created_date => NVL(p_als_auth_date, p_als_created_date));
               
   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 295, 
               p_gqre_gcre_code => p_als_rsd_hrv_lsd_code,
               p_gqre_created_by => NVL(p_als_auth_by, p_als_created_by),
               p_gqre_created_date => NVL(p_als_auth_date, p_als_created_date));
               
   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 201, 
               p_gqre_date_value => p_als_decision_date,
               p_gqre_created_by => NVL(p_als_auth_by, p_als_created_by),
               p_gqre_created_date => NVL(p_als_auth_date, p_als_created_date));
               
   IF p_als_comments IS NOT NULL
   THEN
      create_gqre(p_gqre_assm_refno => p_assm_refno,
                  p_gqre_gque_reference => 200, 
                  p_gqre_text_value => p_als_comments,
                  p_gqre_created_by => NVL(p_als_auth_by, p_als_created_by),
                  p_gqre_created_date => NVL(p_als_auth_date, p_als_created_date));
   END IF;   
END process_apprtr_questions;
/

CREATE OR REPLACE PROCEDURE process_hna_questions(p_app_refno                IN   applications.app_refno%TYPE,
                                                  p_assm_refno               IN   assessments.assm_refno%TYPE,
                                                  p_als_sco_code             IN   applic_list_stage_decisions.als_sco_code%TYPE,
                                                  p_als_hrv_sdr_code         IN   applic_list_stage_decisions.als_hrv_sdr_code%TYPE,
                                                  p_als_rsd_hrv_lsd_code     IN   applic_list_stage_decisions.als_rsd_hrv_lsd_code%TYPE,
                                                  p_als_comments             IN   applic_list_stage_decisions.als_comments%TYPE,
                                                  p_als_created_by           IN   applic_list_stage_decisions.als_created_by%TYPE,
                                                  p_als_created_date         IN   applic_list_stage_decisions.als_created_date%TYPE,
                                                  p_als_auth_by              IN   applic_list_stage_decisions.als_authorised_by%TYPE,
                                                  p_als_auth_date            IN   applic_list_stage_decisions.als_authorised_date%TYPE)
                                       
IS

BEGIN
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 91,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 241,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                                    
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 101,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 285,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
   
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 149,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 243,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 96,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 284,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 107,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 286,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 143,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 242,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 404,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 269,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 41,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 245,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 411,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 270,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 54,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 246,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 66,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 248,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 74,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 250,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
   
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 58,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 247,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
   
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 70,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 249,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 136,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 261,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 82,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 251,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 90,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 253,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 98,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 255,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 86,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 252,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 94,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 254,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 140,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 262,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 416,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 271,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 148,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 263,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
   
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 425,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 272,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 434,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 274,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 447,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 276,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 430,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 273,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 438,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 275,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 451,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 277,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 172,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 287,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 179,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 264,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 183,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 265,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 187,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 266,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 202,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 268,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 194,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 267,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   generate_answer_comments(p_app_refno => p_app_refno,
                            p_que_refno => 821,
                            p_gqre_assm_refno => p_assm_refno,
                            p_gqre_gque_reference => 281,
                            p_gqre_created_by => p_als_created_by,
                            p_gqre_created_date => p_als_created_date);
                        
   generate_answer_comments(p_app_refno => p_app_refno,
                            p_que_refno => 826,
                            p_gqre_assm_refno => p_assm_refno,
                            p_gqre_gque_reference => 282,
                            p_gqre_created_by => p_als_created_by,
                            p_gqre_created_date => p_als_created_date);
   
   generate_answer_comments(p_app_refno => p_app_refno,
                            p_que_refno => 87,
                            p_gqre_assm_refno => p_assm_refno,
                            p_gqre_gque_reference => 283,
                            p_gqre_created_by => p_als_created_by,
                            p_gqre_created_date => p_als_created_date);
                        
   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 196, 
               p_gqre_gcre_code => p_als_hrv_sdr_code,
               p_gqre_created_by => p_als_created_by,
               p_gqre_created_date => p_als_created_date);
               
   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 195, 
               p_gqre_gcre_code => p_als_rsd_hrv_lsd_code,
               p_gqre_created_by => p_als_created_by,
               p_gqre_created_date => p_als_created_date);

   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 197, 
               p_gqre_text_value => p_als_comments,
               p_gqre_created_by => p_als_created_by,
               p_gqre_created_date => p_als_created_date);
               
   IF p_als_sco_code = 'AUT'
   THEN
      create_gqre(p_gqre_assm_refno => p_assm_refno,
                  p_gqre_gque_reference => 294, 
                  p_gqre_boolean_value => 'Y',
                  p_gqre_created_by => p_als_auth_by,
                  p_gqre_created_date => p_als_auth_date);
                  
      UPDATE generic_question_responses
         SET gqre_created_date = p_als_auth_date
       WHERE gqre_assm_refno = p_assm_refno
         AND gqre_gque_reference = 294;
   END IF;
END process_hna_questions;
/

CREATE OR REPLACE PROCEDURE process_hnatr_questions(p_app_refno                IN   applications.app_refno%TYPE,
                                                    p_assm_refno               IN   assessments.assm_refno%TYPE,
                                                    p_als_sco_code             IN   applic_list_stage_decisions.als_sco_code%TYPE,
                                                    p_als_hrv_sdr_code         IN   applic_list_stage_decisions.als_hrv_sdr_code%TYPE,
                                                    p_als_rsd_hrv_lsd_code     IN   applic_list_stage_decisions.als_rsd_hrv_lsd_code%TYPE,
                                                    p_als_comments             IN   applic_list_stage_decisions.als_comments%TYPE,
                                                    p_als_created_by           IN   applic_list_stage_decisions.als_created_by%TYPE,
                                                    p_als_created_date         IN   applic_list_stage_decisions.als_created_date%TYPE,
                                                    p_als_auth_by              IN   applic_list_stage_decisions.als_authorised_by%TYPE,
                                                    p_als_auth_date            IN   applic_list_stage_decisions.als_authorised_date%TYPE)
                                       
IS

BEGIN
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 411,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 270,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 54,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 246,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 66,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 248,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 74,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 250,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 58,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 247,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);                     
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 70,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 249,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 136,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 261,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 106,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 256,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 114,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 258,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 123,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 260,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 110,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 257,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 118,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 259,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 140,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 262,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 416,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 271,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 148,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 263,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 179,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 264,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 183,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 265,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 187,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 266,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 202,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 268,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   create_gqre_from_app(p_app_refno => p_app_refno,
                        p_que_refno => 194,
                        p_gqre_assm_refno => p_assm_refno,
                        p_gqre_gque_reference => 267,
                        p_gqre_created_by => p_als_created_by,
                        p_gqre_created_date => p_als_created_date);
                        
   generate_answer_comments(p_app_refno => p_app_refno,
                            p_que_refno => 821,
                            p_gqre_assm_refno => p_assm_refno,
                            p_gqre_gque_reference => 281,
                            p_gqre_created_by => p_als_created_by,
                            p_gqre_created_date => p_als_created_date);
                        
   generate_answer_comments(p_app_refno => p_app_refno,
                            p_que_refno => 826,
                            p_gqre_assm_refno => p_assm_refno,
                            p_gqre_gque_reference => 282,
                            p_gqre_created_by => p_als_created_by,
                            p_gqre_created_date => p_als_created_date);
                        
   generate_answer_comments(p_app_refno => p_app_refno,
                            p_que_refno => 87,
                            p_gqre_assm_refno => p_assm_refno,
                            p_gqre_gque_reference => 283,
                            p_gqre_created_by => p_als_created_by,
                            p_gqre_created_date => p_als_created_date);
                        
   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 196, 
               p_gqre_gcre_code => p_als_hrv_sdr_code,
               p_gqre_created_by => p_als_created_by,
               p_gqre_created_date => p_als_created_date);
               
   create_gqre(p_gqre_assm_refno => p_assm_refno,
               p_gqre_gque_reference => 311, 
               p_gqre_gcre_code => p_als_rsd_hrv_lsd_code,
               p_gqre_created_by => p_als_created_by,
               p_gqre_created_date => p_als_created_date);

   IF p_als_comments IS NOT NULL
   THEN
      create_gqre(p_gqre_assm_refno => p_assm_refno,
                  p_gqre_gque_reference => 197, 
                  p_gqre_text_value => p_als_comments,
                  p_gqre_created_by => p_als_created_by,
                  p_gqre_created_date => p_als_created_date);
   END IF;        

   IF p_als_sco_code = 'AUT'
   THEN
      create_gqre(p_gqre_assm_refno => p_assm_refno,
                  p_gqre_gque_reference => 294, 
                  p_gqre_boolean_value => 'Y',
                  p_gqre_created_by => p_als_auth_by,
                  p_gqre_created_date => p_als_auth_date);
      
      UPDATE generic_question_responses
         SET gqre_created_date = p_als_auth_date
       WHERE gqre_assm_refno = p_assm_refno
         AND gqre_gque_reference = 294;
   END IF;
END process_hnatr_questions;
/

CREATE OR REPLACE PROCEDURE delete_resp_reviewed_assm(p_assm_refno   IN   assessments.assm_refno%TYPE)
IS

BEGIN
   DELETE
     FROM generic_question_responses
    WHERE gqre_gque_reference in (294, 195, 196, 197, 198, 200, 201, 294, 295, 311)
      AND gqre_assm_refno = (SELECT assm_refno
                               FROM assessments
                              WHERE assm_assm_refno = p_assm_refno);
END delete_resp_reviewed_assm;
/

CREATE OR REPLACE PROCEDURE update_assessment(p_assm_refno         IN   assessments.assm_refno%TYPE,
                                              p_assm_status_date   IN   assessments.assm_status_date%TYPE DEFAULT NULL,
                                              p_assm_created_date  IN   assessments.assm_created_date%TYPE DEFAULT NULL,
                                              p_assm_modified_date IN   assessments.assm_modified_date%TYPE DEFAULT NULL,
                                              p_assm_modified_by   IN   assessments.assm_modified_by%TYPE DEFAULT NULL)
IS

BEGIN
   UPDATE assessments
      SET assm_status_date = NVL(p_assm_status_date, assm_status_date),
          assm_created_date = NVL(p_assm_created_date, assm_created_date),
          assm_modified_date = NVL(p_assm_modified_date, assm_modified_date),
          assm_modified_by = NVL(p_assm_modified_by, assm_modified_by)
    WHERE assm_refno = p_assm_refno;
END update_assessment;
/

DECLARE
   CURSOR C_PARTIES_FOR_NEW_ASSESS IS
      SELECT DISTINCT ipa.ipa_par_refno par_refno,
             ipa.ipa_app_refno app_refno,
             REPLACE(app.app_aun_code, 'A-', '') app_aun_code
        FROM applications app
        LEFT JOIN (SELECT app_refno, 
                          'Y' tr_exists_ind
                     FROM applications
                    WHERE EXISTS (SELECT NULL
                                    FROM applic_list_entries 
                                   WHERE ale_rli_code = 'TR' 
                                     AND ale_app_refno = app_refno)) apptr ON apptr.app_refno = app.app_refno
       INNER JOIN applic_list_stage_decisions als ON als.als_ale_app_refno = app.app_refno
       INNER JOIN involved_parties ipa ON ipa.ipa_app_refno = app.app_refno
                                      AND ipa.ipa_main_applicant_ind = 'Y'
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1);
                                      
   CURSOR C_STAGE_DECISION_COUNT(p_par_refno   NUMBER,
                                 p_app_refno   NUMBER) IS
      SELECT COUNT(*)
        FROM applications app
        LEFT JOIN (SELECT app_refno, 
                          'Y' tr_exists_ind
                     FROM applications
                    WHERE EXISTS (SELECT NULL
                                    FROM applic_list_entries 
                                   WHERE ale_rli_code = 'TR' 
                                     AND ale_app_refno = app_refno)) apptr ON apptr.app_refno = app.app_refno
       INNER JOIN applic_list_stage_decisions als ON als.als_ale_app_refno = app.app_refno
       INNER JOIN involved_parties ipa ON ipa.ipa_app_refno = app.app_refno
                                      AND ipa.ipa_par_refno = p_par_refno
                                      AND ipa.ipa_main_applicant_ind = 'Y'
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1)
       WHERE app.app_refno = p_app_refno; 
                                      
   CURSOR C_LAST_ALS_INFO(p_par_refno   NUMBER,
                          p_app_refno   NUMBER) IS
      SELECT als.als_rls_code,
             als.als_sco_code,
             als.als_rsd_hrv_lsd_code outcome,
             als.als_hrv_sdr_code reason,
             als.als_decision_date decision_date,
             als.als_comments,
             app.app_aun_code,
             als.als_created_by,
             als.als_created_date,
             als.als_modified_by,
             als.als_modified_date,
             als.als_authorised_by,
             als.als_authorised_date
        FROM applications app
        LEFT JOIN (SELECT app_refno, 
                          'Y' tr_exists_ind
                     FROM applications
                    WHERE EXISTS (SELECT NULL
                                    FROM applic_list_entries 
                                   WHERE ale_rli_code = 'TR' 
                                     AND ale_app_refno = app_refno)) apptr ON apptr.app_refno = app.app_refno
       INNER JOIN applic_list_stage_decisions als ON als.als_ale_app_refno = app.app_refno
       INNER JOIN involved_parties ipa ON ipa.ipa_app_refno = app.app_refno
                                      AND ipa.ipa_par_refno = p_par_refno
                                      AND ipa.ipa_main_applicant_ind = 'Y'
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1)
       WHERE app.app_refno = p_app_refno
       ORDER BY NVL(als_modified_date, als_created_date) DESC; 
       
   CURSOR C_FIRST_ALS_INFO(p_par_refno   NUMBER,
                           p_app_refno   NUMBER) IS
      SELECT als.als_rls_code,
             als.als_sco_code,
             als.als_rsd_hrv_lsd_code outcome,
             als.als_hrv_sdr_code reason,
             als.als_decision_date decision_date,
             als.als_status_date,
             als.als_comments,
             app.app_aun_code,
             als.als_created_by,
             als.als_created_date,
             als.als_modified_by,
             als.als_modified_date,
             als.als_authorised_by,
             als.als_authorised_date
        FROM applications app
        LEFT JOIN (SELECT app_refno, 
                          'Y' tr_exists_ind
                     FROM applications
                    WHERE EXISTS (SELECT NULL
                                    FROM applic_list_entries 
                                   WHERE ale_rli_code = 'TR' 
                                     AND ale_app_refno = app_refno)) apptr ON apptr.app_refno = app.app_refno
       INNER JOIN applic_list_stage_decisions als ON als.als_ale_app_refno = app.app_refno
       INNER JOIN involved_parties ipa ON ipa.ipa_app_refno = app.app_refno
                                      AND ipa.ipa_par_refno = p_par_refno
                                      AND ipa.ipa_main_applicant_ind = 'Y'
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1)
       WHERE app.app_refno = p_app_refno
       ORDER BY NVL(als_modified_date, als_created_date); 
  
   l_count         NUMBER(10);
   l_als_info      C_LAST_ALS_INFO%ROWTYPE;
   l_als_info_clo  C_FIRST_ALS_INFO%ROWTYPE;
   l_assm_refno    assessments.assm_refno%TYPE;
   
BEGIN
   FOR l_curr_party IN C_PARTIES_FOR_NEW_ASSESS
   LOOP
      l_count := 0;
      
      OPEN C_STAGE_DECISION_COUNT(l_curr_party.par_refno,
                                  l_curr_party.app_refno);
      FETCH C_STAGE_DECISION_COUNT INTO l_count;
      CLOSE C_STAGE_DECISION_COUNT;
      
      OPEN C_LAST_ALS_INFO(l_curr_party.par_refno,
                           l_curr_party.app_refno);
      FETCH C_LAST_ALS_INFO INTO l_als_info;
      CLOSE C_LAST_ALS_INFO;
      
      IF l_count < 3
      THEN         
         l_assm_refno := assm_refno_seq.NEXTVAL;
         
         IF l_count = 1
         THEN
            dbms_output.put_line('New assessment reference: ' || l_assm_refno);
            
            IF l_als_info.als_rls_code = 'APPROVAL'
            THEN
               IF l_als_info.als_sco_code = 'AUT'
               THEN
                  create_assessment(p_assm_refno => l_assm_refno,
                                    p_assm_asst_code => 'HNA',
                                    p_assm_sequence => 1,
                                    p_assm_assessment_date => l_als_info.decision_date,
                                    p_assm_sco_code => 'CLO',
                                    p_assm_ato_code => l_als_info.outcome,
                                    p_assm_ipp_refno => 96,
                                    p_assm_par_refno => l_curr_party.par_refno,
                                    p_assm_aun_code => l_curr_party.app_aun_code,
                                    p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                    p_assm_created_by => l_als_info.als_created_by,
                                    p_assm_created_date => l_als_info.als_created_date,
                                    p_assm_modified_by => l_als_info.als_authorised_by,
                                    p_assm_modified_date => l_als_info.als_authorised_date,
                                    p_src_app_refno => l_curr_party.app_refno);
                  
                  process_approval_questions(p_assm_refno => l_assm_refno,
                                             p_als_sco_code => l_als_info.als_sco_code,
                                             p_als_hrv_sdr_code => l_als_info.reason,
                                             p_als_rsd_hrv_lsd_code => l_als_info.outcome,
                                             p_als_comments => l_als_info.als_comments,
                                             p_als_decision_date => l_als_info.decision_date,
                                             p_als_created_by => l_als_info.als_created_by,
                                             p_als_created_date => l_als_info.als_created_date,
                                             p_als_auth_by => l_als_info.als_authorised_by,
                                             p_als_auth_date => l_als_info.als_authorised_date);
                                          
                  a_assessments.review_assessment(l_assm_refno, l_als_info.als_authorised_date, FALSE);
                  
                  delete_resp_reviewed_assm(l_assm_refno);
               ELSE
                  create_assessment(p_assm_refno => l_assm_refno,
                                    p_assm_asst_code => 'HNA',
                                    p_assm_sequence => 1,
                                    p_assm_assessment_date => l_als_info.decision_date,
                                    p_assm_sco_code => 'PEN',
                                    p_assm_ato_code => l_als_info.outcome,
                                    p_assm_ipp_refno => 96,
                                    p_assm_par_refno => l_curr_party.par_refno,
                                    p_assm_aun_code => l_curr_party.app_aun_code,
                                    p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                    p_assm_created_by => l_als_info.als_created_by,
                                    p_assm_created_date => l_als_info.als_created_date,
                                    p_assm_modified_by => l_als_info.als_authorised_by,
                                    p_assm_modified_date => l_als_info.als_authorised_date,
                                    p_src_app_refno => l_curr_party.app_refno);
               END IF;
             
            ELSIF l_als_info.als_rls_code = 'HNA'
            THEN
               create_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_asst_code => 'HNA',
                                 p_assm_sequence => 1,
                                 p_assm_assessment_date => l_als_info.decision_date,
                                 p_assm_sco_code => 'PEN',
                                 p_assm_ato_code => NULL,
                                 p_assm_ipp_refno => 96,
                                 p_assm_par_refno => l_curr_party.par_refno,
                                 p_assm_aun_code => l_curr_party.app_aun_code,
                                 p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                 p_assm_created_by => l_als_info.als_created_by,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_by => l_als_info.als_modified_by,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_src_app_refno => l_curr_party.app_refno);
               
               process_hna_questions(p_app_refno => l_curr_party.app_refno,
                                     p_assm_refno => l_assm_refno,
                                     p_als_sco_code => l_als_info.als_sco_code,
                                     p_als_hrv_sdr_code => l_als_info.reason,
                                     p_als_rsd_hrv_lsd_code => l_als_info.outcome,
                                     p_als_comments => l_als_info.als_comments,
                                     p_als_created_by => l_als_info.als_created_by,
                                     p_als_created_date => l_als_info.als_created_date,
                                     p_als_auth_by => l_als_info.als_authorised_by,
                                     p_als_auth_date => l_als_info.als_authorised_date);
                                     
            ELSIF l_als_info.als_rls_code = 'APPRTR'
            THEN
               IF l_als_info.als_sco_code = 'AUT'
               THEN
                  create_assessment(p_assm_refno => l_assm_refno,
                                    p_assm_asst_code => 'TRHNA',
                                    p_assm_sequence => 1,
                                    p_assm_assessment_date => l_als_info.decision_date,
                                    p_assm_sco_code => 'CLO',
                                    p_assm_ato_code => l_als_info.outcome,
                                    p_assm_ipp_refno => 90,
                                    p_assm_par_refno => l_curr_party.par_refno,
                                    p_assm_aun_code => l_curr_party.app_aun_code,
                                    p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                    p_assm_created_by => l_als_info.als_created_by,
                                    p_assm_created_date => l_als_info.als_created_date,
                                    p_assm_modified_by => l_als_info.als_authorised_by,
                                    p_assm_modified_date => l_als_info.als_authorised_date,
                                    p_src_app_refno => l_curr_party.app_refno);
                                    
                  process_apprtr_questions(p_assm_refno => l_assm_refno,
                                           p_als_sco_code => l_als_info.als_sco_code,
                                           p_als_hrv_sdr_code => l_als_info.reason,
                                           p_als_rsd_hrv_lsd_code => l_als_info.outcome,
                                           p_als_comments => l_als_info.als_comments,
                                           p_als_decision_date => l_als_info.decision_date,
                                           p_als_created_by => l_als_info.als_created_by,
                                           p_als_created_date => l_als_info.als_created_date,
                                           p_als_auth_by => l_als_info.als_authorised_by,
                                           p_als_auth_date => l_als_info.als_authorised_date);
                  
                  a_assessments.review_assessment(l_assm_refno, l_als_info.als_authorised_date, FALSE);
                  
                  delete_resp_reviewed_assm(l_assm_refno);
               ELSE
                  create_assessment(p_assm_refno => l_assm_refno,
                                    p_assm_asst_code => 'TRHNA',
                                    p_assm_sequence => 1,
                                    p_assm_assessment_date => l_als_info.decision_date,
                                    p_assm_sco_code => 'PEN',
                                    p_assm_ato_code => l_als_info.outcome,
                                    p_assm_ipp_refno => 90,
                                    p_assm_par_refno => l_curr_party.par_refno,
                                    p_assm_aun_code => l_curr_party.app_aun_code,
                                    p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                    p_assm_created_by => l_als_info.als_created_by,
                                    p_assm_created_date => l_als_info.als_created_date,
                                    p_assm_modified_by => l_als_info.als_authorised_by,
                                    p_assm_modified_date => l_als_info.als_authorised_date,
                                    p_src_app_refno => l_curr_party.app_refno);
               END IF;
            ELSIF l_als_info.als_rls_code = 'HNATR'
            THEN
               create_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_asst_code => 'TRHNA',
                                 p_assm_sequence => 1,
                                 p_assm_assessment_date => l_als_info.decision_date,
                                 p_assm_sco_code => 'PEN',
                                 p_assm_ato_code => NULL,
                                 p_assm_ipp_refno => 90,
                                 p_assm_par_refno => l_curr_party.par_refno,
                                 p_assm_aun_code => l_curr_party.app_aun_code,
                                 p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                 p_assm_created_by => l_als_info.als_created_by,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_by => l_als_info.als_modified_by,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_src_app_refno => l_curr_party.app_refno);
                                 
               process_hnatr_questions(p_app_refno => l_curr_party.app_refno,
                                       p_assm_refno => l_assm_refno,
                                       p_als_sco_code => l_als_info.als_sco_code,
                                       p_als_hrv_sdr_code => l_als_info.reason,
                                       p_als_rsd_hrv_lsd_code => l_als_info.outcome,
                                       p_als_comments => l_als_info.als_comments,
                                       p_als_created_by => l_als_info.als_created_by,
                                       p_als_created_date => l_als_info.als_created_date,
                                       p_als_auth_by => l_als_info.als_authorised_by,
                                       p_als_auth_date => l_als_info.als_authorised_date);
            END IF;
         ELSIF l_count = 2
         THEN
            IF l_als_info.als_rls_code = 'APPROVAL'
            THEN
               create_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_asst_code => 'HNA',
                                 p_assm_sequence => 1,
                                 p_assm_assessment_date => l_als_info.decision_date,
                                 p_assm_sco_code => CASE l_als_info.als_sco_code
                                                       WHEN 'PEN' THEN 'PEN'
                                                       WHEN 'AUT' THEN 'CLO'
                                                    END,
                                 p_assm_ato_code => l_als_info.outcome,
                                 p_assm_ipp_refno => 96,
                                 p_assm_par_refno => l_curr_party.par_refno,
                                 p_assm_aun_code => l_curr_party.app_aun_code,
                                 p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                 p_assm_created_by => l_als_info.als_created_by,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_by => NVL(l_als_info.als_modified_by, l_als_info.als_created_by),
                                 p_assm_modified_date => NVL(l_als_info.als_modified_date, l_als_info.als_created_date),
                                 p_src_app_refno => l_curr_party.app_refno);
                                 
               process_hna_questions(p_app_refno => l_curr_party.app_refno,
                                     p_assm_refno => l_assm_refno,
                                     p_als_sco_code => l_als_info.als_sco_code,
                                     p_als_hrv_sdr_code => l_als_info.reason,
                                     p_als_rsd_hrv_lsd_code => l_als_info.outcome,
                                     p_als_comments => l_als_info.als_comments,
                                     p_als_created_by => l_als_info.als_created_by,
                                     p_als_created_date => l_als_info.als_created_date,
                                     p_als_auth_by => l_als_info.als_authorised_by,
                                     p_als_auth_date => l_als_info.als_authorised_date);
               
               create_gqre(p_gqre_assm_refno => l_assm_refno,
                           p_gqre_gque_reference => 198, 
                           p_gqre_gcre_code => l_als_info.outcome,
                           p_gqre_created_by => NVL(l_als_info.als_authorised_by, l_als_info.als_created_by),
                           p_gqre_created_date => NVL(l_als_info.als_authorised_date, l_als_info.als_created_date));
               
               IF l_als_info.als_comments IS NOT NULL
               THEN
                  create_gqre(p_gqre_assm_refno => l_assm_refno,
                              p_gqre_gque_reference => 200, 
                              p_gqre_text_value => l_als_info.als_comments,
                              p_gqre_created_by => NVL(l_als_info.als_authorised_by, l_als_info.als_created_by),
                              p_gqre_created_date => NVL(l_als_info.als_authorised_date, l_als_info.als_created_date));
               END IF;

               create_gqre(p_gqre_assm_refno => l_assm_refno,
                           p_gqre_gque_reference => 201, 
                           p_gqre_date_value => l_als_info.decision_date,
                           p_gqre_created_by => NVL(l_als_info.als_authorised_by, l_als_info.als_created_by),
                           p_gqre_created_date => NVL(l_als_info.als_authorised_date, l_als_info.als_created_date));
            
               a_assessments.review_assessment(l_assm_refno, NVL(l_als_info.als_modified_date, l_als_info.als_created_date), FALSE);
               
               delete_resp_reviewed_assm(l_assm_refno);
            ELSIF l_als_info.als_rls_code = 'HNA'
            THEN
               OPEN C_FIRST_ALS_INFO(l_curr_party.par_refno,
                                     l_curr_party.app_refno);
               FETCH C_FIRST_ALS_INFO INTO l_als_info_clo;
               CLOSE C_FIRST_ALS_INFO;
               
               create_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_asst_code => 'HNA',
                                 p_assm_sequence => 1,
                                 p_assm_assessment_date => l_als_info_clo.decision_date,
                                 p_assm_sco_code => 'CLO',
                                 p_assm_ato_code => l_als_info_clo.outcome,
                                 p_assm_ipp_refno => 96,
                                 p_assm_par_refno => l_curr_party.par_refno,
                                 p_assm_aun_code => l_curr_party.app_aun_code,
                                 p_assm_status_date => NVL(l_als_info_clo.als_status_date, l_als_info_clo.als_created_date),
                                 p_assm_created_by => l_als_info_clo.als_created_by,
                                 p_assm_created_date => l_als_info_clo.als_created_date,
                                 p_assm_modified_by => l_als_info_clo.als_authorised_by,
                                 p_assm_modified_date => l_als_info_clo.als_authorised_date,
                                 p_src_app_refno => l_curr_party.app_refno);
                                 
               process_approval_questions(p_assm_refno => l_assm_refno,
                                          p_als_sco_code => l_als_info_clo.als_sco_code,
                                          p_als_hrv_sdr_code => l_als_info_clo.reason,
                                          p_als_rsd_hrv_lsd_code => l_als_info_clo.outcome,
                                          p_als_comments => l_als_info_clo.als_comments,
                                          p_als_decision_date => l_als_info_clo.decision_date,
                                          p_als_created_by => l_als_info_clo.als_created_by,
                                          p_als_created_date => l_als_info_clo.als_created_date,
                                          p_als_auth_by => l_als_info_clo.als_authorised_by,
                                          p_als_auth_date => l_als_info_clo.als_authorised_date);
                           
               l_assm_refno := assm_refno_seq.NEXTVAL;
               
               create_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_asst_code => 'HNA',
                                 p_assm_sequence => 2,
                                 p_assm_assessment_date => l_als_info.decision_date,
                                 p_assm_sco_code => 'PEN',
                                 p_assm_ato_code => NULL,
                                 p_assm_ipp_refno => 96,
                                 p_assm_par_refno => l_curr_party.par_refno,
                                 p_assm_aun_code => l_curr_party.app_aun_code,
                                 p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                 p_assm_created_by => l_als_info.als_created_by,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_by => l_als_info.als_modified_by,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_src_app_refno => l_curr_party.app_refno);
                                 
               process_hna_questions(p_app_refno => l_curr_party.app_refno,
                                     p_assm_refno => l_assm_refno,
                                     p_als_sco_code => l_als_info.als_sco_code,
                                     p_als_hrv_sdr_code => l_als_info.reason,
                                     p_als_rsd_hrv_lsd_code => l_als_info.outcome,
                                     p_als_comments => l_als_info.als_comments,
                                     p_als_created_by => l_als_info.als_created_by,
                                     p_als_created_date => l_als_info.als_created_date,
                                     p_als_auth_by => l_als_info.als_authorised_by,
                                     p_als_auth_date => l_als_info.als_authorised_date);
            ELSIF l_als_info.als_rls_code = 'APPRTR'
            THEN
               create_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_asst_code => 'TRHNA',
                                 p_assm_sequence => 1,
                                 p_assm_assessment_date => l_als_info.decision_date,
                                 p_assm_sco_code => CASE l_als_info.als_sco_code
                                                       WHEN 'PEN' THEN 'PEN'
                                                       WHEN 'AUT' THEN 'CLO'
                                                    END,
                                 p_assm_ato_code => l_als_info.outcome,
                                 p_assm_ipp_refno => 90,
                                 p_assm_par_refno => l_curr_party.par_refno,
                                 p_assm_aun_code => l_curr_party.app_aun_code,
                                 p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                 p_assm_created_by => l_als_info.als_created_by,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_by => NVL(l_als_info.als_modified_by, l_als_info.als_created_by),
                                 p_assm_modified_date => NVL(l_als_info.als_modified_date, l_als_info.als_created_date),
                                 p_src_app_refno => l_curr_party.app_refno);
                                 
               process_hnatr_questions(p_app_refno => l_curr_party.app_refno,
                                       p_assm_refno => l_assm_refno,
                                       p_als_sco_code => l_als_info.als_sco_code,
                                       p_als_hrv_sdr_code => l_als_info.reason,
                                       p_als_rsd_hrv_lsd_code => l_als_info.outcome,
                                       p_als_comments => l_als_info.als_comments,
                                       p_als_created_by => l_als_info.als_created_by,
                                       p_als_created_date => l_als_info.als_created_date,
                                       p_als_auth_by => l_als_info.als_authorised_by,
                                       p_als_auth_date => l_als_info.als_authorised_date);
               
               create_gqre(p_gqre_assm_refno => l_assm_refno,
                           p_gqre_gque_reference => 198, 
                           p_gqre_gcre_code => l_als_info.outcome,
                           p_gqre_created_by => NVL(l_als_info.als_authorised_by, l_als_info.als_created_by),
                           p_gqre_created_date => NVL(l_als_info.als_authorised_date, l_als_info.als_created_date));
               
               IF l_als_info.als_comments IS NOT NULL
               THEN
                  create_gqre(p_gqre_assm_refno => l_assm_refno,
                              p_gqre_gque_reference => 200, 
                              p_gqre_text_value => l_als_info.als_comments,
                              p_gqre_created_by => NVL(l_als_info.als_authorised_by, l_als_info.als_created_by),
                              p_gqre_created_date => NVL(l_als_info.als_authorised_date, l_als_info.als_created_date));
               END IF;

               create_gqre(p_gqre_assm_refno => l_assm_refno,
                           p_gqre_gque_reference => 201, 
                           p_gqre_date_value => l_als_info.decision_date,
                           p_gqre_created_by => NVL(l_als_info.als_authorised_by, l_als_info.als_created_by),
                           p_gqre_created_date => NVL(l_als_info.als_authorised_date, l_als_info.als_created_date));
                           
               a_assessments.review_assessment(l_assm_refno, NVL(l_als_info.als_modified_date, l_als_info.als_created_date), FALSE);
               
               delete_resp_reviewed_assm(l_assm_refno);
            ELSIF l_als_info.als_rls_code = 'HNATR'
            THEN
               OPEN C_FIRST_ALS_INFO(l_curr_party.par_refno,
                                     l_curr_party.app_refno);
               FETCH C_FIRST_ALS_INFO INTO l_als_info_clo;
               CLOSE C_FIRST_ALS_INFO;
               
               create_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_asst_code => 'TRHNA',
                                 p_assm_sequence => 1,
                                 p_assm_assessment_date => l_als_info_clo.decision_date,
                                 p_assm_sco_code => 'CLO',
                                 p_assm_ato_code => l_als_info_clo.outcome,
                                 p_assm_ipp_refno => 90,
                                 p_assm_par_refno => l_curr_party.par_refno,
                                 p_assm_aun_code => l_curr_party.app_aun_code,
                                 p_assm_status_date => NVL(l_als_info_clo.als_status_date, l_als_info_clo.als_created_date),
                                 p_assm_created_by => l_als_info_clo.als_created_by,
                                 p_assm_created_date => l_als_info_clo.als_created_date,
                                 p_assm_modified_by => l_als_info_clo.als_authorised_by,
                                 p_assm_modified_date => l_als_info_clo.als_authorised_date,
                                 p_src_app_refno => l_curr_party.app_refno);
                                 
               process_apprtr_questions(p_assm_refno => l_assm_refno,
                                        p_als_sco_code => l_als_info_clo.als_sco_code,
                                        p_als_hrv_sdr_code => l_als_info_clo.reason,
                                        p_als_rsd_hrv_lsd_code => l_als_info_clo.outcome,
                                        p_als_comments => l_als_info_clo.als_comments,
                                        p_als_decision_date => l_als_info_clo.decision_date,
                                        p_als_created_by => l_als_info_clo.als_created_by,
                                        p_als_created_date => l_als_info_clo.als_created_date,
                                        p_als_auth_by => l_als_info_clo.als_authorised_by,
                                        p_als_auth_date => l_als_info_clo.als_authorised_date);   

               l_assm_refno := assm_refno_seq.NEXTVAL;
               
               create_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_asst_code => 'TRHNA',
                                 p_assm_sequence => 2,
                                 p_assm_assessment_date => l_als_info.decision_date,
                                 p_assm_sco_code => 'PEN',
                                 p_assm_ato_code => NULL,
                                 p_assm_ipp_refno => 90,
                                 p_assm_par_refno => l_curr_party.par_refno,
                                 p_assm_aun_code => l_curr_party.app_aun_code,
                                 p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                 p_assm_created_by => l_als_info.als_created_by,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_by => l_als_info.als_modified_by,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_src_app_refno => l_curr_party.app_refno);
                                 
               process_hnatr_questions(p_app_refno => l_curr_party.app_refno,
                                       p_assm_refno => l_assm_refno,
                                       p_als_sco_code => l_als_info.als_sco_code,
                                       p_als_hrv_sdr_code => l_als_info.reason,
                                       p_als_rsd_hrv_lsd_code => l_als_info.outcome,
                                       p_als_comments => l_als_info.als_comments,
                                       p_als_created_by => l_als_info.als_created_by,
                                       p_als_created_date => l_als_info.als_created_date,
                                       p_als_auth_by => l_als_info.als_authorised_by,
                                       p_als_auth_date => l_als_info.als_authorised_date);
            END IF;
         END IF;
      END IF;
   END LOOP;
EXCEPTION
WHEN OTHERS THEN
   dbms_output.put_line(SQLERRM);
END;
/

ALTER TRIGGER ASSM_BR_U DISABLE;

DECLARE
   CURSOR C_PARTIES_FOR_NEW_ASSESS IS
      SELECT DISTINCT ipa.ipa_par_refno par_refno,
             ipa.ipa_app_refno app_refno,
             REPLACE(app.app_aun_code, 'A-', '') app_aun_code
        FROM applications app
        LEFT JOIN (SELECT app_refno, 
                          'Y' tr_exists_ind
                     FROM applications
                    WHERE EXISTS (SELECT NULL
                                    FROM applic_list_entries 
                                   WHERE ale_rli_code = 'TR' 
                                     AND ale_app_refno = app_refno)) apptr ON apptr.app_refno = app.app_refno
       INNER JOIN applic_list_stage_decisions als ON als.als_ale_app_refno = app.app_refno
       INNER JOIN involved_parties ipa ON ipa.ipa_app_refno = app.app_refno
                                      AND ipa.ipa_main_applicant_ind = 'Y'
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1);
                                      
   CURSOR C_STAGE_DECISION_COUNT(p_par_refno   NUMBER,
                                 p_app_refno   NUMBER) IS
      SELECT COUNT(*)
        FROM applications app
        LEFT JOIN (SELECT app_refno, 
                          'Y' tr_exists_ind
                     FROM applications
                    WHERE EXISTS (SELECT NULL
                                    FROM applic_list_entries 
                                   WHERE ale_rli_code = 'TR' 
                                     AND ale_app_refno = app_refno)) apptr ON apptr.app_refno = app.app_refno
       INNER JOIN applic_list_stage_decisions als ON als.als_ale_app_refno = app.app_refno
       INNER JOIN involved_parties ipa ON ipa.ipa_app_refno = app.app_refno
                                      AND ipa.ipa_par_refno = p_par_refno
                                      AND ipa.ipa_main_applicant_ind = 'Y'
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1)
       WHERE app.app_refno = p_app_refno; 
                                      
   CURSOR C_LAST_ALS_INFO(p_par_refno   NUMBER,
                          p_app_refno   NUMBER) IS
      SELECT als.als_rls_code,
             als.als_sco_code,
             als.als_rsd_hrv_lsd_code outcome,
             als.als_hrv_sdr_code reason,
             als.als_decision_date decision_date,
             als.als_comments,
             app.app_aun_code,
             als.als_created_by,
             als.als_created_date,
             als.als_modified_by,
             als.als_modified_date,
             als.als_authorised_by,
             als.als_authorised_date
        FROM applications app
        LEFT JOIN (SELECT app_refno, 
                          'Y' tr_exists_ind
                     FROM applications
                    WHERE EXISTS (SELECT NULL
                                    FROM applic_list_entries 
                                   WHERE ale_rli_code = 'TR' 
                                     AND ale_app_refno = app_refno)) apptr ON apptr.app_refno = app.app_refno
       INNER JOIN applic_list_stage_decisions als ON als.als_ale_app_refno = app.app_refno
       INNER JOIN involved_parties ipa ON ipa.ipa_app_refno = app.app_refno
                                      AND ipa.ipa_par_refno = p_par_refno
                                      AND ipa.ipa_main_applicant_ind = 'Y'
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1)
       WHERE app.app_refno = p_app_refno
       ORDER BY NVL(als_modified_date, als_created_date) DESC; 
       
   CURSOR C_FIRST_ALS_INFO(p_par_refno   NUMBER,
                           p_app_refno   NUMBER) IS
      SELECT als.als_rls_code,
             als.als_sco_code,
             als.als_rsd_hrv_lsd_code outcome,
             als.als_hrv_sdr_code reason,
             als.als_decision_date decision_date,
             als.als_status_date,
             als.als_comments,
             app.app_aun_code,
             als.als_created_by,
             als.als_created_date,
             als.als_modified_by,
             als.als_modified_date,
             als.als_authorised_by,
             als.als_authorised_date
        FROM applications app
        LEFT JOIN (SELECT app_refno, 
                          'Y' tr_exists_ind
                     FROM applications
                    WHERE EXISTS (SELECT NULL
                                    FROM applic_list_entries 
                                   WHERE ale_rli_code = 'TR' 
                                     AND ale_app_refno = app_refno)) apptr ON apptr.app_refno = app.app_refno
       INNER JOIN applic_list_stage_decisions als ON als.als_ale_app_refno = app.app_refno
       INNER JOIN involved_parties ipa ON ipa.ipa_app_refno = app.app_refno
                                      AND ipa.ipa_par_refno = p_par_refno
                                      AND ipa.ipa_main_applicant_ind = 'Y'
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1)
       WHERE app.app_refno = p_app_refno
       ORDER BY NVL(als_modified_date, als_created_date); 
       
   CURSOR C_CURR_ASSM(p_app_refno       IN   NUMBER,
                      p_assm_sco_code   IN   VARCHAR2) IS
      SELECT assm_refno
        FROM assessments
       WHERE assm_comments = 'Source application: ' || p_app_refno
         AND assm_sco_code = p_assm_sco_code;
  
   l_count         NUMBER(10);
   l_als_info      C_LAST_ALS_INFO%ROWTYPE;
   l_als_info_clo  C_FIRST_ALS_INFO%ROWTYPE;
   l_assm_refno    assessments.assm_refno%TYPE;
   l_curr_status   applic_list_stage_decisions.als_sco_code%TYPE;
   
BEGIN
   FOR l_curr_party IN C_PARTIES_FOR_NEW_ASSESS
   LOOP
      l_count := 0;
      
      OPEN C_STAGE_DECISION_COUNT(l_curr_party.par_refno,
                                  l_curr_party.app_refno);
      FETCH C_STAGE_DECISION_COUNT INTO l_count;
      CLOSE C_STAGE_DECISION_COUNT;
      
      OPEN C_LAST_ALS_INFO(l_curr_party.par_refno,
                           l_curr_party.app_refno);
      FETCH C_LAST_ALS_INFO INTO l_als_info;
      CLOSE C_LAST_ALS_INFO;
      
      IF l_count < 3
      THEN         
         IF l_count = 1
         THEN
            
            IF l_als_info.als_rls_code = 'APPROVAL'
            THEN
               IF l_als_info.als_sco_code = 'AUT'
               THEN
                  OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                   'CLO');
                  FETCH C_CURR_ASSM INTO l_assm_refno;
                  CLOSE C_CURR_ASSM;
                 
                  update_assessment(p_assm_refno => l_assm_refno, 
                                    p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                    p_assm_created_date => l_als_info.als_created_date,
                                    p_assm_modified_date => l_als_info.als_authorised_date,
                                    p_assm_modified_by => l_als_info.als_authorised_by);
                 
               ELSE
                  OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                   'PEN');
                  FETCH C_CURR_ASSM INTO l_assm_refno;
                  CLOSE C_CURR_ASSM;
                  
                  update_assessment(p_assm_refno => l_assm_refno, 
                                    p_assm_created_date => l_als_info.als_created_date,
                                    p_assm_modified_date => l_als_info.als_authorised_date,
                                    p_assm_modified_by => l_als_info.als_authorised_by);
               END IF;
             
            ELSIF l_als_info.als_rls_code = 'HNA'
            THEN
               OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                'PEN');
               FETCH C_CURR_ASSM INTO l_assm_refno;
               CLOSE C_CURR_ASSM;
               
               update_assessment(p_assm_refno => l_assm_refno, 
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_assm_modified_by => l_als_info.als_modified_by);
            ELSIF l_als_info.als_rls_code = 'APPRTR'
            THEN
               IF l_als_info.als_sco_code = 'AUT'
               THEN
                  OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                   'CLO');
                  FETCH C_CURR_ASSM INTO l_assm_refno;
                  CLOSE C_CURR_ASSM;
                  
                  update_assessment(p_assm_refno => l_assm_refno,
                                    p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                    p_assm_created_date => l_als_info.als_created_date,
                                    p_assm_modified_date => l_als_info.als_authorised_date,
                                    p_assm_modified_by => l_als_info.als_authorised_by);
               ELSE
                  OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                   'PEN');
                  FETCH C_CURR_ASSM INTO l_assm_refno;
                  CLOSE C_CURR_ASSM;
                  
                  update_assessment(p_assm_refno => l_assm_refno,
                                    p_assm_created_date => l_als_info.als_created_date,
                                    p_assm_modified_date => l_als_info.als_authorised_date,
                                    p_assm_modified_by => l_als_info.als_authorised_by);
               END IF;
            ELSIF l_als_info.als_rls_code = 'HNATR'
            THEN
               OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                'PEN');
               FETCH C_CURR_ASSM INTO l_assm_refno;
               CLOSE C_CURR_ASSM;
               
               update_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_assm_modified_by => l_als_info.als_modified_by);
            END IF;
         ELSIF l_count = 2
         THEN
            IF l_als_info.als_rls_code = 'APPROVAL'
            THEN
               l_curr_status := CASE l_als_info.als_sco_code
                                    WHEN 'PEN' THEN 'PEN'
                                    WHEN 'AUT' THEN 'CLO'
                                 END;
               
               OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                l_curr_status);
               FETCH C_CURR_ASSM INTO l_assm_refno;
               CLOSE C_CURR_ASSM;
               
               update_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_status_date => NVL(l_als_info.als_status_date, l_als_info.als_created_date),
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_date => NVL(l_als_info.als_modified_date, l_als_info.als_created_date),
                                 p_assm_modified_by => NVL(l_als_info.als_modified_by, l_als_info.als_created_by));
            ELSIF l_als_info.als_rls_code = 'HNA'
            THEN
               OPEN C_FIRST_ALS_INFO(l_curr_party.par_refno,
                                     l_curr_party.app_refno);
               FETCH C_FIRST_ALS_INFO INTO l_als_info_clo;
               CLOSE C_FIRST_ALS_INFO;
               
               OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                'CLO');
               FETCH C_CURR_ASSM INTO l_assm_refno;
               CLOSE C_CURR_ASSM;
               
               update_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_status_date => NVL(l_als_info_clo.als_status_date, l_als_info_clo.als_created_date),
                                 p_assm_created_date => l_als_info_clo.als_created_date,
                                 p_assm_modified_date => l_als_info_clo.als_authorised_date,
                                 p_assm_modified_by => l_als_info_clo.als_authorised_by);
               
               OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                'PEN');
               FETCH C_CURR_ASSM INTO l_assm_refno;
               CLOSE C_CURR_ASSM;
               
               update_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_assm_modified_by => l_als_info.als_modified_by);
            ELSIF l_als_info.als_rls_code = 'APPRTR'
            THEN
               l_curr_status := CASE l_als_info.als_sco_code
                                    WHEN 'PEN' THEN 'PEN'
                                    WHEN 'AUT' THEN 'CLO'
                                 END;
               
               OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                l_curr_status);
               FETCH C_CURR_ASSM INTO l_assm_refno;
               CLOSE C_CURR_ASSM;
               
               update_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_assm_modified_by => l_als_info.als_modified_by);
            ELSIF l_als_info.als_rls_code = 'HNATR'
            THEN
               OPEN C_FIRST_ALS_INFO(l_curr_party.par_refno,
                                     l_curr_party.app_refno);
               FETCH C_FIRST_ALS_INFO INTO l_als_info_clo;
               CLOSE C_FIRST_ALS_INFO;
               
               OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                'CLO');
               FETCH C_CURR_ASSM INTO l_assm_refno;
               CLOSE C_CURR_ASSM;
               
               update_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_created_date => l_als_info_clo.als_created_date,
                                 p_assm_modified_date => l_als_info_clo.als_modified_date,
                                 p_assm_modified_by => l_als_info_clo.als_modified_by);
               
               OPEN C_CURR_ASSM(l_curr_party.app_refno,
                                'PEN');
               FETCH C_CURR_ASSM INTO l_assm_refno;
               CLOSE C_CURR_ASSM;
               
               update_assessment(p_assm_refno => l_assm_refno,
                                 p_assm_created_date => l_als_info.als_created_date,
                                 p_assm_modified_date => l_als_info.als_modified_date,
                                 p_assm_modified_by => l_als_info.als_modified_by);
            END IF;
         END IF;
      END IF;
   END LOOP;
EXCEPTION
WHEN OTHERS THEN
   dbms_output.put_line(SQLERRM);
END;
/

ALTER TRIGGER ASSM_BR_U ENABLE;

DROP PROCEDURE process_hnatr_questions;
DROP PROCEDURE process_hna_questions;
DROP PROCEDURE process_apprtr_questions;
DROP PROCEDURE process_approval_questions;
DROP PROCEDURE generate_answer_comments;
DROP PROCEDURE delete_resp_reviewed_assm;
DROP PROCEDURE create_gqre_from_app;
DROP PROCEDURE create_gqre;
DROP PROCEDURE create_assessment;
