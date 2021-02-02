spoo apps_reactivate.log
SET SERVEROUTPUT ON

CREATE OR REPLACE PROCEDURE update_list_status
IS
   l_error   VARCHAR2(1);
   
   CURSOR C_APPS_TO_UPDATE IS
      SELECT DISTINCT ale2.ale_app_refno,
             ale2.ale_rli_code
        FROM involved_parties ipa
       INNER JOIN applic_list_entries ale ON ale.ale_app_refno = ipa.ipa_app_refno
                                         AND ale.ale_defined_hty_code = 'INELIGIBLE'
                                         AND ale.ale_hrv_aps_code = 'CANATO'
                                         AND ale.ale_rli_code IN ('GEN', 'CHP', 'COOP')
                                         AND ale.ale_lst_code = 'CAN'
       INNER JOIN applic_list_entry_history leh ON leh.leh_app_refno = ale.ale_app_refno
                                               AND leh.leh_rli_code = ale.ale_rli_code
                                               AND NVL(leh.leh_hty_code, 'INELIGIBLE') != 'INELIGIBLE'
        LEFT JOIN general_answers gan ON gan.gan_app_refno = ale.ale_app_refno
                                     AND gan.gan_que_refno = 1152
                                     AND gan.gan_char_value = 'Y'
        LEFT JOIN applic_list_entry_history lehd ON lehd.leh_app_refno = ale.ale_app_refno
                                                AND lehd.leh_application_status_reason = 'DEFATO'
                                                AND lehd.leh_lst_code = 'DEF'
       INNER JOIN applic_list_entries ale2 ON ale2.ale_app_refno = ale.ale_app_refno
       WHERE ipa.ipa_app_refno = ale.ale_app_refno
         AND ipa.ipa_main_applicant_ind = 'Y'
         AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa_end_date), SYSDATE + 1)
         AND gan.gan_app_refno IS NULL
         AND lehd.leh_app_refno IS NULL
         AND NOT EXISTS (SELECT null 
                           FROM involved_parties ipa2
                          INNER JOIN applications app ON app.app_refno = ipa2.ipa_app_refno
                                                     AND app.app_sco_code IN ('CUR','NEW')
                          WHERE ipa2.ipa_app_refno != ipa.ipa_app_refno 
                            AND ipa2.ipa_par_refno = ipa.ipa_par_refno
                            AND ipa2.ipa_main_applicant_ind = 'Y' 
                            AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa2.ipa_start_date) AND NVL(TRUNC(ipa2.ipa_end_date), SYSDATE + 1))
       ORDER BY ale2.ale_app_refno,
                ale2.ale_rli_code;
BEGIN
   FOR l_curr_app IN C_APPS_TO_UPDATE 
   LOOP
      BEGIN
         s_applic_list_entries.update_ale_list_status_proc(p_ale_app_refno => l_curr_app.ale_app_refno,
                                                           p_ale_rli_code =>  l_curr_app.ale_rli_code,
                                                           p_ale_lst_code => 'ACT',
                                                           p_ale_status_start_date  => null,
                                                           p_ale_status_review_date => null,
                                                           p_ale_hrv_aps_code => null,
                                                           p_old_lst_code => 'CAN');
         dbms_output.put_line('Updating Application '|| l_curr_app.ale_app_refno ||', list entry type ' || l_curr_app.ale_rli_code);
      EXCEPTION
      WHEN OTHERS THEN
         dbms_output.put_line('Error Raised while updating Application ' || l_curr_app.ale_app_refno || ', list entry type ' || l_curr_app.ale_rli_code);
      END;
   END LOOP;            
END update_list_status;
/

EXEC update_list_status;
DROP PROCEDURE update_list_status;

COMMIT;

spoo off