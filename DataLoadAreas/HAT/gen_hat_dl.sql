--
-- gen_hat_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Allocations Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 17-JUL-2007   1.0  5.12.0 PH    Added Comments section.
--                                 Added new table script 
--                                 Added path of scripts as should now
--                                 be used with GPI Process fdl300 and
--                                 removed Spool Command
-- 21-AUG-2007   1.1  5.12.0 PH    Removed hat_dlas as we now have one
--                                 generic script
-- 03-DEC-2007   1.2  5.12.1 PH    Replaced bin with dload
--
-- 14-Jan-2013   1.3  6.6.0  PJD   Remove refernce to PROD_DATALOAD
-- 
--
-------------------------------------------------------------------------------
--
@./dl_hat_tab_new.sql
@./s_dl_hat_utils.pks
@./s_dl_hat_utils.pkb
@./s_dl_hat_applications.pks
@./s_dl_hat_applications.pkb
@./s_dl_hat_applic_list_entries.pks
@./s_dl_hat_applic_list_entries.pkb
@./s_dl_hat_applic_statuses.pks
@./s_dl_hat_applic_statuses.pkb
@./s_dl_hat_involved_parties.pks
@./s_dl_hat_involved_parties.pkb
@./s_dl_hat_general_answers.pks
@./s_dl_hat_general_answers.pkb
@./s_dl_hat_lettings_area_ans.pks
@./s_dl_hat_lettings_area_ans.pkb
@./s_dl_hat_medical_answers.pks
@./s_dl_hat_medical_answers.pkb
@./s_dl_hat_medical_referrals.pks
@./s_dl_hat_medical_referrals.pkb
@./s_dl_hat_involved_party_ans.pks
@./s_dl_hat_involved_party_ans.pkb
@./s_dl_hat_applic_list_ent_hist.pks
@./s_dl_hat_applic_list_ent_hist.pkb
@./s_dl_hat_applic_list_stage.pks
@./s_dl_hat_applic_list_stage.pkb
@./s_dl_hat_hml_applications.pks
@./s_dl_hat_hml_applications.pkb
