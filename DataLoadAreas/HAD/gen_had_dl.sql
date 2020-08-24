--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   14-DEC-2015  Control comments section added
--                         gen_had_pre_dl.sql no longer required
--                         ..._registered_address amended to ..._registered_addresses
--                         dl_had_nologging.sql removed
--                         had_pra_cachesize_inc.sql removed
--                         hd2_errs_in.sql removed - now run by gen_all_dl.sql
--                         had_dlas.sql removed - now run by gen_all_dl.sql                        
--                         had_indexes.sql removed - now run by gen_all_dl.sql 
--
--***********************************************************************
--
spool gen_had_dl
--@gen_had_pre_dl.sql
@dl_had_tab_new.sql
@s_dl_had_adv_case_hsg_optn_his.pks
@s_dl_had_adv_case_hsg_optn_his.pkb
@s_dl_had_adv_case_questn_resp.pks
@s_dl_had_adv_case_questn_resp.pkb
@s_dl_had_adv_rsn_casewrk_evts.pks
@s_dl_had_adv_rsn_casewrk_evts.pkb
@s_dl_had_advice_case_hsg_optn.pks
@s_dl_had_advice_case_hsg_optn.pkb
@s_dl_had_advice_case_people.pks
@s_dl_had_advice_case_people.pkb
@s_dl_had_advice_case_reasons.pks
@s_dl_had_advice_case_reasons.pkb
@s_dl_had_advice_cases.pks
@s_dl_had_advice_cases.pkb
@s_dl_had_bonds.pks
@s_dl_had_bonds.pkb
@s_dl_had_households.pks
@s_dl_had_households.pkb
@s_dl_had_household_people.pks
@s_dl_had_household_people.pkb
@s_dl_had_prevention_payments.pks
@s_dl_had_prevention_payments.pkb
@s_dl_had_registered_addresses.pks
@s_dl_had_registered_addresses.pkb
@s_dl_had_reg_address_lettings.pks
@s_dl_had_reg_address_lettings.pkb
@had_indexes.sql
@had_dlas.sql
--@dl_had_grants.sql
--@dl_had_nologging.sql
--@had_pra_cachesize_inc.sql
--@hd2_errs_in.sql
spool off