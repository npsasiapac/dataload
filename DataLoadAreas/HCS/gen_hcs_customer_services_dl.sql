--
-- gen_hcs_customer_services_dl.sql
--------------------------------- Comment -------------------------------------
-- Script to install Customer Services data load area
-------------------------------------------------------------------------------
-- Date          Version  DB Version  Name  Amendment(s)
-- ----          ---      ------      ----  ------------
--    OCT-2009   1.0      v5.16.0     VRS   Initial Creation
-- 02-SEP-2015   1.1      v6.12.0     AJ    Change Control added
-- 14-SEP-2015   1.2      v6.12.0     AJ    Using Standard grants indexes synonyms
--                                          and hdl_errs_in sqls now
--
-------------------------------------------------------------------------------
--
-- Business Actions install --
--
spool gen_hcs_customer_services_dl
@dl_hcs_customer_services_tab_new.sql
@s_dl_hcs_business_actions.pks
@s_dl_hcs_business_actions.pkb
@s_dl_hcs_business_act_events.pks
@s_dl_hcs_business_act_events.pkb
@dl_hcs_customer_services_dlas.sql
--@dl_hcs_customer_services_grants.sql
--@dl_hcs_customer_services_index.sql
--@hd1_errs_in.sql
spool off
