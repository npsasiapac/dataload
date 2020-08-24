--
-- gen_hra_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Repairs Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 26-NOV-2002   2.0  5.3.0  PJD   Added in s_dl_hra_utils
--                                 and s_dl_hra_parallel_rents
-- 26-JUL-2004   2.2  5.5.0  IR    Removed s_dl_hra_service_charge_rates
--                                 and s_dl_hra_service_usages
-- 12-AUG-2004   2.3  5.6.0  PH    Added in Debit Details Dataload as
--                                 now part of Standard Rents.
-- 11-MAY-2005   2.4  5.7.0  PH    Added hra_dlas.sql as Debit Details
--                                 in not in dl_load_areas.
-- 17-JUL-2007   3.0  5.12.0 PH    Tidy up comments section
--                                 Added new table script
--                                 Added path of scripts as should now
--                                 be used with GPI Process fdl300 and
--                                 removed Spool Command
-- 21-AUG-2007   3.1  5.12.0 PH    Removed hra_dlas as we now have one
--                                 generic script
-- 22-AUG-2007   3.2  5.12.0 PH    Added nct_aun_links
-- 03-DEC-2007   3.3  5.12.1 PH    Replaced bin with dload
-- 14-JAN-2013   3.4  6.6.0  PJD   Replace $PROD_HOME
--
-------------------------------------------------------------------------------
--
@./dl_hra_tab_new.sql
@./s_dl_hra_utils.pks
@./s_dl_hra_utils.pkb
@./s_dl_hra_revenue_accounts.pks
@./s_dl_hra_revenue_accounts.pkb
@./s_dl_hra_debit_breakdowns.pks
@./s_dl_hra_debit_breakdowns.pkb
@./s_dl_hra_transactions.pks
@./s_dl_hra_transactions.pkb
@./s_dl_hra_debit_details.pks
@./s_dl_hra_debit_details.pkb
@./s_dl_hra_account_balances.pks
@./s_dl_hra_account_balances.pkb
@./s_dl_hra_account_arrears_act.pks
@./s_dl_hra_account_arrears_act.pkb
@./s_dl_hra_void_summaries.pks
@./s_dl_hra_void_summaries.pkb
@./s_dl_hra_payment_contracts.pks
@./s_dl_hra_payment_contracts.pkb
@./s_dl_hra_payment_methods.pks
@./s_dl_hra_payment_methods.pkb
@./s_dl_hra_parallel_rents.pks
@./s_dl_hra_parallel_rents.pkb
@./s_dl_hra_nct_aun_links.pks
@./s_dl_hra_nct_aun_links.pkb