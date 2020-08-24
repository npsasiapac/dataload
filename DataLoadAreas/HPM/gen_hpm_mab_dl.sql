--
-- gen_hpm_mab_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Bespoke Admin Unit Security for Budgets Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 25-OCT-2013   1.0  6.9.0  AJ    Initial Creation
--
-------------------------------------------------------------------------------
--
@./dl_hpm_mab_tab_new.sql
@./s_dl_hpm_man_area_budgets.pkb
@./s_dl_hpm_man_area_budgets.pks
@./hd2_errs_in.sql
@./hd1_errs_in.sql
@./hdl_errs_in.sql
@./dlo_errs_in.sql
@./cdl_errs_in.sql
@./hdl_hpm_mab_dlas_in.sql
@./hdl_hpm_mab_grants.sql
@./hdl_hpm_mab_synonyms.sql
@./dl_hpm_mab_indexes.sql

