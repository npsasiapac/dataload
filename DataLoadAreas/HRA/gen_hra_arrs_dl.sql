--
-- gen_hra_arrs_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Bespoke Arrears Arrangements Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 11-NOV-2013   1.0  6.9.0  AJ    Initial Creation
--
-------------------------------------------------------------------------------
--
@./dl_hra_arrs_tab_new.sql
@./s_dl_hra_arrears_arrangements.pkb
@./s_dl_hra_arrears_arrangements.pks
@./s_dl_hra_arrears_instalments.pkb
@./s_dl_hra_arrears_instalments.pks
@./hd2_errs_in.sql
@./hdl_errs_in.sql
@./hd1_errs_in.sql
@./dlo_errs_in.sql
@./cdl_errs_in.sql
@./dl_hra_arrs_tab_new.sql
@./hdl_grants.sql
@./hdl_synonyms.sql
@./dl_indexes.sql

