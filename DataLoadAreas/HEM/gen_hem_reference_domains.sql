--
-- gen_hem_reference_domains.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Reference Domains Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver   Name  Amendment(s)
-- ----          ---  ------   ----  ------------
-- 28-MAR-2018   1.0  6.16.1   AJ    Initial Created for OHMS migration
--
--
-------------------------------------------------------------------------------
--
@./dl_hem_reference_domains_tab_new.sql
@./s_dl_hem_first_ref_domains.pks
@./s_dl_hem_first_ref_domains.pkb
@./s_dl_hem_first_ref_values.pks
@./s_dl_hem_first_ref_values.pkb
@./dlas_in_hem_reference_domains.sql

