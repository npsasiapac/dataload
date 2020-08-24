--
-- gen_hco_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Contractors Dataload Code
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
-- 17-JUL-2007   1.1  5.12.0 PH    Added new dataload for con_sor_products
-- 21-AUG-2007   1.2  5.12.0 PH    Removed hco_dlas as we now have one
--                                 generic script
-- 03-DEC-2007   1.3  5.12.1 PH    Replaced bin with dload
-- 25-SEP-2009   1.4  5.15.1 PH    Added dl_hco_col_fix.sql which removes the 
--                                 incorrect columns created in product
-- 14-Jan-2013   1.5  6.6.0 PJD    Replace PROD_HOME/dload
-- 25-SEP-2015   1.6  6.11  AJ     s_dl_hrm_sor_cmpt_specs.pks/pkb part of bespoke
--                                 to be added to standard DL's at v613
--
-------------------------------------------------------------------------------
--
@./dl_hco_tab_new.sql
@./hd1_errs_in.sql
@./s_dl_hco_products.pks
@./s_dl_hco_products.pkb
@./s_dl_hco_stores.pks
@./s_dl_hco_stores.pkb
@./s_dl_hco_store_stock_items.pks
@./s_dl_hco_store_stock_items.pkb
@./s_dl_hrm_sor_effort.pks
@./s_dl_hrm_sor_effort.pkb
@./s_dl_hrm_con_sor_effort.pks
@./s_dl_hrm_con_sor_effort.pkb
@./s_dl_hco_depots.pks
@./s_dl_hco_depots.pkb
@./s_dl_hco_cos_depots.pks
@./s_dl_hco_cos_depots.pkb
@./s_dl_hco_teams.pks
@./s_dl_hco_teams.pkb
@./s_dl_hco_vehicle_operatives.pks
@./s_dl_hco_vehicle_operatives.pkb
@./s_dl_hco_operative_type_grades.pks
@./s_dl_hco_operative_type_grades.pkb
@./s_dl_hco_operative_details.pks
@./s_dl_hco_operative_details.pkb
@./s_dl_hco_operative_skills.pks
@./s_dl_hco_operative_skills.pkb
@./s_dl_hco_sor_prdt_specificatn.pks
@./s_dl_hco_sor_prdt_specificatn.pkb
@./s_dl_hco_con_sor_products.pks
@./s_dl_hco_con_sor_products.pkb
--@./s_dl_hrm_sor_cmpt_specs.pks
--@./s_dl_hrm_sor_cmpt_specs.pkb
@./s_dl_hrm_sor_components.pks
@./s_dl_hrm_sor_components.pkb
@./dl_hco_col_fix.sql



