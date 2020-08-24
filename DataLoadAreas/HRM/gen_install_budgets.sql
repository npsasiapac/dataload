--
-- Install Script for Budgets Dataloads
-- 
-- Instructions
-- 1) save all files into the same folder
-- 2) from the folder start a SQLPLUS session as the HOU user
-- 3) at the sqlplus prompt type @gen_install_budgets.sql. This will run this file and install all the Budgets dataloaders
-- 4) copy the control files (*.ctl) to the $PROD_HOME/bin folder on the server
-- Created AJ 01-MAR-2016 
-- 
@dl_budgets_tab_new.sql
@s_dl_hrm_budgets.pks
@s_dl_hrm_budgets.pkb
@hdl_grants_budgets.sql
@dl_indexes_budgets.sql
@hdl_synonyms_budgets.sql
@hdl_invalid_budgets.sql
@budgets_dlas_in.sql