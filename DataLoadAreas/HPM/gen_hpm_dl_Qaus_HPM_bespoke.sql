--
-- gen_hpm_dl_Qaus_HPM_bespoke.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Planned Maintenance Dataload Code
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
-- 03-DEC-2007   1.1  5.12.1 PH    Replaced bin with dload
-- 20-NOV-2009   2.0  5.15.1 PH    Added in new dataload areas
-- 10-MAR-2010   2.1  6.1.1  PH    Added in new dataload areas
-- 14-JAN-2012   2.3  6.6.0  PJD   Replace $PROD_HOME
-- 22-FEB-2017   2.4  6.15   AJ    Added bespoke changes for CR462 with
--                                 addition of s_dl_hpm_task_payments
-- 06-MAR-2017   2.5  6.15   AJ    removed reference to survey addresses and results
--                                 as not included in this bespoke version and
--                                 added s_dl_hpm_deliverable_val
--                                 added QAus tag to script name as bespoke version
--                                 and amended where QAus tag has been added to script
--                                 name
--
-------------------------------------------------------------------------------
--
@./dl_hpm_tab_new_Qaus_HPM_bespoke.sql
@./s_dl_hpm_contract_addresses.pks
@./s_dl_hpm_contract_addresses.pkb
@./s_dl_hpm_contracts.pks
@./s_dl_hpm_contracts.pkb
@./s_dl_hpm_deliverables.pks
@./s_dl_hpm_deliverables.pkb
@./s_dl_hpm_payment_task_dets.pks
@./s_dl_hpm_payment_task_dets.pkb
@./s_dl_hpm_task_groups.pks
@./s_dl_hpm_task_groups.pkb
@./s_dl_hpm_tasks.pks
@./s_dl_hpm_tasks.pkb
@./s_dl_hpm_contract_sors.pks
@./s_dl_hpm_contract_sors.pkb
@./s_dl_hpm_deliverable_cmpts.pks
@./s_dl_hpm_deliverable_cmpts.pkb
@./s_dl_hpm_task_payments.pks
@./s_dl_hpm_task_payments.pkb
@./s_dl_hpm_deliverable_val.pks
@./s_dl_hpm_deliverable_val.pkb




