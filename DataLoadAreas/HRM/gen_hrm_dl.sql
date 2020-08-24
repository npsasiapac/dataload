--
-- gen_hrm_dl.sql
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
-- 17-JUL-2007   1.0  5.12.0 PH    Added Comments section.
--                                 Added new table script
--                                 Added path of scripts as should now
--                                 be used with GPI Process fdl300 and
--                                 removed Spool Command
-- 21-AUG-2007   1.1  5.12.0 PH    Removed hrm_dlas as we now have one
--                                 generic script
-- 03-DEC-2007   1.2  5.12.1 PH    Replaced bin with dload
-- 14-Jan-2013   1.3  6.6.0  PJD   Replace $PROD_HOME
--
-------------------------------------------------------------------------------
--
@./dl_hrm_tab_new.sql
@./s_dl_hrm_con_site_prices.pks
@./s_dl_hrm_con_site_prices.pkb
@./s_dl_hrm_inspections.pks
@./s_dl_hrm_inspections.pkb
@./s_dl_hrm_jobs.pks
@./s_dl_hrm_jobs.pkb
@./s_dl_hrm_schedule_of_rates.pks
@./s_dl_hrm_schedule_of_rates.pkb
@./s_dl_hrm_service_requests.pks
@./s_dl_hrm_service_requests.pkb
@./s_dl_hrm_work_descriptions.pks
@./s_dl_hrm_work_descriptions.pkb
@./s_dl_hrm_works_orders.pks
@./s_dl_hrm_works_orders.pkb
@./s_dl_hrm_works_order_versions.pks
@./s_dl_hrm_works_order_versions.pkb