--
-- gen_hem_voids_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Bespoke Void Events Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver   DB Ver  Name   Amendment(s)
-- ----          ---   ------  ----   ------------
-- 18-NOV-2015   1.0   6.13    MJK    Initial Creation
-- 21-MAR-2016   1.1   6.13    AJ     names amended to match changes in sqls 
-- 25-May-2016   1.2   6.13    MOK    added void Instances
-- 09-AUG-2016   1.3   6.13    AJ     slight change of order
-- 01-FEB-2017   1.4   6.14    AJ     hdl_voids_errs_in.sql removed as now errors
--                                    also in hd3 not just hdl error files
-- 06-FEB-2017   1.5   6.14    AJ     added void status history for Instances
--                                    CR461 for Queensland
--
--
-------------------------------------------------------------------------------
--
@dl_hem_voids_tab_new.sql
@s_dl_hem_void_events.pks
@s_dl_hem_void_events.pkb
@s_dl_hem_void_instances.pks
@s_dl_hem_void_instances.pkb
@s_dl_hem_void_status_hist.pks
@s_dl_hem_void_status_hist.pkb
--@hdl_errs_in.sql
--@hd3_errs_in.sql
@dl_hem_voids_dlas_in.sql
@hdl_grants_void_instances_events.sql
@hdl_synonyms_void_instances_events.sql
@dl_indexes_void_events.sql


