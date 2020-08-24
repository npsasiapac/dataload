--
-- gen_hpp_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Property Purchase Dataload Code
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
-- 21-AUG-2007   1.1  5.12.0 PH    Removed hpp_dlas as we now have one
--                                 generic script
-- 03-DEC-2007   1.2  5.12.1 PH    Replaced bin with dload
-- 14-~Jan-2013  1.3  6.6.0  PJD   Replace $PROD_HOME 
--
-------------------------------------------------------------------------------
--
@./dl_hpp_tab_new.sql
@./s_dl_hpp_pp_applications.pks
@./s_dl_hpp_pp_applications.pkb
@./s_dl_hpp_pp_appln_parties.pks
@./s_dl_hpp_pp_appln_parties.pkb
@./s_dl_hpp_pp_events.pks
@./s_dl_hpp_pp_events.pkb
@./s_dl_hpp_pp_tenancy_histories.pks
@./s_dl_hpp_pp_tenancy_histories.pkb
@./s_dl_hpp_pp_valuations.pks
@./s_dl_hpp_pp_valuations.pkb
@./s_dl_hpp_pp_tenant_improvs.pks
@./s_dl_hpp_pp_tenant_improvs.pkb
@./s_dl_hpp_pp_valuation_defects.pks
@./s_dl_hpp_pp_valuation_defects.pkb
