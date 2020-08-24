--
-- gen_hrm_cont_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Bespoke Repairs Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 27-JUL-2012   1.0  6.5.0 PH     Initial Creation
-- 01-FEB-2016   2.0  6.13  AJ     Removed PROD_HOME/dload/ from front of lines
-- 11-JUL-2016   2.1  6.13  AJ     added con_site_job_roles data load
--
-------------------------------------------------------------------------------
--
@dl_hrm_cont_tab_new.sql
@s_dl_hrm_contractors.pks
@s_dl_hrm_contractors.pkb
@s_dl_hrm_con_site_job_roles.pks
@s_dl_hrm_con_site_job_roles.pkb
@s_dl_hrm_con_site_trades.pks
@s_dl_hrm_con_site_trades.pkb
@s_dl_hrm_pp_con_sites.pks
@s_dl_hrm_pp_con_sites.pkb
@hdl_cont_dlas_in.sql
--@hdl_grants.sql    -- use current standard data load version
--@hdl_synonyms.sql  -- use current standard data load version
--@dl_indexes.sql    -- use current standard data load version
--@hdl_invalid.sql   -- use current standard data load version
