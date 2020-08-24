--
-- gen_hem_org_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Estates Organisations Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 02-MAR-2016   1.0  6.13   AJ    Initial Creation
-- 06-APR-2017   1.1  6.15   AJ    Updated with hierarchy and admin units
-- 22-MAY-2017   1.2  6.15   DLB   Corrected s_dl_hem_org_hierarchy.pks run twice.
--
--
-------------------------------------------------------------------------------
--
@dl_hem_org_tab_new.sql
@s_dl_hem_org_admin_units.pks
@s_dl_hem_org_admin_units.pkb
@s_dl_hem_org_hierarchy.pks
@s_dl_hem_org_hierarchy.pkb
