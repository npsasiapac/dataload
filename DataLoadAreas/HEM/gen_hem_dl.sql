--
-- gen_hem_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Estates Dataload Code
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
-- 14-JAN-2013   1.2  6.6.0  PJD   Replace $PROD_HOME
-- 06-AUG-2015   1.3  6.11   AJ    property_landlords added
--
-------------------------------------------------------------------------------
--
@./dl_hem_tab_new.sql
@./s_dl_hem_utils.pks
@./s_dl_hem_utils.pkb
@./s_dl_hem_admin_properties.pks
@./s_dl_hem_admin_properties.pkb
@./s_dl_hem_property_statuses.pks
@./s_dl_hem_property_statuses.pkb
@./s_dl_hem_people.pks
@./s_dl_hem_people.pkb
@./s_dl_hem_tenancies.pks
@./s_dl_hem_tenancies.pkb
@./s_dl_hem_properties.pks
@./s_dl_hem_properties.pkb
@./s_dl_hem_property_elements.pks
@./s_dl_hem_property_elements.pkb
@./s_dl_hem_property_landlords.pks
@./s_dl_hem_property_landlords.pkb
@./s_dl_hem_admin_units.pks
@./s_dl_hem_admin_units.pkb
@./s_dl_hem_admin_groupings.pks
@./s_dl_hem_admin_groupings.pkb
@./s_dl_hem_addresses.pks
@./s_dl_hem_addresses.pkb
@./s_dl_hem_link_tenancies.pks
@./s_dl_hem_link_tenancies.pkb