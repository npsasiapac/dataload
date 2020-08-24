--
-- gen_hat_Org_Offers.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Allocations Organisation Offers Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 24-MAY-2018   1.0  6.15   AJ    Initial Creation as bespoke
--
--
-------------------------------------------------------------------------------
--
@./dl_hat_org_offers_tab_new.sql
@./s_dl_hat_organisation_offers.pks
@./s_dl_hat_organisation_offers.pkb
@./dlas_in_hat_org_offers.sql

