--
-- gen_hpl_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Private Sector Leasing Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver  Name  Amendment(s)
-- ----          ---  ------  ----  ------------
-- 04-MAR-2013   1.0  6.6.0   PJD   Initial Creation
-- 21-AUG-2015   1.1  6.10.0  AJ    amended to be included in gen_all_dl.sql
--                                  in standard data load
--
-------------------------------------------------------------------------------
--
@dl_hpl_tab_new.sql
@s_dl_hpl_psl_leases.pks
@s_dl_hpl_psl_leases.pkb
@s_dl_hpl_psl_lease_rents.pks
@s_dl_hpl_psl_lease_rents.pkb
