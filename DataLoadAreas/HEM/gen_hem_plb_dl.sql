--
-- gen_hem_plb_dl.sql
-- Script to install the Estates Property Landlord Bank Details Dataload Code
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 15-JUL-2016   1.0  6.13   MB    initial creation
-- 22-JUL-2016   1.1  6.13   AJ    Control added and script name changed
--
-------------------------------------------------------------------------------
@dl_hem_plb_tab_new.sql
@s_dl_hem_prop_landlord_banks.pks
@s_dl_hem_prop_landlord_banks.pkb
@dl_hem_plb_indexes.sql
@dl_hem_plb_grants.sql
@dl_hem_plb_synonyms.sql
@dl_hem_plb_dlas_in.sql