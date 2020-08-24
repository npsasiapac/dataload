--
-- gen_hsc_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Service Charges Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 06-JUN-2016   1.0         AJ    Bespoke for Person Attributes based one
--                                 gen_hsc_dl.sql v2.4 (23-MAY-2016)
--
-------------------------------------------------------------------------------
--
spool gen_hem_person_attributes_dl
@dl_hem_person_attributes_tab.sql
@s_dl_hem_person_peo_attributes.pks
@s_dl_hem_person_peo_attributes.pkb
@s_dl_hem_person_peo_att_hists.pks
@s_dl_hem_person_peo_att_hists.pkb
@hem_person_attributes_grants.sql
@hem_person_attributes_indexes.sql
@hem_person_attributes_synonyms.sql
@hem_person_attributes_dlas_in.sql
spool off