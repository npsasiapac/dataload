--
-- gen_hss_dl.sql
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 07-FEB-2019   1.0  6.18   AJ    Created For new SAHT loader
--                                 Support Services Referrals
-- 22-MAR-2019   1.1  6.18   AJ    final version
--
--********************************************************************
spool gen_hss_dl
@dl_hss_tab_new.sql
@s_dl_hss_referrals.pks
@s_dl_hss_referrals.pkb
@hss_dlas_in.sql
@hss_hdl_grants.sql
@hss_hdl_synonyms.sql
@hss_dl_indexes.sql
spool off
