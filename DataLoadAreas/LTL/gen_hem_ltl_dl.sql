-- *********************************************************************
--
-- Version    DBase   Who           Date       Why
-- 1.0                Ian Rowell    25-Sep-09  Initial Creation
--
-- 2.0                Vishad Shah   04-FEB-10  s_dl_ltl_utils package no longer
--                                             required.
-- 2.1        6.12    MJK           12-NOV-15  Now uses hd1_errs_in_ltl.sql instead of hd1_errs_in.sql
-- 2.2        6.14    AJ            18-MAR-16  hd1_errs_in_ltl.sql removed use hd1_errs_in.sql
--                                             so only 1 error file is maintained
--                                             amended to include new files created
--                                             change control amended to include DB version 
-- 
-- *********************************************************************
--
spool gen_hem_ltl_dl
--
@dl_hem_ltl_tab_new.sql
--@hd1_errs_in_ltl.sql need latest hd1_errs_in.sql for erros
@hem_ltl_dlas.sql
@s_dl_ltl_land_titles.pks
@s_dl_ltl_land_titles.pkb
@s_dl_ltl_land_title_assign.pks
@s_dl_ltl_land_title_assign.pkb
@s_dl_ltl_land_title_releases.pks
@s_dl_ltl_land_title_releases.pkb
@hem_ltl_cachesize_inc.sql
@hem_ltl_grants.sql
@hem_ltl_synonyms.sql
@hem_ltl_indexes.sql
--
spool off


