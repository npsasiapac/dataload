--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0               VS   2008         Initial Creation
--  1.0     6.11      AJ   22-Dec-2015  Added new functionality
--                                      released at v611 income liabilities
--                                      added change control and removed 
--                                      hd2 and hd1 errs as standard ones can be
--                                      supplied and used
--
--
--***********************************************************************
--
-- Bespoke Income Grants for Income Data Load NonICS v6.11
--
spool income_dl_install.lst
@income_dl_tab_new.sql
@s_dl_hem_income_headers.pks
@s_dl_hem_income_headers.pkb
@s_dl_hem_income_header_usages.pks
@s_dl_hem_income_header_usages.pkb
@s_dl_hem_income_details.pks
@s_dl_hem_income_details.pkb
@s_dl_hem_assets.pks
@s_dl_hem_assets.pkb
@s_dl_hem_inc_det_deductions.pks
@s_dl_hem_inc_det_deductions.pkb
@s_dl_hem_income_liabilities.pks
@s_dl_hem_income_liabilities.pkb
@income_dlas.sql
@income_indexes.sql
@income_grants.sql
@income_synonyms.sql
spool off