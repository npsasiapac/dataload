-- *********************************************************************
--
-- Version      Who         Date       Why
-- 01.00        Ian Rowell  25-Sep-09  Initial Creation
--  2.00        Paul Hearty 07-JUL-11  Added new area subsidy_debt_assmnts and
--                                     removed bespoke debit details and also
--                                     call common code for grants/synonyms/indexes
--
-- *********************************************************************
--
spool gen_hra_sub_dl
@dl_hra_subsidy_tab_new.sql
@s_dl_hra_subsidy_applications.pks
@s_dl_hra_subsidy_applications.pkb
@s_dl_hra_group_subsidy_reviews.pks
@s_dl_hra_group_subsidy_reviews.pkb
@s_dl_hra_subsidy_debt_assmnts.pks
@s_dl_hra_subsidy_debt_assmnts.pkb
@s_dl_hra_subsidy_reviews.pks
@s_dl_hra_subsidy_reviews.pkb
@s_dl_hra_subsidy_income_items.pks
@s_dl_hra_subsidy_income_items.pkb
@s_dl_hra_account_rent_limits.pks
@s_dl_hra_account_rent_limits.pkb
@s_dl_hra_subsidy_grace_periods.pks
@s_dl_hra_subsidy_grace_periods.pkb
@s_dl_hra_subsidy_letters.pks
@s_dl_hra_subsidy_letters.pkb
@hra_sub_dlas.sql
@hdl_grants.sql
@hdl_synonyms.sql
@dl_indexes.sql
@hd2_errs_in.sql
@hdl_invalid.sql
spool off