--
-- gen_mad_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to ensure the Multi Area dataloads of Contact Details, Notepads and
-- Other Fields have the correct columns. This script does not drop the
-- dataload tables, just adds in any missing columns.
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver    Name  Amendment(s)
-- ----          ---  ------    ----  ------------
-- 04-MAR-2015   1.0  6.10/11   AJ    Initial Creation.
--                                    Contact Details was FSC (System)  now MAD (Multi Area)
--                                    Notepads        was HEM (Estates) now MAD (Multi Area)
--                                    Other Fields    was FSC (System)  now MAD (Multi Area)
--                                    The dl_mad_otherfields_extra_index.sql creates two indexes that
--                                    are not created by the standard hdl_indexes.sql script
-- 04-JUN-2015   1.1  6.10/11   AJ    dl_mad_contact_details_tab.sql changed to dl_mad_contact_details_tab_new.sql
--
-------------------------------------------------------------------------------
--
spool gen_mad_dl.lst
@dl_mad_contact_details_tab_new.sql
@dl_mad_notepads_tab_new.sql
@dl_mad_otherfields_tab_new.sql
@s_dl_mad_contact_details.pks
@s_dl_mad_contact_details.pkb
@s_dl_mad_notepads.pks
@s_dl_mad_notepads.pkb
@s_dl_mad_other_field_values.pks
@s_dl_mad_other_field_values.pkb
@s_dl_mad_other_field_val_hist.pks
@s_dl_mad_other_field_val_hist.pkb
@dl_mad_otherfields_extra_index.sql
spool off