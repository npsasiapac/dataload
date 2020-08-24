--
-- dl_hem_plc_gen.sql
--------------------------------- Comment -------------------------------------
-- Script to compile Property Life Cycle data Load
-------------------------------------------------------------------------------
-- Date          Version  DB Version  Name  Amendment(s)
-- ----          ---      ------      ----  ------------
-- 01-MAR-2010   1.0      v5.16.0     VRS   Initial Creation
-- 02-SEP-2015   1.1      v6.12.0     AJ    Change Control added and checked
--                                          PLC_PROP_REQ_HIST no longer available
--                                          so removed
-- 14-SEP-2015   1.2      v6.12.0     AJ    The following removed as standard ones used;
--                                          @dl_hem_plc_alter_table_pre.sql
--                                          @dl_hem_plc_indexes.sql
--                                          @dl_hem_plc_grants.sql
--
-------------------------------------------------------------------------------
--
spool dl_hem_plc_gen
@dl_hem_plc_tab_new.sql
@s_dl_hem_plc_prop_requests.pks
@s_dl_hem_plc_prop_requests.pkb
@s_dl_hem_plc_request_props.pks
@s_dl_hem_plc_request_props.pkb
@s_dl_hem_plc_req_data_items.pks
@s_dl_hem_plc_req_data_items.pkb
@s_dl_hem_plc_req_act_hist.pks
@s_dl_hem_plc_req_act_hist.pkb
@s_dl_hem_plc_req_prop_links.pks
@s_dl_hem_plc_req_prop_links.pkb
@s_dl_hem_plc_prop_act_hist.pks
@s_dl_hem_plc_prop_act_hist.pkb
@dl_hem_plc_dlas.sql
@dl_hem_plc_errs_in.sql
spool off