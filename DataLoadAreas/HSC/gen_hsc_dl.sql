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
-- 08-MAY-2006   1.0         PH    Included Invoices
-- 17-JUL-2007   2.0  5.12.0 PH    Added Comments section.
--                                 Added new table script 
--                                 Added path of scripts as should now
--                                 be used with GPI Process fdl300 and
--                                 removed Spool Command
-- 21-AUG-2007   2.1  5.12.0 PH    Removed hsc_dlas as we now have one
--                                 generic script
-- 03-DEC-2007   2.2  5.12.1 PH    Replaced bin with dload
-- 14-JAN-2013   2.3  6.6.0  PJd   Replace $PROD_DATALOAD
--
-------------------------------------------------------------------------------
--
@./dl_hsc_tab_new.sql
@./s_dl_hsc_leases.pks
@./s_dl_hsc_leases.pkb
@./s_dl_hsc_lease_assignments.pks
@./s_dl_hsc_lease_assignments.pkb
@./s_dl_hsc_lease_parties.pks
@./s_dl_hsc_lease_parties.pkb
@./s_dl_hsc_service_charge_bases.pks
@./s_dl_hsc_service_charge_bases.pkb
@./s_dl_hsc_service_charge_rates.pks
@./s_dl_hsc_service_charge_rates.pkb
@./s_dl_hsc_service_assignments.pks
@./s_dl_hsc_service_assignments.pkb
@./s_dl_hsc_service_usages.pks
@./s_dl_hsc_service_usages.pkb
@./s_dl_hsc_lease_summaries.pks
@./s_dl_hsc_lease_summaries.pkb
@./s_dl_hsc_utils.pks
@./s_dl_hsc_utils.pkb
@./s_dl_hsc_credit_allocations.pks
@./s_dl_hsc_credit_allocations.pkb
@./s_dl_hsc_credit_memo_balances.pks
@./s_dl_hsc_credit_memo_balances.pkb
@./s_dl_hsc_customer_credit_memos.pks
@./s_dl_hsc_customer_credit_memos.pkb
@./s_dl_hsc_customer_invoices.pks
@./s_dl_hsc_customer_invoices.pkb
@./s_dl_hsc_inactive_scp_est.pks
@./s_dl_hsc_inactive_scp_est.pkb
@./s_dl_hsc_invoice_balances.pks
@./s_dl_hsc_invoice_balances.pkb
@./s_dl_hsc_payment_balances.pks
@./s_dl_hsc_payment_balances.pkb
@./s_dl_hsc_sci_invoice_adj.pks
@./s_dl_hsc_sci_invoice_adj.pkb
@./s_dl_hsc_sci_service_chg_items.pks
@./s_dl_hsc_sci_service_chg_items.pkb
@./s_dl_hsc_invoice_parties.pks
@./s_dl_hsc_invoice_parties.pkb
@./s_dl_hsc_cust_inv_arrears_act.pks
@./s_dl_hsc_cust_inv_arrears_act.pkb