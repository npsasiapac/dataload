	--
-- gen_all_dl.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Dataload Code for all modules
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver Name  Amendment(s)
-- ----          ---  ------ ----  ------------
-- 17-JUL-2007   1.0  5.12.0 PH    Added Comments section.
--                                 Added path of scripts as should now
--                                 be used with GPI Process fdl300
-- 03-DEC-2007   1.1  5.12.1 PH    Replaced bin with dload
-- 04-MAR-2008   1.2  5.13.0 PH    Added hdl_invalid.sql and dl_indexes
-- 16-APR-2008   1.3  5.13.0 PH    Added hdl_dlas_in.sql
-- 14-Jan-2012   1.4  6.6.0  PJD   Remove reference to $PROD_DATALOAD
-- 27-FEB-2015   1.5  6.11   AJ    Checked for v6.10 and v6.10  BUT
--                                 **Does not included gen_hpl_dl needs running**
--                                 **separately if required PSL_Leases**
--                                 hd2_errs_in added
-- 05-MAR-2015   1.6  6.11   AJ    Added gen_mad_dl.sql for Multi Area Data Load
-- 21-AUG-2015   1.7  6.10   AJ    Added gen_hpl_dl.sql for Private Sector Leasing
--
-------------------------------------------------------------------------------
--
@./dload/gen_hem_dl.sql
@./dload/gen_hat_dl.sql
@./dload/gen_hra_dl.sql
@./dload/gen_hrm_dl.sql
@./dload/gen_hpl_dl.sql
@./dload/gen_hpp_dl.sql
@./dload/gen_hpm_dl.sql
@./dload/gen_hsc_dl.sql
@./dload/gen_hco_dl.sql
@./dload/gen_mad_dl.sql
@./dload/hdl_indexes.sql
@./dload/hdl_grants.sql
@./dload/hdl_synonyms.sql
@./dload/hdl_errs_in.sql
@./dload/hd1_errs_in.sql
@./dload/hd2_errs_in.sql
@./dload/cdl_errs_in.sql
@./dload/dlo_errs_in.sql
@./dload/hdl_dlas_in.sql
@./dload/dl_indexes.sql
@./dload/hdl_invalid.sql



