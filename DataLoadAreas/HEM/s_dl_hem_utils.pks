-----------------------------------------------------------------------------
-- s_dl_hem_utils.pks
--
-- Change History
-- Version DB Ver   User   Date        Reason
--   1.0                               iWorld Estates Dataload
--   1.1   5.1.6    PJD    30-May-2002 added papp_refno into insert_address proc
--   1.2   5.4.0    PJD    20-Oct-2003 added new version of comp_stats
--                                     added new function orig_rows
--   1.3   5.8.0    PH     19-JUL-2005 Added new fields on insert address
--                                     adr_eastings and adr_northings
--   2.0   5.12.0   PH     17-JUL-2007 Amended code to allow for Housing Advice
--                                     Cases, these are REG PHYSICAL Addresses.
--   3.0   5.13.1   PH     08-MAY-2008 Added new field on insert address
--                                     adr_uprn
--   3.1   5.15.1   PH     06-APR-2009 Added additional fields for Landlord
--                                     information to insert_address
--   6.0   6.9      PJD    31-JAN-2014 Default values for parameters in
--                                     insert_address proc
--                                     information to insert_address
--   3.2   6.11     AJ     18-JUN-2015 Added procedure insert_bank_details_mlang
--                                     used in s_dl_adim_units.pkb
--   3.3   6.11     PJD    30-JUN-2015 Added in chk_is_numeric and chk_is_integer
--                                     functions 
--                                     plus first version of  mlang_street 
--
------------------------------------------------------------------------------
CREATE OR REPLACE
PACKAGE s_dl_hem_utils
AS
   FUNCTION exists_propref
         (p_propref IN VARCHAR2 ) RETURN BOOLEAN;
   FUNCTION pro_refno_for_propref
         (p_propref IN VARCHAR2 ) RETURN NUMBER;
   FUNCTION exists_frv
         (p_domain     IN VARCHAR2,
          p_code       IN VARCHAR2,
          p_allow_null IN VARCHAR2 default 'N') RETURN BOOLEAN;
   FUNCTION exists_hrv
         (p_table_name IN VARCHAR2,
          p_code IN VARCHAR2) RETURN BOOLEAN;
   FUNCTION yorn
          (p_value in VARCHAR2) RETURN BOOLEAN;
   FUNCTION yornornull
          (p_value in VARCHAR2) RETURN BOOLEAN;
   FUNCTION dateorder
          (p_date1 in DATE,
           p_date2 in DATE) RETURN BOOLEAN;
   FUNCTION exists_aun_code
         (p_aun_code IN VARCHAR2) RETURN BOOLEAN;
   FUNCTION tcy_refno_for_alt_ref
         (p_alt_ref  IN VARCHAR2 ) RETURN INTEGER;
   FUNCTION tcy_start_date_for_alt_ref
         (p_alt_ref  IN VARCHAR2 ) RETURN DATE;
   FUNCTION tho_tcy_refno_for_date
         (p_pro_propref IN VARCHAR2,
          p_start_date  IN DATE    ,
          p_end_date    IN DATE    )RETURN INTEGER;
   FUNCTION dl_comp_stats
         (p_table_name IN VARCHAR2)
                                  RETURN VARCHAR2;
   FUNCTION dl_comp_stats
         (p_table_name IN VARCHAR2
         ,p_orig_rows  IN INTEGER
         ,p_proc_rows  IN INTEGER)
                                  RETURN VARCHAR2;
   FUNCTION dl_orig_rows
         (p_table_name IN VARCHAR2)
                                  RETURN INTEGER;
   FUNCTION f_bru_run_no
         (p_bru_aun_code VARCHAR2
         ,p_effective_date DATE)
                                  RETURN NUMBER;
   PROCEDURE get_address
         (p_mode                  IN OUT VARCHAR2
         ,p_street_index_code IN OUT VARCHAR2
         ,p_adr_refno             IN OUT integer
         ,p_flat                  IN VARCHAR2
         ,p_building              IN VARCHAR2
         ,p_street_number         IN VARCHAR2
         ,p_sub_street1           IN VARCHAR2
         ,p_sub_street2           IN VARCHAR2
         ,p_sub_street3           IN VARCHAR2
         ,p_street                IN VARCHAR2
         ,p_area                  IN VARCHAR2
         ,p_town                  IN VARCHAR2
         ,p_county                IN VARCHAR2
         ,p_pcode                 IN VARCHAR2
         );
 PROCEDURE insert_address
         (p_fao_code              IN VARCHAR2 DEFAULT NULL
         ,p_far_code              IN VARCHAR2 DEFAULT NULL
         ,p_street_index_code     IN OUT VARCHAR2
         ,p_adr_refno             IN OUT integer
         ,p_flat                  IN VARCHAR2
         ,p_building              IN VARCHAR2
         ,p_street_number         IN VARCHAR2
         ,p_sub_street1           IN VARCHAR2 DEFAULT NULL
         ,p_sub_street2           IN VARCHAR2 DEFAULT NULL
         ,p_sub_street3           IN VARCHAR2 DEFAULT NULL
         ,p_street                IN VARCHAR2
         ,p_area                  IN VARCHAR2
         ,p_town                  IN VARCHAR2
         ,p_county                IN VARCHAR2
         ,p_pcode                 IN VARCHAR2
         ,p_country               IN VARCHAR2
         ,p_aun_code              IN VARCHAR2 DEFAULT NULL
         ,p_pro_refno             IN NUMBER DEFAULT NULL
         ,p_par_refno             IN NUMBER DEFAULT NULL
         ,p_tcy_refno             IN NUMBER DEFAULT NULL
         ,p_bde_refno             IN NUMBER DEFAULT NULL
         ,p_pof_refno             IN NUMBER DEFAULT NULL
         ,p_start_date            IN DATE
         ,p_end_date              IN DATE
         ,p_local_ind             IN VARCHAR2 default 'N'
         ,p_abroad_ind            IN VARCHAR2 default 'Y'
         ,p_app_refno             IN number   default NULL
         ,p_cos_code              IN varchar2 default NULL
         ,p_cou_code              IN varchar2 default NULL
         ,p_aer_id                IN NUMBER   default NULL
         ,p_nom_refno             IN NUMBER   default NULL
         ,p_papp_refno            IN NUMBER   default NULL
         ,p_aus_contact_name      IN VARCHAR2 default NULL
         ,p_eastings              IN VARCHAR2
         ,p_northings             IN VARCHAR2
         ,p_acas_reference        IN NUMBER   default null
         ,p_uprn                  IN VARCHAR2
         ,p_landlord_par_refno    IN NUMBER DEFAULT NULL
         ,p_llt_code              IN VARCHAR2 DEFAULT NULL
         ,p_aat_code              IN VARCHAR2 DEFAULT NULL
         ,p_pty_code              IN VARCHAR2 DEFAULT NULL
         ,p_property_size         IN VARCHAR2 DEFAULT NULL
         ,p_floor_level           IN NUMBER DEFAULT NULL
         ,p_hrv_alr_code          IN VARCHAR2 DEFAULT NULL
         ,p_tenancy_leave_date    IN DATE DEFAULT NULL
         ,p_arrears_amount        IN NUMBER DEFAULT NULL
         ,p_storage_ind           IN VARCHAR2 DEFAULT NULL
         ,p_storage_unit_cost     IN NUMBER DEFAULT NULL
         ,p_storage_cost          IN NUMBER DEFAULT NULL
         );
--
PROCEDURE display_error
         (p_batch              IN VARCHAR2
         ,p_process            IN VARCHAR2
         ,p_date               IN DATE
         ,p_table              IN VARCHAR2
         ,p_sequence           IN NUMBER
         ,p_hdl                IN VARCHAR2
         ,p_code               IN NUMBER
         );
--
PROCEDURE insert_bank_details
    (p_bde_bank_name         IN varchar2,
     p_bde_branch_name       IN varchar2,
     p_bad_account_no        IN varchar2,
     p_bad_account_name      IN varchar2,
     p_bad_sort_code         IN varchar2,
     p_bad_start_date        IN DATE,
     p_bde_refno             OUT integer,
     p_bad_refno             OUT integer
     );
--
FUNCTION hps_hpc_code_for_date
         (p_pro_propref IN VARCHAR2,
          p_date        IN DATE    ) RETURN VARCHAR2;
--
PROCEDURE insert_bank_details_mlang
    (p_bde_bank_name         IN varchar2,
     p_bde_branch_name       IN varchar2,
     p_bde_bank_name_mlang   IN varchar2,
     p_bde_branch_name_mlang IN varchar2,
     p_bad_account_no        IN varchar2,
     p_bad_account_name      IN varchar2,
     p_bad_sort_code         IN varchar2,
     p_bad_start_date        IN DATE,
     p_bde_refno             OUT integer,
     p_bad_refno             OUT integer
     );
--
FUNCTION chk_is_numeric (p_char VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION chk_is_integer (p_char VARCHAR2)
RETURN BOOLEAN;
--
FUNCTION mlang_street  (p_street VARCHAR2)
RETURN VARCHAR2;
--
END  s_dl_hem_utils;
/

