--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO          WHEN         WHY
--  1.0                                         Initial Creation
--  1.1     6.10      AJ           18/12/15     added change control v610
--                                              when doing HAD data load and
--                                              added drop index so you don't
--                                              get error message if run when
--                                              they already exist 
--
--***********************************************************************
--
drop index tcy_alt_ref_i;
create index tcy_alt_ref_i
    on tenancies(tcy_alt_ref,tcy_refno);

drop index app_legacy_ref_i; 
create index app_legacy_ref_i
    on applications(app_legacy_ref,app_refno);

drop index srq_alternative_refno_i;
create index srq_alternative_refno_i
    on service_requests(srq_alternative_refno,srq_no);

drop index aus_object_reference_i;	
create index aus_object_reference_i
     on address_usages(aus_object_reference,aus_aut_fao_code,aus_aut_far_code,
                       aus_start_date);
 