-- Script Name = hdl_dlas_in.sql
---------------------------------------------------------------------- 
--
-- This script replaces all other dlas scripts
--
-- Script to insert Housing DL Load Area records
--
--   Ver   DB Ver  Who    Date     Reason
--   1.0   5.5.0   PJD    24/05/04 Standard Dataload Install Script
--   1.1   5.6.0   PH     12/08/04 Changed PARALLEL RENTS to
--                                 PARALLEL_RENTS
--   1.2   5.10.0  PJD    01/11/06 Changed ins_dla proc to allow for possible
--                                 existance of non-housing product areas.
--   2.0   5.12.0  PH     21/08/07 Removed HAT Address Usages. Added in
--                                 all other dataload areas (HCO, HSC etc)
--                                 added missing question for property elements
--                                 and debit breakdowns
--   2.1   5.12.0  PH     22/08/07 Added NCT_AUN_LINKS
--   2.2   5.13.0  PH     18/09/08 Added in Works Order Versions
--   2.3   5.15.1  PH     27/11/09 Added in Con Sor Products
--   2.4   5.15.1  MB     03/12/09 Added in Homeless instances, 
--                                 answers, stage decisions
--   2.5   5.16.1  PH     19/02/09 Added question for Void Summaries
--   3.0   6.1.1   PH     10/03/10 Added new dataload areas for HPM
--   3.1   6.1.1   PH     27/06/11 Amended text of Void Summaries
--   3.2   6.1.1   MB     29/09/11 Addition of Property Landlords
--   3.3   6.1.1   MB	  25/10/11 Addition of missing questions to Tasks
--                                 and Deliverables for Anchor
--   3.4   6.1.1   MB     12/10/12 Missing Link Tenancies
--   3.5   6.11    AJ     04/03/15 Added Multi Area Data load(MAD)
--                                 Contact Details
--                                 Amended NOTEPAD from HEM to MAD
--                                 line 164 removed
--                                 added as new at line 339
--                                 Added Multi Area Dataload (MAD)
--                                 Other fields Values/History
--   3.6   6.11    PJD    03/06/15 Added extra questions against 
--                                 People and Service Requests
--   3.7   6.10/11 AJ     18/08/15 Added HPL lease and lease_rents
--   3.8   6.10/11 AJ     19/08/15 Amended so p_product_area(e.g.HEM) and
--                                 p_dataload_area(e.g.NOTEPADS) must match
--                                 before an UPDATE is considered else insert,
--                                 as issue was found when inserting a DLAS for a
--                                 NEW product area where the data load area
--                                 already exists.
--  3.9   6.10/11 AJ      21/08/15 HPL PSL_LEASES and PSL_LEASE_RENTS order in list
--                                 moved so they are together and added batch question
--  3.10  6.12    PJD     02/03/16 Added missing batch question for Jobs DL
--  3.11  6.13    AJ      02/03/16 Added HEM ORG_HIERARCHY data load area for Organisations
--  3.12  6.13    AJ      11/03/16 removed redundant batch question for Service Requests DL 
--                                 'HRM','SERVICE_REQUESTS','Y','Retain Existing SRQ Refno'
--  3.13  All     AJ      19/10/16 Remove update batch question from HPM Tasks data loader
--                                 as for specific to Anchor only
--  3.14  6.13    PJD     03/11/16 Payment Methods was missing
--  3.15  6.14	  PH	  14/02/17 Changed HPP party applications to look for package directly as
--                                 package name doesn't seem to find it and validation fails
--  3.16  6.14/15 AJ      27/03/17 Added batch question No.2 to MAD contact details for
--                                 organisation contacts create organisation_contact record if not found 
--  3.17  6.14/5  AJ      28/03/17 Removed batch question to contact details for
--                                 organisation contacts changed to Y/N fields in file
--  3.18  6.14/5  AJ      28/03/17 Changed nulls for blanks for contact_details
--  3.19  6.14/15 AJ      06/04/17 Added HEM-ORG_ADMIN_UNITS with batch Question Update Current Record?
--  3.19  6.14/5  AJ      18/07/17 Removed batch question for HEM People for 'Default Address From Tenancies'
--                                 as old and no longer in use Other batch question should be in its place
--                                 concerning mandatory Surname for HOU and BOTH Parties
--  3.20  6.14/5  ??      21/03/17 Update Change Control ONLY - AJONES
--                                 ARREARS_ARRANGE_PAY_METHODS added (13th Mar 2017) also INOVICE and PAYMENT_BALANCES
--                                 balances added as some point
--  3.21  6.14/5  AJ      21/03/17 Added the following as found in previous version but commented out as could not
--                                 find associated DL definitely not in standard folders
--                                 HSC - ACTIVE_SCP_EST COMP_ACTUALS COMP_EST INACTIVE_SCP_EST
--  3.22  6.14/5  AJ      19/07/18 Commented out 'HAT','APPLIC_LIST_ENTRY_HISTO' as now 'APPLIC_LIST_ENT_HIST' from
--                                 v614 version onwards
--  3.23  6.14/5  AJ      23/07/18 1) Also added 'HAT','APP_LEGACY_REF','ANSWER_HISTORY' and 'INVOLVED_PARTY_HIST'all
--                                 added  from v614 version onwards
--                                 2) HAT ORGANISATION OFFERS removed as Bespoke and has separate dlas script
--                                 3) HEM Tenancies - added batch question around void summaries
--                                 4) Added new/missing dlas for HSC  SCI_SERVICE_CHARGE_ITEMS HSC  SUNDRY_INV_ITEMS
--                                 originally added by PJD in an other version  
--
------------------------------------------------------------------------------- 
--
--
set serverout on size 1000000
--
DECLARE
--
PROCEDURE ins_dla(p_product_area VARCHAR2, p_dataload_area VARCHAR2,
                  p_load_allowed VARCHAR2, p_question1     VARCHAR2,
                  p_question2    VARCHAR2, p_package_name  VARCHAR2) is
--
CURSOR c1 (p_dataload_area VARCHAR2, p_product_area VARCHAR2) IS
SELECT 
DLA_PRODUCT_AREA  
,DLA_LOAD_ALLOWED  
,DLA_QUESTION1     
,DLA_QUESTION2     
,DLA_PACKAGE_NAME  
FROM   dl_load_areas
WHERE  dla_dataload_area = p_dataload_area
AND    dla_product_area = p_product_area;
--
l_product_area VARCHAR2(3);
l_load_allowed VARCHAR2(1);
l_question1    VARCHAR2(50);
l_question2    VARCHAR2(50);
l_package_name VARCHAR2(30);
--
BEGIN
--
l_product_area := NULL;
l_load_allowed := NULL;
l_question1    := NULL;
l_question2    := NULL;
l_package_name := NULL;
--
OPEN c1(p_dataload_area, p_product_area);
FETCH c1 INTO l_product_area, l_load_allowed,
              l_question1,    l_question2,
              l_package_name;
CLOSE c1;
--
BEGIN
--
IF l_product_area IS NULL THEN
 
  dbms_output.put_line('Dataload Area '||p_product_area||' '||
                     p_dataload_area||' will be inserted.'); 
 INSERT INTO dl_load_areas 
  (DLA_PRODUCT_AREA  ,DLA_DATALOAD_AREA 
   ,DLA_LOAD_ALLOWED ,DLA_QUESTION1     
   ,DLA_QUESTION2    ,DLA_PACKAGE_NAME)
  values 
  (p_product_area, p_dataload_area, 
   p_load_allowed, p_question1, 
   p_question2,    p_package_name);
 
ELSIF
   (nvl(p_product_area,'~')  != nvl(l_product_area,'~')
    OR
    nvl(p_load_allowed,'~') != nvl(l_load_allowed,'~')
    OR
    nvl(p_question1,'~')    != nvl(l_question1,'~')
    OR
    nvl(p_question2,'~')    != nvl(l_question2,'~')
    OR
    nvl(p_package_name,'~') != nvl(l_package_name,'~')
   )
   THEN
     dbms_output.put_line('Dataload Area '||p_product_area||' '||
                           p_dataload_area||' will be updated');

     UPDATE dl_load_areas
     SET 
     DLA_PRODUCT_AREA     = p_product_area  
    ,DLA_LOAD_ALLOWED     = p_load_allowed
    ,DLA_QUESTION1        = p_question1
    ,DLA_QUESTION2        = p_question2
    ,DLA_PACKAGE_NAME     = p_package_name
     WHERE dla_dataload_area = p_dataload_area
     AND   dla_product_area = p_product_area;
--
dbms_output.put_line('Dataload Area '||p_product_area||' '||
                     p_dataload_area||' updated.');
END IF;
EXCEPTION
WHEN OTHERS 
THEN
dbms_output.put_line('Error encountered with above action...continuing with next record');
END;
--
END ins_dla;
--
--
BEGIN
--
ins_dla('HAT','APPLICATIONS','Y','','','');
--
ins_dla('HAT','APPLIC_LIST_ENTRIES','Y','','','');
--
ins_dla('HAT','APPLIC_LIST_ENT_HIST','Y','','','');
--
ins_dla('HAT','APPLIC_LIST_STAGE_DECIS','Y','','','APPLIC_LIST_STAGE');
--
ins_dla('HAT','APPLIC_LIST_STAGE_DECISIONS','Y','','','APPLIC_LIST_STAGE');
--
ins_dla('HAT','APPLIC_STATUSES','Y','','','');
--
ins_dla('HAT','GENERAL_ANSWERS','Y','','','');
--
ins_dla('HAT','HML_APPLICATIONS','Y','','','');
--
ins_dla('HAT','INVOLVED_PARTIES','Y','','','');
--
ins_dla('HAT','INVOLVED_PARTY_ANSWERS','Y','','','INVOLVED_PARTY_ANS');
--
ins_dla('HAT','INVOLVED_PARTY_HIST','Y','','','');
--
ins_dla('HAT','LETTINGS_AREA_ANSWERS','Y','','','LETTINGS_AREA_ANS');
--
ins_dla('HAT','MEDICAL_ANSWERS','Y','','','');
--
ins_dla('HAT','MEDICAL_REFERRALS','Y','','','');
--
ins_dla('HAT','APP_LEGACY_REF','Y','','','');
--
ins_dla('HAT','ANSWER_HISTORY','Y','','','');
--
ins_dla('HEM','ADDRESSES','Y','Allow Automatic Reformatting of FFA Addresses','','');
--
ins_dla('HEM','ADMIN_GROUPINGS','Y','','','');
--
ins_dla('HEM','ADMIN_PROPERTIES','Y','','','');
--
ins_dla('HEM','ADMIN_UNITS','Y','','','');
--
ins_dla('HEM','PEOPLE','Y','Must Surnames be supplied for People','','');
--
ins_dla('HEM','PEOPLE_MERGE','Y','','','');
--
ins_dla('HEM','PROPERTIES','Y','PROPERTY UPDATE ALLOWED','Create PreAllocated Pay Refs','');
--
ins_dla('HEM','PROPERTY_ELEMENTS','Y','End Existing Elements','','');
--
ins_dla('HEM','PROPERTY_GROUPINGS','N','','','');
--
ins_dla('HEM','PROPERTY_STATUSES','N','','','');
--
ins_dla('HEM','TENANCIES','Y','Override Void Property Status?','','');
--
ins_dla('HEM','LINK_TENANCIES','Y','','','');
--
ins_dla('HEM','TENANCY_PEOPLE','Y','','','');
--
ins_dla('HEM','TERMINATE_TENANCY','Y','','','');
--
ins_dla('HEM','PROPERTY_LANDLORDS','Y','','','');
--
ins_dla('HEM','ORG_HIERARCHY','Y','','','');
--
ins_dla('HEM','ORG_ADMIN_UNITS','Y','Update Current Record','','');
--
ins_dla('HPM','SURVEY_ADDRESSES','Y','','','');
--
ins_dla('HPM','SURVEY_RESULTS','Y','','','');
--
ins_dla('HPM','CONTRACTS','Y','','','');
--
ins_dla('HPM','CONTRACT_ADDRESSES','Y','','','');
--
ins_dla('HPM','DELIVERABLES','Y','Update Planned Start Date Only?','','');
--
ins_dla('HPM','TASK_GROUPS','Y','','','');
--
ins_dla('HPM','TASKS','Y','','','');
--
ins_dla('HPM','PAYMENT_TASK_DETS','Y','','','');
--
ins_dla('HPM','CONTRACT_SORS','Y','','','');
--
ins_dla('HPM','DELIVERABLE_COMPONENTS','Y','','','');
--
ins_dla('HPP','PP_APPLICATIONS','Y','Using Tcy Refno in place of Alt Ref?','','');
--
--ins_dla('HPP','PP_APPLICATION_PARTIES','Y','Using Par Refno in place of Alt Ref?','','PP_APPLN_PARTIES');
ins_dla('HPP','PP_APPLN_PARTIES','Y','Using Par Refno in place of Alt Ref?','','');
--
ins_dla('HPP','PP_EVENTS','Y','','','');
--
ins_dla('HPP','PP_TENANCY_HISTORIES','Y','Using Par Refno in place of Alt Ref?','','');
--
ins_dla('HPP','PP_TENANT_IMPROVS','Y','','','');
--
ins_dla('HPP','PP_VALUATIONS','Y','','','');
--
ins_dla('HPP','PP_VALUATION_DEFECTS','Y','','','');
--
ins_dla('HRA','ACCOUNT_ARREARS_ACTIONS','Y','','','ACCOUNT_ARREARS_ACT');
--
ins_dla('HRA','ACCOUNT_BALANCES','Y','','','');
--
ins_dla('HRA','ARREARS_ARRANGEMENTS','Y','','','');
--
ins_dla('HRA','ARREARS_INSTALLMENTS','Y','','','');
--
ins_dla('HRA','ARREARS_ARRANGE_PAY_METHODS','Y',null,null,null);
--
ins_dla('HRA','DEBIT_BREAKDOWNS','Y','Create Property Element','End Existing Charges','');
--
ins_dla('HRA','DEBIT_DETAILS','Y','','','');
--
ins_dla('HRA','PARALLEL_RENTS','N','','','');
--
ins_dla('HRA','REVENUE_ACCOUNTS','Y','','','');
--
ins_dla('HRA','TRANSACTIONS','Y','','','');
--
ins_dla('HRA','PAYMENT_METHODS','Y','','','');
--
ins_dla('HRA','VOID_SUMMARIES','N','Skip Removal of HRA069 Entries from Batch Runs','','');
--
ins_dla('HRA','NCT_AUN_LINKS','Y','','','');
--
ins_dla('HRM','SCHEDULE_OF_RATES','Y',null,null,null);
--
ins_dla('HRM','WORK_DESCRIPTIONS','Y',null,null,null);
--
ins_dla('HRM','CON_SITE_PRICES','Y',null,null,null);
--
ins_dla('HRM','SERVICE_REQUESTS','Y',null,null,null);
--
ins_dla('HRM','WORKS_ORDERS','Y',null,null,null);
--
ins_dla('HRM','JOBS','Y','Update Budgets and Rep Trans',null,null);
--
ins_dla('HRM','INSPECTIONS','Y',null,null,null);
--
ins_dla('HRM','WORKS_ORDER_VERSIONS','Y',null,null,null);
--
-- commented out not standard data loaders (AJ)
--
--ins_dla('HSC','ACTIVE_SCP_EST','Y','','','');
--
--ins_dla('HSC','COMP_ACTUALS','Y','','','');
--
--ins_dla('HSC','COMP_EST','Y','','','');
--
--ins_dla('HSC','INACTIVE_SCP_EST','Y','','','');
--
ins_dla('HSC','LEASES','Y','','','');
--
ins_dla('HSC','LEASE_ASSIGNMENTS','Y','','','');
--
ins_dla('HSC','LEASE_PARTIES','Y','Using Par Refno in place of Alt Ref?','','');
--
ins_dla('HSC','LEASE_SUMMARIES','Y','','','');
--
ins_dla('HSC','SERVICE_CHARGE_BASES','Y','','','');
--
ins_dla('HSC','SERVICE_CHARGE_RATES','Y','','','');
--
ins_dla('HSC','SERVICE_ASSIGNMENTS','Y','','','');
--
ins_dla('HSC','SERVICE_USAGES','Y','','','');
--
ins_dla('HSC','INVOICE_PARTIES','Y','','','');
--
ins_dla('HSC','CUST_INV_ARREARS_ACT','Y','','','');
--
ins_dla('HSC','CREDIT_ALLOCATIONS','Y','','','');
--
ins_dla('HSC','INVOICE_BALANCES','Y','','','');
--
ins_dla('HSC','PAYMENT_BALANCES','Y','','','');
--
ins_dla('HSC','SCI_SERVICE_CHARGE_ITEMS','Y','','','');
--
ins_dla('HSC','SUNDRY_INV_ITEMS','Y','','','');
--
ins_dla('HCO','PRODUCTS','Y','','','');
--
ins_dla('HCO','CON_SOR_PRODUCTS','Y','','','');
--
ins_dla('HCO','SOR_PRDT_SPECIFICATN','Y','','','');
--
ins_dla('HCO','STORES','Y','','','');
--
ins_dla('HCO','STORE_STOCK_ITEMS','Y','','','');
--
ins_dla('HRM','SOR_EFFORT','Y','','','');
--
ins_dla('HRM','CON_SOR_EFFORT','Y','','','');
--
ins_dla('HCO','DEPOTS','Y','','','');
--
ins_dla('HCO','COS_DEPOTS','Y','','','');
--
ins_dla('HCO','TEAMS','Y','','','');
--
ins_dla('HCO','VEHICLE_OPERATIVES','Y','','','');
--
ins_dla('HCO','OPERATIVE_TYPE_GRADES','Y','','','');
--
ins_dla('HCO','OPERATIVE_DETAILS','Y','','','');
--
ins_dla('HCO','OPERATIVE_SKILLS','Y','','','');
--
ins_dla('HRM','SOR_CMPT_SPECS','Y','','','');
--
ins_dla('HRM','SOR_COMPONENTS','Y','','','');
--
ins_dla('HRM', 'CON_SOR_CMPT_SPECS','Y','','','');
--
ins_dla('HRM','CON_SOR_COMPONENTS','Y','','','');
--
ins_dla('HPL','PSL_LEASES','Y','','','');
--
ins_dla('HPL','PSL_LEASE_RENTS','Y','End Existing?','','');
--
ins_dla('HAT','HLESS_INSTANCES','Y','','','');
--
ins_dla('HAT','HLESS_INS_ANSWERS','Y','','','');
--
ins_dla('HAT','HLESS_INS_STAGE_DECIS','Y','','','');
--
ins_dla('MAD','CONTACT_DETAILS','Y','End Existing Contact Method','','');
--
ins_dla('MAD','NOTEPADS','Y','','','');
--
ins_dla('MAD','OTHER_FIELD_VALUES','Y','','','');
--
ins_dla('MAD','OTHER_FIELD_VAL_HIST','Y','','','');
--
END;
/

