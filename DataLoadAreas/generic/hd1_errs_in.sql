-- Script Name = hd1_errs_in.sql
---------------------------------------------------------------------- 
--
-- Script to insert HDX Error messages
--
--   Ver   DB Ver  Who    Date        Reason
--   1.0   5.8.0   PJD    2005        New HD1 error code prefix
--   1.1   5.8.0   VST	  12-2005     Added errors for Service Charge Bases
--   1.2   5.9.0   PH     19-DEC-2005 New errors for Link Tenancies
--   1.3   5.9.0   VRS    23-FEB-2006 New Codes for Placements Dataload
--   1.4   5.9.0   VRS    23-MAR-2006 New Codes for PSL_LANDLORD_PAYMENT_HDRS
--                                    & PSL_LANDLORD_PAY_DTLS Dataload
--
--   1.5   5.9.0   VRS    04-APR-2006 New Error Codes for Placement Rooms Datalaod
--   1.6   5.9.0   PH     05-MAY-2006 New code for Service Usages (101)
--   1.7   5.9.0   PH     11-MAY-2006 New codes for Bespoke Interested Party 
--                                    Usages (102-106)
--   1.8   5.9.0   PH     12-MAY-2006 New Error Codes for Invoice Parties
--                                    Dataload  (107-110)
--   1.9   5.10.0  PH     19-MAY-2006 NEw error for Job Status introduced into
--                                    5.10 code. (111)
--   1.10  5.9.0   PH     22-MAY-2006 New error codes for Customer Invoice Arrears
--                                    Actions Dataload (112-114).
--
--   1.11  5.9.0   VRS    13-JUN-2006 New Codes for Service Charge Items Dataload
--
--   1.12  5.9.0   VRS    13-JUN-2006 New Codes for Service Charge Invoice Balances 
--                                    Dataload
--
--   1.13  5.9.0   VRS    13-JUN-2006 New Codes for Service Charge Customer Invoices 
--                                    Dataload
--
--   1.14  5.9.0   VRS    22-JUN-2006 New Codes for Major Works Items Dataload
--                                    
--   1.15  5.9.0   VRS	  26-JUN-2006 New Codes for Credit Allocations Dataload
--
--   1.16  5.10.0  VRS    21-JUL-2006 New Codes for Account Invoice Category Pay Profiles
--                                    Dataload
--
--   1.17  5.10.0  VRS    24-JUL-2006 New Codes for Invoice Instalment Plans Dataload
--
--   1.18  5.10.0  VRS    24-JUL-2006 New Codes for Invoice Instalments Dataload
--
--   1.19  5.10.0  PH     16-AUG-2006 New Codes for SOR Products Dataload.
--   1.20  5.10.0  PH     21-SEP-2006 New codes for service_charge_rates
--                                    dataload  (189,190) 
--   1.21  5.10.0  PJD    05-OCT-2006 Codes for Property Landlord errors
--                                    assigned to range 201 to 216
--   1.22  5.10.0  PH     20-OCT-2006 New Validation Checks on Revenue Accounts
--                                    Class Codes (217 to 219)
--   1.23  5.10.0  PH     23-OCT-2006 New error for Debit Breakdowns (220)
--   1.24  5.10.0  PH     25-OCT-2006 New errors for Revenue Accounts (221, 222)
--   1.25  5.10.0  PH     02-NOV-2006 New error code for Transactions (223)
--   1.26  5.10.0  PH     02-NOV-2006 New error code for Leases (224)
--
--   1.27  5.10.0  VRS    08-JAN-2007 New error code for Contractor Products (225)
--
--   1.28  5.10.0  VRS    24-FEB-2007 New error codes for SOR_COMPONENTS Load (226 - 234)
--
--   1.29  5.10.0  VRS    24-FEB-2007 New error codes for SOR_CMPT_SPECIFICATIONS Load (235 - 239)
--   1.30  5.10.0  PH     09-MAR-2007 New codes for People (staff id and cos code)
--   1.31  5.10.0  PH     16-APR-2007 New code for debit Breakdowns (246)
--   1.32  5.10.0  PH     30-APR-2007 New codes for Revenue Accounts (247, 248)
--   1.33  5.10.0  VRS    11-MAY-2007 New code for Revenue Accounts (249)
--
--   1.34  5.10.0  VRS    29-MAY-2007 New codes for CON_SOR_CMPT_SPECIFICATIONS (250 - 261)
--
--   1.35  5.10.0  VRS    10-JUL-2007 New codes for CON_SOR_COMPONENTS (262 - 268)
--   2.0   5.12.0  PH     16-JUl-2007 New codes for People Dataload (269 - 273)
--                                    New Codes for Applications and Homeless
--                                    Applications (274 - 275)
--                                    Corrected spelling mistake on 231.
--   2.1   5.12.0  PH     20-JUl-2007 New code for CON SOR Products Dataload (276)
--   2.2   5.12.0  PH     21-AUG-2007 New code for Debit Breakdowns 277 (and
--                                    corrected line above as wrong version).
--   2.3   5.12.0  VRS    05-SEP-2007 New Codes for Invoice Payment Profiles 
--				      Dataload.(278 - 290)
--
--   2.4   5.12.0  VRS    12-SEP-2007 New Codes for Contracts Dataload.(291 - 329)
--
--   2.5   5.12.0  VRS    13-SEP-2007 New Codes for Contract_addresses Dataload.
--                                    (330 - 337)
--
--   2.6   5.12.0  VRS    14-SEP-2007 New Codes for Deliverables Dataload.
--                                    (338 - 355)
--
--   2.6   5.12.0  VRS    20-SEP-2007 New Codes for Task_groups Dataload.
--                                    (356 - 366)
--
--   2.7   5.12.0  VRS    24-SEP-2007 New Codes for Task/Task_Versions Dataload.
--                                    (367 - 392)
--
--   2.8   5.12.0  VRS    27-SEP-2007 New Codes for Contracts Dataload.(393-395)
--                                    New Codes for Contract_Addresses Dataload
--				      (396-397)
--                                    New Code for Task/Task_Versions Dataload
--				      (398)
--
--   2.9   5.12.0  PH     12-OCT-2007 New code for Contracts Dataload.(399-404)
--
--   3.0   5.12.0  VRS    16-OCT-2007 New code for Payment Task Details Dataload
--                                    (405 - 416)
--   3.1   5.12.0  PH     29-OCT-2007 Reserved 417 to 499 for New South Wales
--                                    Land Titles Dataload
--                                    New code for General Answers (500)
--   3.2   5.12.0  PH     01-NOV-2007 New code for Deliverables (501)
--   3.3   5.12.0  PH     14-NOV-2007 New codes for tasks (502-506)
--   3.4   5.12.0  PH     14-NOV-2007 New code for Other Fields (507)
--   3.5   5.12.0  PH     16-NOV-2007 New code for Contracts (508)
--
--   3.6   5.12.0  VRS    20-NOV-2007 New code for Documents Dataload (509 - 516)
--   3.4   5.12.0  PH     06-DEC-2007 New code for Other Fields (517)
--   3.7   5.12.0  VRS    08-FEB-2008 New code for MIDLAND HEART DELIVERABLES_UPD
--                                    Dataload (518)
--   3.8   5.12.0  PH     04-MAR-2008 New code for Operative Skills (522)
--   3.9   5.12.0  PH     04-MAR-2008 New code for Revenue Accounts (523, 524)
--                                    New code for Jobs (525, 526)
--   3.10  5.12.0  PH     05-MAR-2008 New code for Survey Results 527
--   3.11  5.12.0  PP     20-APR-2008 New codes for Stock Adjustments (528 - 533)
--   3.12  5.13.0  PH     25-APR-2008 New code for Void Transactions (534 - 538)
--   3.13  5.13.0  PH     28-APR-2008 New code for Void Debit Details (539)
--   3.14  5.13.0  PH     08-AUG-2008 New Code for Property Elements (540)
--   3.15  5.13.0  PH     15-SEP-2008 New Code for Tenancies (541)
--   3.16  5.14.0  PH     23-SEP-2008 New codes for bespoke DDI dataloads (542 to 549)
--   3.17  5.15.0  PH     23-FEB-2009 New code for Payment Details (550)
--   3.18  5.15.0  PH     06-APR-2009 New Codes for Address Usages (551 to 556)
--   3.19  5.15.0  PH     01-MAY-2009 New code for Deliverables (557)
--   3.20  5.15.0  PP     08-MAY-2009 New codes for bespoke Survey Answers (558 to 566)
--   3.21  5.15.0  PH     11-AUG-2009 New codes for Tasks (568)
--   3.22  5.16.1  PH     28-AUG-2009 New Code for Other Fields (569)
--   3.23  5.16.1  VS     21-SEP-2009 New Code for Other Fields (570-573)
--   3.24  5.16.1  VS     23-SEP-2009 New Code for Survey Answers (574-588)
--   3.25  5.16.1  VS     23-SEP-2009 New Code for Survey Answers Histories (589)
--   3.26  5.16.1  VS     23-SEP-2009 New Code for Business Actions (590-607)
--   3.27  5.16.1  VS     29-SEP-2009 New Code for Business Actions (608-619)
--   2.28  5.16.1  PH     23-OCT-2009 New Codes for Contract SOR Dataload.(620-626)
--   3.29  5.16.1  VS     24-OCT-2009 New Code for Business Actions (627-628)
--   3.30  5.16.1  VS     28-OCT-2009 New Code for Issued Surveys (629)
--   3.31  5.16.1  VS     28-OCT-2009 New Code for Issued Survey Questions (630 - 631)
--   3.32  5.16.1  VS     28-OCT-2009 New Code for Survey Answers (632-633)
--   3.33  5.16.1  VS     28-OCT-2009 New Code for Survey Answer Histories (634)
--   3.34  5.16.1  VS     03-NOV-2009 New Code for Other Fields (635 - 637)
--   3.34  5.16.1  VS     03-NOV-2009 New Code for Other Fields RESERVED 638-670
--   3.35  5.16.1  PH     17-NOV-2009 New Code for Oxford Bespoke Other Fields (671)
--   3.36  5.16.1  PH     18-NOV-2009 New Code for Anchor Bespoke Non Access
--                                    Dataload (672-679)
--   3.37  5.16.1  PH     21-DEC-2009 New code for Anchor bespoke Other Fields
--                                    dataload (686-687)
--   3.38  5.16.1  PH     22-DEC-2009 New code for Anchor bespoke contract sors
--                                    dataload (688-691)
--   3.39  5.16.1  PH     20-JAN-2010 New code for Anchor bespoke contract sors
--                                    dataload (692-693)
--   3.40  5.16.1  CB     25-JAN-2010 New code for Bracknell Forest bespoke XY coords
--                                    dataload (694)
--   3.41  5.16.1  PH     02-FEB-2010 New codes for Anchor Bespoke Update Survey
--                                    Answers (696 to 705)
--   3.42  5.16.1  PH     15-FEB-2010 New codes for Operative Details (706/707)
--   3.43  5.16.1  VS     22-APR-2010 New codes for deliverable_cmpts (708-711)
--                                    for defect id 4235
--   3.44  5.16.1  MB     14-MAY-2010 New codes for bespoke loaders for OHMS migrations:
--                                    Elements, Attributes, APT codes, APT Attributes, 
--                                    Lettings AReas(712 - 727)
--   3.45  5.16.1  MB     20-MAY-2010 New codes for additional HPM validation for HNSW
--                                    (728-734)
--
--   3.46  5.16.1  VS     28-MAY-2010 New code for additional HPM Contract Addresses
--                                    validation for HNSW(735)
--
--   3.47  5.16.1  VS     02-JUN-2010 New code for Other Fields validation for HNSW(663-664)
--   3.48  5.17.1  PH     22-JUN-2010 Amended text for 686. Also added new Codes
--                                    for GHA Bespoke (736 to 738)
--   3.49  5.17.1  PH     02-NOV-2010 New codes for bespoke scanned documents
--                                    (739 to 743)
--   3.50  5.17.1  PH     02-DEC-2010 New codes for bespoke payment profiles
--                                    (744)
--   3.51  5.17.2  MK     03-JAN-2011 New codes for bespoke involved parties (191)
--                                    (744)
--   3.52  5.17.2  VS     07-JAN-2011 Renumbering STEVENAGE codes 620-626 for SDL110.sh WO_UPLOAD script
--                                    to start from 745-751 to avoid them being over written after upgrades.
--                                    (744)
--
--   3.53  5.17.2  VS     16-FEB-2011 Adding new codes for Housing NZ contact details dataload
--                                    (752-755)
--
--   3.54  6.3.0    MB    08-APR-2011 Expected Payments bespoke loads re-indexed incorrect error numbers
--                                    (756 - 776)
--   3.55  6.3.0   PH     11-APR-2011 New code for Other Fields (777 - lucky)
--
--   3.56  6.4.0   MK     MAY-2011    Codes for HNZC Assessments bespoke dataloads (778 - 819)
--
--   3.57  6.4.0   MB/VS  06-MAY-2011 First few Contacts and Cont_bus_reasons errors (820 - 856)
--   3.58  6.4.0   PH     13-MAY-2011 More codes for bespoke scanned documents
--                                    (857 to 858)
--
--   3.59  6.4.0   VS     13-MAY-2011 Housing NZ Contacts errors (859 - 860)
--
--   3.60  6.4.0   VS     13-JUN-2011 Housing NZ Contacts errors (862)
--
--   3.61  6.4.0   VS     17-JUN-2011 Housing NZ Subject Contact bus reasons errors (863)
--
--   3.62  6.5.0   VS     04-JUL-2011 Housing NZ Income Details error codes (864-865)
--   3.63  6.5.0   PH     02-MAR-2012 New codes for YHN Bespoke (866 and 867)
--   3.64  6.5.0   PH     15-MAR-2012 New code for YHN Bespoke (868)
--   3.64  6.5.0   PJD    13-APR-2012 New code for Transactions linked to Invoices (870)
--   3.65  6.5.0   PH     27-JUL-2012 Merged two versions which is why we have
--                                    two 3.64. Added new codes for Contractors
--   3.66  6.9.0   AJ     19-JUN-2014 Updated error code wording (103)
--   3.70  6.10.0  PAH    30-JAN-2015 Added 877-879 for account service charge status
--   3.71  6.12    AJ     13-NOV-2015 Added 428-471 Land Titles Data Load errors for NSW
--                                    and Queensland errors originally supplied in the sql
--                                    hd1_errs_in_ltl.sql 
--   3.72  6.13    AJ     16-MAR-2016 Added 418-427 for Land Titles Data Load errors for NSW
--                                    and Queensland errors gone awol on a presumed bespoke
--                                    version (feb2008) were not in hd1_errs_in_ltl.sql
--   3.73  6.13    AJ     25-APR-2016 Added 896 Status code check for Properties DL as HDL734
--                                    wording has been changed over the years and now wrong
--   3.74  6.13    AJ     27-APR-2016 removed error 896 as change duplicate of one done by PAH
--                                    and error HD2 890 done on the 25th also
--   3.75  6.13    PJD    07-JUN-2016 error 896 now used for PPP check 
--   3.76  6.13    AJ     25-JUL-2016 Reference to V5 removed from 50,56,205,206,211 and 212
--   3.77  6.16    AJ  05/06-FEB-2018 Further System General Contacts errors added (665-670)
--                                    which were previously reserved
--   3.77a 6.15    VRS    08-FEB-2018 Added additional validation for Wandsworth Bespoke Properties DL
--                                    3.77 now 3.77a ADDED to main version as nearlt copied over(AJ 15-Mar-2018)
------------------------------------------------------------------------------- 
--
--
set serverout on size 1000000
--
DECLARE
--
PROCEDURE ins_err(p_err_refno NUMBER,p_err_text VARCHAR2) is
--
CURSOR c1 IS
SELECT err_message
FROM   errors
WHERE  err_refno = p_err_refno
AND    err_object_shortname = 'HD1';
--
l_err_message VARCHAR2(120);
--
BEGIN
--
l_err_message := NULL;
--
OPEN c1;
FETCH c1 INTO l_err_message;
CLOSE c1;
--
IF l_err_message IS NULL THEN
  INSERT INTO errors 
  (err_object_shortname,err_refno,err_message,err_type,err_popup)         
  values('HD1',p_err_refno,         
  p_err_text,        
  'V','N');
dbms_output.put_line('Error number '||p_err_refno||' inserted.');
ELSIF
   l_err_message != p_err_text       
   THEN
     UPDATE errors
     SET    err_message = p_err_text    
     WHERE  err_object_shortname = 'HD1'
       AND   err_refno = p_err_refno;
dbms_output.put_line('Error number '||p_err_refno||' updated.');
END IF;
--
END ins_err;
--
--
BEGIN
ins_err(1,  'Question reference already exists on Questions');
ins_err(2,  'Combination of Question group and sequence already exist on Questions');
ins_err(3,  'Summary sequence already exists on Questions');
ins_err(4,  'Question reference must be supplied');
ins_err(5,  'Question type must be one of PQU or DNQ');
ins_err(6,  'Invalid answer type Value');
ins_err(7,  'Question Group Code does not exist on QUESTION_GROUPS');
ins_err(8,  'Invalid Question Category Code');
ins_err(9,  'Question sequence must be supplied');
ins_err(10, 'Question summary sequence must be supplied');
ins_err(11, 'Question group sequence must be supplied');
ins_err(12, 'Summary description must be supplied');
ins_err(13, 'Question description must be supplied');
ins_err(14, 'Child question must have parent question reference');
ins_err(15, 'Derived question must have question header reference');
ins_err(16, 'Disable Index Indicator must be Y or N');
ins_err(17, 'Parent Child Indicator must be P, C or N');
ins_err(18, 'Call Procedure Indicator must be Y or N');
ins_err(19, 'Property Related Indicator must be Y or N');
ins_err(20, 'History Required Indicator must be Y or N');
ins_err(21, 'Show Summary Indicatory must be Y or N');
ins_err(22, 'Model Indicator must be Y or N');
ins_err(23, 'Required Indicator must be Y or N');
ins_err(24, 'Scheme code does not exist in Priority Schemes');
ins_err(25, 'No Question/Question Group combination could be found');
ins_err(26, 'Repair Level must be JOB or WO');
ins_err(27, 'Extract from Repairs Indicator must be Y or N');
ins_err(28, 'Include Repairs from Properties Indicator must be Y or N');
ins_err(29, 'Admin Unit Type does not exist in ADMIN_UNIT_TYPES');
ins_err(30, 'Discount Scheme Code does not exists in Discount Schemes');
ins_err(31, 'Link Child Tenancy cannot be found on Tenancies');
ins_err(32, 'Link Parent Tenancy cannot be found on Tenancies');
ins_err(33, 'Link Child Tenancy is not a current Tenancy');
ins_err(34, 'Link Parent Tenancy is not a current Tenancy');
ins_err(35, 'Link reason code not found in domain TCY_LINK_REASON');
ins_err(36, 'One tenant must be associated to both link tenancies');
ins_err(37, 'Link Parent Tenancy must not be assigned to a Non Residential Property');
ins_err(38, 'Link Child Tenancy already exists as a Linked Parent');
ins_err(39, 'Link Parent Tenancy already exists as a Linked Child');
ins_err(40, 'Link Start Date must be supplied');
ins_err(41, 'Link Start Date must not be before Child Tenancy Start Date');
ins_err(42, 'Link Start Date must not be before Parent Tenancy Start Date');
--
ins_err(43, 'Placement Legacy Reference must be Supplied');
ins_err(44, 'Placement Reference not assigned by Control File');
ins_err(45, 'Placement already exists');
ins_err(46, 'Placement Offered Date must be Supplied');
ins_err(47, 'Legacy Application Ref or the iWorld Application Ref must be Supplied');
ins_err(48, 'Only Legacy Application Ref or the iWorld Application Ref must be Supplied, not both');
ins_err(49, 'Application Legacy Ref Supplied does not exist');
ins_err(50, 'Internal Application Ref Supplied does not exist');
ins_err(51, 'Placement Status Code must be Supplied');
ins_err(52, 'Placement Status Code is invalid');
ins_err(53, 'Property Ref or the iWorld Property Refno must be Supplied');
ins_err(54, 'Only Property Ref or the iWorld Property Refno must be Supplied, not both');
ins_err(55, 'Property Ref Supplied does not exist on PROPERTIES');
ins_err(56, 'Internal Property Refno does not exist on PROPERTIES');
ins_err(57, 'Placement Effective Status Date must be Supplied');
ins_err(58, 'Placement Status Changed Date must be Supplied');
ins_err(59, 'Placement Start Date must be Supplied');
ins_err(60, 'Placement End Date must be later than Placement Start Date');
ins_err(61, 'Placement Concluded Date must be later than Placement Start Date');
ins_err(62, 'Placement Reasonable Refusal Indicator must be Y or N');
--
ins_err(63, 'Placement Property Room Number must be Supplied');
ins_err(64, 'Property Allow Placement Indicator must be Y or N');
ins_err(65, 'Placement Property Room Maximum Places must be Supplied');
ins_err(66, 'Invalid Placement Property Rooms Cost Period Code Supplied');
--
ins_err(67, 'PSL Lease Reference Number must be Supplied');
ins_err(68, 'Invalid PSL Lease Reference Number Supplied');
ins_err(69, 'Property Landlord Reference Number must be Supplied');
ins_err(70, 'Invalid Property Landlord Reference Number Supplied');
ins_err(71, 'PSL Lease Landlord Payment Header Status Code must be Supplied');
ins_err(72, 'Invalid PSL Lease Landlord Payment Header Status Code Supplied');
ins_err(73, 'More Than 1 RAI Landlord Payment Header Status Code Found');
ins_err(74, 'Total Value of the payment, inclusive of tax must be Supplied');
ins_err(75, 'Payment FROM Date must be Supplied');
ins_err(76, 'Payment TO Date must be Supplied');
ins_err(77, 'Payment Status Date must be Supplied');
ins_err(78, 'Payment Returned Indicator must be Y or N');
ins_err(79, 'Duplicate PSL Lease Landlord Payment Header Record');
ins_err(80, 'Payment FROM Date earlier than the Lease Rent Start Date');
ins_err(81, 'Payment FROM Date greater than the Lease Rent End Date');
ins_err(82, 'Payment Header Status must be HLD when Lease status is HLD');
ins_err(83, 'Payment Header Status is RAI,SCH when Lease status is HBK');
--
ins_err(84, 'Payment Details, Payment Type must be supplied');
ins_err(85, 'Invalid Payment Details Payment Type Supplied. Must be PYDR or PYDN');
ins_err(86, 'Payment Details, Payment Reason Code must be Supplied');
ins_err(87, 'Invalid Payment Details Payment Reason Code Supplied');
ins_err(88, 'Payment Details, Payment Effective Date must be Supplied');
ins_err(89, 'Payment Effective Date must be between Payment Header Payment FROM/TO Date');
ins_err(90, 'Total of Payment Details does not equal Payment Header Credit Amount Inc Tax');
--
ins_err(91, 'Payment Header does not exist for this Payment Detail Record');
--
ins_err(92, 'Placement Dataload Batch Id must be supplied');
ins_err(93, 'Unable to derive Placement Reference for given Batch id and Placement Legacy Ref');
ins_err(94, 'Placement does not exist');
ins_err(95, 'Placement Rooms Start Date must be Supplied');
ins_err(96, 'Placement Rooms Number of Rooms to be Occupied must be Supplied');
ins_err(97, 'Placement Rooms End Date must be later than Placement Rooms Start Date');
ins_err(98, 'Placement Rooms Start Date is Earlier than the Placements Start Date');
ins_err(99, 'Placement Rooms bedroom number is invalid for property');
ins_err(100, 'Placement Rooms Record already exists');
ins_err(101, 'No Service Assignment exists for this Admin Unit, Element, Attribute and Start Date');
ins_err(102, 'Interested Party does not exist');
ins_err(103, 'Invalid Legacy Type, must be one of APP,APP2,TCY,PAPP,SCS,CNT,AUN,PRO,REAL,REGA ');
ins_err(104, 'PP Application does not exist on PP_APPLICATIONS');
ins_err(105, 'Stock Condition Survey does not exist');
ins_err(106, 'Contract does not exist');
ins_err(107, 'Invoice Party already exists for this Account, Person and Start Date');
ins_err(108, 'Record overlaps an existing Invoice Party for the Account and Person');
ins_err(109, 'Invoice Party Start Date is before Account Start Date');
ins_err(110, 'Invoice Party End Date is after Account End Date');
ins_err(111, 'Job Status must be one of CLO COM or UNC');
ins_err(112, 'Effective Date must be supplied');
ins_err(113, 'Total Invoice Balance must be supplied');
ins_err(114, 'Undisputed Balance must be supplied');
--
ins_err(115,'Not all Service Charge Rate values Supplied for SERV Class Code');
ins_err(116,'Not all Service Usage values Supplied for SERV Class Code');
ins_err(117,'The Service Charge Rate and Service Usage must be for the same Service');
ins_err(118,'The Service Charge Rate and Service Usage must be for the same Property');
ins_err(119,'Service Charge Rate values Supplied for non SERV Class Code');
ins_err(120,'Service Usage values supplied for non SERV Class Code');
ins_err(121,'Estimated and Actual Weight values Supplied for non SERV Class Code');
ins_err(122,'Not all Management Cost Group values Supplied for MGMT Class Code');
ins_err(123,'Management Cost Group values supplied for non MGMT Class Code');
ins_err(124,'The associated Customer Liability Invoice must be a SCI_SCHED subtype.');
--
ins_err(125,'Customer Liability Invoice Reference is Mandatory');
ins_err(126,'Invoice Balance Sequence No is Mandatory');
ins_err(127,'Invoice Balance Date is Mandatory');
ins_err(128,'Invoice Total Balance is Madatoty');
ins_err(129,'Invoice Undisputed Balance is Mandatory');
ins_err(130,'Invoice Balance Already Exists');
ins_err(131,'Duplicate Invoice Balance record in Load File');
--
ins_err(132,'A Major Works Project must be assigned for a Major Works Invoice type');
ins_err(133,'A Major Works Project can only be assigned for a Major Works Invoice type');
ins_err(134,'Invalid Major Works Project Code');
ins_err(135,'Person does not exist on Parties table.');
--
ins_err(136,'No matching Customer Invoices found');
ins_err(137,'The associated Customer Liability Invoice must be a MWI subtype.');
ins_err(138,'Major Works Item Sequence No is Mandatory');
ins_err(139,'Customer Invoice Reference/Seqno already exists');
ins_err(140,'Estimated Cost of Work is Mandatory');
ins_err(141,'Major Works Project Reference is Mandatory');
ins_err(142,'Major Works Job Reference is Mandatory');
ins_err(143,'Major Works Project Actual Start Date is Mandatory');
ins_err(144,'Admin Unit Code is Mandatory');
ins_err(145,'Admin Unit Year is Mandatory');
ins_err(146,'Class Code is Mandatory');
ins_err(147,'Invalid Class Code Supplied');
ins_err(148,'Cannot derive mjs_seqno from mw_job_stages');
ins_err(149,'Cannot derive mjsc_refno from mw_job_stage_caps');
ins_err(150,'No PROP_JOB_STAGE_ESTIMATES reference found for MW Project Ref and MW Job Ref');
ins_err(151,'No PROP_JOB_STAGE_ACTUALS reference found for MW Project Ref and MW Job Ref');
--
ins_err(152,'Allocated Credit Amount is Mandatory');
ins_err(153,'Transaction Reference No Credited From is Mandatory');
ins_err(154,'Transaction Credit cannot be allocated to a destination that is also the source of the credit');
ins_err(155,'Invoice Credit cannot be allocated to a destination that is also the source of the credit');
ins_err(156,'Credit must be allocated from either a Transaction, or a Credit Memo, or an Invoice');
ins_err(157,'Credit must be allocated to either a Transaction, or an Invoice (Not Both)');
ins_err(158,'Credit must be allocated to either a Transaction, or an Invoice');
--
ins_err(159,'Payment Reference is Mandatory');
ins_err(160,'Invoice Category Code is Mandatory');
ins_err(161,'Invoice Payment Profile Code is Mandatory');
ins_err(162,'Invalid Payment Reference Supplied');
ins_err(163,'Invalid Invoice Category Code Supplied');
ins_err(164,'Invalid Payment Profile Code Supplied');
ins_err(165,'Invoice Payment Profile does not exist for Category/Payment Profile combination');
ins_err(166,'Record already exists for Account/Category combination');
ins_err(167,'Invalid Direct Debit Instructions Core Reference Supplied');
--
ins_err(168,'Status Code is Mandatory');
ins_err(169,'Starting Balance is Mandatory');
ins_err(170,'Number of Instalments is Mandatory');
ins_err(171,'Customer Liability Invoice Reference not found');
ins_err(172,'Status Code must be (DDC, CAN, RAI, COM, NDD, ACT)');
ins_err(173,'Instalment Plan Already exists for Invoice/Payment Profile/Status Code combination');
--
ins_err(174,'Instalment Number is Mandatory');
ins_err(175,'Instalment Amount is Mandatory');
ins_err(176,'Instalment Payment Due Date is Mandatory');
ins_err(177,'Instalment Plan record not found');
ins_err(178,'Instalment Record already exists');
ins_err(179,'Invalid Batch Run No supplied');
--
ins_err(180,'SOR Code must be supplied');
ins_err(181,'Start Date must be supplied');
ins_err(182,'Product Code must be Supplied');
ins_err(183,'Default Quantity must be supplied');
ins_err(184,'Unit of Measure must be Supplied');
ins_err(185,'A record already exists for this SOR Code and Start Date');
ins_err(186,'Product Code does not exist on Product Table');
ins_err(187,'Product Default Quantity must be > 0');
ins_err(188,'End Date must not be before Start Date');
ins_err(189,'Service Charge Basis does not exist for the Start Date, Period and Element/Attribute');
ins_err(190,'Record already exists on Service Charge Rates');
ins_err(191,'A valid person name change reason code must be supplied');
--
ins_err(201, 'Property Landlord Reference not assigned by Control File');
ins_err(202, 'Property Landlord Reference already exists');
ins_err(203, 'Property Reference must be Supplied');
ins_err(204, 'Property Landlord Start Date must be Supplied');
ins_err(205, 'Either the internal Par Refno or the Alternative Party Ref must be Supplied');
ins_err(206, 'Either the internal Par Refno or the Alternative Party Ref must be Supplied, not both');
ins_err(207, 'Alternative Party Ref Supplied does not exist in PARTIES');
ins_err(208, 'Par Refno Supplied does not exist in PARTIES');
ins_err(209, 'Pay Landlord Directly Indicator must be Y or N');
ins_err(210, 'Landlord Living Abroad Indicator must be Y or N');
ins_err(211, 'Either the internal Agent Par Refno or the Agent Alternative Party Ref must be Supplied');
ins_err(212, 'Either the internal Agent Par Refno or the Agent Alternative Party Ref must be Supplied, not both');
ins_err(213, 'Agent Alternative Party Ref Supplied does not exist in PARTIES');
ins_err(214, 'Agent Par Refno Supplied does not exist in PARTIES');
ins_err(215, 'Property Landlord End Date must be later than Property Landlord Start Date');
ins_err(216, 'Property Landlord Record already exists');
--
ins_err(217, 'Invalid Class Code');
ins_err(218, 'Class code must be REN for Account Type REN');
ins_err(219, 'Class Code must be one of SER, LIA or MWO for Acount Type SER');
--
ins_err(220, 'Debit Breakdown Start Date must not be before Account Start Date');
--
ins_err(221, 'Reporting Property Reference must not be NULL');
ins_err(222, 'Property does not exist for the Lease Property Reference supplied');
ins_err(223, 'Transaction Effective Date is over 20 years old, please check dates');
ins_err(224, 'Status of Leased Property is not Closed');
--
ins_err(225, 'Invalid Issued in Unit (Unit of Measure Code)');
--
ins_err(226, 'External Reference must be supplied');
ins_err(227, 'SOR Component Type must be supplied');
ins_err(228, 'SOR Component Cost must be supplied');
ins_err(229, 'SOR Component Indicator must be supplied');
ins_err(230, 'SOR Cmpt Specification Record does not exists for SOR/Start Date Combination');
ins_err(231, 'Invalid SOR Component Type');
ins_err(232, 'SOR Component Indicator must be Y or N');
ins_err(233, 'SOR Component Cost cannot be less than 0.00');
ins_err(234, 'SOR Component already exists');
--
ins_err(235, 'Component Specification SOR Code must be supplied');
ins_err(236, 'SOR Component Specification Start Date must be supplied');
ins_err(237, 'Invalid Component Specification SOR Code supplied');
ins_err(238, 'Component Specification End Date must not be before Start Date');
ins_err(239, 'SOR Cmpt Specification Record already exists for SOR/Start Date Combination');
--
ins_err(240, 'Staff ID is mandatory for this Interested Party Type');
ins_err(241, 'Staff ID is not allowed for this Interested Party Type');
ins_err(242, 'Contractor Site is mandatory for this Interested Party Type');
ins_err(243, 'Contractor Site is not allowed for this Interested Party Type');
ins_err(244, 'Contractor Site does not exist');
ins_err(245, 'Staff ID exists for another Interested Party');
ins_err(246, 'Rent Elements cannot be Date type');
ins_err(247, 'Account is not in the same REN Admin Unit as associated Property');
ins_err(248, 'Account is not in the same SER Admin Unit as associated Property');
--
ins_err(249, 'Lease Assignment does not exist for supplied lease data');
--
ins_err(250, 'Contractor Site Code must be supplied');
ins_err(251, 'Invalid Contractor Site Code supplied');
ins_err(252, 'Pricing Policy Group Code must be supplied');
ins_err(253, 'Invalid Pricing Policy Group Code supplied');
ins_err(254, 'Work Programme Code must be supplied');
ins_err(255, 'Invalid Work Programme Code supplied');
ins_err(256, 'Pricing Policy Programme Start Date must be supplied');
ins_err(257, 'Contractor Site Price Group Start Date must be supplied');
ins_err(258, 'Contractor SOR Component Specification Start Date must be supplied');
ins_err(259, 'Contractor Site Price Group does not exist');
ins_err(260, 'Contractor SOR Component Specification record already exists');
ins_err(261, 'Contractor Site Prices record does not exists');
--
ins_err(262, 'SOR Component Type Code must be supplied');
ins_err(263, 'Invalid SOR Component Type Code supplied');
ins_err(264, 'Cost of the Overhead Component must be supplied');
ins_err(265, 'Overhead Component Cost must be > 0.00');
ins_err(266, 'Component Cost Type Indicator must be Y or N');
ins_err(267, 'Contractor SOR Component Specification record does not exist');
ins_err(268, 'Contractor SOR Component record already exists');
--
ins_err(269, 'Invalid At Risk Indicator - must be Y or N');
ins_err(270, 'Invalid Nationality Code');
ins_err(271, 'Invalid Sexual Orientation Code');
ins_err(272, 'Invalid Religion Code');
ins_err(273, 'Invalid Economic Status Code');
--
ins_err(274, 'Invalid Source code for the Application');
ins_err(275, 'Housing Advice Case does not exist for supplied Case Reference');
--
ins_err(276, 'Contractor Site Code must be supplied');
--
ins_err(277, 'Current Debit Breakdown starts before Rent Element Rate - Summary Rent would be incorrect');
--
ins_err(278, 'Invoice Payment Profile Code must be supplied');
ins_err(279, 'Invoice Payment Profile Description must be supplied');
ins_err(280, 'Frequency of Instalments must be supplied');
ins_err(281, 'Number of Instalments to create must be supplied');
ins_err(282, 'Instalment collected by Direct Debit Indicator must be supplied');
ins_err(283, 'Allowed Payment Days must be supplied');
ins_err(284, 'Maximum Instalment Value must be supplied');
ins_err(285, 'Minimum Instalment Value must be supplied');
ins_err(286, 'Instalment Remainder Period must be supplied');
ins_err(287, 'Invoice Payment Profile Code already exists');
ins_err(288, 'Frequency of instalments supplied must be W/M/Q/H/Y');
ins_err(289, 'Direct Debit Indicator must be Y or N');
ins_err(290, 'Payment Day of Week must be MON/TUE/WED/THU/FRI/SAT/SUN');
--
ins_err(291, 'Contractor reference must be supplied');
ins_err(292, 'Project Reference must be supplied');
ins_err(293, 'Contractor Status Start Date must be supplied');
ins_err(294, 'Warn Repeairs Users of Planned Works Indicator must be supplied');
ins_err(295, 'Drawings exists Indicator must be supplied');
ins_err(296, 'Reschedule Allowed Indictor must be supplied');
ins_err(297, 'Contract Version Number must be supplied');
ins_err(298, 'Contract Versions Current Indicator must be supplied');
ins_err(299, 'Contract Versions Description must be supplied');
ins_err(300, 'Repeat Planned Work Indicator must be supplied');
ins_err(301, 'Contractor Reference Supplied already exists');
ins_err(302, 'Invalid Project Reference supplied');
ins_err(303, 'Warn Repeairs Users of Planned Works Indicator must be Y or N');
ins_err(304, 'Drawings exists Indicator must be Y or N');
ins_err(305, 'Reschedule Allowed Indicator must be Y or N');
ins_err(306, 'Contractor Site not assigned to the Admin Unit Supplied');
ins_err(307, 'Current Indicator must be Y or N');
ins_err(308, 'Repeat Planned Work Indicator must be Y or N');
ins_err(309, 'Associated Contractor Reference Supplied does not exist');
ins_err(310, 'Non Comp Damages Unit must be DAY/WDAY/WEEK/MNTH/YEAR');
ins_err(311, 'Interim Payment Interval Unit must be DAY/WDAY/WEEK/MNTH/YEAR');
ins_err(312, 'Repeat Period Unit must be DAY/WDAY/WEEK/MNTH/YEAR');
ins_err(313, 'Contract Reference/Contract Version Number Already exists');
ins_err(314, 'Projected Cost cannot be negative');
ins_err(315, 'Projected Cost Tax cannot be negative');
ins_err(316, 'Contract Value cannot be negative');
ins_err(317, 'Maximum Variation Amount cannot be negative');
ins_err(318, 'Maximum Variation Tax Amount cannot be negative');
ins_err(319, 'Non Completion Damages cannot be negative');
ins_err(320, 'Penultimate Retention Percentage cannot be negative');
ins_err(321, 'Interim Retention Percentage cannot be negative');
ins_err(322, 'Liability Period must be in the range 1 - 999');
ins_err(323, 'Interim Payment Interval must be in the range 1 - 999');
ins_err(324, 'Final Measurement Period must be in the range 1 - 999');
ins_err(325, 'Maximum No Of Repeats must be in the range 0 - 999');
ins_err(326, 'Only one Contract Version for the Contract may have the Current Indicator set to Y');
ins_err(327, 'Budget Calander Year must be supplied');
ins_err(328, 'Invalid Budget Head Code Supplied');
ins_err(329, 'Budget Reference not found for budget head code and budget calendar year');
--
ins_err(330,'Contract Address Property Reference or Admin Unit Code is Manadatory');
ins_err(331,'Contract Address Property or Admin Unit Indicator is Manadatory');
ins_err(332,'Contract Address Instance Start Date is Manadatory');
ins_err(333,'Invalid Contract Reference Supplied');
ins_err(334,'Invalid Record Type : P - Property Ref, A - Admin Unit');
ins_err(335,'Invalid Contract Address Reason for Adding Code');
ins_err(336,'Invalid Contract Address Reason for Terminating Code');
ins_err(337,'Invalid Contract Section Number');
--
ins_err(338,'Current Status Start Date must be supplied');
ins_err(339,'Display Order Sequence must be supplied');
ins_err(340,'Standard Deliverables Code must be supplied');
ins_err(341,'Planned Start Date of Work/Service is Mandatory');
ins_err(342,'Estimated Cost must be supplied');
ins_err(343,'Contract Address does not exist');
ins_err(344,'Invalid Schedule of Rates Code');
ins_err(345,'Invalid VAT Category Code');
ins_err(346,'Invalid Standard Deliverables Code');
ins_err(347,'Invalid Quantity, Unit of Measure Code');
ins_err(348,'Invalid Deliverable Location Code');
ins_err(349,'Unit Cost must be supplied');
ins_err(350,'Unit Cost cannot be negative');
ins_err(351,'Estimated Cost cannot be negative');
ins_err(352,'Description must be supplied if SOR Code is supplied');
ins_err(353,'Quantity must be supplied if SOR Code is supplied');
ins_err(354,'Quantity Unit of Measure must be supplied if SOR Code is supplied');
ins_err(355,'SOR Code supplied is not a CONTRACT SOR');
--
ins_err(356,'Source Reference must be supplied');
ins_err(357,'Task Group Code must be supplied');
ins_err(358,'Task Group Source Type must be supplied');
ins_err(359,'Task Description must be supplied');
ins_err(360,'Task Group Start Date must be supplied');
ins_err(361,'Task Group Type must be supplied');
ins_err(362,'Task Group already exists');
ins_err(363,'Invalid Standard Template Code');
ins_err(364,'Task Group Type must be NPAY(Non Payment)/BUDG(Budget)/PAYT(Payment)');
ins_err(365,'Task Source Type must be CNT(Contract)/PRG(Programme)/PRJ(Project)');
ins_err(366,'Invalid Programme Reference Supplied');
--
ins_err(367,'Task Type Indicator must be supplied');
ins_err(368,'Standard Task code must be supplied');
ins_err(369,'Version Number must be supplied');
ins_err(370,'Budget Task Amount Budget Head Code must be supplied');
ins_err(371,'Budget Task Amount Budget Calander Year must be supplied');
ins_err(372,'Budget Task Amount Net Amount must be supplied');
ins_err(373,'Budget Task Amount Tax Amount must be supplied');
ins_err(374,'Task Already exists');
ins_err(375,'Task Group does not exist');
ins_err(376,'Status must be RAI');
ins_err(377,'Invalid Standard Task Code');
ins_err(378,'Invalid Task Type Indicator');
ins_err(379,'A Task of type Payment may only be associated with a Standard Task of type Payment');
ins_err(380,'A Task of type Non Financial may only be associated with a Standard Task of type Non Financial');
ins_err(381,'A Task of type Budget may only be associated with a Standard Task of type Budget');
ins_err(382,'A Task of type Payment must be associated with a Task Group either of type Payment or Budget');
ins_err(383,'A Task of type Non Financial must be associated with a Task Group of type Payment, Budget or Non Payment');
ins_err(384,'A Task of type Budget must be associated with a Task Group of type Budget');
ins_err(385,'Task Version already exists');
ins_err(386,'A Vat Category can only be assigned to a Task of type Payment or Budget');
ins_err(387,'A Budget Calendar can only be assigned to a Task of type Payment or Budget');
ins_err(388,'A Net Amount can only be assigned to a Task of type Payment or Budget');
ins_err(389,'A Tax Amount can only be assigned to a Task of type Payment or Budget');
ins_err(390,'Retention values can only be assigned to a Task of type Payment');
ins_err(391,'Invaild Task User Status');
ins_err(392,'Only one Task Version for the Task may have the Current Indicator set to Y');
--
ins_err(393,'Retentions Indicator must be Y or N');
ins_err(394,'Final Measure Period Unit must be DAY/WDAY/WEEK/MNTH/YEAR');
ins_err(395,'Invalid Penalty Rule Code');
ins_err(396,'Contract Address already exists');
ins_err(397,'Contract Address Instance already exists');
ins_err(398,'Sum of Task Budget Amount Net Amlunt != Task Net Amount');
ins_err(399,'Repeat Period Indicator must be one of D, W, M, or Y');
ins_err(400,'Invalid Primary Contract Type');
ins_err(401,'Invalid Contract Type 2');
ins_err(402,'Invalid Contract Type 3');
ins_err(403,'Invalid Contract Type 4');
ins_err(404,'Secondary Contract Types must not be supplied if Primary Contract Type is null');
--
ins_err(405,'Task Version record does not exist');
ins_err(406,'Task Budget Amount record already exists');
ins_err(407,'Invalid GL Segment 1 Value supplied');
ins_err(408,'Invalid GL Segment 2 Value supplied');
ins_err(409,'Invalid GL Segment 3 Value supplied');
ins_err(410,'Invalid GL Segment 4 Value supplied');
ins_err(411,'Invalid GL Segment 5 Value supplied');
ins_err(412,'Invalid GL Segment 6 Value supplied');
ins_err(413,'Invalid GL Segment 7 Value supplied');
ins_err(414,'Invalid GL Segment 8 Value supplied');
ins_err(415,'Invalid GL Segment 9 Value supplied');
ins_err(416,'Invalid GL Segment 10 Value supplied');
--
-- Reserved for NSW Dataloads 417 - 499
-- errors 417 to 427 added as in a bespoke version Feb2008 (AJ) 
--
ins_err(417,'Affecting Easement Indicator must be Y or N');
ins_err(418,'Appurtenant Easement Indicator must be Y or N');
ins_err(419,'Residual Indicator must be Y or N');
ins_err(420,'Plan Number Must Be Supplied');
ins_err(421,'Lot Number Must Be Supplied');
ins_err(422,'Land Title Type Must Be Supplied');
ins_err(423,'Area Measurement Must Be Supplied');
ins_err(424,'Start Date Must Be Supplied');
ins_err(425,'Land Title Start Date Cannot be in the Future');
ins_err(426,'Start Date Type must be A or R');
ins_err(427,'Only one of Volume, Consolidation or Book Number should be supplied');
--
-- Land Titles Data Load (NSW and Queensland)
--
ins_err(428,'Book Sequence Number should be supplied when book number is');
ins_err(429,'Book Sequence Number should be null when Book Number is not supplied');
ins_err(430,'Number of Properties and Number of Properties Owned should be supplied when Title Type is SP');
ins_err(431,'Section Number is not required when Title Type is SP');
ins_err(432,'Number Properties Owned must be less than or equal to Number of Properties');
ins_err(433,'Closed Reason must be supplied when Closed Date is supplied');
ins_err(434,'Closed Date must not be before Land Title Start Date');
ins_err(435,'Previous Lot Number and Previous Type must be supplied when Previous Plan Number is supplied');
ins_err(436,'Modified Date must be supplied when Modified By is supplied');
ins_err(437,'Modified By must be supplied when Modified Date is supplied');
--
ins_err(438,'Plan Number Must Be Supplied');
ins_err(439,'Lot Number Must Be Supplied');
ins_err(440,'Land Title Type Must Be Supplied');
ins_err(441,'Released Date Must Be Supplied');
ins_err(442,'Released To Must Be Supplied');
ins_err(443,'Released Reason Must Be Supplied');
ins_err(444,'Release Date Cannot be in the Future');
ins_err(445,'Return Date must not be before Release Date');
ins_err(446,'Return Date Cannot be in the Future');
ins_err(447,'Modified Date must be supplied when Modified By is supplied');
ins_err(448,'Modified By must be supplied when Modified Date is supplied');
--
ins_err(449,'Plan Number Must Be Supplied');
ins_err(450,'Lot Number Must Be Supplied');
ins_err(451,'Land Title Type Must Be Supplied');
ins_err(452,'Assignment Date Must Be Supplied');
ins_err(453,'Assignment End Date Must not be before Assignment Start Date');
ins_err(454,'Assignment End Reason Must be supplied when Assignment End Date is supplied');
--
ins_err(455,'Land Title Code does not exist');
ins_err(456,'Land Title Closed Reason Code does not exist');
ins_err(457,'Section Number must not be supplied when Plan Type is SP');
ins_err(458,'Land Title Reference exists on Land Titles');
ins_err(459,'Land Title Reference does not exist on Land Titles');
ins_err(460,'Land Type Reference Exists on Land Title Releases');
ins_err(461,'Land Title Party Does Not Exist on Parties Table');
ins_err(462,'Closed Date and By must be supplied when Closed Reason is supplied');
ins_err(463,'Closed Date and By must be supplied when Closed Reason is supplied');
ins_err(464,'Section NUmber must be supplied when Plan Type is DP');
ins_err(465,'Release Reason Code does not exist');
ins_err(466,'Folio Number must be supplied when Volume Number is supplied');
ins_err(467,'Volume Number must be supplied when Folio Number is supplied');
ins_err(468,'Property Reference does not exist on Properties table');
ins_err(469,'Previous Land Title reference does not exist');
ins_err(470,'Assignment close reason does not exist');
ins_err(471,'Supplied data does not identify a unique Party');
--
ins_err(499,'Reserved for NSW Dataloads 417 - 499');
--
ins_err(500,'Only one of Date or Character Value can be supplied');
ins_err(501,'Description must be Supplied');
--
ins_err(502,'Budget Task Amount Budget Head Code must not be supplied for non financial task');
ins_err(503,'Budget Task Amount Budget Calander Year must not be supplied for non financial task');
ins_err(504,'Budget Task Amount Net Amount must not be supplied for non financial task');
ins_err(505,'Budget Task Amount Tax Amount must not be supplied for non financial task');
ins_err(506,'Budget Task Amount Budget year not found on budget_calendars.');
--
ins_err(507,'Contract Version does not exist.');
--
ins_err(508,'The sum of all Contracts exceeds the Project Maximum Value');
--
ins_err(509,'External System Type Code must be Supplied');
ins_err(510,'Legacy Reference must be Supplied');
ins_err(511,'Docyment Type Code must be Supplied');
ins_err(512,'Document Page Number must be Supplied');
ins_err(513,'Document File path must be Supplied');
ins_err(514,'Document Image File Name must be Supplied');
ins_err(515,'Document Image File Type must be Supplied');
ins_err(516,'Invalid External System Type Code Supplied');
ins_err(517,'External System Type Code Process not catered for');
ins_err(518,'Invalid Document Type Code Supplied');
ins_err(519,'Invalid Image File Type Supplied');
--
ins_err(520,'Task Version does not exist.');
--
ins_err(521,'Deliverable to be updated does not exist');
--
ins_err(522,'Operative Skill must be in range 1-9 or M');
--
ins_err(523,'Lease Assignment Start Date supplied, LIALEALAS system parameter set to LEASE');
ins_err(524,'Lease Assignment Start Date not supplied, LIALEALAS system parameter set to ASSIGMENT');
--
ins_err(525,'Job Type of Defect - Invalid Defect Schedule of Rates Code');
ins_err(526,'Job Sequence already exists for this Works Order Version');
--
ins_err(527,'Survey Result already exists for this Property/Element/Location/Attribute/Further Attribute Combination');
--
ins_err(528,'Store does not exist or is not a Depot');
ins_err(529,'Product is not valid for Store');
ins_err(530,'Stock Adjustment Reason is not valid');
ins_err(531,'Reason Code must be supplied');
ins_err(532,'Adjustment Quantity must be supplied');
ins_err(533,'Effective Date must be supplied');
--
ins_err(534,'Transaction Type must be one of VDS or VDA');
ins_err(535,'Debit Amount must be supplied');
ins_err(536,'Effective Date must be supplied');
ins_err(537,'Transaction Date must be supplied');
ins_err(538,'Summarise Indicator must be Y, N or Null');
--
ins_err(539,'Service Charge Period does not exist for Code and Dates supplied');
--
ins_err(540,'Element already exists starting after this record');
--
ins_err(541,'Tenancy Correspond Name must be supplied');
--
ins_err(542,'Account does not have a current Regular Payment Method assigned');
ins_err(543,'Invalid Instruction Status Code');
ins_err(544,'Paperless Indicator must be Y or N');
ins_err(545,'Invalid Account Status Code');
ins_err(546,'Party Bank Account details do not exist');
ins_err(547,'Direct Debit Instruction exists for this Account');
ins_err(548,'Direct Debit Usage exists for this Account and Payment Method');
ins_err(549,'Direct Debit Account exists for this Account and Party');
ins_err(550,'Profile Item does not exist for Admin Unit, Year, Profile and Due Date');
ins_err(551,'Person not found on Parties table for Landlord Party Alt Ref supplied');
ins_err(552,'Landlord Type not found in Domain LANDTYPE');
ins_err(553,'Agreement Type not found in Domain AGRETYPE');
ins_err(554,'Property Type not found in Property Types table');
ins_err(555,'Address Leave Reason not found in Domain ADDLEAVE');
ins_err(556,'Storage Indicator must be Y or N');
--
ins_err(557,'Deliverable Version to be updated does not exist');
--
ins_err(558,'Works Order does not exist');
ins_err(559,'Survey does not exist or is not for Works Orders');
ins_err(560,'Question is not valid for this survey on this date');
ins_err(561,'Answer type does not match question setup');
ins_err(562,'Only a coded answer is valid for this question');
ins_err(563,'This answer is not valid for this question');
ins_err(564,'Value for this answer must be Y or N');
ins_err(565,'One of the answer fields must be supplied');
ins_err(566,'Only one of the answer fields may be supplied');
ins_err(567,'This question has already been answered for this works order');
--
ins_err(568,'Task Version to be updated does not exist');
--
ins_err(569,'Deliverable Version does not exist');
--
ins_err(570,'Interested Party does not exist');
ins_err(571,'Business Action does not exist');
ins_err(572,'Registered Address Lettings does not exist');
ins_err(573,'Business Action Events does not exist');
--
--
ins_err(574,'Invalid Tenancy Refno Supplied');
ins_err(575,'Invalid Survey Code Supplied');
ins_err(576,'Question Number Supplied is not relevant to the Survey Code');
ins_err(577,'Invalid Survey Question Number Supplied');
ins_err(578,'Answer Type for Question is DATE. Date Value not supplied');
ins_err(579,'Answer Type for Question is NUMERIC. Numeric Value not supplied');
ins_err(580,'Answer Type for Question is TEXT. Text Value not supplied');
ins_err(581,'Answer Type for Question is YN. Y/N Value not supplied');
ins_err(582,'Question Type is CSA. No Coded Answer Supplied');
ins_err(583,'Invalid Coded Answer Supplied for Survey Question Number Supplied');
ins_err(584,'Question Type must be CSA or NCA');
ins_err(585,'Value Indicator must be Y or N');
ins_err(586,'At least one Answer field must be supplied');
ins_err(587,'Only one Answer field must be supplied');
ins_err(588,'Invalid Table name Supplied');
--
ins_err(589,'Action Indicator must be (U)pdated or (D)eleted');
--
ins_err(590,'Party Reference cannot be found on the PARTIES table');
ins_err(591,'Property Reference cannot be found on the PROPERTIES table');
ins_err(592,'Admin Unit Code cannot be found on the Admin Units table');
ins_err(593,'Tenancy Reference cannot be found on the TENANCIES table');
ins_err(594,'Interested Party Reference cannot be found on the INTERESTED PARTIES table');
ins_err(595,'Application Reference cannot be found on the APPLICATIONS table');
ins_err(596,'People Groups Code cannot be found on the PEOPLE GROUPS table');
ins_err(597,'Service Request Number cannot be found on the SERVICE REQUESTS table');
ins_err(598,'Contractor can not be found on the CONTRACTOR SITES table');
ins_err(599,'Lease Assignments Property Reference cannot be found on the PROPERTIES table');
ins_err(600,'One or Both Lease Assignment Start Dates missing');
ins_err(601,'Lease Assignments Record cannot be found on the LEASE ASSIGNMENTS table');
ins_err(602,'Invalid Legacy Type Supplied. Must be (PAR/PRO/AUN/SRQ/IPP/APP/PEG/COS/LAS/TCY)');
ins_err(603,'Action Type must be C or N');
ins_err(604,'Invalid Business Reason Code Supplied');
ins_err(605,'Responsible Admin Unit Code cannot be found on the Admin Units table');
ins_err(606,'Invalid Status Code Supplied. Must be (CUR/COM/HLD/CAN/CLO)');
ins_err(607,'BAN REFERENCE already exists in BUSTINESS ACTIONS table');
--
ins_err(608,'Event Type must be A or M');
ins_err(609,'Invalid Action Event Code Supplied');
ins_err(610,'Invalid Business Action Path Code Supplied');
ins_err(611,'Invalid Status Code Supplied. Must be (CUR/COM/HLD/CAN/TRG)');
ins_err(612,'Invalid Previous Status Code Supplied');
ins_err(613,'Expiry Date must be supplied for COURT/NOTICE Events');
ins_err(614,'Inserted Date/Time must be supplied for Manual Event Types');
ins_err(615,'Inserted Date/Time must not be supplied for Automatic Event Types');
ins_err(616,'Action Path Code must be supplied for Automatic Event Types');
ins_err(617,'Action Path Code must not be supplied for Manual Event Types');
ins_err(618,'BAN REFERENCE does not exist in BUSINESS ACTIONS table');
ins_err(619,'Business Action Event record already exists for Ban Reference/Action Event/Sequence combination');
--
ins_err(620,'SOR Code supplied is not a Planned Maintenance SOR');
ins_err(621,'SOR Code supplied is not a current SOR');
ins_err(622,'Record already exists in Contract SOR for the Contract, Version and SOR Code');
ins_err(623,'Contract Version does not exist');
ins_err(624,'Current Contract SOR record already exists for this Contract, Version and SOR COde');
ins_err(625,'Deliverable Component Version Number must be supplied');
ins_err(626,'Budget is not an active Planned Maintenance Budget');
--
ins_err(627,'BAN BAN REFERENCE does not exist in BUSINESS ACTIONS table');
ins_err(628,'Business Action Path Assigns record does not exists for Ban ref/ Path Code/ Path Start Date combination');
--
ins_err(629,'Issued Survey Record already exists for Survey Code/ Legacy Ref/ Version No combination');
ins_err(630,'Issued Survey Record does not exist for Survey Code/ Legacy Ref/ Version No combination');
ins_err(631,'Issued Survey Question Record already exists for Issue Survey Ref/ Survey Code/ Question No combination');
--
ins_err(632,'Issued Survey Record does not exist for Issue Survey ref/ Survey Ref/ Question No combination');
ins_err(633,'Survey Answer Record already exists for Issue Survey ref/ Survey Ref/ Question No combination');
ins_err(634,'Survey Answer Record does not exist for Issue Survey ref/ Survey Ref/ Question No combination');
--
ins_err(635,'Unable to establish IPT_PGP_REFNO for IPT_CODE supplied');
ins_err(636,'Parameter Definition Usages record does not exist for otherfield name and pgp_refno');
ins_err(637,'Otherfield object not catered by dataload');
--
ins_err(638,'Unable to derive the Housing Options PGP Refno');
ins_err(639,'Invalid Housing Options Other Field Name Supplied');
ins_err(640,'Unable to derive the Business Reason PGP Refno');
ins_err(641,'Invalid Business Action Other Field Name Supplied');
ins_err(642,'Unable to derive the Business Action Event PGP Refno');
ins_err(643,'Invalid Business Action Event Other Field Name Supplied');
ins_err(644,'Parameter Values records does not exist');
--
-- Notepads Errors
--
ins_err(645,'Account Arrears Arrangement reference does not exist');
ins_err(646,'Inspection reference does not exist');
ins_err(647,'People Group code does not exist');
ins_err(648,'Placement property room reference does not exist');
ins_err(649,'Referrals reference does not exist');
ins_err(650,'Application type must be A or H');
--
-- System General Contacts Errors
--
ins_err(651,'Invalid table name supplied');
ins_err(652,'Application and rehousing list combination does not exist');
ins_err(653,'Interested party shortname and type does not exist');
ins_err(654,'Advice Case reference does not exist');
ins_err(655,'Reply Required Ind should be Y or N');
ins_err(656,'Re-registration type contact Ind should be Y or N');
ins_err(657,'Re-register application Ind should be Y or N');
ins_err(658,'Reply by date must not be before send date');
ins_err(659,'Module Name does not exist in the MODULES table');
--
ins_err(660,'Prevention Payment record does not exist for alternative reference supplied');
ins_err(661,'Unable to derive the Prevention Payment Type PGP Refno');
ins_err(662,'Invalid Prevention Payment Other Field Name Supplied');
--
ins_err(663,'Unable to derive the Registered Address Lettings (Housing Options) PGP Refno');
ins_err(664,'Invalid Registered Address Lettings Other Field Name Supplied');
--
ins_err(665,'Table name must be supplied');
ins_err(666,'Legacy Reference must be supplied');
ins_err(667,'Module Name must be supplied');
ins_err(668,'The Secondary Reference must be supplied for this table name');
ins_err(669,'Application Cannot be found using app_legacy_ref supplied');
ins_err(670,'Contact Sent Date must not be greater than today');
--
ins_err(671,'Registered Address does not exist');
--
ins_err(672,'Non-Access record already exists for the combination of Deliverable, Date and time');
ins_err(673,'Notify Ocupant Indicator must be Y or N');
ins_err(674,'Contractor Site Association Indicator must be Y or N');
ins_err(675,'Non-Access Reason Code does not exist in Domain DELNONACC');
ins_err(676,'Deliverable Display Sequence must be supplied');
ins_err(677,'Deliverable Schedule Of Rates Code must be supplied');
ins_err(678,'Non-Access Date Time must be supplied');
ins_err(679,'Non-Access Date Time must not be in the future');
--
-- hless instances errors MB 03/12/2009
ins_err(680,'Homeless Instance does not exist on HLESS_INSTANCES table');
ins_err(681,'Answer already exists on HLESS_INS_ANSWERS table');
ins_err(682,'Question is not HI category question');
ins_err(683,'The stage is already present on this Homeless Instance');
ins_err(684,'Rehousing List is not a (H) Homeless Temporary type');
ins_err(685,'Homeless Instance already exists on HLESS_INSTANCES table');
--
ins_err(686,'Update only allowed for Contractor Sites, Contracts, Deliverables and Tasks Other Fields');
ins_err(687,'Other Field Value does not exist');
--
ins_err(688,'Contract SOR already exists for this combination of Contract, Version and SOR Code');
ins_err(689,'Repeat Period Indicator supplied, Repeat Period Units not supplied');
ins_err(690,'Repeat Period Units Supplied, Repeat Period Indicator not supplied');
ins_err(691,'Contract Reference must be supplied');
ins_err(692,'If Price Start Date supplied Price must also be supplied');
ins_err(693,'If Price supplied Price Start Date must also be supplied');
ins_err(694,'Eastings / Northings already exist for this property / physical address');
--
ins_err(695,'Survey Answer does not exist for Deliverable, Survey and Question');
ins_err(696,'Survey Answer does not exist for Contract, Survey and Question');
ins_err(697,'Survey Answer does not exist for Tenancy, Survey and Question');
ins_err(698,'Survey Answer does not exist for Works Order, Survey and Question');
ins_err(699,'Deliverable Component does not exist');
ins_err(700,'Survey Answer does not exist for Deliverable Component, Survey and Question');
ins_err(701,'Answer value must be supplied');
ins_err(702,'Only one answer value may be supplied');
ins_err(703,'Answer supplied is not a valid datatype for the question');
ins_err(704,'Issued Survey Version must be supplied');
ins_err(705,'Survey Question must be supplied');
--
ins_err(706,'Start Location Ind must be H or D');
ins_err(707,'End Location Ind must be H or D');
--
ins_err(708,'Budget exists at DELIVERABLE Level - It cannot exist at DELIVERABLE CMPT Level as well');
ins_err(709,'Budget must exist at DELIVERABLES or DELIVERABLE CMPT Level');
ins_err(710,'DELIVERABLE COMPONENTS can only be loaded against CONTRACTS at RAIsed Status');
ins_err(711,'DELIVERABLES can only be loaded against CONTRACTS at RAIsed Status');
--
ins_err(712,'Attribute Code must be supplied');
ins_err(713,'Alloc Prop Type code already exists');
ins_err(714,'Initial Type Indicator must be Y or N');
ins_err(715,'An Initial Type APT Code already exists');
ins_err(716,'Alloc Prop Type Group does not exist');
ins_err(717,'APT Code does not exist');
ins_err(718,'Element already exists for this APT Code');
ins_err(719,'An Attribute code should only be supplied for a Coded Element');
ins_err(720,'An Element Values should only be supplied for a Numeric Element');
ins_err(721,'Letting Area Code already exists');
ins_err(722,'Parent Letting Area Code does not exist');
ins_err(723,'Letting Area type should be C(hild) or P(arent)');
ins_err(724,'Element and Attribute must be supplied for Child Letting Area');
ins_err(725,'Element and Attribute must not be supplied for Parent Letting Area');
ins_err(726,'Letting Area Code must be supplied');
ins_err(727,'Parent Letting Area Code must be supplied');
--
ins_err(728,'Management Area Admin Unit Type must match value against parameter MALEVCNT');
ins_err(729,'Management Area Admin Unit must be the same or a child of Project Management Area Admin Unit');
ins_err(730,'Property/Admin Unit is not assigned to the Contract Management Area');
ins_err(731,'Survey Group Code does not exist');
ins_err(732,'Planned Maintenance Indicator must be U or N');
ins_err(733,'Attribute already exists for this Element');
ins_err(734,'Element must be of value type C or M');
--
ins_err(735,'Contract Address cannot be assigned to a closed property');
--
ins_err(736,'Pricing Policy Con Site does not exist for Group, Work Programme, Con Site and Date');
ins_err(737,'Con Site Price Group does not exist for Group, Work Programme, SOR Code, Con Site and Date');
ins_err(738,'Record already exists in bespoke ISG_GHA_SOR_CMPTS table for Group, Work Programme, SOR Code, Con Site and Date');
--
ins_err(739,'Invalid External Reference supplied');
ins_err(740,'Document Type does not exist on Documnet Types table');
ins_err(741,'Image Location must be supplied');
ins_err(742,'Image File must be supplied');
ins_err(743,'Image Type must be supplied');
--
ins_err(744,'Payment Profile Item already exists for Admin Unit, Year, Profile and Due Date');
--
-- STEVENAGE SDL110 codes
--
ins_err(745,'Outstanding Variation Request Exists');
ins_err(746,'Outstanding Inspection Visit Exists');
ins_err(747,'Status is not ISSUED');
ins_err(748,'Completion Date is before Raised/Issued/Auathorised Date');
ins_err(749,'Date cannot be in the future');
ins_err(750,'Invoice exists for this order');
ins_err(751,'Date Format it different to that expected');
--
-- Housing NZ Contact Details load
--
ins_err(752,'Contract Site Contact does not exists in CON_SITE_CONTACTS for site code and contact name supplied');
ins_err(753,'Contact Method cannot be found in CONTACT_METHODS table');
ins_err(754,'Invalid EMAIL contact value supplied');
ins_err(755,'Communication Preference Code does not exist in Domain TEL_COMM_PREF');
--
-- Expected Payments
--
ins_err(756,'An Arrangement already exists for these dates');
ins_err(757,'Payment Expectation type must be supplied');
ins_err(758,'Invalid Payment Expectation type');
ins_err(759,'Payment Frequency must be supplied');
ins_err(760,'Invalid Frequency code');
ins_err(761,'Allocate to Future Payment Ind must be Y or N');
ins_err(762,'Due Date of First Payment must be supplied');
ins_err(763,'Amount must be supplied');
ins_err(764,'Priority must be supplied');
ins_err(765,'Allocate to Future Payment Ind must be supplied');
ins_err(766,'Tolerance must be supplied');
ins_err(767,'Either Transaction Type or Type/Subtype must be supplied');
ins_err(768,'If Subtype is supplied, transaction type for subtype must also be supplied');
ins_err(769,'If Transaction Type is supplied, the subtype fields must be blank');
ins_err(770,'No Payment Expectation found');
ins_err(771,'Invalid Expected Payment type');
ins_err(772,'Unpaid Balance must be supplied');
ins_err(773,'Overdue Date must be supplied');
ins_err(774,'Due Date must be supplied');
ins_err(775,'Payment Amount must be supplied');
ins_err(776,'An alternative reference must be supplied');
--
ins_err(777,'No matching Invoice Arrears Action');
--
-- HNZ Assessments Errors
--
ins_err(778, 'Assessment Type LASSM_ASST_CODE must exist on the ASSESSMENT_TYPES table');
ins_err(779, 'Type of Assessment (LASSM_TYPE) must be R, N or O');
ins_err(780, 'Assessment Status (LASSM_SCO_CODE) must be CUR, SGN or CLO');
ins_err(781, 'Assessment Status Date cannot be greater than today');
ins_err(782, 'Support Worker Shortname (LIPP_SHORTNAME) must identify an interested party of type (IPP_IPT_CODE) SUPW');
ins_err(783, 'Support Worker reference (LIPP_REFNO) must identify an interested party of type (IPP_IPT_CODE) SUPW');
ins_err(784, 'Created By (LASSM_CREATED_BY) must identify an valid USER');
ins_err(785, 'Created Date, if supplied must not be greater than today');
ins_err(786, 'A previous outcome code may only be supplied if an outcome has been supplied');
ins_err(787, 'If Signed Off Date is supplied then Signed Off By must be supplied and visa versa');
ins_err(788, 'If Approved By is supplied the Approved Date must be supplied and visa versa');
ins_err(789, 'If Assessment Type Sign Off Required is set to N then Sign Off Date/Signed Off By must not be supplied');
ins_err(790, 'If Assessment Type Approval Required is set to N then Approved Date/Approved By must not be supplied');
ins_err(791, 'If User who approved the assessment is supplied then status must be SGN or CLO');  
ins_err(792, 'If Date the assessment was approved is supplied then status must be SGN or CLO');  
ins_err(796, 'Assessment date cannot be greater than today');
ins_err(797, 'Signed Off date cannot be greater than today');
ins_err(798, 'Signed Off By (LASSM_SIGNED_OFF_BY) must identify an valid USER');
ins_err(799, 'Review Date be greater or equal to the Assessment Date');
ins_err(800, 'Review Date must not be supplied if an assessment date is not supplied');
ins_err(801, 'Reason for cancelling the Assessment - must exist as a valid code in the HRV_ASSESS_CANCEL_REASON view');
ins_err(802, 'Admin Unit must exist as a valid admin unit in the ADMIN_UNITS table');
ins_err(803, 'The Client the Assessment is for must identify a party on the PARTIES table');
ins_err(804, 'The Client the Assessment is for must not be supplied if the Party Reference is supplied');
ins_err(805, 'Either the Party Reference or the Alternative Party Reference must be supplied');
ins_err(806, 'Outcome Code - must exist as a valid code on the ASSESSMENT_TYPE_OUTCOMES table');
ins_err(807, 'User who assigned / generated the outcome must correspond to a valid user on the Northgate Housing system');
ins_err(808, 'Date the outcome code was assigned must not be greater than today');
ins_err(809, 'User who approved the assessment must correspond to a valid user on the Northgate Housing system');
ins_err(810, 'Date the assessment was approved must be less than or equal to today');
ins_err(811, 'Date client agreed the assessment must be less than or equal to today');
ins_err(812, 'Previous Outcome Code must exist as a valid code on the ASSESSMENT_TYPE_OUTCOMES table');  
--
-- HNZ Assessment Control Measures Errors
--
ins_err(813, 'One of lassm_par_refno or lpar_per_alt_ref must be supplied');  
ins_err(814, 'Combination of lpar_per_alt_ref, lassm_asst_code and lassm_sequence must identify an assessment');  
ins_err(815, 'Combination of lassm_par_refno, lassm_asst_code and lassm_sequence must identify an assessment');  
ins_err(816, 'Lascm_hrv_come_code must exist in the HRV_CONTROL_MEASURE view');  
ins_err(817, 'The date that the Control Measure started must be less than or equal to today');  
ins_err(818, 'The user who created the control measure must be a valid housing user');  
ins_err(819, 'Created date of the the control measure must not be greater than today');    
--
-- HNZ Contacts, Sub_cont_bus_reasons, non_subj_cont_bus_reasons
--
ins_err(820, 'Contact Alternative Reference does not match to a Contacts record'); 
ins_err(821, 'Business Reason already exists on this Contact');
ins_err(822, 'Invalid Business Reason Class Code');
ins_err(823, 'Invalid Business Reason Code'); 
ins_err(824, 'Business Reason must be of a Non-Subject type'); 
ins_err(825, 'Status must be one of LOG, COM, CLO, or CAN'); 
ins_err(826, 'Contact Alternative Reference must be supplied'); 
ins_err(827, 'Business Reason code must be supplied'); 
ins_err(828, 'Status Code must be supplied'); 
ins_err(829, 'Status Date must be supplied'); 
ins_err(830, 'Business Reason Class must be supplied'); 
ins_err(831, 'Major Subject Indicator must be supplied'); 
ins_err(832, 'Major Subject Indicator must be either Y or N'); 
ins_err(833, 'A Major Subject Business Reason already exists on this Contact'); 
ins_err(834, 'If Actual Date is supplied, the status must be COM'); 
ins_err(835, 'If Status is COM, then Actual Date must be supplied'); 
ins_err(836, 'Contact Alternative Reference already has been succsessfully created on another dataload batch'); 
ins_err(837, 'Received Date must not be in the future');
ins_err(838, 'Contact Method Code does not exist in Domain CONTTYPE');
ins_err(839, 'Contact Source Type must exist as a valid code on the CONTACT_SOURCE_TYPES table');  
ins_err(840, 'Contact Status must be one of LOG, COM, or CAN');
ins_err(841, 'Status Date must not be in the future');
ins_err(842, 'Object Source Type must be one of PRO, PAR, PRF, TCY, TAR, IPP, COS, APP, ALR or PEG');
ins_err(843, 'Party Ref Specific To Supplied does not exist in PARTIES');
ins_err(844, 'Tenancy Refno cannot be found on the TENANCIES table');
ins_err(845, 'Tenancy Alt Reference cannot be found on the TENANCIES table');
ins_err(846, 'Interested Party cannot be found on the TENANCIES table for ipt_code/shortname combination');
ins_err(847, 'Address Refno must only be supplied if Object Source type is PAR, PRF, IPP, APP, ALR or PEG');
ins_err(848, 'ADR Refno cannot be found on the ADDRESSES table');
ins_err(849, 'Please enter a valid combination of job role, or user, or job role user, if required');
ins_err(850, 'Job Role User Start Date can only be supplied if both the Job Role and Username are supplied');
ins_err(851, 'Outcome Code must exist as a valid code on the CNANSOUT table');
ins_err(852, 'All the Lease Property Ref, Lease Start Date, Lease Assignment Start Date must be supplied');
ins_err(853, 'Subject Contact Business Reason Status must be one of ACT, LOG, COM, CLO, or CAN');
ins_err(854, 'If BAN REFERENCE is supplied, the status must be ACT');
ins_err(855, 'Subject Source Type must be one of PAR, PRF, TCY, TAR, IPP, COS, APP, ALR, PEG, PRO, LAS, AUN or SRQ');
ins_err(856, 'Business Reason Completion Date must not be in the future');
--
ins_err(857, 'Billable Account Reference does not exist on BILLABLE_ACCOUNTS table');
ins_err(858, 'Claim Reference does not exist on CLAIMS table');
--
ins_err(859, 'Job Role Code supplied does not exist on JOB_ROLES table');
ins_err(860, 'Combination of job role, job role user, job role user start date does not exist on JOB_ROLE_USERS table');
--
-- HNZ Assessment Control Measures Further Error
--
ins_err(861, 'Generated Ind must be Y or N');
--
ins_err(862, 'Please enter a valid combination of job role, or user, or job role user, if required');
--
--
ins_err(863, 'Subject Contact Business Reasons record already exists for combination of data supplied');
--
--
ins_err(864, 'Party Reference Type must be one of PAR, PRF');
ins_err(865, 'Header Usage Reference Type must be one of TCY, TAR, APP, ALR, SURV, ACAS');
--
ins_err(866, 'Property is not currently void');
ins_err(867, 'Alternative Reference must be supplied');
ins_err(868, 'A person already exists with this NINO and different Date of Birth');
--
ins_err(870, 'Invoice Ref is for a different Account');
--
ins_err(871, 'Third Party Appointment Ind must be Y or N');
ins_err(872, 'Auto Invoice Minimum Delay Ind must be Y or N');
ins_err(873, 'Auto Job Complete Ind must be Y or N');
ins_err(874, 'Tax Start Date must be before Tax End Date');
ins_err(875, 'Appointment Type Ind must be A or S');
ins_err(876, 'Auto Job Complete Delay must be Greater than Zero');
--
-- MET Account Service Charges Data load
--
ins_err(877, 'Invalid status code supplied. Must be W,C,A or P');
ins_err(878, 'Disputed Indicator must be Y or N');
ins_err(879, 'Weighting value non-numeric');
ins_err(880, 'Service Charge Element must be supplied(lasc_svc_att_ele_code)');
ins_err(881, 'Service Charge Period(lasc_scp_code)must be supplied');
ins_err(882, 'Service Charge Period start date(lasc_scp_start_date)must be supplied');
ins_err(883, 'Estimated Cost (lasc_est_deb_basis_cost) must be supplied');
ins_err(884, 'Status Code(lasc_asc_status)must be supplied'); 
ins_err(885, 'Element and Attribute Combination does not exist in ATTRIBUTES table)');
ins_err(886, 'Service Charge start date(lasc_start_date)must be within the Service Charge Period');
ins_err(887, 'Service Charge end date(lasc_end_date)must be within the Service Charge Period');
ins_err(888, 'Service Charge Rates reference(lasc_scr_propref_auncode)of property or admin unit must be supplied');
ins_err(889, 'Service Charge Rates type of P or A(lasc_pro_aun_type)must be supplied');
ins_err(890, 'Service Charge Rates type must be P or A (lasc_pro_aun_type)');
ins_err(891, 'Service Charge Rates record does not exist for Combination supplied)');
ins_err(892, 'Property reference(lasc_scr_propref_auncode)does not exist on PROPERTIES');
ins_err(893, 'The Property refs (lasc_scr_propref_auncode)and(lasc_pro_propref)MUST be the same if Type P');
ins_err(894, 'fields 7 and 8 must both be supplied (Service Charge) or neither (Management Cost)');
ins_err(895, 'No Service Usages exists for this Admin Unit, Element, Attribute and Start Date');
--
ins_err(896, 'No Matching Pricing Policy Programme');
--
-- Wandsworth Bespoke Property Dataload validation (VHS)
ins_err(897, 'Saffron Property Number must be supplied');
ins_err(898, 'Supplied Saffron Property Number already exists in the PROPERTIES table');
--
END;
/

