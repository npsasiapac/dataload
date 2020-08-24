-- Script Name = hdl_err_in.sql
---------------------------------------------------------------------- 
--
-- Script to insert HDL Error messages
--
--   Ver   DB Ver  Who    Date     Reason
--   1.0   5.1.2   PJD    2001     iWorld
--   2.0   5.1.5   PJD    23/04/02 New Format
--   2.1   5.1.5   PH     26/04/02 Corrected spelling on descriptions
--   2.2   5.1.6   PH     13/06/02 Added new Property Purchase Error codes
--   2.3   5.1.6   PH     20/06/02 Added new Errors for Bespoke dataloads
--   2.4   5.1.6   PH     27/06/02 Added new Errors for Bespoke Dataloads
--   3.0   5.2.0   PH     11/07/02 Software Release 5.2.0. New errors added.
--   3.1   5.2.0   PH     10/08/02 Added new Errors for Bespoke Dataloads
--   3.2   5.2.0   MH     30/09/02 Added new Errors for Bespoke Dataloads
--   3.3   5.2.0   PH     07/10/02 New Error for Tenancies Validate.
--   3.4   5.2.0   PH     17/10/02 Removed single quotes from some descriptions
--   3.5   5.2.0   PJD    21/11/02 Put back in the missing notepad error codes
--   3.6   5.2.0   SB     22/11/02 Added new Error for Prop Elements
--   3.7   5.2.0   PH     10/12/02 Added new Error Codes for Repairs
--   3.8   5.3.0   SB     07/02/03 Added new code for Arrears Actions
--   3.9   5.3.0   PH     10/03/03 Added new code for SOR Price
--   3.10  5.3.0   MH     25/03/03 Added missing entry for 342
--   3.11  5.3.0   PH     29/05/03 Added new code for sor description
--   3.12  5.3.0   PH     09/06/03 Changed description on code 203
--   3.13  5.3.0   PH     04/07/03 Added new codes for Repairs (994 and 995)
--   3.14  5.3.0   PJD    05/07/03 New code 679 for Payment Contracts Dataload 
--   3.15  5.3.0   PJD    24/07/03 Changed text on 854 to include Null dates
--   3.16  5.3.0   PH     01/09/03 New Error Code 46 for Element Start Date
--   3.17  5.3.0   PH     02/09/03 New Error Code 996 for Bespoke Notepads
--   3.18  5.3.0   PH     04/09/03 New Error Codes for Leases Dataload 
--                                 numbers 571 to 578 inclusive.
--   3.19  5.3.0   PH     07/09/03 New Error Codes for Lease_assignments and 
--                                 lease_parties. Numbers 579 to 586 incusive.
--   3.2.0 5.3.0   PH     16/09/03 New Error Code for Revenue Accounts 587
--   3.2.0 5.3.0   PH     16/09/03 New Error Code for additional Addresses column
--   3.3.0 5.3.0   PH     16/09/03 New Error Code 175
--         5.4.0   PJD    10/12/03 New Error Code 193
--   3.3.1 5.4.0   MH     07/01/04 Added Error Codes for Bespoke DBR Dataload 
--   3.3.2 5.4.0   PJD    17/02/04 New Error Codes 588 to 591 (People Merge)
--   3.3.3 5.5.0   PH     26/02/04 New Error Code 592,593 for Debit Breakdowns
--   3.3.4 5.5.0   PH     02/03/04 New Error Code 249 for Applic List Entries
--   3.3.5 5.5.0   PH     02/03/04 New Error Code 250 for Stage Decisions
--   3.3.6 5.5.0   PH     21/04/04 New Error Codes for Interested Parties
--                                 Codes 424 to 434
--   3.3.7 5.5.0   PH     28/04/04 New error code for SOR's - 435.
--   3.3.8 5.5.0   PH     26/07/04 New Error Codes for Organisation Parties
--                                 Codes 438 to 438
--   3.3.9 5.6.0   PH     13/08/04 New error code for List Entries - 251
--                                 New error code for Properties - 194
--   3.4.0 5.6.0   PH     13/10/04 New error code for aun_code check - 195.
--                                 Also new error codes for pp_application_parties
--                                 196 and 197.
--   3.4.1 5.6.0   VRS    13/10/04 Added new codes for Birmingham Bespoke HFI
--				           dataloads 440-485
--   3.4.2 5.6.0   PH     18/10/04 New Code for nct_aun_links dataload - 198, 199
--   3.4.3 5.6.0   VRS/   02/11/04 Added new codes for Birmingham Bespoke USERS
--		       PJD		     dataloads 390-396, 680-690
--   3.4.4 5.6.0   PH     05/11/04 Added New code for Oldham Bespoke Contact
--                                 Details Dataload 691-694
--   3.4.5 5.6.0   PH     12/11/04 New Codes added for ECHG Bespoke Bank Details
--                                 Dataload 695-697.
--   3.4.6 5.6.0   PJD    18/11/04 New Codes (357-363) added.
--                                 Wording changed to 356, 768 and 992
--   3.4.7 5.6.0   PH     22/11/04 Corrected Spelling mistake on 990
--   3.4.8 5.6.0   PH     07/12/04 New codes added for Arrears Arrangements
--                                 698 and 699.
--                                 Message for error 59 slightly altered.
--   3.4.9 5.7.0   PH     12/1/05  New Codes for 570 Release....
--                                 Schedule of Rates (486-489)
--                                 Tenancies (99)
--                                 Service Usages (397 and 398)
--   3.4.10 5.7.0  PH     19/01/05 New Codes added for Service Charges (594-596)
--                                 and new code for pp_appln_parties 181, 182
--   3.4.11 5.7.0  DH     05/05/05 New codes added for Private Leasing (364-377)
--
--   3.4.12 5.9.0  VRS    09/03/06 Added new code for transactions fix (29)
--
--   3.4.13 5.9.0  VRS    29/03/06 Added new code for HULL BESPOKE SURVEY_RESULTS 
--                                 OTHERFIELDS LOAD. (47)
--   3.4.14 5.10.0 PH     06/11/06 Amended description for HDL093 was previously
--                                 misleading
--   3.4.15 5.10.0 PJD    15/11/06 Code 597 added (522 previously used by Ser Req
--                                                 and Inspections)
--   3.4.16 5.10.0 PH     19/02/07 Code 289 added for Applic Statuses check on
--                                 mandatory questions, previously used 290
--   3.5.0  5.12.0 PH     26/07/07 Amended text of HDL195.
--   3.6.0  5.15.1 PH     20/11/09 New Error code for transactions (95)
--   3.6.1  6.1.1  PH     02/12/10 Corrected error on 495
--   3.6.2  6.1.1  VS     16/02/11 Amended code 691
--   3.6.2  6.6.0  PJD    04/03/13 Wording changed for 548
--   3.6.3  6.9.0  PJD    29/04/14 Wording changed for 673
--   3.6.3  6.9.0  PJD    02/06/15 Added 378 - Dup SRQ and 379 Invalid SRQ Number
--   3.6.4  6.11   AJ     04/03/15 Amended code 691 again as done by Vish in Feb11 but for site V2C only
--   3.7    6.11   AJ     02/06/15 Added 3.6.3(PJD) changes and re numbered 3.6.3(AJ) changes to 3.6.4
--   3.7.1  6.11   AJ     18/08/15 Amended wording on 856 "/person" and Added 380 and 381 - Void Events DL
--   3.7.2  6.11   AJ     14/12/15 added 048 HAD error originally added to bespoke version of hdl_errs_in
--                                 sent to site on 14/01/2014 (v6.9) 
--
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
AND    err_object_shortname = 'HDL';
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
  values('HDL',p_err_refno,         
  p_err_text,        
  'V','N');
dbms_output.put_line('Error number '||p_err_refno||' inserted.');
ELSIF
   l_err_message != p_err_text       
   THEN
     UPDATE errors
     SET    err_message = p_err_text    
     WHERE  err_object_shortname = 'HDL'
       AND   err_refno = p_err_refno;
dbms_output.put_line('Error number '||p_err_refno||' updated.');
END IF;
--
END ins_err;
--
--
BEGIN
ins_err(1,'Property already exists on PROPERTIES');
ins_err(2,'Invalid owned property indicator - must be Y or N');
ins_err(3,'Invalid residential property indicator - must be Y or N');
ins_err(4,'Invalid revenue account indicator - must be Y or N');
ins_err(5,'Invalid local indicator - must be Y or N');
ins_err(6,'Invalid service property indicator - must be Y or N');
ins_err(7,'Invalid defects indicator - must be Y or N');
ins_err(8,'Invalid property ownership type');
ins_err(9,'Invalid property sub-type');
ins_err(10,'Invalid property maintenance responsibility type');
ins_err(11,'Invalid property source type');
ins_err(12,'Invalid property type / subtype combination');
ins_err(13,'No freeholder details supplied');
ins_err(14,'Property status must be null or of type C (closed)');
ins_err(15,'Property status start date must be entered if property status is not null');
ins_err(16,'Property street code must be null or exist on C_ADDRESS_ELEMENTS');
ins_err(17,'Either the area or the town or both should be entered');
ins_err(18,'Freeholder name may not be entered if property owned by organisation');
ins_err(19,'Freeholder address line 1 may not be entered if property owned by organisation');
ins_err(20,'Freeholder address line 2 may not be entered if property owned by organisation');
ins_err(21,'Freeholder address line 3 may not be entered if property owned by organisation');
ins_err(22,'Freeholder post code may not be entered if property owned by organisation');
ins_err(23,'Freeholder phone number may not be entered if property owned by organisation');
ins_err(24,'Leasehold start date may not be entered if property owned by organisation');
ins_err(25,'Leasehold review date may not be entered if property owned by organisation');
ins_err(26,'Maintenance type may not be entered if property owned by organisation');
ins_err(27,'Lease type may not be entered if property is owned by organisation');
ins_err(28,'Invalid property status');
ins_err(29,'Effective date is not a valid DEB effective date for RAC CLASS CODE (LIA)');
ins_err(30,'Property reference does not exist on PROPERTIES');
ins_err(31,'Invalid element type code');
ins_err(32,'Derived attribute type inconsistent with attribute data provided');
ins_err(33,'Coded element has no attribute code');
ins_err(34,'Multi-value element has no attribute code');
ins_err(35,'Numeric element has no value');
ins_err(36,'Date element has no date');
ins_err(37,'Coded element may not have a date or numeric value');
ins_err(38,'Multi-value element may not have a date or numeric value');
ins_err(39,'Numeric element may not have a date or coded value');
ins_err(40,'Date element may not have a numeric or coded value');
ins_err(41,'Invalid attribute code');
ins_err(42,'Invalid repair category');
ins_err(43,'Element end date must be later than element start date');
ins_err(44,'No year exists on ADMIN_YEARS for the debit breakdown start date and relevant admin unit');
ins_err(45,'Property already linked to an admin unit of this type');
ins_err(46,'Element Start Date must be supplied');
ins_err(47,'Invalid Stock Condition Survey Results Ref');
ins_err(48,'Street Index conflicts with an existing record');
ins_err(50,'Admin unit does not exist on ADMIN_UNITS');
ins_err(51,'Admin unit not current');
ins_err(52,'Notice to quit reason supplied without a notice to quit date');
ins_err(53,'Right to buy deferred reason supplied without a deferred date');
ins_err(54,'Right to buy cancelled reason supplied without a cancelled date');
ins_err(55,'Notice to quit date must not be earlier than tenancy start date');
ins_err(56,'Expected end date must not be earlier than tenancy start date');
ins_err(57,'Notice received date must not be earlier than tenancy start date');
ins_err(59,'Tenancy already exists for this date/period');
ins_err(60,'Invalid tenure type');
ins_err(61,'Invalid tenancy source');
ins_err(62,'Invalid tenancy type');
ins_err(63,'Invalid termination reason');
ins_err(64,'Invalid notice to quit reason');
ins_err(65,'Invalid right to buy deferred reason');
ins_err(66,'Invalid right to buy cancelled reason');
ins_err(67,'Invalid tenancy status');
ins_err(69,'Secondary properties may not be residential');
ins_err(70,'Not a housing property');
ins_err(71,'Tenancy holding start date earlier than tenancy start date');
ins_err(72,'Tenancy holding end date later than tenancy end date');
ins_err(73,'No termination reason code supplied');
ins_err(74,'No notice to quit reason code supplied');
ins_err(75,'Secure tenancy must start on rent week start specified in HOU_SYS_PARAMS (RWSTART)');
ins_err(76,'Tenancy end date earlier than tenancy start date');
ins_err(77,'Tenancy holding end date earlier than tenancy holding start date');
ins_err(78,'Tenancy holding end date supplied without a start date');
ins_err(79,'Termination code supplied without a tenancy end date');
ins_err(80,'Tenancy reference does not exist on TENANCIES');
ins_err(81,'Invalid disabled indicator - must be Y or N');
ins_err(82,'Invalid OAP indicator - must be Y or N');
ins_err(83,'Invalid tenancy indicator - must be Y or N');
ins_err(84,'Invalid succession indicator - must be Y or N');
ins_err(85,'Invalid start date reason');
ins_err(86,'Invalid sex code');
ins_err(87,'Invalid relationship');
ins_err(88,'invalid marital status');
ins_err(89,'Invalid end date reason');
ins_err(90,'Tenancy instance start date may not be earlier than tenancy start date');
ins_err(91,'Tenancy instance end date may not be later than tenancy end date');
ins_err(92,'Invalid ethnic origin');
ins_err(93,'Person End Reason/End Date - One cannot be supplied without the other');
ins_err(94,'Person end date must not be earlier than person start date');
ins_err(95,'A DRS/DRA must have a tra_dr value');
ins_err(97,'Another tenancy exists for the same property with the same start date');
ins_err(98,'Tenancy holding end date must be supplied if tenancy end date supplied');
ins_err(99,'Permanent/Temporary Indicator must be Y or N');
ins_err(100,'Account already exists on REVENUE_ACCOUNTS');
ins_err(101,'Invalid first direct debit taken indicator - must be Y or N');
ins_err(102,'Invalid account type');
ins_err(103,'Invalid payment method');
ins_err(104,'No payment profile exists for standing order or direct debit');
ins_err(105,'Payment method start date is before account start date');
ins_err(106,'Account end date is before account start date');
ins_err(107,'Last statement date is in the future');
ins_err(108,'Last statement date is before account start date');
ins_err(109,'One or more bank details missing');
ins_err(110,'One or more bank details supplied but no bank account number supplied');
ins_err(111,'No tenancy exists for this account');
ins_err(112,'Account start date is before tenancy start date');
ins_err(113,'Property does not exist on TCY_HOLDINGS for related tenancy');
ins_err(114,'Debit_to date is not a valid admin period end date');
ins_err(115,'Account balance already exists on ACCOUNT_BALANCES');
ins_err(116,'Balance date is not a valid admin period end date');
ins_err(117,'Payment reference does not exist on REVENUE_ACCOUNTS');
ins_err(118,'Debit breakdown already exists on DEBIT_BREAKDOWNS');
ins_err(119,'Debit breakdown end date must be null or later than start date');
ins_err(120,'Either an attribute code or an element value must be supplied');
ins_err(121,'An attribute code and element value may not be supplied together');
ins_err(122,'Invalid VAT ID');
ins_err(123,'No VAT rate current for this VAT ID and debit breakdown start date');
ins_err(124,'Property is not linked to the admin unit');
ins_err(125,'No entry exists on TCY_HOLDINGS for this property, tenancy and dates');
ins_err(126,'Property element does not exist');
ins_err(127,'Attribute code on debit breakdown does not match property element');
ins_err(128,'Element value on debit breakdown does not match property element');
ins_err(129,'Not a rents admin unit');
ins_err(130,'Invalid transaction type');
ins_err(131,'A transaction subtype must be supplied for this transaction type');
ins_err(132,'Invalid transaction subtype');
ins_err(133,'Invalid payment method');
ins_err(134,'Transaction date must not be before revenue account start date');
ins_err(135,'Effective date must not be before revenue account start date');
ins_err(136,'Payment date must not be before revenue account start date');
ins_err(137,'Effective date is not a valid admin period effective date');
ins_err(138,'Either a debit or credit amount must be supplied');
ins_err(139,'Either a debit or credit amount must be supplied but not both');
ins_err(140,'Either a VAT debit or credit amount may be supplied but not both');
ins_err(141,'A VAT credit may not be supplied with a debit amount');
ins_err(142,'A VAT debit may not be supplied with a credit amount');
ins_err(143,'The balance indicator must be null or N');
ins_err(144,'If a balance year has been supplied, a balance period must also be supplied');
ins_err(145,'If a balance period has been supplied, a balance year must also be supplied');
ins_err(146,'Invalid balance year');
ins_err(147,'Invalid balance period number');
ins_err(148,'Invalid arrears action code');
ins_err(149,'Log date must not be before revenue account start date');
ins_err(150,'Expiry date must not be before account start date');
ins_err(151,'Next action date must not be before revenue account start date');
ins_err(152,'Authorised date must not be before revenue account start date');
ins_err(153,'Completed date must not be before revenue account start date');
ins_err(154,'Arrears dispute start date must not be before revenue account start date');
ins_err(155,'Arrears dispute end date must not be before revenue account start date');
ins_err(156,'Invalid arrears dispute indicator - must be Y or N');
ins_err(157,'If an arrears dispute is indicated, a start date must be supplied');
ins_err(158,'If arrears dispute details are supplied the indicator must be set to Y');
ins_err(159,'Arrears dispute end date must not be before arrears dispute start date');
ins_err(160,'No summary rent exists for this account on the given date');
ins_err(161,'Gross rent on SUMMARY_RENTS does not match value supplied');
ins_err(162,'No balance period exists on ADMIN_PERIODS for this account');
ins_err(163,'Deleted date must be before revenue account start date');
ins_err(164,'Balance on ACCOUNT_BALANCES does not match balance supplied');
ins_err(166,'Status code must be one of COMP,AUTH,PEND,DEL');
ins_err(167,'A status of COMP requires a completed date and USERNAME');
ins_err(168,'A status of AUTH requires an authorised date and USERNAME');
ins_err(169,'A status of DEL requires a deleted date and USERNAME');
ins_err(170,'Arrears dispute end date supplied without a start date');
ins_err(171,'No balance period exists on ADMIN_PERIODS for this transaction');
ins_err(172,'No suitable record found in Tenancy Holdings');
ins_err(173,'Account linked to another tenancy'); 
ins_err(174,'Account already linked to this property');
ins_err(175,'Invalid Transaction type for Suspense Account');
ins_err(176,'Notice type actions must have an Expiry Date');
ins_err(180,'Not a service charge admin unit');
ins_err(181,'Admitted Ind must be Y or N for a Non Tenant');
ins_err(182,'Party does not exist in tenancy_instances for this application');
ins_err(190,'Invalid Initial Period Start Reason Code.');
ins_err(191,'Refernce Period Start Reason Code.');
ins_err(192,'Invalid Legislation Code.');
ins_err(193,'Details already exist for this arrangement');
ins_err(194,'Agent does not exist on Parties table');
ins_err(195,'Admin Unit Code contains Invalid Characters only A-Z, 0-9 and hyphens allowed');
ins_err(196,'Invalid Deny Reason Code');
ins_err(197,'If Denied Reason Code supplied then Denied Indicator must be N.');
ins_err(198,'Account already linked to this admin unit in NCT_AUN_LINKS');
ins_err(199,'Account linked to a tenancy and cannot have a NCT_AUN_LINK entry');
ins_err(200,'User application reference already exists on APPLIC_LIST_ENTRIES (ALE_APPLIC_ID)');
ins_err(201,'No applicant exists on HDL_INVOLVED_PARTIES for this application refno');
ins_err(202,'Application received date must not be later than today');
ins_err(203,'The application Sco Code must be NEW, CUR, HSD or CLD.');
ins_err(204,'The date the applicant is expected to be homeless is required');
ins_err(205,'There should not be an existing rehousing list decision held against the application.');
ins_err(206,'A valid rehousing list decision code should have been supplied.');
ins_err(207,'An application list entry history should relate to a valid application list entry.');
ins_err(208,'The type indicator should be S tatus,C ategory or G eneral.');
ins_err(209,'The action indicator should be U pdated or I nserted.');
ins_err(210,'Application already exists on APPLICATIONS');
ins_err(211,'Interested Party is not of the correct type');
ins_err(212,'Case worker does not exist in Interested_parties');
ins_err(213,'Invalid homeless reason');
ins_err(214,'Invalid homeless origin');
ins_err(215,'This row already exists on HOMELESS_DECISIONS');
ins_err(216,'No corresponding row exists on APPLICATIONS');
ins_err(217,'The rehousing list code does not represent a homeless list');
ins_err(218,'The rehousing list code does not exist on REHOUSING_LISTS');
ins_err(219,'Invalid stage sequence number');
ins_err(220,'Invalid stage code');
ins_err(221,'Invalid homeless decision code');
ins_err(222,'Invalid homeless decision reason');
ins_err(223,'This row already exists on APPLIC_LIST_ENTRIES');
ins_err(224,'A homeless caseworker has not been supplied for this application'	);
ins_err(225,'A general application must not appear on a homeless list');
ins_err(226,'This is a transfer list entry but no tenancy number exists on the corresponding application');
ins_err(228,'Admin unit is not of type OFF');
ins_err(229,'Registration date must not be a future date');
ins_err(230,'Re-registration date must be later than the registration date');
ins_err(231,'Invalid list qualification reason code');
ins_err(232,'Invalid application category');
ins_err(233,'No matching Application found');
ins_err(235,'Invalid joint applicant indicator - must be Y or N');
ins_err(236,'Invalid living apart indicator - must be Y or N');
ins_err(237,'Invalid rehouse indicator - must be Y or N');
ins_err(238,'Party start date must be later than the application received date');
ins_err(239,'Joint applicant indicator must be Y for main applicant');
ins_err(240,'Living apart indicator must be N for main applicant');
ins_err(241,'Rehouse indicator must be Y for main applicant');
ins_err(242,'Tenancy refno does not match the one on the application');
ins_err(243,'Alternative person reference does not exist on PEOPLE');
ins_err(244,'This person does not exist on TCY_INSTANCES for this tenancy');
ins_err(245,'This row already exists on INVOLVED_PARTIES');
ins_err(246,'Too many people exist on PEOPLE matching the supplied details');
ins_err(247,'This person has not been uniquely identified on PEOPLE but a tenancy ref has been supplied');
ins_err(248,'User application reference does not exist on APPLICATIONS');
ins_err(249,'Invalid Rehousing List/Application Category Combination');
ins_err(250,'Invalid List Stage/Decision combination');
ins_err(251,'Category Start Date must be later than the registration date');
ins_err(260,'Involved party sequence ref must be null for APPL address usage type');
ins_err(261,'Involved party sequence ref does not exist on INVOLVED_PARTIES for this application');
ins_err(262,'Invalid address usage');
ins_err(263,'Address usage already exists on ADDRESS_USAGES');
ins_err(270,'Question refno does not exist on QUESTIONS');
ins_err(271,'A lettings area code must be entered for an area preference answer');
ins_err(272,'The lettings area code does not exist on LETTINGS_AREAS');
ins_err(273,'The lettings area code must be a bottom level code on LETTINGS_AREAS');
ins_err(274,'An involved party seqno must be entered for an involved party answer');
ins_err(275,'Mandatory answer has a value of null');
ins_err(276,'Answer to a date question is an invalid date');
ins_err(277,'Answer to a number question is an invalid number');
ins_err(278,'Answer to a number question is out of permitted range(s)');
ins_err(279,'Answer to a coded question is an invalid code');
ins_err(280,'Answer to a Y/N question is not Y or N');
ins_err(281,'Answer already exists on APPLIC_ANSWERS');
ins_err(289,'Not all mandatory questions have been answered');
ins_err(290,'Application status code does not exist on APPLIC_STATUSES');
ins_err(291,'This status code requires both start and end dates');
ins_err(292,'This status code requires a reason code');
ins_err(293,'Invalid status reason code');
ins_err(294,'A mandatory general question has not been answered');
ins_err(295,'A mandatory involved question has not been answered');
ins_err(296,'A mandatory area preferences question has not been answered');
ins_err(297,'Optional response does not exist on QUESTION_OPTIONAL_RESPONSES');
ins_err(298,'No row exists on APPLIC_LIST_ENTRIES for this application and rehousing list code');
ins_err(299,'Answer to an area preferences question must be Y or N');
ins_err(300,'Invalid property/admin unit indicator - must be P or A');
ins_err(301,'Survey reference does not exist on STOCK_CONDITN_SURVEYS');
ins_err(302,'Action code does not exist under domain SVACTION');
ins_err(303,'Urgency code does not exist under domain REPURGENCY');
ins_err(304,'Material code does not exist under domain MATERIAL');
ins_err(305,'Repair type does not exist under domain REPRTYPE');
ins_err(306,'Unit code does not exist under domain PMUNITS');
ins_err(307,'Survey sub-component type does not exist under domain SUBC_TYPE');
ins_err(308,'Survey component code does not exist on SURVEY_COMPONENT_CODES');
ins_err(309,'A sub-component coded value has been entered without a sub-component type');
ins_err(310,'A sub-component date value has been entered without a sub-component type');
ins_err(311,'A sub-component numeric value has been entered without a sub-component type');
ins_err(312,'A sub-component coded value has not been entered');
ins_err(313,'Survey sub-component code does not exist on SURVEY_SUB_COMPONENT_CODES');
ins_err(314,'A sub-component date value has been entered against a coded or multi sub-component type');
ins_err(315,'A sub-component numeric value has been entered against a coded or multi sub-component type');
ins_err(316,'A sub-component date value has not been entered');
ins_err(317,'A sub-component coded value has been entered against a date sub-component type');
ins_err(318,'A sub-component numeric value has been entered against a date sub-component type');
ins_err(319,'A sub-component numeric value has not been entered');
ins_err(320,'A sub-component coded value has been entered against a numeric sub-component type');
ins_err(321,'A sub-component date value has been entered against a numeric sub-component type');
ins_err(322,'A user-defined attribute value has been entered without a corresponding entry on UDA_DEFINITIONS');
ins_err(323,'No corresponding entry exists on SURVEY_ADDRESSES');
ins_err(324,'No corresponding entry exists on SURVEY_COMPONENTS');
ins_err(325,'Coded element already exists for this property for this period.');
ins_err(326,'Invalid Geographic Origin Code ');
ins_err(327,'Invalid Language Code ');
ins_err(328,'Invalid Further Att Code ');
ins_err(329,'Invalid Element Location ');
ins_err(330,'A valid (List Statuses) List Code should be entered.');
ins_err(331,'Main Applicant Indicator should be Y or N.');
ins_err(332,'This general answer already exists');
ins_err(333,'Question_category should be one of the following CC/HR/OS/GA/SG/MD/DQ');
ins_err(334,'This involved party answer already exists');
ins_err(335,'For involved party answers the que_qgr_question_category should be IP');
ins_err(336,'This is reference value does not exist in the INVOLVED PARTIES table');
ins_err(337,'The record must not already exist on the medical answers table');
ins_err(338,'For medical answers the que_qgr_question_category should be MA');
ins_err(339,'This record must not already exist on the lettings_area_answers table');
ins_err(340,'For lettings areas the question category (que_question_category) should be LA');
ins_err(341,'For this status code a start date is required');
ins_err(342,'A Valid language is required for this person');
ins_err(343,'This medical referral already exists');
ins_err(344,'The referrals status code should be LOG or CUR or CLD');
ins_err(345,'Referrals status code is CUR, both the award date and assessment code should be supplied');
ins_err(346,'A valid medical referral assessment code should be supplied');
ins_err(347,'This medical referral assessment code supplied should also be in the medical referrals table');
ins_err(348,'This referral date supplied should also be in the medical referrals table');
ins_err(349,'Either the referral date or medical assessment reference should be supplied');
ins_err(350,'No Matching Transaction Found');
ins_err(351,'Parent Property Not Found');
ins_err(352,'Arrears Action Type does not match that derived');
ins_err(353,'Repair Condition does not exist under domain REPCOND');
ins_err(354,'Handheld Indicator has to be Y, N or NULL');
ins_err(355,'Copied Indicator has to be Y, N or NULL');
ins_err(356,'No matching Defect Type Code');
ins_err(357,'No Tenant found for the tenancy');
ins_err(358,'No matching Void Group');
ins_err(359,'No matching Void Status');
ins_err(360,'No matching Void Class');
ins_err(361,'No matching Start Event');
ins_err(362,'No matching Void Event');
ins_err(363,'No matching End Event');
ins_err(364,'Property reference must be supplied');
ins_err(365,'Property does not exist in Property Landlords');
ins_err(366,'Property reference does not exist in Properties table');
ins_err(367,'Lease start date must be supplied');
ins_err(368,'Lease start day is inconsistent with lease type');
ins_err(369,'Lease end date must be supplied');
ins_err(370,'Lease end date must not be before lease start date');
ins_err(371,'Lease type must be supplied');
ins_err(372,'Lease type is invalid');
ins_err(373,'Lease status code must be supplied');
ins_err(374,'Lease status code is invalid');
ins_err(375,'Lease status date must be supplied');
ins_err(376,'Scheme code must be supplied');
ins_err(377,'Invalid scheme code');
ins_err(378,'Duplicate Service Request Number');
ins_err(379,'Invalid Format for Service Request Number');
ins_err(380,'No Matching Void Instance');
ins_err(381,'Duplicate Void Event Sequence');
ins_err(389,'Pro Alt Ref already exists for a different property');
ins_err(390,'No matching Login Profile found');
ins_err(391,'User with this Username already exists');
ins_err(392,'Password change date must be supplied');
ins_err(393,'Encrypt password flag must be Y or N or null');
ins_err(394,'Force password change ind must be Y or N or null');
ins_err(395,'Password must be supplied');
ins_err(396,'GPI Notification Ind must be Y or N or null');
ins_err(397,'Origine must be P, E or Q');
ins_err(398,'Chargeable Indicator must be Y or N');
ins_err(400,'Invalid Arrears Action Type');
ins_err(401,'Invalid Arrears Action Status');
ins_err(402,'Invalid Arrears Dispute Indicator');
ins_err(403,'Arrears Escalation Policy Not Found');
ins_err(404,'Arrears Escalation Policy must be supplied or an AUTO action');
ins_err(405,'Invalid Tenancy Termination Condition Code');
ins_err(406,'Invalid Tenancy/Property1 termination reason code');
ins_err(407,'Invalid Tenancy/Property2 termination reason code');
ins_err(408,'Invalid Tenancy/Property3 termination reason code');
ins_err(409,'Invalid Tenancy/Property4 termination reason code');
ins_err(410,'Invalid Tenancy/Property5 termination reason code');
ins_err(411,'Invalid Tenancy/Property6 termination reason code');
ins_err(412,'Main Tenant Indicator must be Y or N');
ins_err(413,'Invalid Person/Tenancy End Reason Code');
ins_err(414,'Invalid Job Class');
ins_err(415,'Invalid Location Code');
ins_err(416,'Not a valid policy/action combination');
ins_err(417,'Overlapping Property Status for Property1');
ins_err(418,'Overlapping Property Status for Property2');
ins_err(419,'Overlapping Property Status for Property3');
ins_err(420,'Overlapping Property Status for Property4');
ins_err(421,'Overlapping Property Status for Property5');
ins_err(422,'Overlapping Property Status for Property6');
ins_err(423,'Overlapping Property Status');
ins_err(424,'Interested Party shortname must be supplied if other Interested Party fields supplied');
ins_err(425,'Interested Party already exists.');
ins_err(426,'If Interested Shortname supplied, Placement Indicator must also be supplied');
ins_err(427,'If Interested Shortname supplied, Current Indicator must also be supplied');
ins_err(428,'If Interested Shortname supplied, Party Type must also be supplied');
ins_err(429,'Placement Indicator must be Y or N');
ins_err(430,'Current Indicator must be Y or N');
ins_err(431,'Invalid Interested Party Type');
ins_err(432,'Username does not exist on users table.');
ins_err(433,'Printer does not exist on system printers table');
ins_err(434,'Invalid Admin Unit code');
ins_err(435,'Area Code must be HRM or HPM');
ins_err(436,'Party Type must be one of ORG, HOUP, FREE or BOTP');
ins_err(437,'Organisation type can only be entered for party types ORG');
ins_err(438,'Organisation type does not exist in ORG_TYPE domain');
ins_err(440,'Mapping Code is Mandatory');
ins_err(441,'Mapping Code does not exist in hfi_mappings table');
ins_err(442,'Mapping Type is Mandatory');
ins_err(443,'Mapping Type does not exist in hfi_mapping_types table');
ins_err(444,'Mapping Type not linked to a Mapping Code');
ins_err(445,'Map Type Code is Mandatory');
ins_err(446,'Map Type Code does not exist for Mapping Type ADMIN_UNITS');
ins_err(447,'Map Type Code does not exist for Mapping Type ATTRIBUTES');
ins_err(448,'Map Type Code does not exist for Mapping Type ELEMENTS ');
ins_err(449,'Map Type Code does not exist for Mapping Type HOU_PROP_STATUS_CODES');
ins_err(450,'Map Type Code does not exist for Mapping Type HRV_ACCOUNT_TYPES domain RAC_TYPE');
ins_err(451,'Map Type Code does not exist for Mapping Type HRV_JOB_CLASS domain JOB_CLASS');
ins_err(452,'Map Type Code does not exist for Mapping Type HRV_LIABILITY domain LIABLE');
ins_err(453,'Map Type Code does not exist for Mapping Type HRV_TRADE domain TRADE');
ins_err(454,'Map Type Code does not exist for Mapping Type PRIORITIES');
ins_err(456,'Map Type Code does not exist for Mapping Type SCHEDULE_OF_RATES');
ins_err(457,'Map Type Code does not exist for Mapping Type TRANSACTION_SUBTYPES');
ins_err(458,'Map Type Code does not exist for Mapping Type TRANSACTION_TYPES');
ins_err(459,'Map Type Code does not exist for Mapping Type WORK_PROGRAMMES');
ins_err(460,'Map Type SubCode only required for Mapping Type TRANSACTION_SUBTYPES and ATTRIBUTES');
ins_err(461,'Map Type SubCode not supplied for Mapping Type TRANSACTION_SUBTYPES and ATTRIBUTES');
ins_err(462,'Mapping Value is Mandatory');
ins_err(463,'Mapping Code already exists');
ins_err(464,'Admin Type not supplied for Mapping Type ADMIN_UNITS');
ins_err(465,'Admin Type does not exist in ADMIN_UNITS table for Mapping Type ADMIN_UNITS');
ins_err(466,'Either HFI_DEFAULT_TYPE or HFI_DEFAULT_SUBTYPE can be supplied, not both');
ins_err(467,'HFI Segment Code is Mandatory');
ins_err(468,'HFI SEQNO is Mandatory');
ins_err(469,'HFI Segment Type is Mandatory');
ins_err(470,'Invalid HFI Segment Type Supplied');
ins_err(471,'HFI segment object mapping type only required for Segment type MSE');
ins_err(472,'HFI Mapping Name not Supplied for Segment Type MSE');
ins_err(473,'Mapping Name does not exist in hfi_mappings table');
ins_err(474,'Mapping Name only required for Segment Type MSE');
ins_err(475,'HFI Constant not supplied for Segment type CSE');
ins_err(476,'HFI Constant only required for Segment type CSE');
ins_err(477,'Neither HFI_MAPPING_NAME or HFI_CONSTANT supplied');
ins_err(478,'Data Type is Mandatory');
ins_err(479,'Invalid Data Type Supplied');
ins_err(480,'Account Type is Mandatory');
ins_err(481,'Account Segment Element is Mandatory');
ins_err(482,'Account Segment Element does not exist in hfi_segment_elements table');
ins_err(483,'Control Segment Element is Mandatory');
ins_err(484,'Control Segment Element does not exist in hfi_segment_elements table');
ins_err(485,'Sequence Number is Mandatory');
ins_err(486,'Repeat Period must be D, W, M or Y');
ins_err(487,'HRM Element Update Indicator must be Y or N');
ins_err(488,'HPM Element Update Indicator must be Y or N');
ins_err(489,'Allow Break Indicator must be Y or N');
ins_err(490,'Deliverable not found for key - contract/property/standard deliverable code');
ins_err(491,'Deliverable found but has wrong status - must be AUThorised');
ins_err(492,'Incoming deliverable status must be COM or DEL');
ins_err(493,'Reason code unrecognised');
ins_err(494,'Planned start date is outside contract_version start and end');
ins_err(495,'Completion date cannot be greater than todays date');
ins_err(496,'Completion date must be after the start date');
ins_err(497,'Estimated cost cannot be changed on a deiverable which is for a standard SOR');
ins_err(498,'Warranty code does not exist');
ins_err(499,'The sum of the deliverable costs for contract will exceed the maximum variation amount for the contract');
ins_err(500,'Invalid Property Purchase Defect code.');
ins_err(501,'Invalid Major Work code.');
ins_err(502,'The Charge End date must fall on a Financial Year End for the Admin Unit.');
ins_err(505,'Element Types related to the Property are not active.');
ins_err(506,'Element Types related to the Propertys Admin Unit are not active.');
ins_err(507,'Displayed Reference must be supplied.');
ins_err(508,'Correspondence Name must be supplied.');
ins_err(509,'Current Status Start Date must be supplied.');
ins_err(510,'Matching Party cannot be found on Parties table.');
ins_err(511,'Improvement Sequence must be supplied.');
ins_err(512,'Improvement description must be supplied.');
ins_err(513,'Valuation sequence must be supplied if valuation amount supplied.');
ins_err(514,'Valuation sequence does not exist on pp_valuations for application.');
ins_err(515,'Valuation sequence must be supplied.');
ins_err(516,'Valuation Status Date must be supplied.');
ins_err(517,'Valuation Requested Date must be supplied.');
ins_err(518,'Period Start Date must be supplied.');
ins_err(519,'Period End Date must be supplied.');
ins_err(520,'Tenant Name must be supplied.');
ins_err(521,'Tenant Address must be supplied');
ins_err(522,'Current Landlord Indicator must be Y or N.');
ins_err(523,'Negative Indicator must be Y or N.');
ins_err(524,'Repeat Warning Indicator must be Y or N.');
ins_err(525,'Warning Issued Indicator must be Y or N.');
ins_err(526,'Budget Head Code must be supplied.');
ins_err(527,'Budget Head Description must be supplied.');
ins_err(528,'Budget year not found on budget_calendars.');
ins_err(529,'Budget Type must be C or N.');
ins_err(530,'Budget Amount must be supplied.');
ins_err(531,'Invalid Status Code.');
ins_err(532,'Parent Code not found.');
ins_err(533,'Budget Profile code not found on budget_profile_calendars');
ins_err(534,'Interested Party shortname must be supplied.');
ins_err(535,'Either username or surname must be supplied.');
ins_err(536,'Only one of username and surname must be supplied.');
ins_err(537,'Invalid Title.');
ins_err(538,'Job role does not exist on job_roles.');
ins_err(539,'Parent Admin Unit code does not exist');
ins_err(540,'Attribute Code does not exist on ATTRIBUTES for element supplied');
ins_err(541,'Start Date must be supplied');
ins_err(542,'End date must not be before Start Date');
ins_err(543,'Parent Admin Unit Element Code does not exist on ELEMENTS');
ins_err(544,'Parent Admin Unit Attribute Code does not exist on ATTRIBUTES for element supplied');
ins_err(545,'Parent Admin Unit Start Date cannot be before Start Date');
ins_err(546,'Parent Admin Unit not linked to Child Admin Unit');
ins_err(547,'Period Code must be supplied');
ins_err(548,'Start Date clashes with an existing record.');
ins_err(549,'End Date must be supplied');
ins_err(550,'Status Code must be one of A, I, R');
ins_err(551,'Start Date must be after original start date of period rolled forward from');
ins_err(552,'Entry does not exist on Service Charge Periods for code and date supplied');
ins_err(553,'End date must be later than rolled forward start date');
ins_err(554,'Invalid cost basis. Must be one of P, W, M, Q, H, Y');
ins_err(555,'Apportioned Indicator must be Y or N');
ins_err(556,'Capping Indicator must be Y or N');
ins_err(557,'Complete Indicator must be Y or N');
ins_err(558,'Rebateable Indicator must be Y or N');
ins_err(559,'Increase type must be one of A, P, F');
ins_err(560,'Tax Indicator must be one of A, B');
ins_err(561,'Service Charge Applicable Indicator must be one of R, L, B');
ins_err(562,'Adjustment Method must be one of SYS, EXT, NO');
ins_err(563,'Debit Effective Date must not be before Start Date');
ins_err(564,'Description must be supplied');
ins_err(565,'Weighting Element does not exist on ELEMENTS');
ins_err(566,'Cost Group Percentage must be between 0.00 and 100.00');
ins_err(567,'Cost Group attribute Code does not exist on ATTRIBUTES for element supplied');
ins_err(568,'Entry does not exist in Service Charge Bases for Code, Date, Element and Attribute');
ins_err(569,'Entry does not exist in Management Cost Groups for Code, Date, Element and Attribute');
ins_err(570,'Tax code must be Y or N');
ins_err(571,'Invalid Legislation Code');
ins_err(572,'Interested Party not found');
ins_err(573,'Invalid Initial Lease Start Reason Code');
ins_err(574,'Invalid Lease Termination Reason Code');
ins_err(575,'Invalid Lease Ref Period Start Reason Code');
ins_err(576,'Lease Start Date must be supplied');
ins_err(577,'Lease Record Type Indicator must be L or F');
ins_err(578,'Lease End Date must not be before Lease Start Date');
ins_err(579,'Lease does not exist for Property and Date');
ins_err(580,'Lease Assignment Start Date must be supplied');
ins_err(581,'Lease Assignment End Date must not be before Lease Assignment Start Date');
ins_err(582,'Lease Assignment Start Date must not be before Lease Start Date');
ins_err(583,'Lease Assignment does not exist for Property, lease date and lease assignment date');
ins_err(584,'Lease Party Start Date must be supplied');
ins_err(585,'Lease Party End Date must not be before Lease Party Start Date');
ins_err(586,'Lease Party Start Date must not be before Lease Assignment Start Date');
ins_err(587,'Lease Does not exist on Lease Assignments');
ins_err(588,'Retained Party Ref does not Exist');
ins_err(589,'Party to be Deleted does not Exist');
ins_err(590,'Delete Process not Applicable for this Dataload');
ins_err(591,'Party References cannot be the same');
ins_err(592,'Person does not exist in the household for the Tenancy/Account');
ins_err(593,'Element does not exist as an SP Service');
ins_err(594,'Invalid Estimated/Actual Indicator');
ins_err(595,'No matching Service Charge Bases');
ins_err(596,'Increase Type not consistent with Bases');
ins_err(597,'Ser Req status code must be RAI, CAN, COM');
ins_err(601,'Property does not have required service');
ins_err(602,'Service Charge Rate has been reconciled');
ins_err(603,'Service Charge Rate has associated Estimated components');
ins_err(604,'Associated Service Charge Basis has been completed');
ins_err(605,'Service Charge Rate does not exist');
ins_err(606,'Service Charge Rate has associated Actual components');
ins_err(607,'Associated SCP is not Inactive');
ins_err(608,'Associated Service Charge Period status is NOT valid');
ins_err(609,'Rate conflict(s) exist for the SERVICE CHARGE BASIS associated with this SERVICE CHARGE RATE ');
ins_err(610,'Mapping of Admin Unit failed.');
ins_err(611,'Mapping of Element failed.');
ins_err(612,'Mapping of Attribute failed.');
ins_err(613,'Invalid Service Charge Period / Start Date.');
ins_err(614,'Invalid Service.');
ins_err(615,'The sum of Service Charge Rate Component amounts does not equal Amount held against Service Charge Rate.');
ins_err(618,'Invalid Record Type');
ins_err(619,'Admin Year already exists on admin years');
ins_err(620,'Active/Closed Indicator must be A, C or N');
ins_err(621,'Debit basis must be W or M');
ins_err(622,'Pro-Rata indicator must be A, B, S or T');
ins_err(623,'Force Period Indicator must be Y or N');
ins_err(624,'Weeks in year must be between 45 and 54');
ins_err(625,'Invalid Rent Week Start Date');
ins_err(626,'Admin Year Start Date must be supplied');
ins_err(627,'Admin Year Must be supplied');
ins_err(628,'Record already exists on admin_year_multipliers');
ins_err(629,'Admin year does not exist on admin_years');
ins_err(630,'Active Indicator must be Y or N');
ins_err(631,'Invalid modify reason');
ins_err(632,'Start Date must be supplied');
ins_err(633,'End Date must be supplied');
ins_err(634,'Multiplier Value must be supplied');
ins_err(635,'Admin period Type must be BAL or DEB');
ins_err(636,'Record already exists on admin_periods');
ins_err(637,'Period No must be supplied');
ins_err(638,'Invalid payment profile code');
ins_err(639,'Payment Profile already exists');
ins_err(640,'Payment Profile record does not exist');
ins_err(641,'Add Remainder Indicator must be Y or N');
ins_err(642,'Payment Due Date must be supplied');
ins_err(643,'Invalid Credit Balance Indicator');
ins_err(644,'Invalid Debit Balance Indicator');
ins_err(645,'No of Instalment must be supplied if Debit Balance Ind = A');
ins_err(646,'Minimum Balance Variation must be supplied');
ins_err(647,'Minimum Rent Change must be supplied');
ins_err(648,'DR amount supplied but Debit Balance Ind not A');
ins_err(649,'Standard amount supplied but Debit Balance Ind not S');
ins_err(650,'Summary Setup record already exists');
ins_err(651,'Record does not exist in summary_setups');
ins_err(652,'Summary Admin Level record already exists');
ins_err(653,'Invalid Admin Unit Type');
ins_err(654,'Mandatory Indicator must be Y or N');
ins_err(655,'Invalid Transaction Type');
ins_err(656,'Invalid VAT Code');
ins_err(657,'Transaction sub type must be supplied');
ins_err(658,'Transaction subtype name must be supplied');
ins_err(659,'Restricted Indicator must be Y or N');
ins_err(660,'Invalid Transaction subtype type');
ins_err(661,'Transaction Type must be supplied');
ins_err(662,'Invalid Element type');
ins_err(663,'Invalid Further attribute domain');
ins_err(664,'Invalid Element value type');
ins_err(665,'Element already exists');
ins_err(666,'Element Description must be supplied');
ins_err(667,'Log Reason Indicator must be Y or N');
ins_err(668,'Current Indicator must be Y or N');
ins_err(669,'Domain already exists');
ins_err(670,'Domain Code must be supplied');
ins_err(671,'Domain Name must be supplied');
ins_err(672,'Domain does not exist');
ins_err(673,'Matching/Duplicate/Conflicting Record already exists');
ins_err(674,'Reference code must be supplied');
ins_err(675,'Reference Name must be supplied');
ins_err(676,'Default Indicator must be Y or N');
ins_err(677,'Validation Rule with this name already exists');
ins_err(678,'No matching Domain Code found');
ins_err(679,'Payment Contract conflicts with existing record');
ins_err(680,'Username is Mandatory');
ins_err(681,'Job Role is Mandatory');
ins_err(682,'Start Date is Mandatory');
ins_err(683,'Username not setup on System');
ins_err(684,'Job Role not setup on System');
ins_err(685,'Username already linked to Job Role');
ins_err(686,'Current Indicator Must be Y or N or NULL');
ins_err(687,'Admin Unit is Mandatory');
ins_err(688,'Invalid Admin Unit Supplied. Admin Unit not setup in the System');
ins_err(689,'Default Indicator Must be Y or N or NULL');
ins_err(690,'Username already linked to Admin Unit');
ins_err(691,'Legacy Type must be one of PRO, AUN, PAR, PRF, BDE, COS, SRQ or PEG');
ins_err(692,'Bank Details do not exist for this Bank Name');
ins_err(693,'Service Request cannot be found in Service Requests');
ins_err(694,'Contact Method has not been found in domain CONTMETH');
ins_err(695,'Bank Details already exist for the Bank/Branch.');
ins_err(696,'Bank Name must be supplied.');
ins_err(697,'Branch Name must be supplied.');
ins_err(698,'Actual End Date must not be before Start Date.');
ins_err(699,'End Due Date must not be before Start Date.');
ins_err(700,'Work description code already exists.');
ins_err(701,'Item type has not been found under domain ITEMTYPE.');
ins_err(702,'Trade has not been found under domain TRADE.');
ins_err(703,'Liability code not found under domain LIABILITY.');
ins_err(704,'Unit of measure not found under domain UNITS.');
ins_err(705,'Vat code not found in vat_rates table.');
ins_err(706,'Post inspection column must contain Y or N.');
ins_err(707,'Pre inspection column must contain Y or N.');
ins_err(708,'Current flag can only be set Y or N.');
ins_err(709,'Default priority code not found in priority_times table.');
ins_err(710,'Job class has not been found under domain JOBCLASS.');
ins_err(711,'Building category code not found in building_work_categories table.');
ins_err(712,'Schedule of rates code already exists.');
ins_err(713,'Work Description Code not found in WORK_DESCRIPTIONS.');
ins_err(714,'Approved ind Y or N has not been supplied.');
ins_err(715,'Schedule of rates code can not be found.');
ins_err(716,'Work Programme can not be found in Domain WORK_PROG.');
ins_err(717,'Policy Code can not be found in contractor_policies.');
ins_err(718,'Contractor can not be found in contractor_sites.');
ins_err(719,'Repair method not found in domain REP_METHOD.');
ins_err(720,'Repair type not found in domain REP_TYPE.');
ins_err(721,'Fault caused not found in domain FAU_CAUSE.');
ins_err(722,'Status code not set LOG or CAN.');
ins_err(723,'Neither property code or admin unit code supplied - one must be present.');
ins_err(724,'Both property code and admin unit code supplied only - one must be present.');
ins_err(725,'Admin unit code not found in admin_units table.');
ins_err(726,'Property code not present in properties table.');
ins_err(727,'Access not present in domain ACCESS.');
ins_err(728,'Invalid access pattern supplied for access_am.');
ins_err(729,'Invalid access pattern supplied for access_pm.');
ins_err(730,'Emergency must be either Y or N.');
ins_err(731,'Priority code not found in priority_times table.');
ins_err(732,'Rechargeable repair ind not Y or N.');
ins_err(733,'Right to repair ind not Y or N.');
ins_err(734,'Status code not RAI,AUT,ISS,COM,CAN,CLO,HLD.');
ins_err(735,'Confirmation ind not Y or N.');
ins_err(736,'Contractor site code does not exist.');
ins_err(737,'User defined works order status not found in domain WOSTATUS.');
ins_err(738,'Held status works order held date not supplied.');
ins_err(739,'Issued works order authorised/issued date has not been supplied');
ins_err(740,'Closed works order system completed date has not been supplied.');
ins_err(741,'Closed works order invoiced date has not been supplied.');
ins_err(742,'Rechage type not found in domain RECHARGE.');
ins_err(743,'Sundry cleared flag not set Y or N.');
ins_err(744,'Invalid previous status has been supplied.');
ins_err(745,'Supplied rent account number does not exist.');
ins_err(746,'Repair/seq/version does not exist.');
ins_err(747,'Liability code not found in Domain LIABLE.');
ins_err(748,'Trade code not found in Domain TRADE.');
ins_err(749,'Unit code not found in Domain UNITS.');
ins_err(750,'Job class not found in Domain JOB_CLASS.');
ins_err(751,'Original priority code not found in priority_times table.');
ins_err(752,'Budget code can not be found in the year the wo was raised.');
ins_err(753,'W/O status COM system complete date not supplied.');
ins_err(754,'W/O has invoice date invoice cost not supplied.');
ins_err(755,'W/O has invoice date invoice date not supplied.');
ins_err(756,'MANUAL job code used no description lines supplied.');
ins_err(757,'Inspection type not found in domain INS_TYPE.');
ins_err(758,'Inspection rsn not found in domain INS_REASON.');
ins_err(759,'Inspection status not RAI,CAN,ISS or COM.');
ins_err(760,'Inspection Priority code not found.');
ins_err(761,'Inspector short name not present in interested parties.');
ins_err(762,'Version supplied is not 1 only version 1 currently supported.');
ins_err(763,'Dont print flag is not Y or N.');
ins_err(764,'Current flag is not Y or N.');
ins_err(765,'Alternative Reference Number is not unique');
ins_err(766,'Multi element type/code already exists for this property for this period');
ins_err(767,'SOR Type must be S or M.');
ins_err(768,'Reorder / Repeat Period Unit must be D or M.');
ins_err(769,'Warranty Period Unit must be M.');
ins_err(770,'Liability Type Indicator must be O.');
ins_err(771,'Pricing Policy Start Date must be supplied.');
ins_err(772,'Schedule of Rates Unit Price must be supplied.');
ins_err(773,'Location Code not found in domain LOCATION.');
ins_err(774,'Source of the request must be MANUAL, INSAUTO or WOAUTO.');
ins_err(775,'Inspection Indicator must be Y or N.');
ins_err(776,'Works Order Indicator must be Y or N.');
ins_err(777,'Status Date must be supplied.');
ins_err(778,'Printed Indicator must be Y or N.');
ins_err(779,'Service Request does not exist (for this Works Order.)');
ins_err(780,'Status code of AUT requires Authorised by/date data.');
ins_err(781,'Status code of DEC requires Authorised by/date to be null.');
ins_err(782,'Status code of PRO requires the System code to be null.');
ins_err(783,'Entry must be supplied for Version Location Notes.');
ins_err(784,'Version raising reason code not found under domain VARREASON.');
ins_err(785,'Tenant Ticket Printed Indicator must be Y or N.');
ins_err(786,'RTR Reassign Indicator must be Y or N.');
ins_err(787,'Default Contract Indicator must be Y, N or X.');
ins_err(788,'Raised date must be supplied.');
ins_err(789,'Target Date must be supplied.');
ins_err(790,'Invalid status code relationship between Works Order and Works Order Version.');
ins_err(791,'No Contractor Site Price Group found for contractor, policy, program and dates.');
ins_err(792,'Job Type not in DEF, DIS or SOR.');
ins_err(793,'Job Quantity must be supplied.');
ins_err(794,'Job Estimated Cost must be supplied.');
ins_err(795,'Service Request does not exist for this Inspection.');
ins_err(796,'Completed Date must be supplied for Completed Inspection.');
ins_err(797,'Visit Result code not found in inspection_results.');
ins_err(798,'Works Order Version Type must be C.');
ins_err(799,'Estimated Tax amount supplied, no Estimated Cost Amount supplied.');
ins_err(800,'Entitled to HB must be Y or N');
ins_err(801,'HB withdrawn must be Y or N');
ins_err(802,'Landlord must be Y or N');
ins_err(803,'Application Reference Already Exists');
ins_err(804,'Application Reference does not Exist');
ins_err(805,'Application Property Reference does not Exist');
ins_err(806,'Application Tenancy Reference does not Exist');
ins_err(807,'Application Type does not Exist');
ins_err(808,'Application Status does not Exist');
ins_err(809,'Application Type is not Valid');
ins_err(810,'Application Status is not Valid');
ins_err(811,'Application Priority Code is not Valid');
ins_err(812,'Application Landlord Reference is not Valid');
ins_err(813,'Application Lender Reference is not Valid');
ins_err(814,'Application Solicitor Reference is not Valid');
ins_err(815,'Application Insurer Reference is not Valid');
ins_err(816,'Application Status Code not found');
ins_err(817,'Application Previous Status Code not found');
ins_err(818,'Application User Status is not Valid');
ins_err(819,'Application Sale Type is not Valid');
ins_err(820,'Application Lease Type is not Valid');
ins_err(821,'Further applications for this property/tenancy combination exist with an application date later than an existing record ');
ins_err(822,'The related application reference does not exist in this batch or in Housing.');
ins_err(823,'The alternative person reference does not exist in the housing PEOPLE table.');
ins_err(824,'The Party Type must be either TEN or FAM.');
ins_err(825,'Principal Home must be either Y or N.');
ins_err(826,'Wish to Buy must be either Y or N.');
ins_err(827,'Lived One Year must be either Y or N.');
ins_err(828,'Party Verified must be either Y or N.');
ins_err(829,'Signature Verified must be either Y or N.');
ins_err(830,'Invalid code supplied for relationship to tenant.');
ins_err(831,'An application party record already exists for this application/person combination.');
ins_err(832,'Invalid code supplied for this defect.');
ins_err(833,'The sequence for this application is not valid.');
ins_err(834,'The Tenancy History Grouping Code is not Valid.');
ins_err(835,'The Period End date is before the Period Start date.');
ins_err(836,'The Valuation Status Code is not valid.');
ins_err(837,'Application Valuer Reference is not Valid.');
ins_err(838,'Application Surveyor Reference is not Valid.');
ins_err(839,'The Valuation Request date is after the Valuation Date.');
ins_err(840,'The Application Event Code is not valid.');
ins_err(841,'The Application Event Code is unique where the Event Sequence is does not equal 1. This is noy valid.');
ins_err(842,'The improvement sequence for tenancy improvement records in this batch and those in housing are invalid.');
ins_err(843,'A pp_valuation record does not exist in housing for this application and valuation sequence.');
ins_err(844,'Improvement Verified must be either Y or N.');
ins_err(845,'Invalid Admin Unit Type ');
ins_err(846,'Invalid Tenancy Work Start Day ');
ins_err(847,'Parent Admin Unit does not exist ');
ins_err(848,'Child Admin Unit does not exist ');
ins_err(849,'Link to Parent Admin Unit already exists for this Admin Unit ');
ins_err(850,'Admin Unit must not link to itself ');
ins_err(851,'Child Admin Unit is already linked to a Parent of this type ');
ins_err(852,'Link already exists, via an intermeadiate Admin Unit ');
ins_err(853,'Incomplete Bank Details supplied ');
ins_err(854,'Tenancy start date is NULL or is the wrong day of the week');
ins_err(855,'Tenancy end date is the wrong day of the week');
ins_err(856,'Surname of tenant/person must be supplied');
ins_err(857,'Correspondence name must be supplied');
ins_err(858,'Property is not void');
ins_err(859,'Physical address of this property not known');
ins_err(860,'Tenancy Reference is not Valid');
ins_err(861,'Property will not be void on tenancy start date');
ins_err(862,'Start Date Inconsistent for this tenancy ref');
ins_err(863,'Invoiced Tax amount supplied, no Invoiced Cost Amount supplied.');
ins_err(864,'Default Job Role not found for domain SORJR.');
ins_err(865,'Start Date is Required');
ins_err(866,'Invalid Bank Type');
ins_err(867,'Invalid Organisation Bank Account');
ins_err(868,'No matching Party Found');
ins_err(869,'Second property reference does not exist on PROPERTIES');
ins_err(870,'Third property reference does not exist on PROPERTIES');
ins_err(871,'Fourth property reference does not exist on PROPERTIES');
ins_err(872,'Fifth property reference does not exist on PROPERTIES');
ins_err(873,'Sixth property reference does not exist on PROPERTIES');
ins_err(874,'Another tenancy exists for the second property with the same start date');
ins_err(875,'Another tenancy exists for the third property with the same start date');
ins_err(876,'Another tenancy exists for the fourth property with the same start date');
ins_err(877,'Another tenancy exists for the fifth property with the same start date');
ins_err(878,'Another tenancy exists for the sixth property with the same start date');
ins_err(879,'No Matching Service Charge Period');
ins_err(880,'Reconciled Ind nust be Y or N');
ins_err(881,'Estimated Amount is Mandatory');
ins_err(882,'End Date cannot be before Start Date');
ins_err(883,'Start Date is required');
ins_err(884,'End Date is required');
ins_err(885,'Recoverable Ind must be Y or N');
ins_err(886,'VAT Ind must be Y or N');
ins_err(887,'Dispute Ind must be Y or N');
ins_err(888,'Suspend Statement Ind must be Y or N');
ins_err(889,'No Corresponding Row exists in Void Edition Instances');
ins_err(890,'Date of Response should be equal to or less than SYSDATE');
ins_err(891,'Invalid Request Reason Code');
ins_err(892,'Number of Remaining Slips Required');
ins_err(893,'Admin Unit Code Must Be Unique');
ins_err(894,'Version Status Code is not Valid');
ins_err(895,'Contractor Site Code already exists');
ins_err(896,'Contractor Business Number already exists');
ins_err(897,'Contractor Business Details but no Number');
ins_err(898,'Invalid Calendar Code');
ins_err(899,'Invalid Printer Name');
ins_err(900,'Invalid Status Code');
ins_err(901,'Invalid Contractor Type');
ins_err(902,'Tax Reg Ind not Y or N');
ins_err(903,'Equal Op Ind not Y or N');
ins_err(904,'HS Cert Ind not Y or N');
ins_err(905,'Quality Assured Ind not Y or N');
ins_err(906,'Asbestos Approved Ind not Y or N');
ins_err(907,'HPM Authorised Ind not Y or N');
ins_err(908,'Con Code Required');
ins_err(909,'Con Name Required');
ins_err(910,'Cos Code Required');
ins_err(911,'Cos Name Required');
ins_err(912,'Max WO No is Required');
ins_err(913,'WO Total Value is Required');
ins_err(914,'Max WO Value is Required');
ins_err(915,'Current WO No is Required');
ins_err(916,'Current WO Value is Required');
ins_err(917,'No corresponding Entry exists in Pricing Policy Con Sites');
ins_err(918,'Invalid Work Programme Code');
ins_err(919,'Invalid Price Policy Group Code');
ins_err(920,'Invalid System Area Code');
ins_err(921,'Invalid Type Code');
ins_err(922,'Invalid Current Ind');
ins_err(923,'Invalid Highlighted Ind');
ins_err(924,'Invalid Comment/Text/Value');
ins_err(925,'Field name already exists on PARAMETER_DEFINITIONS');
ins_err(926,'Field name must be supplied');
ins_err(927,'Description must be supplied');
ins_err(928,'Current Indicator must be Y or N');
ins_err(929,'Wildcard Indicator must be Y or N');
ins_err(930,'Lower Case Indicator must be Y or N');
ins_err(931,'Updateable Indicator must be Y or N');
ins_err(932,'Required Indicator must be Y or N');
ins_err(933,'Invalid Datatype value');
ins_err(934,'Module name does not exist in MODULES');
ins_err(935,'Length must be supplied if datatype is TEXT');
ins_err(936,'Validation Rule does not exist on VALIDATION_RULES');
ins_err(937,'Invalid table name supplied');
ins_err(938,'Invalid Works Order Ref');
ins_err(939,'Invalid Contractor Ref');
ins_err(940,'Invalid Name');
ins_err(941,'No Matching Arrears Action');
ins_err(942,'Local Indicator must be Y or N');
ins_err(943,'Street name must be supplied');
ins_err(944,'Flat, Building or Street No must be supplied');
ins_err(945,'Property is not void on date supplied');
ins_err(946,'Event code does not exist on Event Types');
ins_err(947,'Target Date must not be before Event Start Date');
ins_err(948,'Sequence Number must be supplied');
ins_err(949,'Period Code is not valid');
ins_err(950,'Invalid End Reason Code');
ins_err(951,'Balance Amount must be supplied');
ins_err(952,'Created Date must be supplied');
ins_err(953,'Creation User must be supplied');
ins_err(954,'Period length must be supplied');
ins_err(955,'Either a default amount or a end due date must be supplied');
ins_err(956,'Inconsistency between End Date and End Username');
ins_err(957,'No Matching Arrangement found');
ins_err(958,'Amend Ind must be Y or N');
ins_err(959,'Sequence must be supplied');
ins_err(960,'Amount must be supplied');
ins_err(961,'Date must be supplied');
ins_err(962,'Invalid Address Format / Combination');
ins_err(963,'Sub Building/Flat No supplied without Building or Street No');
ins_err(964,'Postal Adr Flat, Building or Street No. must be supplied');
ins_err(965,'Postal Adr Street name must be supplied');
ins_err(966,'Fwd Address Flat No supplied without building or street No');
ins_err(967,'Fwd Address Flat, Building or Street No must be supplied');
ins_err(968,'Fwd Address Street name must be supplied');
ins_err(969,'Alt Ref already exists');
ins_err(970,'Admin Unit Code is Required');
ins_err(971,'Admin Unit Name is Required');  
ins_err(972,'Abroad Indicator must be Y or N');
ins_err(973,'Debit effective date is not a valid admin period effective date');
ins_err(974,'Tenant Notified Indicator must be Y or N');
ins_err(975,'Rent Changed Indicator must be Y or N');
ins_err(976,'Rebate Changed Indicator must be Y or N');
ins_err(978,'Capped Indicator must be Y or N');
ins_err(979,'New payer Indicator must be Y or N');
ins_err(980,'DD Extracted date must not be in the future');
ins_err(981,'Element is not a PR Element Type');
ins_err(982,'Payment Contract already exists on Payment Contracts');
ins_err(983,'Property not linked to REP Admin Unit');
ins_err(984,'Inspector Shortname must be supplied');
ins_err(985,'Visit Status must be supplied');
ins_err(986,'Visit Status date must be supplied');
ins_err(987,'Admin Unit not linked to REP Admin Unit');
ins_err(988,'Issued works order, issued date has not been supplied');
ins_err(989,'Issued works order, authorised date has not been supplied');
ins_err(990,'Arrangement Action must be loaded via arrangements dataload');
ins_err(991,'Schedule of Rate Price must be supplied.');
ins_err(992,'Description must be supplied.');
ins_err(993,'Job Liability Type must be O, F or S.');
ins_err(994,'Works Order does not exist.');
ins_err(995,'Previous Status Date must be supplied if status is HLD.');
ins_err(996,'Arrears Action does not exist for Payment Reference and Date.');
ins_err(997,'Contact Name not allowed for this Address Usage Type');
ins_err(998,'A debit breakdown already exists or is pending for this acc/ele/att combination');
ins_err(999,'A property element already exists in the future for this prop/ele/att combination');
--
END;

/

