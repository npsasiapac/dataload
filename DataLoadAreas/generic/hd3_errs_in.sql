-- Script Name = hd3_errs_in.sql
---------------------------------------------------------------------- 
--
-- Script to insert HDX Error messages
--
--   Ver   DB Ver  Who    Date        Reason
--
--   1.0   6.13    AJ     02-AUG-2016 New errors script used hd2_errs_in as a base (1-11)
--                                    initially extra errors added for Landlord Property Banks 
--   1.1   6.13    AJ     06-OCT-2016 Further errors MAD Other fields Data load (12-14)
--   1.2   6.13    AJ     20-OCT-2016 Further errors for HPM Programme of Works DL (15)
--                                    Further error for MAD Other Fields Data Load (16)
--   1.3   6.13    VS     07-NOV-2016 Adding Network Homes PLC Interface DL Error Codes
--   1.4   6.13    AJ     13-DEC-2016 Further errors for HRA Revenue Accounts (17-20)
--                                    Further errors for HEM Estates(21)
--   1.5   6.13    AJ     14-DEC-2016 HRA HEM and MAD errors 16 to 21 used by Vish for NETWORK
--                                    on copy found in v613 folder amalgamated both version and
--                                    alter these 6 duplicates as less work - reissued and updated
--                                    associated pkbs in v613/4 folders
--   1.5   6.14    AJ     21-DEC-2016 Added for Bespoke Manitoba Bank details Data loader(79-83)
--   1.6   6.14    AJ     16-JAN-2017 Added for New Organisation Admin Units data loader(84-94 max 110)
--   1.7   6.13/4  MOK    27-JAN-2017 Added for s_dl_addresses Reserved Character Used in Street Index (111)
--   1.8   6.13/4  AJ     27-JAN-2017 Added for Bespoke HPM data loader for Queensland (112-121)
--   1.9   6.13/4  AJ     07-FEB-2017 Added for Bespoke Voids data loader for Queensland (122-131)
--   2.0   6.13/4  AJ     20-FEB-2017 Added for Changes to addresses to allow loading of addresses only (132-133)
--   2.1   6.15    AJ     27-FEB-2017 Added more for Bespoke HPM data loader for Queensland CR462(134-159)
--   2.2   6.15    AJ     02-MAR-2017 Added more for Bespoke HPM data loader for Queensland CR462(160)
--   2.3   6.15    AJ     03-MAR-2017 Added more for Bespoke HPM data loader for Queensland CR462(161-183)
--   2.4   6.15    AJ     30-MAR-2017 Added more for MAD Contacts for Queensland CR502(184-191)
--   2.5   6.15    AJ     04-APR-2017 Added more for MAD Contacts for Queensland CR502(192-194)
--   2.6   6.15    AJ   05/6-APR-2017 Added more for New Organisation Admin Units CR502( now 84-110 and 195-200 )
--   2.7   6.15    AJ     12-APR-2017 Added more for Bespoke HPM data loader for Queensland CR462(203)
--                                    Amended 181 and 182 wording corrected to read Deliverable Valuation 
--   2.8   6.15    MJK    04-MAY-2017 Errors for psl_schemes datalaoad
--   2.9   6.15    AJ     19-MAY-2017 Added more for New Organisation Admin Units CR502(204-206)
--   3.0   6.15    DLB    22-MAY-2017 Renamed errors 204-206 to 218-220
--   3.1   6.15    AJ     23-MAY-2017 further Errors for psl_schemes datalaoad (221-225)
--   3.2   6.15    AJ     25-MAY-2017 further Errors for psl_leases datalaoad (226-240)
--   3.3   6.15    AJ     22-JUN-2017 further Errors for MAD Other Fields Data Load (241-250)
--   3.4   6.15    AJ     25-JUN-2017 further Errors for MAD Other Fields Data Load (241-251) amended
--   3.5   6.15    AJ     18-JUL2017  further Errors for HEM people Data Load (252-253)
--   3.6   6.15    AJ     20-JUL2017  further Errors for HCS Customer Services Data Load (254-278)
--   3.7   6.15    AJ     25-JUL2017  Errors for HIN Housing Initiatives Data Load (279-315)
--   3.8   6.15    AJ     05-SEP-2017 Errors for HAT Bespoke for GNB (316-328)
--   3.9   6.15    AJ     09-OCT-2017 Further Errors for HAT Bespoke for GNB (329-335)
--   4.0   6.15    AJ     11-OCT-2017 Further Errors for HAT Bespoke for GNB (336-337)
--   4.1   6.15    AJ     12-OCT-2017 Further Errors for Subsidy data load (338)
--   4.2   6.15    AJ     17-OCT-2017 Further Errors for Subsidy data load (339)
--   4.3   6.15    AJ     02-NOV-2017 Further Errors for HAT Bespoke for GNB (340-341)
--   4.4   6.14/5  AJ   8/13-NOV-2017 Further Errors for Estates data loader (342-348)
--   4.5   6.15    AJ     16-NOV-2017 Added error 63 as added to old version by Vish on the
--                                    29-JUN-2017 for Network Homes PLC Interface
--   4.6   6.15    MJK    27-NOV-2017 HAD errors added (349 - 358) change control added(AJ 08/12)
--   4.7   6.15    AJ     08-DEC-2017 HAD further errors added (359 - 362)
--   4.8   6.15    AJ/MJK 13-DEC-2017 HEM and LOAN errors from (363 - 381) added by MJK
--   4.9   6.15    AJ/MJK 10-JAN-2018 HEM and LOAN errors from (382-403) added by MJK
--   5.0   6.15    AJ     11-JAN-2018 HAT Bespoke Answer History errors for GNB (404-423)
--   5.1   6.15    AJ/MJK 10-JAN-2018 HEM and LOAN errors from (424-440) added by MJK
--   5.2   6.15    AJ     17-JAN-2018 HAT Bespoke Involved Party History for GNB (441-464)
--   5.3   6.15    MJK    23-JAN-2018 LOAN errors from (490-508) added by MJK
--   5.4   6.15    AJ     25-JAN-2018 Additional for HAT Bespoke Involved Parties for GNB (509-522)
--   5.5   6.15    AJ     01-FEB-2018 HAT Bespoke Warning Histories for GNB (523-539)
--   5.6   6.15    AJ     12-FEB-2018 HAT Bespoke Admin Unit Security for GNB (540-541)
--   5.7   6.15    AJ  16+19-FEB-2018 HAT Bespoke Incomes data load for GNB (542-548)
--   5.8   6.15    AJ     20-FEB-2018 Further HAT Bespoke Incomes data load for GNB (549-555)
--   5.9   6.15    PJD    11-MAR-2018 Rent Elemrate DL Errors 480-489
--                                    Sundry Invoice Items DL 
--                                    Errors 476-479
--   6.0   6.15    PAH    10-APR-2018 HEM hou_prop_status overlapping errors (568-573)
--   6.1   6.16    AJ     11-APR-2018 Furthers for Revenue Accounts data load (574-575)
--   6.2   6.16    AJ  18/23-APR-2018 HEM first ref values and domains data load (576-596)
--   6.3   6.16    AJ  11/18-JUN-2018 Added further errors for Offers and Service Charges loaders (597-610)
--   6.4   6.16    AJ     03-JUL-2018 Added further errors for Offers (611)
--   6.5   6.16    AJ     17-JUL-2018 SAHT HCS Contacts Bespoke (613)
--   6.6   6.17    AJ     02-AUG-2018 GNB HRA Subsidy Debit Breakdowns process (641-645)
--   6.7   6.17    AJ     17-OCT-2018 Job Role Action Groups (646-647)
--   6.5   6.17    AJ     26-OCT-2018 Reserved 614 - 640 for SAM ASBESTOS DATA LOADERS (AJ)
--   6.6   6.17    AJ     01-NOV-2018 SAM ASBESTOS DATA LOADERS added (614-640) and (648-660)
--   6.7   6.17    AJ     08-NOV-2018 More SAM ASBESTOS DATA LOADERS added (661-666)
--   6.8   6.17    PAH    12-DEC-2018 More errors for HAD Advice Case People (667-668)
--   6.9   6.17    JT     10-JAN-2019 Person Also Known As (669-673)
--   6.10  6.17    JT     22-JAN-2019 More Person Also Known As errors (674-676)
--   6.11  6.18    AJ     25-JAN-2019 Reserved numbers for People Groups QAus(677-686)
--   6.12  6.18    AJ     25-JAN-2019 Added extra check for advice case reasons(687)
--   6.13  6.18    AJ     28-JAN-2019 Added extra check for HCS Events also reserved numbers(688-699)
--   6.14  6.18    AJ     01-FEB-2019 Further Reserved numbers for QAus data loads(700-711)
--   6.15  6.17    JT     01-FEB-2019 Additional HAT Placement Errors (712-725)
--   6.16  6.17    JT     05-FEB-2019 Additional HAT Placement Errors (726-740)
--   6.17  6.17    JT     08-FEB-2019 Definitions added for 712-740 & 741-742 added.
--   6.18  6.18    AJ     11-FEB-2019 Reserved numbers for HCS Action Parties DL QAus(743-762)
--   6.19  6.17    JT     12-FEB-2019 Additional HAT Placement Errors (763-765)
--   6.20  6.18    AJ     13-FEB-2019 Reserved numbers for Tenant Allowances JH QAus(766-785)
--   6.21  6.18    AJ  13/14-FEB-2019 HFI Mapping Value Updating (786-811)
--   6.22  6.18    AJ     17-FEB-2019 Errors Added for Reserved QAus loaders from (700-811)
--   6.23  6.18    AJ     17-FEB-2019 Reserved numbers for Referrals Loader AJ(812-831)
--   6.24  6.18    JT     18-FEB-2018 Used unused 764, 765 for Contractor Site Contacts
--   6.25  6.18    AJ     25-FEB-2019 Additional ASB Asbestos Errors (832-833)
--   6.26  6.18    AJ     12-MAR-2019 Additional Estates Errors (834)
--   6.27  6.18    AJ     18-MAR-2019 Additional Estates Errors (835-837)
--   6.28  6.18    JT     18-MAR-2019 HCS Contact Reasons Errors (838-844)
--   6.29  6.18    PJD    19-MAR-2019 HRM PPP Errors (850- 851)
--   6.30  6.18    AJ     22-MAR-2019 Additional Referrals data loader(852-853)and(855-859)
--   6.31  6.18    JT     22-MAR-2019 HEM Tenancy Termination Errors (854)
--   6.32  6.18    AJ     26-MAR-2019 HSS Referrals Errors (855-859) added
--                                    HCS People Groups Errors (677-686) added to control
--                                    HCS People Groups Errors (688-692)
--
----------------------------------------------------------------------
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
AND    err_object_shortname = 'HD3';
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
  values('HD3',p_err_refno,         
  p_err_text,        
  'V','N');
dbms_output.put_line('Error number '||p_err_refno||' inserted.');
ELSIF
   l_err_message != p_err_text       
   THEN
     UPDATE errors
     SET    err_message = p_err_text    
     WHERE  err_object_shortname = 'HD3'
       AND   err_refno = p_err_refno;
dbms_output.put_line('Error number '||p_err_refno||' updated.');
END IF;
--
END ins_err;
--
--
BEGIN
--
ins_err(1,'Bank Account number supplied is too long');
ins_err(2,'Multi Language Bank and Branch name combination already in use');
ins_err(3,'Property Landlord not found for Party Property Date supplied');
ins_err(4,'Bank Account number supplied is too short');
ins_err(5,'Bank Account number supplied is too short');
ins_err(6,'Bank Account Name must be supplied');
ins_err(7,'Bank Account Number must be supplied');
ins_err(8,'Bank Account Sort Code must be supplied');
ins_err(9,'Bank Name must be supplied');
ins_err(10,'Branch Name and Branch Code are mandatory for this Bank Type');
ins_err(11,'Bank Code is mandatory for this Bank Type');
--
-- additional MAD area DL errors
ins_err(12,'Start Date of the Organisation Contact(lpva_secondary_date)must be supplied');
ins_err(13,'Organisation Contact Forename(lpva_further_ref2)must be supplied');
ins_err(14,'Organisation Contact Surname(lpva_further_ref3)must be supplied');
-- additional HPM area DL errors
ins_err(15,'TASKS can only be loaded against CONTRACTS at a RAIsed Status');
--
-- Network Homes PLC Interface Error Codes (Cover Sheet Load)
--
ins_err(16, 'Field Label must be supplied');
ins_err(17, 'Field Label does not exist in PLC_DATA_ITEMS');
ins_err(18, 'Value for Field Label must be supplied');
--
-- Network Homes PLC Interface Error Codes (Unit Sheet Load)
--
ins_err(19, 'Invalid Warranty Provider Code Supplied');
ins_err(20, 'Invalid Right To Acquire Code Supplied');
ins_err(21, 'Invalid Local Authority Admin Unit Code Supplied');
ins_err(22, 'Property Unit Type Code must be supplied');
ins_err(23, 'Invalid Property Type Code Supplied');
ins_err(24, 'FLOOR Element Code must be supplied');
ins_err(25, 'Invalid FLOOR Element Code Supplied');
ins_err(26, 'Invalid MOBILITY Element Code Supplied');
ins_err(27, 'MAXOCC Element Code must be supplied');
ins_err(28, 'Network Ownership Type Code must be supplied');
ins_err(29, 'Invalid Network Ownership Type Code Supplied, valid values as per FIRST REF DOMAIN : NET_OWN');
ins_err(30, 'Owned Indicator must be supplied');
ins_err(31, 'Invalid Owned Indicator Code Supplied');
ins_err(32, 'Property Ownership Code must be supplied');
ins_err(33, 'Invalid Property Ownership Code Supplied, valid values as per FIRST REF DOMAIN : OWN_TYPE');
ins_err(34, 'Invalid Insurance Code Supplied');
ins_err(35, 'Invalid Tenure Type Code Supplied');
ins_err(36, 'Invalid Affordable Rent Type Code Supplied');
ins_err(37, 'Secion 106 Code must be supplied');
ins_err(38, 'Invalid Section 106 Code Supplied');
ins_err(39, '1999 Value must be supplied');
ins_err(40, 'Invalid Building Standard Code Supplied');
ins_err(41, 'Invalid Asbestos Code Supplied');
ins_err(42, 'Lift in Property Code must be supplied');
ins_err(43, 'Invalid Lift in Property Code Supplied');
ins_err(44, 'Invalid Parking Provision Code Supplied');
ins_err(45, 'Invalid Heating Type Code Supplied');
ins_err(46, 'Invalid Gas Supply Code Supplied');
ins_err(47, 'Invalid Sustainable Energy Code Supplied');
ins_err(48, 'Invalid Garden Code Supplied');
ins_err(49, 'Invalid Balcony Code Supplied');
ins_err(50, 'Invalid Door Entry Code Supplied');
ins_err(51, 'Invalid Fob or Key Code Supplied');
ins_err(52, 'Invalid Energy Efficiency Rating Code Supplied');
ins_err(53, 'Invalid Play Equipment Code Supplied');
ins_err(54, 'Invalid Roof Area Code Supplied');
--
ins_err(55, 'Street Name must be Supplied');
ins_err(56, 'Town must be Supplied');
ins_err(57, 'Property PostCode must be Supplied');
ins_err(58, 'Flat No, Building Name or Street Door No must be Supplied');
ins_err(59, 'Flat No Supplied without Building Name or Street Door No');
--
ins_err(60, 'Invalid CTAX Band Value Supplied, valid values as per FIRST REF DOMAIN : CTAX_BAND');
ins_err(61, 'Invalid Lift No Supplied for Further Attribute value against Element/Attribute LIFT');
ins_err(62, 'Lift No Supplied when Lift in Property value does not = LIFT');
ins_err(63, 'Door No may contain Non Alphanumeric Characters');
--
-- 63 to 72 left for Vish for Network Homes PLC Interface further Error Codes
--
-- additional MAD area DL errors
ins_err(73,'The lpva_char_value does not exist in the linked reference domain');
--
-- additional HRA area DL errors
ins_err(74,'mlang Bank Name must be supplied when mlang Branch Name has been');
ins_err(75,'Bank Details are incomplete');
ins_err(76,'Bank Account or Bank Name Sort Code or Bank Type is missing');
ins_err(77,'No matching Party Found');
ins_err(78,'The Person Start Date(lpar_hop_start_date)is mandatory when linking to a Tenancy(lpa_tcy_alt_ref)');
--
-- bespoke HRA Bank details data loader for Manitoba
ins_err(79,'Bank Name(lbde_bank_name)is a mandatory item');
ins_err(80,'Bank Type Code(lbde_type)is a mandatory item');
ins_err(81,'Bank Details already exist for Name/Branch combination');
ins_err(82,'Bank Type Code(lbde_type)does not exist');
ins_err(83,'All address fields and post code must be provided');
--
-- New Standard Organisations Admin Units and Parent Child Data Loaders for Queensland
ins_err(84,'Organisation Name(lorau_par_org_name)must be supplied');
ins_err(85,'Organisation Short Name must be supplied if Party Reference is not supplied');
ins_err(86,'Organisation Admin Unit Code(lorau_aun_code)must be supplied');
ins_err(87,'Link Start Date(lorau_start_date)must be supplied');
ins_err(88,'Link Reason Code(lorau_frv_oar_code)must be supplied');
ins_err(89,'The Organisation Name and Reference combination does not exist');
ins_err(90,'The Organisation Name and Short Name combination does not exist');
ins_err(91,'Multiple Organisations found with the same Name and Short Name combination supplied');
ins_err(92,'The Organisation Name Short Name and Org Type combination does not exist');
ins_err(93,'Multiple Organisations found with Name Short Name and Org Type combination supplied');
ins_err(94,'The Organisation Type Code(lorau_par_org_frv_oty_code)does not exist');
ins_err(95,'A Organisation Admin Unit Code(lorau_aun_code)does not exist');
ins_err(96,'Admin Unit Link Reason does not exist in domain ORG_AUN_REASON');
ins_err(97,'An Organisation Admin Unit Link already exists for all or part of the period supplied');
ins_err(98,'Parent Organisation Name(lorhi_par_org_name)must be supplied');
ins_err(99,'Child Organisation Name(lorhi_par_org_name_c)must be supplied');
ins_err(100,'Child Organisation Short Name must be supplied if Child Party Reference is not supplied');
ins_err(101,'Start Date(lorhi_start_date)must be supplied');
ins_err(102,'Relationship Type Code must be supplied');
ins_err(103,'Parent Organisation Type Code does not exist');
ins_err(104,'Child Organisation Type Code does not exist');
ins_err(105,'Parent Child Relationship Type Code supplied does not exist');
ins_err(106,'Relationship End Date must be greater than the Start date');
ins_err(107,'Parent Organisation Name and Reference combination does not exist');
ins_err(108,'Multiple Parent Organisations found for combination supplied');
ins_err(109,'Child Organisation Name and Reference combination does not exist');
ins_err(110,'Multiple Child Organisations found for combination supplied');
-- will need at least another 15 so reserve up to 110
--
ins_err(111,'Reserved Character Used in Street Index');
--
-- additional errors for Bespoke HPM Data Loader for Queensland
ins_err(112,'Task Status must be either RAI COM AUT');
ins_err(113,'Tasks can only be loaded against PROGRAMMES at a RAIsed Status');
ins_err(114,'Tasks can only be loaded against PROJECTS at a RAIsed Status');
ins_err(115,'Non Financial tasks against a Raised Contract must be a Status of RAI or COM');
ins_err(116,'Financial tasks against a RAI(sed)Contract must be a Status of RAI');
ins_err(117,'Tasks against an AUT(horised)Contract must be a Status of AUT or COM');
ins_err(118,'CONTRACTS must be at a RAIsed or AUThorised Status when loading a Task');
ins_err(119,'Deliverables against a RAI(sed)Contract must be a Status of RAI');
ins_err(120,'Deliverables against an AUT(horised)Contract must be a Status of AUT or COM');
ins_err(121,'CONTRACTS must be at a RAIsed or AUThorised Status when load a Deliverable');
--
-- additional errors for Bespoke Voids Data Loader for Queensland
ins_err(122,'The Void Path Event Sequence and Code do not exist in the default_events table');
ins_err(123,'Void Status Started Date mst be supplied');
ins_err(124,'Modified Date must be supplied');
ins_err(125,'Property Status Start Date must be supplied');
ins_err(126,'Property Status Code Date must be supplied');
ins_err(127,'Property Status End Date is before Start Date');
ins_err(128,'Void Instance does not exist for vin refno retured from hps comments');
ins_err(129,'Void Instance for property does not exist for vin refno retured from hps comments');
ins_err(130,'Start and Status cannot match current void instance');
ins_err(131,'Void Stataus History already exists');
--
-- additional errors for HEM Addresses Data Loader for PNB
ins_err(132,'When Loading Addresses only the legacy reference (field 1) must not be supplied');
ins_err(133,'The Legacy Reference (field 1) must be supplied');
--
-- additional errors for Bespoke HPM Data Loader for Queensland CR462
ins_err(134,'Task Alternative Reference must be supplied');
ins_err(135,'Task Alternative Reference must be Unqiue but already exists on the tasks table');
ins_err(136,'Duplicate Task Alternative References exists in dataload batch');
ins_err(137,'Task Alternative Reference does not exists on the tasks table');
ins_err(138,'Duplicate Task Alternative References exists on tasks table');
ins_err(139,'Contract Reference must be supplied');
ins_err(140,'Task Group of type PAYT for the Contract Reference does not exist');
ins_err(141,'Task Payment Status must be supplied');
ins_err(142,'Task Payment Status must be either RAI or CLO');
ins_err(143,'Task Payment Net Amount must be supplied');
ins_err(144,'Task Payment Tax Amount must be supplied');
ins_err(145,'Paid Dates and External References must be supplied for a Status of CLO');
ins_err(146,'Paid Dates and External References cannot supplied for a Status of RAI');
ins_err(147,'Task Standard Code must be supplied');
ins_err(148,'Task Standard Code supplied does not exist');
ins_err(149,'Task Contract References do not match for the Task Alternate Reference supplied');
ins_err(150,'Task Group Codes do not match for the Task Alternate Reference supplied');
ins_err(151,'Task Source Reference is not CNT(Contract) for the Task Alt Ref supplied');
ins_err(152,'Task is not at a status of COM for the Task Alternate Reference supplied');
ins_err(153,'Task is not a Payment Task for the Task Alternate Reference supplied');
ins_err(154,'Task Standard Codes do not match for the Task Alternate Reference supplied');
ins_err(155,'Task Payment already exists for task');
ins_err(156,'External Payment Date cannot be before The Paid Date');
ins_err(157,'Net and Tax amounts supplied do not match current task version amounts');
ins_err(158,'Net and Tax amounts supplied do not match current task budget amounts');
ins_err(159,'Current task version amounts do not match current task budget amounts');
ins_err(160,'Contract must be at a status of AUT');
ins_err(161,'The Valuation Date must be supplied');
ins_err(162,'The Task Payment does not exist in the task_payments table');
ins_err(163,'Property UPRN or Admin Unit Code for the Deliverable must be supplied');
ins_err(164,'Property UPRN or Admin Unit Indicator must be supplied');
ins_err(165,'Property UPRN or Admin Unit Indicator must be a P or A');
ins_err(166,'Property UPRN supplied does not exist');
ins_err(167,'Admin Unit Code supplied does not exist');
ins_err(168,'A Contract Address does not exist for the combination supplied');
ins_err(169,'The Deliverable Versions Display Sequence must be supplied');
ins_err(170,'The Standard Deliverables Code must be supplied');
ins_err(171,'The Standard Deliverable Code supplied does not exist');
ins_err(172,'The Deliverables Estimated Cost must be supplied');
ins_err(173,'The Budget Head Code must be supplied');
ins_err(174,'The Budget Calendar Year must be supplied');
ins_err(175,'The Budget Head Code does not exist');
ins_err(176,'The Budget Calendar Year does not exist');
ins_err(177,'Quantities Units Projected and Unit Costs must all be supplied');
ins_err(178,'Deliverable does not exist');
ins_err(179,'Deliverable is not at the status of COM');
ins_err(180,'Deliverable Task Payment and Task amounts do not match');
ins_err(181,'Deliverable Valuation already exists for Deliverable');
ins_err(182,'Deliverable Valuation already exists for Task Payment');
ins_err(183,'The Budget for the Task Payment and Deliverable do not match');
--
-- additional MAD Contacts for Queensland CR502
ins_err(184,'The Update Marker must be set to Y or N or left Blank');
ins_err(185,'The Create Marker must be set to Y or N or left Blank');
ins_err(186,'The lcde_oco_start_date must be supplied if the Create or Update Marker set to Y');
ins_err(187,'The lcde_oco_signatory_ind must be supplied if the Create or Update Marker set to Y');
ins_err(188,'The lcde_oco_frv_title supplied does not exist in the reference domain TITLE');
ins_err(189,'The Organisation Contacts Start Date is greater than End Date');
ins_err(190,'The lcde_oco_frv_ocr_code supplied does not exist in domain ORG_CONTACT_ROLE');
ins_err(191,'The lcde_oco_frv_pl_code supplied does not exist in domain ORG_PREF_LANGUAGE');
ins_err(192, 'More than 1 Organisation or Organisation Contact matches the combination Supplied');
ins_err(193, 'Organisation Short Name and Type does not match combination Supplied');
ins_err(194,'The Create and Update Markers cannot both be set to Y');
--
-- additional Standard Organisations Admin Units and Parent Child Data Loaders for Queensland
ins_err(195,'Parent and Child Organisations must not be the same');
ins_err(196,'Parent and Child Organisation Combination already exist');
ins_err(197,'Child as Parent and Parent as Child Organisation Combination already exist');
ins_err(198,'Parent Organisation currently exists as a Child');
ins_err(199,'Child Organisation can only be against 1 Organisation at a time');
ins_err(200,'Child Organisation currently exists as a Parent');
ins_err(201,'Parent Organisation also in load batch as Child Organisation');
ins_err(202,'Child Organisation also in load batch as Parent Organisation');
--
-- additional errors for Bespoke HPM Data Loader for Queensland CR462
ins_err(203,'The Deliverable Valuation Type Code supplied does not exist');
--
-- Errors for psl_schemes datalaoad
ins_err(204,'LPSLS_CODE must be supplied'); 
ins_err(205,'LPSLS_DESCRIPTION must be supplied'); 
ins_err(206,'LPSLS_START_DATE must be supplied'); 
ins_err(207,'LPSLS_SCO_CODE must be supplied'); 
ins_err(208,'LPSLS_STATUS_DATE must be supplied'); 
ins_err(209,'LPSLS_PAR_REFNO must be supplied'); 
ins_err(210,'LPSLS_PSTY_CODE must be supplied'); 
ins_err(211,'LPSLSS_SCO_CODE must be one of CLO, CUR or PEN');
ins_err(212,'LPSLS_STATUS_DATE Cannot be greater than todays date'); 
ins_err(213,'LPSLS_PAR_PER_ALT_REF must exists in the PARTIES table'); 
ins_err(214,'LPSLS_PSTY_CODE must exist on the PSL_SCHEME_TYPES table'); 
ins_err(215,'LPSLS_PROPOSED_END_DATE, if supplied, must be greater than LPSLS_STATUS_DATE'); 
ins_err(216,'LPSLS_ACTUAL_END_DATE, if supplied, must be greater than LPSLS_STATUS_DATE'); 
ins_err(217,'LPSLS_AUN_CODE must exist on the ADMIN_UNITS table'); 
-- 
-- additional Standard Organisations Admin Units and Parent Child Data Loaders for Queensland
ins_err(218,'Relationship End Date must be greater than the Start date');
ins_err(219,'Parent Organisation Name and Reference combination does not exist');
ins_err(220,'Child Organisation Name and Reference combination does not exist');
--
-- further Errors for psl_schemes datalaoad
ins_err(221,'PSL scheme code already exists in the psl_schemes table'); 
ins_err(222,'PSL scheme code is duplicated in data load batch');
ins_err(223,'PSL MLANG scheme code already exists in the psl_schemes table'); 
ins_err(224,'PSL MLANG scheme code is duplicated in data load batch');
ins_err(225,'The PSLS_CREATED_DATE cannot be a future date');
--
-- further Errors for psl_leases datalaoad
ins_err(226,'The PSL Leases Legacy Reference must be supplied');
ins_err(227,'The PSL Leases Annual Landlord Rent Charge must be supplied');
ins_err(228,'The PSL Scheme does not exist for the Lease period required');
ins_err(229,'A PSL Lease already exists for the property for some or part of the period required');
ins_err(230,'The Extension End Date is earlier than the Lease End Date');
ins_err(231,'The Legacy Reference is duplicated in the batch');
ins_err(232,'The Lease for the property overlaps another Lease for the same property in the batch');
ins_err(233,'The Lease Start Date cannot be Greater than the Lease Rent Start Date');
ins_err(234,'The Lease Start Date is Greater than the Lease Rent End Date');
ins_err(235,'The Lease Rent Start Date is Greater than the Lease Rent End Date');
ins_err(236,'The Lease in Schemes Start Date is before the Lease in Schemes End Date');
ins_err(237,'The Scheme Lease Rents Start Date is before the Scheme Lease Rents End Date');
ins_err(238,'The Landlord Paid in Advance IndIcator Must be Y or N');
ins_err(239,'The Rents Start Date is before the Rents Review Date');
ins_err(240,'The Latest Lease Status Date is greater than Today');
--
-- further Errors for MAD Other Fields datalaoad
ins_err(241,'The Other Fields Name supplied does not exist in the parameter definitions table');
ins_err(242,'The Organisation supplied has no Organisation Type set');
ins_err(243,'The Other Field is not set up against the Organisation at TABLE level');
ins_err(244,'The Other Field is not set up against the Organisation at ORGTYPE level');
ins_err(245,'The Other Field already exists against the Organisation at ORGTYPE level');
ins_err(246,'The Other Field already exists against the Organisation at TABLE level');
ins_err(247,'Organisation Table is wrong or has not been supplied');
ins_err(248,'The Legacey reference must be supplied');
ins_err(249,'Duplicate Other Field records exists in dataload batch');
ins_err(250,'The Parameter Name (lpva_pdf_name)must be supplied');
ins_err(251,'No matching Organisation for Legacy ref Organisation Type Name and Short Name supplied');
--
-- further Errors for HEM People data load
ins_err(252,'Duplicate Organisation record in dataload batch');
ins_err(253,'Party fields other than the Alternative Refs(par_per_alt_ref)should not be provided when creating an Organisation');
--
-- further Errors for HCS Customer Services Dataloader
ins_err(254,'Organisation not found');
ins_err(255,'Business Action Reference must be supplied');
ins_err(256,'Object Legacy Reference must be supplied');
ins_err(257,'Object Legacy Reference Type must be supplied');
ins_err(258,'Business Action Type must be supplied');
ins_err(259,'Business Reason Code must be supplied');
ins_err(260,'Responsible Admin Unit must be supplied');
ins_err(261,'Business Actions Status Code must be supplied');
ins_err(262,'Business Actions Status Date must be supplied');
ins_err(263,'Both the Secondary Ref and Further Ref must be supplied');
ins_err(264,'Secondary Ref Further Ref and Further Ref2 must NOT be supplied');
ins_err(265,'Further Ref and Further Ref2 must NOT be supplied');
ins_err(266,'This Business Reason is not set for Parties Objects(PAR)');
ins_err(267,'This Business Reason is not set for Properties Objects(PRO)');
ins_err(268,'This Business Reason is not set for Admin Unit Objects(AUN)');
ins_err(269,'This Business Reason is not set for Tenancy Objects(TCY)');
ins_err(270,'This Business Reason is not set for Interested Party Objects(IPP)');
ins_err(271,'This Business Reason is not set for Application Objects(APP)');
ins_err(272,'This Business Reason is not set for People Groups Objects(PEG)');
ins_err(273,'This Business Reason is not set for Service Request Objects(SRQ)');
ins_err(274,'This Business Reason is not set for Contractor Sites Objects(COS)');
ins_err(275,'This Business Reason is not set for Leases Objects(LAS)');
ins_err(276,'This Business Reason is not set for Organisation Objects(ORG)');
ins_err(277,'This Business Reason is not set for Loan Objects(LOA)');
ins_err(278,'Loan Application not found');
--
-- Errors for HIN Housing Initiatives Data loader
ins_err(279,'The Approval Product Area Code must be supplied');
ins_err(280,'The Approval Code must be supplied');
ins_err(281,'The Approval Status Code must be supplied');
ins_err(282,'The Approval Status Date must be supplied');
ins_err(283,'A Unique Approval Alternative Reference must be supplied');
ins_err(284,'The Initiative Code must be supplied');
ins_err(285,'The Approval Initiative Start Date must be supplied');
ins_err(286,'For an Interested party both ipp type and shortname must be supplied');
ins_err(287,'The Approval Product Area Code must HAD HSS or HIN');
ins_err(288,'The Interested Party combination not found');
ins_err(289,'Approval Admin Unit not found');
ins_err(290,'Approval already exists for the Alternative Reference supplied');
ins_err(291,'For a Property both UPRN and start date must be supplied');
ins_err(292,'Only 1 of Address Register or Property or Interested Party can be supplied');
ins_err(293,'Approval Status and Code Combination not found');
ins_err(294,'Approval Code not found');
ins_err(295,'Registered Address Approvals are not catered for');
ins_err(296,'For IPP Approvals Interested Party detail must be supplied');
ins_err(297,'For ADRE Approvals Address Register Code must be supplied');
ins_err(298,'The Admin Unit Type does not match the required Approval Admin Unit Type allowed');
ins_err(299,'IPP type must match the value in System Parameter LANDLORD AGENT IPT');
ins_err(300,'An Approval Property can not be created for this Approval Type');
ins_err(301,'An Approval Property must be supplied for this Approval Type');
ins_err(302,'An Approval Property must be supplied for a Rent Supplement Approval Type');
ins_err(303,'Selected Property must have a current Lease');
ins_err(304,'Selected Property must have a current Property Landlord');
ins_err(305,'Approval Property Start Date must be greater than or equal to Lease Start Date');
ins_err(306,'Approval Property Start Date must be equal to Approval Initiative Start Date');
ins_err(307,'Approval Property Start Date cannot be greater than today');
ins_err(308,'Approval Property End Date must be less than or equal to Lease End Date');
ins_err(309,'A current Approval Property already exists for this Property against Approval');
ins_err(310,'Product Area Type (HIN HAD HSS)does not match Approval Area Type');
ins_err(311,'Initiative Code not found');
ins_err(312,'Approval Initiative start date is less than the Initiative Start Date');
ins_err(313,'Approval Initiative Start or End date is greater than the Initiative End Date');
ins_err(314,'Initiative Start must be equal to or greater than the Approval Property Start Date');
ins_err(315,'Address Register Code supplied is not valid');
--
-- Errors for HAT Allocations Migration Bespoke for GNB
ins_err(316,'An Application can only be for a HOU Admin Unit Type');
ins_err(317,'The app_refno supplied does not exist in the applications table');
ins_err(318,'Household Type supplied does not exist in the hhold_types table');
ins_err(319,'Legacy Ref must be supplied');
ins_err(320,'Rehousing List Code must be supplied');
ins_err(321,'Rehousing List Change Indicator Code must be supplied');
ins_err(322,'Created Date must be supplied');
ins_err(323,'Created by must be supplied');
ins_err(324,'Modifed Date must be supplied');
ins_err(325,'Modified by must be supplied');
ins_err(326,'List Entry Indicator must be supplied');
ins_err(327,'Active Status Indicator must be supplied');
ins_err(328,'Registration Date must be supplied');
ins_err(329,'Application Reference must be supplied');
ins_err(330,'Record Type Code must be one of APP, ALE');
ins_err(331,'Rehousing List Code and Alternate Reference are not required for APP records');
ins_err(332,'Application Legacy Ref is not required for ALE records');
ins_err(333,'Alternate Reference must be supplied');
ins_err(334,'app_refno and rli_code combination not found in applic_list_entries table');
ins_err(335,'Application Reference found does not match one supplied');
ins_err(336,'Status Code must be INA or INE if pay market rent ind is set to Y');
ins_err(337,'Status Code must be RAI,ASS,AUT,ACT,CAN,INA,INE if pay market rent ind is set to N');
--
-- Errors for HRA Bespoke Subsidy dataloads
ins_err(338,'Subsidy Application Legacy Reference must be supplied');
ins_err(339,'Revenue Account of type REN not found for Tenancy Reference supplied');
--
-- Errors for HAT Allocations Migration Bespoke for GNB
ins_err(340,'The Lettings Area Code must be supplied');
ins_err(341,'Modified Date cannot be before Created Date');
--
-- Errors for HEM Estates
ins_err(342,'The Housedhold Groupings Param is set to No so Head and Group Number not required');
ins_err(343,'The Housedhold Groupings Param is set to Yes so Head and Group Number required');
ins_err(344,'Head of Household and Group should not be supplied');
ins_err(345,'Head of Household not found in either household persons or data load batch');
ins_err(346,'More than 1 Head of Household found in household persons and data load batch');
ins_err(347,'Person Title supplied does not exist in reference domain TITLE');
ins_err(348,'Person Title is Mandatory in Person Matrix for Create or Update Person');
--
-- Errors for HAD Advice Case People
ins_err(349,'Head of Household not found for the advice case group');
ins_err(350,'More than 1 Head of Household found for the advice case group');
--
-- Errors for HAD Advice Case Associations
ins_err(351,'Either an Application Legacy Reference or a Tenancy Alternative Reference must be supplied');
ins_err(352,'The supplied Application Legacy Reference does not exist on the Housing database');
ins_err(353,'The supplied Tenancy Alternative Reference does not exist on the Housing database');
ins_err(354,'An Advice Case Association Reason Code must be supplied');
ins_err(355,'The supplied Advice Case Association Reason Code must exist on hrv_adv_case_app_assoc_rsns');
ins_err(356,'The supplied Advice Case Association Reason Code must exist on hrv_adv_case_tcy_assoc_rsns');
--
-- Errors for HAD Household People
ins_err(357,'Head of Household not found for the household/group');
ins_err(358,'More than 1 Head of Household found for the household/group');
--
-- More Errors for HAD Advice Case Associations
ins_err(359,'More than one linked object reference supplied');
ins_err(360,'Created date supplied must not be later than today');
ins_err(361,'Modified Date is before the Created date');
ins_err(362,'Both the Modified Date and Modified by must be supplied if required');
--
-- Errors for Loan Applications
ins_err(363,'Type of loan application must be PAR or ORG');
ins_err(364,'Loan application date cannot be in the future');
ins_err(365,'Budget admin unit of the loan application is not a valid admin unit code');
ins_err(366,'Delivery admin unit of the loan application is not a valid admin unit code');
ins_err(367,'Status of the loan application is not a valid loan application status code');
ins_err(368,'Loan status date cannot be in the future');
ins_err(369,'Loan application system status must be COM or CUR');
ins_err(370,'Priority of the loan application is not a valid loan application priority code');
ins_err(371,'Advice case alternative reference does not reference an existing advice case');
ins_err(372,'Loan status reason code is not a valid code');
--
-- Errors for Loan Options
ins_err(373,'Supplied alternative reference does not match any Loan Application');
ins_err(374,'Supplied loan option type is invalid or does not exist');
ins_err(375,'Supplied loan policy code does not exist');
ins_err(376,'Supplied loan interest rate code does not exist');
ins_err(377,'Supplied loan interest rate value does not exist for the supplied interest rate code');
--
-- More Errors for HEM Estates GNB Bespoke
ins_err(378,'Modified date supplied must not be later than today');
ins_err(379,'Both party end date and party end reason must be supplied');
ins_err(380,'Party End Reason does not exist in Domain PEO_END');
ins_err(381,'The Party End Date does not match the other periods supplied');
--
-- Errors for Loan Addresses
ins_err(382,'Supplied loan application reference does not reference an existing loan application');
ins_err(383,'Supplied main address indicator must be Y or N');
ins_err(384,'Supplied address UPRN does not reference an existing address');
ins_err(385,'Supplied property alternative reference does not reference an existing property');
ins_err(386,'Supplied admin unit code does not reference an existing admin unit');
--
-- Errors for Loan Parties
ins_err(387,'Supplied party alt ref does not reference an existing party');
ins_err(388,'Supplied main party indicator must be Y or N');
ins_err(389,'Supplied signatory indicator must be Y or N');
ins_err(390,'Supplied loan status reason code is not valid');
ins_err(391,'Supplied loan party end reason code is not valid');
ins_err(392,'Supplied relationship code is not valid');
--
-- Errors for Loan Accounts
ins_err(393,'Supplied payment reference does not reference an existing account');
ins_err(394,'Supplied loan type does not exist');
ins_err(395,'Loan account status date cannot be greater than today');
ins_err(396,'Supplied loan account status code is invalid');
--
-- Errors for Loan Account Transactions
ins_err(397,'Supplied payment reference does not reference an existing loan account');
ins_err(398,'Supplied payment/transaction reference does not reference an existing transaction');
ins_err(399,'Interest calculated on payment indicoator must be Y or N');
--
-- Errors for Payment Expectations
ins_err(400,'Payment exception type must be a valid payment_exception_type and match the system parameter LOAN_PEXT_TYPE');
ins_err(401,'Transaction type is invalid');
ins_err(402,'Transaction subtype is invalid');
ins_err(403,'Supplied party reference does not reference an existing party');
--
-- Errors for Answer History bespoke for GNB
ins_err(404,'The record type must be supplied');
ins_err(405,'Record type is NOT either GEN(General Answers) or LAA(Lettings Area Answers) so cannot be processed');
ins_err(406,'Application Legacy Ref must be supplied');
ins_err(407,'Question number must be supplied');
ins_err(408,'Action Indicator must be supplied');
ins_err(409,'Action Indicator must be either a U(update) or D(delete)');
ins_err(410,'Modified by must be supplied');
ins_err(411,'Modified Date must be supplied');
ins_err(412,'Created By must be supplied');
ins_err(413,'Created Date must be supplied');
ins_err(414,'Lettings Area Code must be supplied');
ins_err(415,'The application cannot be found on the applications table');
ins_err(416,'The application legacy reference supplied must be unique on the applications table');
ins_err(417,'Question for the Application does not exist in General Answers table');
ins_err(418,'Question for the Application does not exist in Lettings Area Answers table');
ins_err(419,'Question Reference does not exist in the Questions table');
ins_err(420,'Lettings Area Code does not exist in Lettings Areas table');
ins_err(421,'Lettings Area Code must only be supplied for LAA record types');
ins_err(422,'Optional Response Code and Question combination does not exist in question_optional_responses table');
ins_err(423,'A Duplicate record already exists in answer_history table');
--
-- Errors for Loan Account Reviews
ins_err(424,'If repayable years is supplied the loan type must have loty_repayable_ind = Y');
ins_err(425,'If repayable months is supplied the loan type must have loty_repayable_ind = Y');
ins_err(426,'If period remaining is supplied the loan type must have loty_repayable_ind = Y');
ins_err(427,'Effective date cannot be grater than today''s date');
ins_err(428,'Supplied loan review reason code is not a valid loan review reason code');
ins_err(429,'Supplied loan policy code is not a valid loan policy code');
ins_err(430,'Repayable months, if supplied, must be between 1 and 11');
ins_err(431,'If repayable years is supplied then repayable months must also be supplied');
ins_err(432,'If period remaining is supplied then repayable years must also be supplied');
ins_err(433,'Period remaining, if supplied, must match calculated value');
ins_err(434,'Interest Rate must only be populated if the Loan Account Review is in respect of a repayable Loan Account');
ins_err(435,'Supplied interest rate code code is not a valid interest rate code');
-- Further errors for Loan Applications
ins_err(436,'Priority of the loan application must be left blank');
ins_err(437,'Loan alternative reference must be unique');
-- Further errors for Loan Options
ins_err(438,'Loan Application of type ''ORG'' has more than one loan option with an end date');
ins_err(439,'End date must be greater than start date');
-- Further errors for Loan Addresses
ins_err(440,'Loan Application of type ''PAR'' has more than one loan address');
--
-- Errors for Involved Party History bespoke for GNB
ins_err(441,'Involved Party Start Date (liph_ipa_start_date)must be supplied');
ins_err(442,'Modified Date(liph_modified_date)must be supplied');
ins_err(443,'Modified By(liph_modified_by)must be supplied');
ins_err(444,'Action Indicator(liph_action_ind)must be supplied');
ins_err(445,'Action Indicator(liph_action_ind)must be set to U for Update');
ins_err(446,'Created Date(liph_created_date)must be supplied');
ins_err(447,'Created By(liph_created_by)must be supplied');
ins_err(448,'Start Date(iph_start_date)must be supplied');
ins_err(449,'Party Alternative(liph_par_per_alt_ref)must be supplied');
ins_err(450,'Party cannot be found using Party Alternate Reference Supplied');
ins_err(451,'Application Legacy Reference must be supplied');
ins_err(452,'Application cannot be found using Legacy Reference Supplied');
ins_err(453,'Involved Party cannot be found using Party Legacy Reference and IPA start date Supplied');
ins_err(454,'Application Start Date cannot be greater than involved party start date');
ins_err(455,'Main Applicant Indicator must be Y or N');
ins_err(456,'Joint Applicant Indicator must be Y or N');
ins_err(457,'Living Apart Indicator must be Y or N');
ins_err(458,'Rehousing Indicator must be Y or N');
ins_err(459,'Joint applicant indicator must be Y for main applicant');
ins_err(460,'Living apart indicator must be N for main applicant');
ins_err(461,'Rehouse indicator must be Y for main applicant');
ins_err(462,'Invalid relationship code');
ins_err(463,'Duplicate History record supplied');
ins_err(464,'End Date is earlier than Start Date supplied');
--
ins_err(476,'Invoice Category mismatch for this Admin Task');
ins_err(477,'Parent Invoice Not Correct Class for these Items');
ins_err(478,'Invoice Amount must be supplied');
ins_err(479,'Valid Admin Charge Code must be Supplied');
--      
ins_err(480,'Not a valid Element Code');
ins_err(481,'Element Type must be PR or CL');
ins_err(482,'Start Date must be in the future');
ins_err(483,'Value Type must be C or N');
ins_err(484,'Supplied Value Type does not match');
ins_err(485,'Period Code must be W,M or Y');
ins_err(486,'Apply Multiplier Ind must be Y or N');
ins_err(487,'Vat on Voids Ind must be Y or N');
ins_err(488,'Full Period Ind must be Y or N');
ins_err(489,'Reserved for Alliance Homes');
-- Further errors for Loan Accounts
ins_err(490,'Loan policy admin unit does not match account admin unit');
ins_err(491,'The supplied loan type is not valid for an organisation application');
ins_err(492,'The supplied loan type is not valid for a party application');
ins_err(493,'The combination of Loan Option Type and Loan Type must be unique for the Alternative Reference supplied');
ins_err(494,'Interest Adjustment Date should only be populated if the Loan Account is not Grant');
ins_err(495,'Next Gale Date should only be populated if the Loan Account is Repayable');
ins_err(496,'Accrued Interest Amount should only be populated if the Loan Account is Repayable');
-- Further errors for Loan Addresses
ins_err(497,'Loan Application has more than one loan address with main indicator set to ''Y''');
ins_err(498,'Only one of Address Reference, Admin Unit and Property Reference may be populated for each loan address');
ins_err(499,'Address Reference cannot be populated since there is no Loan Option, of a type that allows addresses');
ins_err(500,'Admin Unit cannot be populated since there is no Loan Option, of a type that allows admin units');
ins_err(501,'Property Reference cannot be populated since there is no Loan Option, of a type that allows properties');
-- Further errors for Loan Parties
ins_err(502,'Loan Application of type ''ORG'' has more than one loan party');
ins_err(503,'Loan Party associated with Loan Application of type ''ORG'' must be main party');
ins_err(504,'Loan Party associated with Loan Application of type ''ORG'' must not be signatory');
-- Further generic errors for Loans
ins_err(505,'If you supply a created by user, the user must exist');
ins_err(506,'If you supply a created date, the date cannot be greater than today''s date');
ins_err(507,'Loan application status reason is required');
ins_err(508,'Status review date is required');
--
-- Errors for Involved Party bespoke for GNB
ins_err(509,'Involved Party Start Date (lipa_start_date)must be supplied');
ins_err(510,'IPA Created Date(lipa_created_date)must be supplied');
ins_err(511,'IPA Created By(lipa_created_by)must be supplied');
ins_err(512,'PAR Created Date(lpar_created_date)must be supplied');
ins_err(513,'PAR Created By(lpar_created_by) must be supplied');
ins_err(514,'Party Surname must be supplied');
ins_err(515,'Party Forename must be supplied');
ins_err(516,'IPA Start Date cannot be greater than IPA End Date supplied');
ins_err(517,'Party type must be either HOUP or BOTP only');
ins_err(518,'IPA Created date must not be greater than today');
ins_err(519,'Head of Household Indicator must be Y or N if HHold Group No.is Supplied');
ins_err(520,'Duplicate IPA record found in involved parties table');
ins_err(521,'Duplicate IPA records found in data load batch');
ins_err(522,'The app_legacy_ref found against more than one application');
--
-- Errors for Warning Histories bespoke for GNB
ins_err(523,'Record Type must be APP or PAR');
ins_err(524,'App Legacy Reference(lwhi_app_legacy_ref)must be supplied for APP record');
ins_err(525,'App Legacy Reference found against more than 1 application');
ins_err(526,'Party Alternative(lwhi_par_per_alt_ref)must be supplied for PAR record');
ins_err(527,'More than 1 Party found using Party Alternate Reference Supplied');
ins_err(528,'Variable Warning Code must be supplied');
ins_err(529,'Action Point Code must be supplied');
ins_err(530,'The Variable Warning and Action Point Codes combination does not exist');
ins_err(531,'The Created date cannot be later than today (truncated sysdate)');
ins_err(532,'Warning Severity type must one of W R A or E');
ins_err(533,'Authorised Indicator must be Y or N');
ins_err(534,'The Admin Unit Code supplied does not exist');
ins_err(535,'The Override Reason does not exist for the Variable Warning Code supplied');
ins_err(536,'Session Marker must always be set to ~NONE~');
ins_err(537,'Party Object cannot be found using Party Alt Ref Supplied');
ins_err(538,'Warnings Histories record supplied already exists in table');
ins_err(539,'Duplicate Warnings Histories record supplied in data load file');
--
-- Errors for Admin Unit Security bespoke for GNB
ins_err(540,'Created Date cannot be greater than the Modified Date');
ins_err(541,'Created Date cannot be greater than today (sysdate)');
--
-- Errors for NonICS Income bespoke for GNB
ins_err(542,'The Person Alternative Reference must be supplied');
ins_err(543,'The Start Date must be supplied');
ins_err(544,'The Status Date must be supplied');
ins_err(545,'The Status Code must be supplied');
ins_err(546,'The Income Header Legacy reference must be supplied');
ins_err(547,'The Income Usages Reference Type must be supplied');
ins_err(548,'The Income Usages Reference Value must be supplied');
ins_err(549,'Income Detail Legacy Reference must be supplied');
ins_err(550,'Either Employer or Boarder Reference is needed if party type is supplied');
ins_err(551,'Income Code must be supplied');
ins_err(552,'Asset Code must be supplied');
ins_err(553,'Deduction Amount must be supplied');
ins_err(554,'Deduction Regular Amount must be supplied');
ins_err(555,'The Party Reference Type must be supplied');
--
-- Errors for Organisation Offers
ins_err(556,'The Respond By Date must be greater than the Offer Date');
ins_err(557,'The supplied Property Reference is not an existing property reference');
ins_err(558,'The number of offers for the void will exceed the system maximum');
ins_err(559,'No applic_list_entries record was found for the application with the supplied Rehousing List Code');
ins_err(560,'The number of offers for the application reference will exceed the system maximum');
ins_err(561,'Tenure Type does not exist');
ins_err(562,'Tenancy Type does not exist');
ins_err(563,'Stage Code does not exist');
ins_err(564,'Expected tenancy start date must be greater than or equal to Offer Date');
ins_err(565,'Offer type must be MOF i.e. Manual Offer');
ins_err(566,'Status code must be one of CUR – Current, CON – Confirmed, WIT - Withdraw, REF - Refused or ACC - Accepted ');
ins_err(567,'If you supply a Created Date, the date cannot be greater than today''s date');
--
-- Error for Tenancy Data loader
ins_err(568,'Overlapping Occupied Housing property Status for Property1');
ins_err(569,'Overlapping Occupied Housing property Status Property2');
ins_err(570,'Overlapping Occupied Housing property Status Property3');
ins_err(571,'Overlapping Occupied Housing property Status Property4');
ins_err(572,'Overlapping Occupied Housing property Status Property5');
ins_err(573,'Overlapping Occupied Housing property Status Property6');
--
-- Error for HRA Revenue Accounts Data loader
ins_err(574,'The Residential Indicator must be Y or N');
ins_err(575,'An escalation policy cannot be provided for the class code supplied');
--
-- Error for HEM first ref values and domains Data loader (AJ)
ins_err(576,'The Domain and frv_code or frv_code_mlang combination already exist');
ins_err(577,'The Domain Code must be supplied');
ins_err(578,'The Domain Code supplied does not exist');
ins_err(579,'The First Ref Value Code must be supplied');
ins_err(580,'The First Ref Value Description must be supplied');
ins_err(581,'The Current Indicator if supplied must be Y or N');
ins_err(582,'The Default Indicator if supplied must be Y or N');
ins_err(583,'The Usage Type if supplied must be USR');
ins_err(584,'The Sequence number must be between 0 and 999999');
ins_err(585,'The First Ref Value Code already exists against the domain supplied');
ins_err(586,'Duplicate records exist in the data load batch for the First Ref Value Code and Domain');
ins_err(587,'The First Ref Value MLANG Code already exists against the domain supplied');
ins_err(588,'Duplicate records exist in the data load batch for the First Ref Value MLANG Code and Domain');
ins_err(589,'Bilingual(MLANG) Name must be supplied as the (MLANG) Code has been supplied');
ins_err(590,'Bilingual(MLANG) Code must be supplied as the (MLANG) Name has been supplied');
ins_err(591,'The Domain Code supplied already exists');
ins_err(592,'The Domain Code values length has a maximum of 10');
ins_err(593,'The Domain Code Description must be supplied');
ins_err(594,'The Application Product Indicator must be supplied');
ins_err(595,'The Application Product Indicator must exist in the REFERENCE domain DOMAIN_PRODUCT');
ins_err(596,'The Mlang Domain Code supplied already exists');
--
-- Further Errors for HAT Organisation Offers Data loader (AJ)
ins_err(597,'The Offer Date must be supplied');
ins_err(598,'The Respond by Date must be supplied');
--
-- Service Charges Data Loader (AJ)
ins_err(599,'The supplied Tenancy Alternative Reference does not exist on the Housing database');
ins_err(600,'This Business Reason is not set for Tenancy Objects(TAR)');
--
-- Further Errors for HAT Organisation Offers Data loader (AJ)
ins_err(601,'Multiple Void Instances were found for the combination supplied');
ins_err(602,'A Void Instance cannot be found for the combination supplied');
ins_err(603,'The Tenure Type Code must be supplied');
ins_err(604,'The Tenancy Type Code must be supplied');
ins_err(605,'The Offer Stage Types Code must be supplied');
ins_err(606,'The Expected Tenancy Start Date must be supplied');
ins_err(607,'The Expected Tenancy Start Date must be supplied');
ins_err(608,'The Offer Status MUST BE "CUR" current if supplied');
ins_err(609,'The number of offers for the application will exceed the system max in this batch');
ins_err(610,'The number of offers for the property void will exceed the system max in this batch');
ins_err(611,'At Least one Other Void Instance field must be supplied as well as the status start date');
ins_err(612,'The Void Instance Reference cannot be found');
--
-- SAHT Bespoke HCS Contacts Loader
ins_err(613,'SAHT Bespoke HCS Contacts first error message');
--
-- SAM ASBESTOS DATA LOADER (AJ) and 648 - 
ins_err(614,'Property or Admin Unit Identifier must be supplied');
ins_err(615,'Property or Admin Unit Reference must be supplied');
ins_err(616,'Property or Admin Unit Element Code must be supplied');
ins_err(617,'Property or Admin Unit Element Attribute Code must be supplied');
ins_err(618,'Property or Admin Unit Element Location Code must be supplied');
ins_err(619,'Property or Admin Unit Element Further Attribute Code must be supplied');
ins_err(620,'All SAM Asbestos Codes 1 to 11 must be supplied');
ins_err(621,'The Calculate Score field must be left blank or Y or N');
ins_err(622,'The Asbestos Material Score must be supplied');
ins_err(623,'The Asbestos Priority Score must be supplied');
ins_err(624,'The Asbestos Total Score must be supplied');
ins_err(625,'No matching Part Location Code found in domain list ASBESTOS_PART_LOC');
ins_err(626,'Property Admin Unit Identifier must be either a P or A');
ins_err(627,'No matching Admin Unit Element record found');
ins_err(628,'No matching Property Element record found');
ins_err(629,'Code 1 Damage No matching Sam Asbestos Code found');
ins_err(630,'Code 2 Surface Treatment No matching Sam Asbestos Code found');
ins_err(631,'Code 3 Activity No matching Sam Asbestos Code found');
ins_err(632,'Code 4 Location No matching Sam Asbestos Code found');
ins_err(633,'Code 5 Accessibility No matching Sam Asbestos Code found');
ins_err(634,'Code 6 Extent No matching Sam Asbestos Code found');
ins_err(635,'Code 7 No of Occupants No matching Sam Asbestos Code found');
ins_err(636,'Code 8 Frequency of User No matching Sam Asbestos Code found');
ins_err(637,'Code 9 Time in Use No matching Sam Asbestos Code found');
ins_err(638,'Code 10 Maintenance Activity No matching Sam Asbestos Code found');
ins_err(639,'Code 11 Frequency of Maintenance No matching Sam Asbestos Code found');
ins_err(640,'This is not a valid Asbestos Element');
--
-- GNB error for subsidy debit breakdowns process for PH and NRH Properties (AJ)
ins_err(641,'A Debit Breakdown for the revenue account already exists');
ins_err(642,'The Account Rent Limit Revenue Account number has not been found');
ins_err(643,'The linked Revenue Account Start Date has not been found');
ins_err(644,'A linked Admin Year Start Date has not been found');
ins_err(645,'The linked Account Rent Limit Start Date has not been found');
--
-- Further Errors for HEM Job Role Action Groups Data loader (AJ)
ins_err(646,'Action Group Name is Mandatory');
ins_err(647,'Job Role Usage is Mandatory');
--
-- Further SAM ASBESTOS DATA LOADER (AJ)
ins_err(648,'At Least one of Action Code Date or Comment must be supplied');
ins_err(649,'No matching Asbestos Action Code found in domain list ASBESTOS_ACTION_CODE');
ins_err(650,'No matching Asbestos Element Details record found for Admin Unit Element');
ins_err(651,'No matching Asbestos Element Details record found for Property Element');
ins_err(652,'The Action Date cannot be a future date');
ins_err(653,'The Action Date cannot be before the Asbestos Element start date');
ins_err(654,'At Least one of Sample Code Date Number or Request Date or Comments must be supplied');
ins_err(655,'No matching Asbestos Action Code found in domain list ASBESTOS_SAMP_RESULT');
ins_err(656,'The Request Date cannot be a future date');
ins_err(657,'The Request Date cannot be before the Asbestos Element start date');
ins_err(658,'The Sample Date cannot be a future date');
ins_err(659,'The Sample Date cannot be before the Asbestos Element start date');
ins_err(660,'The Asbestos Element Start Date cannot be in the future');
ins_err(661,'Asbestos Details Record already exists for Property Element found');
ins_err(662,'There are duplicate Asbestos Details Records in the data load batch');
ins_err(663,'An Asbestos Element Action Record already exists with the same detail');
ins_err(664,'There are duplicate Asbestos Action Records in the data load batch');
ins_err(665,'An Asbestos Element Sample Record already exists with the same detail');
ins_err(666,'There are duplicate Asbestos Sample Records in the data load batch');
--
-- Errors for HAD Advice Case People
ins_err(667,'Advice case client not found for the advice case');
ins_err(668,'More than 1 Client found for the advice case');
--
-- Estates-Parties-Person Also Known as Data loader Errors (JT)
ins_err(669,'At least one of AKA FORENAME or AKA SURNAME is required.');
ins_err(670,'The PAKA_REFNO already exists.');
ins_err(671,'This combination of Also Known As information already exists for this person.');
ins_err(672,'The Reason Code given does not exist in the Reference Domain ALSOKNOWNASREASON.');
ins_err(673,'Duplicate records exist in the data load batch for the Person, Forename, Surname and Start Date.');
ins_err(674,'Multiple People match with the details provided.');
ins_err(675,'No People match using the details provided.');
ins_err(676,'Person Alt Ref and/or Person Name must be provided to perform a lookup.');
--
-- Reserved for HCS People Groups Loader
ins_err(677,'People Group Code must be supplied');
ins_err(678,'People Group Code supplied already exists');
ins_err(679,'Record is a duplicate in the data load batch');
ins_err(680,'Peg_Description must be supplied');
ins_err(681,'Start Date must be supplied');
ins_err(682,'People Group Type must be supplied');
ins_err(683,'People Group Type supplied is invalid');
ins_err(684,'Status code must be supplied');
ins_err(685,'Status code supplied is invalid');
ins_err(686,'No matching People Group Members record found for the dates specified');
--
--
ins_err(687,'Advice Case Reason and Outcome combination must exist in advice_reason_outcomes table');
--
-- Further Errors for HCS Actions Events and Contacts
ins_err(688,'The Event Code and path combination are not setup for Automaic Generation');
ins_err(689,'The Subject Type must be supplied if the Subject Legacy Ref is supplied');
ins_err(690,'The Subject Legacy Ref must be supplied if the Subject Type is supplied');
ins_err(691,'If the Subject Type is IPP then the Subject Secondary Reference must be supplied');
ins_err(692,'This is a Subject Business Reason so the Subject Legacy Ref must be supplied');
--ins_err(693,'Reserved for HCS AJ');
--ins_err(694,'Reserved for HCS AJ');
--ins_err(695,'Reserved for HCS AJ');
--ins_err(696,'Reserved for HCS AJ');
--ins_err(697,'Reserved for HCS AJ');
--ins_err(698,'Reserved for HCS AJ');
--ins_err(699,'Reserved for HCS AJ');
--
-- Further Errors for HCS People Groups Loader 
ins_err(700,'Admin Unit Code supplied is invalid');
ins_err(701,'The Created date cannot be later than today (truncated sysdate)');
ins_err(702,'People Group Code supplied is invalid');
ins_err(703,'Party Reference Number must be supplied');
ins_err(704,'Party Reference Number supplied is invalid');
ins_err(705,'Key Member Indicator supplied is invalid');
ins_err(706,'Dates overlap with an existing record');
ins_err(707,'Group Role Code must be supplied');
ins_err(708,'Group Role Code supplied is invalid');
ins_err(709,'The End Date cannot be on or before Start Date');
ins_err(710,'The End Date cannot be in the future');
ins_err(711,'Start and End Dates must be between the Start and End Dates of a corresponding People Group');
--
-- Additional HAT Placement Errors
ins_err(712,'Property is not a placement Property.');
ins_err(713,'Property does not have a Valid Property Type.');
ins_err(714,'Placement Property Room already exists.');
ins_err(715,'Unable to match to an existing Property.');
ins_err(716,'Placement Room Cost must be supplied.');
ins_err(717,'Placement Cost Period Code must be supplied.');
ins_err(718,'Placement Offered Date must not be later than today');
ins_err(719,'Placement End Reason must be supplied.');
ins_err(720,'Placement End Reason must be a valid Reason.');
ins_err(721,'A status reason is required with this status.');
ins_err(722,'The status reason supplied is not valid for this status.');
ins_err(723,'Overlapping Placements Found for this application.');
ins_err(724,'Placement Offered must be on or before the placement start.');
ins_err(725,'No Placement Matching Option Provided.');
ins_err(726,'Placement Order Reference must be supplied.');
ins_err(727,'Placement Order Reference already exists.');
ins_err(728,'Placement Order Status must be supplied.');
ins_err(729,'Placement Order Status is invalid.');
ins_err(730,'Placement Order Date must be supplied.');
ins_err(731,'Placement Order Status Date must be supplied.');
ins_err(732,'Placement Order Tax Rate must be provided.');
ins_err(733,'Placement Order Tax Rate is not valid.');
ins_err(734,'Placement Order Reference does not exist.');
ins_err(735,'Placement Order Line Number must be supplied.');
ins_err(736,'Placement Order Line already exists.');
ins_err(737,'Placement Order Line From Date must be supplied.');
ins_err(738,'Placement Line To Date must be after From Date.');
ins_err(739,'Order Line dates inconsistent with placement occupation dates.');
ins_err(740,'Placement Status Refused or Withdrawn, no placement room expected.');
ins_err(741,'Placement Room allocation Exceeds Max Occupancy for the room.');
ins_err(742,'Placement Room end date cannot exceed placement end date.');
--
-- Reserved for HCS Business Action Parties Loader
ins_err(743,'Business Action Reference must be supplied');
ins_err(744,'Business Action Reference supplied is invalid');
ins_err(745,'Start Date must be supplied');
ins_err(746,'Object Type must be supplied');
ins_err(747,'Object Reference must be supplied');
ins_err(748,'Main Party Indicator supplied is invalid');
ins_err(749,'Only one party for the business action may have the Main Party Indicator set to Y');
ins_err(750,'Business Action Role Code supplied is invalid');
ins_err(751,'Party Reference Number supplied is invalid');
ins_err(752,'Tenancy Reference Number supplied is invalid');
ins_err(753,'Interested Party Reference Number supplied is invalid');
ins_err(754,'Application Reference Number supplied is invalid');
ins_err(755,'People Group Code supplied is invalid');
ins_err(756,'Contractor Site code supplied is invalid');
ins_err(757,'Object Type supplied is invalid');
ins_err(758,'The End Date cannot be less than Start Date');
ins_err(759,'Dates overlap with an existing record');
ins_err(760,'Record is a duplicate in the data load batch');
ins_err(761,'The Created date cannot be later than today (truncated sysdate)');
ins_err(762,'Reserved for HCS Business Action Parties Loader');
--
-- Additional HAT Placement Errors
ins_err(763,'Placement Room Information Not valid for this Placement.');
--
-- Contractor Site Contacts Errors
ins_err(764,'Contractor Site Contact must be supplied.');
ins_err(765,'This Contact already exists for Contractor Site.');
--
-- Reserved for Tenant Allowances
ins_err(766,'Tenancy Reference Number must be supplied');
ins_err(767,'Tenancy Reference Number supplied is invalid');
ins_err(768,'Allowance Type Code must be supplied');
ins_err(769,'Allowance Type Code supplied is invalid');
ins_err(770,'Automatic indicator of Allowance Type must be N');
ins_err(771,'Start Date must be supplied');
ins_err(772,'The Start Date cannot be before today');
ins_err(773,'The End Date cannot be less than Start Date');
ins_err(774,'Amount must be supplied');
ins_err(775,'Amount must be between 0.01 and 99999999.99');
ins_err(776,'Approved Date must be supplied');
ins_err(777,'Approved Date cannot be after today');
ins_err(778,'Next Payment Due Date must be supplied');
ins_err(779,'Next Payment Due Date must be on or after Start Date');
ins_err(780,'This allowance can only be paid to a tenant who is being charged based on actual water usage');
ins_err(781,'Dates overlap with an existing record');
ins_err(782,'Record is a duplicate in the data load batch');
ins_err(783,'The Created date cannot be later than today (truncated sysdate)');
ins_err(784,'Reserved for Tenant Allowances Loader');
ins_err(785,'Reserved for Tenant Allowances Loader');
--
-- Additional HFI Mapping Value Updates loader (AJ)
ins_err(786,'Data load does not cater for HPM Mappings');
ins_err(787,'Mapping Code does not exist in mappings table');
ins_err(788,'Mapping Types of TRADE LIABILITY PRIORITIES JOB_CLASS SCHEDULE_OF_RATES PROP_STATUS_CODES are not allowed');
ins_err(789,'For the Mapping Type of ADMIN_UNITS the Admin Unit code must be supplied');
ins_err(790,'For the Mapping Type of ADMIN_UNITS only the Admin Unit code must be supplied');
ins_err(791,'A Current mapping values record was not found for the Admin Unit supplied');
ins_err(792,'For the Mapping Type of ELEMENTS the element code must be supplied');
ins_err(793,'For the Mapping Type of ELEMENTS only the Element Code code must be supplied');
ins_err(794,'A Current mapping values record was not found for the Element supplied');
ins_err(795,'For the Mapping Type of ATTRIBUTES both Element and Attribute must be supplied');
ins_err(796,'For the Mapping Type of ATTRIBUTES only the Element and Attribute must be supplied');
ins_err(797,'A Current mapping values record was not found for the Attribute and Element supplied');
ins_err(798,'For the Mapping Type of ACCOUNT_TYPES the Account Type Code must be supplied');
ins_err(799,'For the Mapping Type of ACCOUNT_TYPES only the Account Type Code must be supplied');
ins_err(800,'A Current mapping values record was not found for the Account Type supplied');
ins_err(801,'For the Mapping Type of TRANSACTION_SUBTYPES both the Type and Subtype must be supplied');
ins_err(802,'For the Mapping Type of TRANSACTION_SUBTYPES only the Type and Subtype must be supplied');
ins_err(803,'A Current mapping values record was not found for the Transaction Subtype and Type supplied');
ins_err(804,'For the Mapping Type of TRANSACTION_TYPES Type must be supplied');
ins_err(805,'For the Mapping Type of TRANSACTION_TYPES only the Type must be supplied');
ins_err(806,'A Current mapping values record was not found for the Transaction Type supplied');
ins_err(807,'For the Mapping Type of WORK_PROGRAMMES Work Programme Code must be supplied');
ins_err(808,'For the Mapping Type of WORK_PROGRAMMES only the Work Programme Code must be supplied');
ins_err(809,'A Current mapping values record was not found for the Work Programme supplied');
ins_err(810,'Mapping Type must be OBJ');
ins_err(811,'This is a Duplicate Batch Record for this combination');
--
-- Reserved for Referrals data loader (AJ)
ins_err(812,'Alternative Referral Reference must be supplied');
ins_err(813,'The Referral Alternative Reference already exits in the Referrals table');
ins_err(814,'The Referral Alternative Reference is duplicated within the batch');
ins_err(815,'The Referral Type must be supplied');
ins_err(816,'The Referral Type must be set to Y');
ins_err(817,'You must supply at least 1 of the Client Referral fields');
ins_err(818,'No Party record found for the Party Alternative Reference supplied');
ins_err(819,'More than one Party record found for the Party Alternative Reference supplied');
ins_err(820,'No Party record found for the Party Reference supplied');
ins_err(821,'No Party record found for Forename Surname DOB supplied');
ins_err(822,'More than one Party record found for Forename Surname DOB supplied');
ins_err(823,'The Referral Status Code must be Raised (RAI)');
ins_err(824,'The Referral Status Date cannot be earlier that the Created Date');
ins_err(825,'The Application Referral Received Date must be supplied');
ins_err(826,'The Referral Status Date cannot be earlier than the Application Referral Received Date');
ins_err(827,'The Client Services Code the Referral is for must be supplied');
ins_err(828,'A Current Client Services Code does not exist for the code supplied');
ins_err(829,'A Combination of Client Services Code and Support Provider Code does not exist');
ins_err(830,'A Client Services Referred onto Code does not exist in the support providers table');
ins_err(831,'The Admin Unit Supplied A Client Services Referred onto Code does not exist in the support providers table');
--
-- More for Asbestos Loader(AJ)
ins_err(832,'A unique Admin Unit Elements record was not found');
ins_err(833,'A unique Property Element record was not found');
--
-- More for Estates Loader(AJ)
ins_err(834, 'Duplicates Not allowed but More than 1 Org already exists for the Name Shortname and Type Supplied');
ins_err(835, 'The Contractor Site has a different Organisation Linked and Update Code is set to No');
ins_err(836, 'The Contractor Site is Suspended so cannot be updated');
ins_err(837, 'More than one Organisation Record in batch with the same Contractor Site Code');
--
-- HCS Contact Reasons Other Fields Errors (JT)
ins_err(838, 'Unable to match the Contact Reason information to an existing entry.');
ins_err(839, 'The subject type is not valid for this business reason.');
ins_err(840, 'A unique subject has not been found using the information given.');
ins_err(841, 'The supplied business reason requires a subject association.');
ins_err(842, 'The other field supplied is not valid for this Business Reason on Contacts.');
ins_err(843, 'The contact reference supplied was not found in Contacts.');
ins_err(844, 'The Contact Legacy reference could not be determined using the Dataload Dataset.');
--
-- HRM Pricing Policy Programme Errors   
ins_err(850, 'Invalid Determination Rule supplied.');
ins_err(851, 'Another record is already set as the default.');
--
-- More for Referrals data loader (AJ)
ins_err(852,'This client has already been referred to this support provider for this service');
ins_err(853,'This client has been referred to another support provider for this service');
--
-- HEM Terminate Tenancy Error (JT)
ins_err(854,'A Tenancy Reference or a Property Reference Must be Supplied.');
--
-- More for Referrals data loader (AJ)
ins_err(855,'From the par_refno duplicate records for support provider and service found in the batch');
ins_err(856,'From the Party Alt Ref duplicate records for support provider and service found in the batch');
ins_err(857,'From the Party Name and DOB duplicate records for support provider and service found in the batch');
ins_err(858,'The Referred to and third party Support Providers cannot be the same');
ins_err(859,'A Referral can only be for a Person');
--
END;
/



