-- Script Name = hd2_errs_in.sql
---------------------------------------------------------------------- 
--
-- Script to insert HDX Error messages
--
--   Ver   DB Ver  Who    Date        Reason
--
--   1.0   5.15.0  PH     14-JAN-2009 New Error Codes for Bespoke NSW
--                                    Dataloads
--
--   2.0   5.15.0  VS     17-APR-2009 New Error Code for ADVICE_CASE_PEOPLE
--                                    Dataload
--
--   2.1   5.15.0  VS     27-APR-2009 New Error Code for ADVICE_CASE_HOUSING 
--                                    _OPTIONS Dataload.
--
--   2.2   5.15.0  VS     29-APR-2009 New Error Code for ADVICE_REASON_CASEWORK_
--                                    _EVENTS Dataload.
--
--   2.3   5.15.0  VS     18-MAY-2009 New Error Code for SUBSIDY Dataloads
--
--   2.4   5.15.0  VS     04-JUN-2009 New Error Code for RDS Dataloads
--
--   2.5   5.15.0  VS     20-OCT-2009 New Error Code for SUBSIDY_REVIEWS 
--                                    Dataloads
--
--                                    New Error Codes for GROUP_SUBSIDY_REVIEWS 
--                                    Dataloads
--
--   2.6   5.15.0  VS     16-NOV-2009 New Error Code for RDS Dataload
--
--   2.7   5.15.0  VS     17-NOV-2009 New Error Code for Bespoke IPP Addresses
--                                    Dataload (322)
--
--   2.8   5.15.0  VS     30-NOV-2009 New Error Code for Housing Options
--                                    Dataload (323)
--
--   2.9   5.15.0  VS     03-DEC-2009 New Error Code for Account Rent Limits
--                                    Dataload (324)
--
--   3.0   5.15.0  VS     09-DEC-2009 New Error Code for RDS Account Allocations
--                                    Dataload (325)
--
--   4.0   5.15.0  VS     11-DEC-2009 New Error Code for Housing Advice People
--                                    Dataload (326)
--
--   5.0   5.15.0  VS     03-FEB-2010 New Error Code for Income Detail Requests
--                                    Dataload (327-328)
--
--   5.1   5.15.0  VS     17-FEB-2010 New Error Code for Registered Address Lettings
--                                    Dataload (329-330)
--
--   5.2   5.15.0  VS     08-MAR-2010 New Error Code for Prevention Payments
--                                    Dataload (331-332)
--
--   5.3   5.15.0  VS     24-MAR-2010 New Error Code for Income Detail Requests
--                                    Dataload (333)
--
--   5.4   5.15.0  VS     12-APR-2010 New Error Code for ICS Incomes Dataload
--                                    (334-337)
--
--                                    New Error Code for ICS Request Statuses Dataload
--                                    (338)
--
--   5.5   5.15.0  VS     28-APR-2010 New LWR (RAPS) Data Load Codes (339 - 373)
--
--   5.6   5.15.0  VS     28-MAY-2010 New IPP Addresses Data Load Code (374)
--
--   5.7   5.15.0  VS     07-JUN-2010 New Subsidy Income Items Data Load Code (375-376)
--
--   5.8   5.15.0  VS     22-JUN-2010 New ICS BENEFIT PAYMENTS data load area (377-388)
--                                    New ICS PAYMENT COMPONENTS data load area (389-393, 400)
--                                    New ICS DEDCTIONS data load area (394-399,401)
--
--   5.9   5.15.0  VS     15-JUL-2010 New Error Code for ICS Request Statuses Dataload
--                                    (402)
--
--   6.0   5.15.0  VS     02-SEP-2010 New Error Code for LWR/RAPS Water Charge Calc Audits
--                                    /Apportioned Assess Details Dataload (403-407)
--
--   6.1   5.15.0  VS     22-SEP-2010 New Error Code for VALUATIONS Dataload
--                                    (408-442)
--
--
--   6.2   5.15.0  VS     14-OCT-2010 New Error Code for INCOME_DETAIL_DEDUCTIONS Dataload
--                                    (443-447)
--
--   6.3   5.15.0  VS     16-OCT-2010 New Error Code for VALUATIONS Dataload
--                                    (448-454)
--
--   6.4   5.15.0  MT     18-JUN-2011 Tweaked HD2 73 to be inform ONE par ref must be supplied.
--
--   6.5   6.4.0   PH     04-JUL-2011 New Error Codes for subsidy debt
--                                    assessments (455-459). New codes for 
--                                    changes to other subsidy dataload
--                                    areas (460-467)
--
--   6.6   6.5.0   VS     04-JUL-2011 Amending/Adding new codes for NON ICS Incomes Dataload
--                                    needed by HNZ (468 - 489)
--
--   6.7   6.5.0   MB     13-NOV-2012 Addition of Person Attributes dataload errors previously
--                                    indexed between 455 and 464, now (540-549)
--
--   6.8   6.7.0   VS     21-MAR-2013 Tidy up for ICS Incomes
--
--   6.0   6.8.0   AJ     11-JUL-2013 Amended for warranties dataload (600 - 634)
--
--   6.9   6.7.0   MK     19/09/2013  Addition of Household Persons dataload errors (635 - 641)
--
--   7.0   6.8.0   AJ     21-OCT-2013 Amended Household Persons dataload (639) to remove comma from file 
--
--   7.0   6.8.0  AJ/PJD  21-OCT-2013 Add Other Fields dataload (650 - 652) & PJD
--
--   7.1   6.9.0  AJ      04-NOV-2013 Added Budget_Admin_Unit_Security (645-649 + 672)
--                                    Added Party_Admin_Unit_Security (653 - 671) AND (673 - 677)
--
--   7.2   6.9.0  AJ      12-NOV-2013 Added Arrears Arrangements  (678)
--
--   7.3   6.9.0  MB      29-NOV-2013 Added HPM Budget GL CLassifications (679 - 693)
--
--  7.3A   6.9.0  AJ      13-JAN-2014 Added My Portal Customer Activity dataload for WA (694 - 718)
--
--   7.4   6.9.0  MB      20-JAN-2014 Added more for HPM Budget GL CLassifications (719 - 726)
--   7.5   6.9.0  PJD     12-MAR-2014 Additional Prevention Payment Errors (730 - 731)
--
--   7.6   6.9.0  AJ      14-MAR-2014 Added Other Field Values for Organisations (732 - 744)
--
--   7.7   6.9.0  AJ      31-MAR-2014 Added Other Field Values for Revnue Accounts (745 - 749)
--
--   7.9   6.9.0  AJ      01-JUL-2014 Added Interested Party Usages (750 - 758)
--
--   8.0   6.9.0  AJ      12-AUG-2014 Added PSL Plandord Payment Details for nhhg (759 - 766)
--
--   8.1   6.9.0  AJ      31-AUG-2014 Added Service Charge Rates for Genesis HA (767 - 781)
--   8.2   6.9.0  PJD     03-SEP-2014 UK Payment Arrangement Codes              (782 - 786)
--   8.3   6.9.0  AJ      18-SEP-2014 Service Charge Rates for Genesis HA amended wording for 
--                                    error message 776 and 777 only
--   8.4   6.9.0  MB      23-SEP-2014 added a few for a bespoke LCC dataload for Project IDs (787-790)
--   8.5   6.10   AJ	              Errors added for ELIG_CRITERIAS Data load for LCC (791 - 796)
--   8.6   6.11   MOK     03-JUN-215  added for MLANG HRM dataload v6.11 (797 - 803)
--   8.7   6.10   AJ      04-JUN-2015 added for MAD_OTHER_FIELD_VALUES (804 - 806)
--   8.9   6.11   MOK     ??-JUN-2015 added Works Order dataload (807)
--   9.0   6.11   AJ      17-JUN-2015 added for Admin Unit dataload (808 - 813)
--   9.1   6.11   AJ      21-AUG-2015 added Additional Subsidy Reviews were originally 493 494 and 495
--                                    in bespoke version of hd2_errs_in now 814 - 816
--   9.2   6.11   AJ      18-DEC-2015 added Additional Advice Case errors (817)
--   9.3   6.11   AJ      23-DEC-2015 added Additional Income Data Load errors (818 - 821)
--   9.4   6.13   AJ      25-JAN-2016 added for label extract GNB (822 - 825)
--   9.5   6.13   AJ      09-FEB-2016 added for new contact details checks multiple data loads (828 - 834)
--   9.6   6.13   AJ      22-FEB-2016 added for Allocations Config data loads (835 - 846)
--   9.7   6.13   AJ      23-FEB-2016 added for Housing Advice data load (847 - 851)
--   9.8   6.13   AJ      25-FEB-2016 added further for Housing Advice data load (852 - 859)
--   9.9   6.13   AJ      26-FEB-2016 added further for Allocations data load (860 - 864)
--   10.0  6.13   AJ      29-FEB-2016 added further for Allocations data load (865 - 866)
--   10.1  6.13   AJ      16-MAR-2016 1) added further for GNB label extract for
--                                    New Brunswick Data Load (867 - 872)
--                                    2)amended wording slightly on 823 - 825 also for
--                                    label extract for New Brunswick Data Load
--   10.2  6.13   AJ      22-MAR-2016 added Additional Income Data Load errors (873 - 881)
--   10.3  6.13   AJ      23-MAR-2016 added further for GNB label extract for
--                                    New Brunswick Data Load (882 - 888)
--   10.4  6.13   AJ      24-MAR-2016 added Additional property elements Data Load errors (889) 
--   10.5  6.13   PAH     25-APR-2016 Property status code error for hem properties (890)
--   10.6  6.13   AJ      27-APR-2016 Property status code error for hem properties (890) wording amended
--                                    additional Rents transactions DL errors (891 - 893)
--   10.7  6.13   AJ      11-MAY-2016 MAD Area data load more added (894-895)
--   10.8  6.13   AJ         MAY-2016 additional rent deductions and void instances(896-903 )
--   10.9  6.13   AJ      06-JUN-2016 add bespoke HNZ Consents (was 455 now 910)
--                                                HNZ Name Change History (was 456 now 911)
--                                                HNZ Person Attributes (was 457-464 now 912-919)
--                                    as on old bespoke version of HD2 and numbers have been used for
--                                    other data loaders
--                                    Added additional errors for Other Fields data load changes (920-926)
--   11.0  6.13   AJ      08-JUL-2016 Additional error added for Con Site Job Roles data load part of
--                                    the HRM Contractors data load (927 - 933)
--   11.1  6.13   AJ      11-JUL-2016 Additional error added for Con Site Job Roles data load part of
--                                    the HRM Contractors data load (934 - 935 )
--   11.2  6.13   AJ      17-JUL-2016 Additional error added for MOD data load (936 - 938)
--   11.3  6.13   AJ      05-OCT-2016 Note added as error no.894 about work description code found on version
--                                    in v613 dl_load folder but not on master possible added around SEP-2016
--                                    cannot be used as number already gone and cannot find dl it refers to
--                                    to change the number
--   11.3  6.13   AJ      25-OCT-2016 **FOUND PACKAGE s_dl_hrm_schedule_of_rates**mentioned in 11.3 above
--   11.4  6.14   AJ      27-SEP-2017 reinstated error 82 for subsidy assessment code check
--   11.5  6.14   AJ      09-OCT-2017 wording slightly amended 82
--   11.6  6.14   AJ      23-FEB-2018 spelling error corrected in 72 and 474
--
--   11.7  6.18   VRS     17-DEC-2018 Adding more validation checks for Payment Expectations DL to accommodate
--                                    Queensland 6.18 Requirements for RDS Account Deductions Link
---------------------------------------------------------------------------------- 
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
AND    err_object_shortname = 'HD2';
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
  values('HD2',p_err_refno,         
  p_err_text,        
  'V','N');
dbms_output.put_line('Error number '||p_err_refno||' inserted.');
ELSIF
   l_err_message != p_err_text       
   THEN
     UPDATE errors
     SET    err_message = p_err_text    
     WHERE  err_object_shortname = 'HD2'
       AND   err_refno = p_err_refno;
dbms_output.put_line('Error number '||p_err_refno||' updated.');
END IF;
--
END ins_err;
--
--
BEGIN
--
ins_err(1,  'Person Exists but Forename and Date of Birth missing');
ins_err(2,  'Granted by Person does not exist');
ins_err(3,  'End Date must not be earlier than Start Date');
ins_err(4,  'End Date must not be earlier than Granted Date');
ins_err(5,  'End Reason must be supplied if End Date supplied');
ins_err(6,  'End Date must be supplied if End Reason supplied');
ins_err(7,  'Review Date must be in the future');
ins_err(8,  'Consent Type not found in domain CONSENTTYPE');
ins_err(9,  'Consent Source not found in domain CONSENTSOURCE');
ins_err(10, 'Consent End Reason not found in domain CONSENTENDRSN');
ins_err(11, 'Granted Date must be supplied');
ins_err(12, 'Subsidy application Reference must be supplied');
ins_err(13, 'Subsidy Review Record does not exist for Tenancy and Dates');
ins_err(14, 'Invalid Status Code');
ins_err(15, 'Override Reason not found in domain OVERRIDERSN');
ins_err(16, 'Status Date must be supplied');
ins_err(17, 'No Income Header Record found for Person and Dates supplied');
ins_err(18, 'Invalid Income Code');
ins_err(19, 'Employer Party Reference does not exist on Parties Table');
ins_err(20, 'Wage End Date must not be earlier than Wage Start Date');
ins_err(21, 'If Calculated Wage Supplied then Wage Start/End Date and Gross Wage must be supplied');
ins_err(22, 'Calculated Wage Indicator must be Y or N');
ins_err(23, 'Verification Type not found in domain VERIFIEDTYPE');
ins_err(24, 'Asset Code does not exist in ASSET CODES table');
ins_err(25, 'Percentage Owned must be between 1 and 100');
ins_err(26, 'Value of Asset must be supplied');
ins_err(27, 'Income Detail Type not found in domain INCOMEDETAILTYPE');
ins_err(28, 'Invalid Legacy Type');
ins_err(29, 'Application Legacy Reference not found on Applications Table');
ins_err(30, 'Tenancy Legacy Reference not found on Tenancies Table');
ins_err(31, 'Business Action not found on Business Actions Table');
ins_err(32, 'Subsidy Application not found on Subsidy Applications Table');
ins_err(33, 'Subsidy Review not found on Subsidy Review Table');
ins_err(34, 'Advice Case not found on Advice Case Table');
ins_err(35, 'Financial Period not found on ICS Financial Periods Table');
ins_err(36, 'Status Code should be set to RAI, SEN, UNA, COM, TBC, CAN, ERR, CRS, RNC, CAF, PER');
ins_err(37, 'Partner Indicator must be Y or N');
ins_err(38, 'Request Date must be supplied if Request Type is PIT');
ins_err(39, 'Financial Period Code must be supplied if Request Type is one of FYR, QTR');
ins_err(40, 'Tenancy Reference must be supplied');
ins_err(41, 'Tenancy Reference does not exist on Tenancies Table');
ins_err(42, 'Start Date must be supplied');
ins_err(43, 'Start Date must not be earlier than Tenancy Start Date');
ins_err(44, 'Start Date must be set to a date which falls on the day of the week as Rent Week Start Day');
ins_err(45, 'Received Date must be supplied');
ins_err(46, 'Received Date must not be in the future');
ins_err(47, 'Status Code must be supplied');
ins_err(48, 'Checked Date must not be in the future');
ins_err(49, 'Subsidy Assessment Code must be supplied');
ins_err(50, 'Subsidy Assessment Code not found in domain SUBASSCAT');
ins_err(51, 'Subsidy Termination Reason not found in domain SUBTERMRSN');
ins_err(52, 'Subsidy Application does not exist for legacy reference');
ins_err(53, 'Review Effective Date must be set to a date which falls on the day of the week as Rent Week Start Day');
ins_err(54, 'Review Class Code must be supplied');
ins_err(55, 'Review Class Code must be either TENANCY or SAS');
ins_err(56, 'Review Effective Date must be supplied');
ins_err(57, 'Review Effective Date must not be earlier than Subsidy Application Start Date');
ins_err(58, 'Review Assessment Date must be supplied');
ins_err(59, 'Eligible Indicator must be supplied');
ins_err(60, 'Eligible Indicator must be Y or N');
ins_err(61, 'Subsidy Policy Category Code must be supplied');
ins_err(62, 'Subsidy Policy Category Code not found in domain SUBASSCAT');
ins_err(63, 'Subsidy Policy sequence must be supplied');
ins_err(64, 'Subsidy Policy Category/Sequence does not exists on Subsidy_Policies Table');
ins_err(65, 'Cancellation Reason Code must be supplied if Subsidy Review Status is CAN');
ins_err(66, 'Authorised Date and By must be supplied if Subsidy Review Status is AUT');
ins_err(67, 'Review End Date must not be before the Review Assessment Date');
ins_err(68, 'Review End Date must not be before the Review Effective Date');
ins_err(69, 'Subsidy Review Reason Code must be supplied');
ins_err(70, 'Subsidy Review Reason Code not found in domain SUBREVREAS');
ins_err(71, 'Subsidy Cancellation Reason not found in domain SUBSIDY CANC REASONS');
ins_err(72, 'Subsidy Review Record does not exists for Subsidy Review Legacy Ref supplied');
ins_err(73, 'One value for Person Reference must be supplied');
ins_err(74, 'Subsidy Income Type Code must be supplied');
ins_err(75, 'Subsidy Income Type Code not found in domain SUBINCTYPE');
ins_err(76, 'Income Eligibility Amount must be supplied');
ins_err(77, 'Income Subsidy Amount must be supplied');
ins_err(78, 'Income Overridden Indicator must be supplied');
ins_err(79, 'Income Overridden Indicator must be Y or N');
ins_err(80, 'Subsidy Assessment Rule record does not exist for supplied Assessment Rule Code/Subsidy Category Code/Sequence');
ins_err(81, 'Household Persons record does not exist for Tenancy/Person/Subsidy Application Start Date');
ins_err(82, 'Subsidy Assessment Code not found in table subsiy_assessment_categories');
ins_err(83, 'Subsidy Override Reason not found in domain SUB_INC_ORIDE_RSN');
ins_err(84, 'Rent Limit Type Code must be supplied');
ins_err(85, 'Invalid Rent Limit Type Code supplied');
ins_err(86, 'Rent Limit Start Date must be supplied');
ins_err(87, 'Rent Limit Start Date must not be earlier than Review Effective Date');
ins_err(88, 'Income Subsidy Amount must be supplied');
ins_err(89, 'Rent Limit End Date must not be earlier than Review Effective Date');
--
ins_err(90, 'RDS Authority Reference already exists');
ins_err(91, 'Invalid Pending Status Code');
ins_err(92, 'Suspension End date cannot be before Suspension Start Date');
ins_err(93, 'Suspension Start date cannot be before Start Date');
ins_err(94, 'Agency Code not found in domain RDS_PAY_AGENCY');
ins_err(95, 'Suspension Reason not found in domain RDS_SUS_RSN');
ins_err(96, 'Termination Reason not found in domain RDS_TERM_RSN');
ins_err(97, 'RDS Authority must be supplied');
ins_err(98, 'Agency/Person Reference must be supplied');
ins_err(99, 'RDS Authority Reference does not exist on RDS Authorities table');
ins_err(100,'Record already exists for Authority, Deduction Type, Start Date and Benefit Group');
ins_err(101,'Invalid Status Code, must be one of PND, CON, ACT, ERR, SUS, TRM, CAN');
ins_err(102,'Invalid Pending Status Code, must be one of VAR, SUS, TRM');
ins_err(103,'Deduction Type not found in domain RDS_DED_TYPE');
ins_err(104,'Benefit Group not found in domain RDS_BEN_GRP');
ins_err(105,'RDS Authorised Deduction does not exist for Authority, Deduction Type, Start Date and Benefit Group');
ins_err(106,'Record already exists for Deduction, Account, Deduction Type, Start Date and Benefit Group');
ins_err(107,'Invalid Account Deduction Type Code supplied');
ins_err(108,'Invalid Status Code, must be one of PND, ACT, SUS');
ins_err(109,'Invalid Pending Status Code, must be one of NEW, TRM, SUS, VAR');
ins_err(110,'Requested Amount must be supplied');
ins_err(111,'Fixed Amount Indicator must be Y or N');
ins_err(112,'Minor Arrears Adjustment Indicator must be Y or N');
ins_err(113,  'Advice Case Reference must be supplied');
ins_err(114,  'Advice Case Record already exists for Advice Case Reference supplied');
ins_err(115,  'Approach Date must be supplied');
ins_err(116,  'Adivce Case Status Code must be supplied');
ins_err(117,  'Invalid Advice Case Status Code supplied');
ins_err(118,  'Advice Case Reason Status Code must also be CLO if Advice Case Status Code is CLO');
ins_err(119,  'Advice Case Opened Date must be supplied  if Advice Case Status Code is OPN');
ins_err(120,  'Previous Advice Case Status Code/Date must be supplied if Advice Case Status Code is HLD');
ins_err(121,  'Advice Case Status Date must be supplied');
ins_err(122,  'Case Correspondence Name must be supplied');
ins_err(123,  'Homeless Indicator must be supplied');
ins_err(124,  'Homeless Indicator must be Y or N');
ins_err(125,  'Expected Homeless Date must not be supplied if Homeless Indicator is N');
ins_err(126,  'Admin Unit Responsible for Advice Case must be supplied');
ins_err(127,  'Invalid Admin Unit supplied');
ins_err(128,  'Advice Case Reason Code must be supplied');
ins_err(129,  'Invalid Advice Case Reason Code supplied');
ins_err(130,  'Advice Case Reason Status Code must be supplied');
ins_err(131,  'Invalid Advice Case Reason Status Code supplied');
ins_err(132,  'Advice Case Status Code must also be CLO if Advice Case Reason Status Code is CLO');
ins_err(133,  'Advice Case Reason Status Date must be supplied');
ins_err(134,  'Advice Case Reason Record already exists for Advice Case/Case Reason combination supplied');
ins_err(135,  'Main Reason Indicator must be supplied');
ins_err(136,  'Main Reason Indicator must be Y or N');
ins_err(137,  'Approach Method must be supplied');
ins_err(138,  'Approach Method not found in domain ADV_CASE_APPR_METH');
ins_err(139,  'Casework Type not found in domain ADV_CASE_CASEWK_TYPE');
ins_err(140,  'Advice Case Priority not found in domain ADV_CS_PRIORITY');
ins_err(141,  'Invalid Previous Advice Case Status Code supplied');
ins_err(142,  'Invalid Previous Advice Case Reasons Status Code supplied');
ins_err(143,  'Unable to estable Admin Unit Type for system parameter ADVCASE_AUN_TYPE');
ins_err(144,  'Advice Case Record does not exist for Advice Case Reference supplied');
ins_err(145,  'Invalid Outcome Code supplied');
ins_err(146,  'Outcome Indicator must be Y or N');
ins_err(147,  'Advice Case Person Reference must be supplied');
ins_err(148,  'Invalid Advice Case Person Reference supplied');
ins_err(149,  'Client Indicator must be supplied');
ins_err(150,  'Client Indicator must be Y or N');
ins_err(151,  'Joint Client Indicator must be supplied');
ins_err(152,  'Joint Client Indicator must be Y or N');
ins_err(153,  'Advice Case Person Start Date must be supplied');
ins_err(154,  'Advice Case Person End Date must not be before Advice Case Person Start Date');
ins_err(155,  'Advice Case Person Record already exists for Advice Case/Person Ref combination supplied');
ins_err(156,  'Relationship Code not found in domain RELATION');
ins_err(157,  'Advice Case Housing Option Reference must be supplied');
ins_err(158,  'Advice Case Housing Option Code must be supplied');
ins_err(159,  'Invalid Advice Case Housing Option Code supplied');
ins_err(160,  'Delivery Status Code must be supplied');
ins_err(161,  'Delivery Status Code not found in domain DELIVERYSTATUS');
ins_err(162,  'Advice Case Housing Option Status Date must be supplied');
ins_err(163,  'Advice Case Reason Record does not exists for Advice Case/Case Reason combination supplied');
ins_err(164,  'Delivery Status record does not exists for Housing Option Code/Delivery Status Code combination supplied');
ins_err(165,  'Effective Date must be supplied');
ins_err(166,  'Instruction Amount must be supplied');
ins_err(167,  'RDS Instruction record not found');
ins_err(168,  'Allocated Amount must be supplied');
ins_err(169,  'Deduction Action Type must be supplied');
ins_err(170,  'RDS Account Dedcution record not found');
ins_err(171,  'RDS Allocation record not found');
ins_err(172,  'Priority must be between 1 and 99');
ins_err(173,  'Requested Amount must be supplied');
ins_err(174,  'Fixed Amount Indicator must be Y or N');
ins_err(175,  'Allocated Amount must be supplied');
--
--ADVISE_REASON_CASEWORK_EVENTS
--
ins_err(176,  'Case Event Type Code must be supplied');
ins_err(177,  'Invalid Case Event Type Code supplied');
ins_err(178,  'Event Date/Time must be supplied');
ins_err(179,  'Event Direction Indicator must be supplied');
ins_err(180,  'Invalid Event Direction Indicator supplied');
ins_err(181,  'Client Involvement Indicator must be supplied');
ins_err(182,  'Client Involvement Indicator must be Y or N');
ins_err(183,  'Direct Intervention Indicator must be supplied');
ins_err(184,  'Direct Intervention Indicator must be Y or N');
ins_err(185,  'Invalid Advice Case Housing Option Reference supplied');
ins_err(186,  'Either the Advice Case Reference OR Advice Case Housing Option Reference must be supplied');
ins_err(187,  'Advice Reason Casework Event Record already exists');
--
--ADVICE_CASE_HOUSING_OPTION_HIS
--
ins_err(188,  'Delivery Status Date must be supplied');
--
--BONDS
--
ins_err(189,  'Admin Unit Code must be supplied');
ins_err(190,  'Bond Reference Supplied already exists');
ins_err(191,  'Interested Party Shortname does not exist');
ins_err(192,  'Bond Override Reason Code not found in domain BONDOVERRIDERSN');
ins_err(193,  'Claim Reason Code not found in domain ?????????????');
--
--ADVICE_CASE_QUESTN_RESPONSES
--
ins_err(194,  'Case Question Reference must be supplied');
ins_err(195,  'Invalid Case Question Reference supplied');
ins_err(196,  'Question Type must be supplied');
ins_err(197,  'Invalid Question Type Supplied, must be (C) - Coded, (N) - Non Coded, (Y) - Yes/No');
ins_err(198,  'Invalid Yes/No Response Supplied for Question Type (Y), must be Y or N');
ins_err(199,  'Only Yes/No Response should be supplied for Question Type (Y), Coded/Date/Text/Numeric should not be supplied');
ins_err(200,  'Coded Response should be supplied for Question Type (C)');
ins_err(201,  'Only Coded Response should be supplied for Question Type (C), Yes/No/Date/Text/Numeric should not be supplied');
ins_err(202,  'Coded Question Response does not exist for the combination of Question Reference and Coded Response');
ins_err(203,  'Unable to establish if Coded Question Response is valid because Question Reference has not been supplied');
ins_err(204,  'Coded Question Response or Yes/No Question Response should not be supplied for Question Type (N)');
ins_err(205,  'Date Value Response must be supplied for Data Type (D) against Question Type (N)');
ins_err(206,  'Text or Numeric Value Response should not be supplied if Data Type is (D) against Question Type (N)');
ins_err(207,  'Numeric Value Response must be supplied for Data Type (N) against Question Type (N)');
ins_err(208,  'Text or Date Value Response should not be supplied if Data Type is (N) against Question Type (N)');
ins_err(209,  'Text Value Response must be supplied for Data Type (T) against Question Type (N)');
ins_err(210,  'Numeric or Date Value Response should not be supplied if Data Type is (T) against Question Type (N)');
ins_err(211,  'Question Responses already exists for combination of Question Ref and Advice Case ref or Housing Option Advice case');
--
--REGISTERED_ADDRESS_LETTINGS
--
ins_err(212,  'Registered Address Legacy Reference must be supplied');
ins_err(213,  'Address Register Code must be supplied');
ins_err(214,  'Registered Address Lettings Reference must be supplied');
ins_err(215,  'Registered Address Lettings Reference already exists');
ins_err(216,  'Letting Start Date must be supplied if Status is ACC');
ins_err(217,  'Visit Date/Time must be supplied if Status is VIS');
ins_err(218,  'Either the Advice Case Reference OR Advice Case Housing Option Reference must be supplied, not both');
ins_err(219,  'Proposed End Date must not be earlier than Letting Start Date');
ins_err(220,  'Proposed End Date must not be earlier than Visit Date/Time');
--
ins_err(221, 'Error Reason Code cannot be null when status code is ERR');
ins_err(222, 'Error Text cannot be populated when the status code is different from ERR');
ins_err(223, 'Subsidy Assessment Code not found in domain ICSERRORCODE');
--
ins_err(224, 'Transmission File type must be I or O');
ins_err(225, 'Payment Agency Code not found in domain RDS_PAY_AGENCY');
ins_err(226, 'Transmission File Reference exists on transmission file table');
ins_err(227, 'Transmission File Reference does not exist on transmission file table');
ins_err(228, 'Error File Reference exists on RDS Errors table');
ins_err(229, 'Transaction Reference exists on RDS PYI table');
ins_err(230, 'Authorised Deduction does not exist');
ins_err(231, 'Transmission File Reference does not exist');
ins_err(232, 'CRN Number must be supplied');
ins_err(233, 'Timestamp must be supplied');
ins_err(234, 'Instruction Amount must be supplied');
ins_err(235, 'TP ID must be supplied');
ins_err(236, 'Customer Surname must be supplied');
ins_err(237, 'Authority Deduction Type not found in domain RDS_DED_TYPE');
ins_err(238, 'Benefit Group Code not found in domain RDS_BEN_GRP');
ins_err(239, 'Invalid Deduction Action Type Code');
ins_err(240, 'Override Indicator must be Y or N');
ins_err(241, 'Override Date must be supplied when override ind is Y');
ins_err(242, 'Override By must be supplied when override ind is Y');
ins_err(243, 'Subsidy Grace Period not found in domain SUBGPRSN');
ins_err(244, 'Letter Cancellation Reason not found in domain SUB_LET_CANCEL_RSN');
ins_err(245, 'Advise Case Person does not exist on the PARTIES table');
ins_err(246, 'Interested Party Shortname does not exist on the INTERESTED PARTIES table');
ins_err(247, 'Payee Type is not valid');
ins_err(248, 'Status Code must be supplied');
ins_err(249, 'Status Code does not exist on status codes table');
ins_err(250, 'Payment Type Code must be supplied');
ins_err(251, 'Payment Type Code does not exist on prevention payment types table');
ins_err(252, 'Payment Amount must be supplied');
ins_err(253, 'Payment Date must be supplied');
ins_err(254, 'Status Date must be supplied');
ins_err(255, 'Homeless Prevention Fund code must be supplied');
ins_err(256, 'Homeless Prevention Fund code not found on domain HLESSPREVFUN');
ins_err(257, 'Prevention Payment method must be supplied');
ins_err(258, 'Prevention Payment method not found on domain HLESSPREVPAYMTHD');
ins_err(259, 'Cancellation reason code not found on domain PREV_PAY_CANCEL_RSN');
ins_err(260, 'Address Reference Number must be supplied');
ins_err(261, 'Address Reference Number does not exist on Addresses table');
ins_err(262, 'Address End Reason Code not found in domain REG_ADDR_END_REASON');
ins_err(263, 'Registered Address Code must be supplied');
ins_err(264, 'Registered Address Code does not exist on registered addresses');
ins_err(265, 'Combination of inco_code and hrv_vety_code does not exist on the INCOME CODE VERIFICATIONS table');
ins_err(266, 'Registered address letting does not exist on registered addresses');
ins_err(267, 'Either the Advice Case Reference OR Advice Case Housing Option Reference should be supplied');
ins_err(268, 'Advice Case Reason should not be supplied with the Advice Case Housing Option Reference');
--
-- ADVICE_CASE_PEOPLE addition
--
ins_err(269, 'Client Indicator and Joint Client Indicator cannot both be Y');
--
-- ADVICE_CASE_HOUSING_OPTIONS addition
--
ins_err(270, 'Invalid Housing Option Code/Admin Unit code combination supplied');
--
-- ADVICE_REASON_CASEWORK_EVENTS addition
--
ins_err(271, 'Event DateTime cannot be greater than Advice Case Approach Date');
ins_err(272, 'Either the duration format is invalid or it is > 23:59');
--
-- Subsidy addition
--
ins_err(273, 'Subsidy Application already exists for Subsidy Application legacy reference supplied');
ins_err(274, 'Subsidy Review Legacy Reference must be supplied');
ins_err(275, 'Subsidy Review already exists for Subsidy Review legacy reference supplied');
ins_err(276, 'Subsidy Income Item record already exists for Subsidy Review legacy reference/Party/Income Type combination supplied');
ins_err(277, 'Subsidy Letter Sequence Number must be supplied');
ins_err(278, 'Subsidy Letter Type must be supplied');
ins_err(279, 'Subsidy Letter Type Code not found on domain SUB_LET_TYPE');
ins_err(280, 'Letter Printed Date is earlier than Letter Created Date');
ins_err(281, 'Subsidy Letter record already exists for Subsidy Review legacy reference/Sequence Number supplied');
ins_err(282, 'Letter Cancellation Reason must be supplied if either the status is CAN or cancelled date/by fields are supplied');
ins_err(283, 'Subsidy Grace Period Sequence must be supplied');
ins_err(284, 'Subsidy Grace Period Reason must be supplied');
ins_err(285, 'Subsidy Grace Period Start Date must be supplied');
ins_err(286, 'Subsidy Grace Period Start Date must not be earlier than the Subsidy Application Start Date');
ins_err(287, 'Invalid Household Threshold Income Code supplied');
ins_err(288, 'Subsidy Grace Period record already exists for Subsidy Application legacy reference/Sequence Number supplied');
--
-- Authorised Deductions
--
ins_err(289, 'Authorised Deduction Start Date must be supplied');
ins_err(290, 'Authorised Deduction Type Code must be supplied');
ins_err(300, 'Authorised Deduction Benefit Group Code must be supplied');
ins_err(301, 'Authorised Deduction Start Date must not be earlier than the Authority Start Date');
ins_err(302, 'Authorised Deduction End Date must not be later than the Authority End Date');
--
-- Account Deductions
--
ins_err(303, 'Account Deduction Payment Reference must be supplied');
ins_err(304, 'Invalid Account Deduction Payment Reference supplied');
ins_err(305, 'Account Deduction Type Code must be supplied');
ins_err(306, 'Account Deduction Start Date must be supplied');
ins_err(307, 'Account Deduction Benefit Group Code must be supplied');
ins_err(308, 'Account Deduction Benefit Group not found in domain RDS_BEN_GRP');
ins_err(309, 'Fixed Amount Indicator must be supplied');
ins_err(310, 'Minor Arrears Adjustment Indicator must be supplied');
ins_err(311, 'Revenue Account does not exist for tenancy');
ins_err(312, 'Invalid request action code');
ins_err(313, 'Invalid sending agency code');
ins_err(314, 'Invalid receiving agency code');
ins_err(315, 'Household Person does not exist on PARTIES table');
ins_err(316, 'Household Person does not exist on HOUSEHOLDS table');
--
--
-- Subsidy Reviews
--
ins_err(317, 'Group Subsidy Review Record does not exists for reference supplied');
--
--
-- Group Subsidy Reviews
--
ins_err(318, 'Group Subsidy Review Record already exists for reference supplied');
ins_err(319, 'Group Subsidy Review Issue ICS Requests Indicator must be Y or N');
ins_err(320, 'Group Subsidy Review Issue Income Details Certificate Indicator must be Y or N');
--
--
-- RDS
--
ins_err(321, 'Corresponding PAY transaction does not exist for allocated amount');
--
-- IPP Addresses
--
ins_err(322, 'Address Object Type must be supplied');
--
ins_err(323, 'Revenue Account record does not exist for Payment Reference supplied');
--
ins_err(324, 'Account Rent Limit Reference Value must be supplied');
--
ins_err(325, 'RDS Instruction Effective Date must be supplied');
--
ins_err(326, 'Advice Case Person must be a HOU Person or BOTH');
--
ins_err(327, 'Income Detail Request Reference must be supplied');
ins_err(328, 'Income Detail Request Reference already exists on INCOME DETAIL REQUESTS table');
--
ins_err(329, 'Interested Party Type Code must be supplied if interested party shortname is supplied');
ins_err(330, 'Invalid Interested Party Type Code supplied');
--
ins_err(331, 'Party does not exists on PARTIES table for party reference supplied');
ins_err(332, 'Organisation does not exists on PARTIES table for reference supplied');
--
ins_err(333, 'Income Detail Request Reference does not exists on INCOME DETAIL REQUESTS table');
--
ins_err(334, 'Income Type Indicator can only be TINC or DINC');
ins_err(335, 'Income Type Code not found in domain ICSINCOMETYPE');
ins_err(336, 'Income Frequency can only be supplied for DINC - Detail ICS Benefit Payments');
ins_err(337, 'Frequency Code does not exist on INCOME_FREQUENCIES table');
--
ins_err(338, 'ICS Error Code not found in domain ICSERRORCODE');
--
-- RAPS
--
ins_err(339, 'Batch ID Already exist in LWR_BATCHES');
ins_err(340, 'Batch Type not found in domain LWR_BATCH_TYPE');
ins_err(341, 'Invalid Batch Status supplied. Must be one of (NEW, LOD, LVF, LVA, CLO, APP, CAN, PAD)');
ins_err(342, 'Load From File Indicator must be Y or N');
ins_err(343, 'Interested Party for shortname/ipt_code does not exist on INTERESTED_PARTIES table');
ins_err(344, 'Record does not exist in LWR_ANNUAL_RATES_SCHEDULES for lars_flrs_code, lars_year combination');
ins_err(345, 'Rates Schedule Code/Year, Instalment Number, Annual Batch Instalment Ref must be supplied for ANN/INS Batch Types');
ins_err(346, 'Annual Batch Instalment Reference does not exist on LWR_BATCHES table');
ins_err(347, 'Cancelled Date must be supplied if Cancelled By is supplied');
ins_err(348, 'Cancelled By must be supplied if Cancelled Date is supplied');
ins_err(349, 'Cancelled Reason must be supplied if Cancelled By/Date is supplied');
ins_err(350, 'Cancelled By/Date must be supplied if Cancelled Reason is supplied');
ins_err(351, 'Cancelled Reason not found in domain LWR_BATCH_CAN_RSN');
ins_err(352, 'Batch ID does not exist in LWR_BATCHES');
ins_err(353, 'Invalid Assessment Type supplied. Must be one of (SURI, WAAS, ANRH)');
ins_err(354, 'Invalid Assessment Status supplied. Must be one of (NEW, PAY, DNP, INV)');
ins_err(355, 'Assessment Rate Period End date cannot be before Assessment Rate Period Start Date');
ins_err(356, 'Assessment Override Reason not found in domain LWR_ASS_ORRIDE_RSN');
ins_err(357, 'One or More mandatory values missing for Annual Assessment Batch Type ANRH');
ins_err(358, 'One or More mandatory values missing for Instalment Assessment Batch Type SURI');
ins_err(359, 'One or More mandatory values missing for Water Assessment Batch Type WAAS');
ins_err(360, 'Invalid Credit/Debit Indicator supplied. Must be one of (CR, DR)');
ins_err(361, 'Invalid Assessment Detail Class Code supplied. Must be one of (ANRD, WFRD, IOAC)');
ins_err(362, 'Category End date cannot be before Category Start Date');
ins_err(363, 'Rate Category Code does not exist on RATE_CATEGORIES table');
ins_err(364, 'Record does not exist on LWR_ASSESSMENTS table for Batch Id, Current Assessment Ref, Rate Period Start/End Date');
ins_err(365, 'Rate Category Start/End Date does not fall bewteen on LWR_ASSESSMENTS Rate Period Start/End Date');
ins_err(366, 'One or More mandatory values missing for Annual Assessment Detail Batch ANRD');
ins_err(367, 'One or More mandatory values missing for Instalment Assessment Batch Type IOAC');
ins_err(368, 'One or More mandatory values missing for Water Assessment Batch Type WFRD');
ins_err(369, 'Validation Error Code not found in domain LWR_VAL_ERR');
ins_err(370, 'Record already exists in LWR_ASSESSMENT_VAL_ERRORS for Assessment Ref, Validation Error Code combination');
ins_err(371, 'Water Usage Details Period End date cannot be before Period Start Date');
ins_err(372, 'Record already exists in LWR_APPORTIONED_ASSESSMENTS for lwra_refno/pro_refno combination');
ins_err(373, 'Invalid Water Usage Details Status supplied. Must be one of (APR, REJ, RAP)');
--
ins_err(374, 'Street Index Code supplied does not exist in ADDRESS_ELEMENTS table');
--
ins_err(375, 'Record does not exists in SUBSIDY_INCOME_TYPE_MAPPINGS table for Subsidy Income Type/Policy Category/Policy Sequence');
ins_err(376, 'Record does not exists in SUBSIDY_POLICIES table for Subsidy Policy Category/Policy Sequence');
--
-- ICS_BENEFIT_PAYMENTS
--
ins_err(377, 'A record for the Benefit Payment Reference already exists in ICS_BENEFIT_PAYMENTS');
ins_err(378, 'Benefit Payment Benefit Code not found in domain ICSBENEFIT');
ins_err(379, 'Benefit Payment Status not found in domain ICSPAYSTATUS');
ins_err(380, 'Benefit Payment Type not found in domain ICSPAYTYPE');
ins_err(381, 'Benefit Type Indicator can only be TIBP or DIBP');
ins_err(382, 'Benefit Payment Cancelled Indicator must be Y or N');
ins_err(383, 'Income Frequency can only be supplied for DIBP Benefit Payment Type');
ins_err(384, 'Benefit Payment Max Rate Indicator must be Y or N');
ins_err(385, 'Payment Date can only be supplied for DIBP Benefit Type Indicator');
ins_err(386, 'Payment Date must be supplied for DIBP Benefit Type Indicator');
ins_err(387, 'Number of Paid Days can only be supplied for DIBP ICS Benefit Payment Type');
ins_err(388, 'ICS Benefit Payments record exists for Detail Request Ref./Benefit Code/Payment Status/Payment Type combination');
--
-- ICS_PAYMENT_COMPONENTS
--
ins_err(389, 'A record for the Payment Component Reference already exists in ICS_PAYMENT_COMPONENTS');
ins_err(390, 'Benefit Payment record does not exist for the Benefit Payment Reference supplied');
ins_err(391, 'Component Type Code not found in domain ICSCOMPONENTTYPE');
ins_err(392, 'Component Payment Code must be LEG or ACT');
ins_err(393, 'Payment Component record exists for Component Type Code/Benefit Payment Ref./Component Payment Code combination');
--
-- ICS_DEDUCTIONS
--
ins_err(394, 'A record for the Deduction Reference already exists in ICS_DEDUCTIONS');
ins_err(395, 'Deduction Type Code not found in domain ICSDEDUCTTYPE');
ins_err(396, 'Deduction Type Indicator can only be TICD or DICD');
ins_err(397, 'Deduction Date can only be supplied for DICD ICS Deduction Type Indicator');
ins_err(398, 'Deduction Date must be supplied for DICD Deduction Type Indicator');
ins_err(399, 'Corresponding ICS Benefit Payment record not found for same Benefit Code');
--
ins_err(400, 'ICS Benefit Payments record does not exist for Detail Req Ref./Benefit Code/Payment Status/Payment Type combination');
ins_err(401, 'Deduction Benefit Code not found in domain ICSBENEFIT');
--
ins_err(402, 'ICS Request Statuses Current Indicator must be Y or N');
--
-- WATER_CHARGE_CALC_AUDITS/APPORTIONED_ASSESS_DETAILS
--
ins_err(403, 'Bill Period End date cannot be before Bill Period Start Date');
ins_err(404, 'Invalid Water Charge Calc Audits Status supplied. Must be one of (RAP, CHG, REJ, APR)');
ins_err(405, 'Invalid Water Charge Calc Audits Type supplied. Must be one of (RECON, RECALC, OVERRIDE)');
ins_err(406, 'Unable to derive the WATER_USAGE_DETAILS refno for details supplied');
ins_err(407, 'Unable to derive the LWR_RATE_ASSESSMENT_DETAILS refno for details supplied');
--
-- VALUATIONS 
--
ins_err(408, 'Valuations Round Code already exists in HVA_VALUATION_ROUNDS table');
ins_err(409, 'Invalid Valuation Round Type supplied. Must be one of (HBI, HRV, HFI, HBV, HFV, HRE)');
ins_err(410, 'Asset Category Code not found in domain HVA_ASST_CATEGORIES');
ins_err(411, 'Invalid Status supplied for Round Types (HBI,HBV). Must be one of(RAI,FST,FLD,IDA,KAP,ETP,ERA,EAP,BTA,VAP,VET,CAN)');
ins_err(412, 'Invalid Status supplied for Round Types (HFV,HFI). Must be one of(RAI,FST,FLD,IDA,VAP,VET,CAN)');
ins_err(413, 'Invalid Status supplied for Round Types (HRE,HRV). Must be one of(RAI,FLD,COM,VAP,VET,CAN)');
ins_err(414, 'A Value for Amenity Spilt must not be supplied for Round Type (HRE,HRV)');
ins_err(415, 'Amenity Spilt must be in the range 0 to 100');
ins_err(416, 'Valuation Date must be supplied for Round Types (HBV,HFV)');
ins_err(417, 'Valuation Instructions must only be supplied for Round Types (HBV,HFV)');
ins_err(418, 'Cancellation Comments must only be supplied for status (CAN)');
ins_err(419, 'Cancellation Reason must be supplied for status (CAN)');
ins_err(420, 'Cancellation Reason Code not found in domain HVA_VAL_CNCL_REASONS');
ins_err(421, 'Valuation Review Reason must be supplied for Round Types (HBV,HFV)');
ins_err(422, 'Valuation Review Reason Code not found in domain HVA_VAL_RVW_REASONS');
ins_err(423, 'Valuation Basis Code not found in domain HVA_VAL_BASIS');
ins_err(424, 'Parent Valuations Round Code does not exists in HVA_VALUATION_ROUNDS table');
ins_err(425, 'Valuations Round Code does not exists in HVA_VALUATION_ROUNDS/DL_HEM_HVA_VAL_ROUNDS table');
ins_err(426, 'Record already exists in HVA_ROUND_STATUS_HISTORIES for Valuation Round Code/Created Date combination');
ins_err(427, 'Valuation Review Reason must only be supplied for status (CAN)');
--
ins_err(428, 'Adjustment Reason Code must be supplied if status of Valuation Property is (RAP)');
ins_err(429, 'Adjustment Reason Code not found in domain HVA_VAL_ADJ_REASONS');
ins_err(430, 'Adjustment Reason Code must only be supplied if status of Valuation Property is (RAP)');
ins_err(431, 'Invalid Valuation Type supplied. Must be one of (AV, BP, BV)');
ins_err(432, 'Unable to derive Benchmark Properties Refno for property ref/start date combination');
ins_err(433, 'Benchmark Property Start Date must only be supplied for Valuation Type (BP)');
ins_err(434, 'Columns marked (*extrap*) 10-18 and 43 must only be supplied for (BP,BV) type and status in (ETP,ERA,EAP,BTA,VAP,VET).');
ins_err(435, 'Values for Block Title Adjustments columns 19-21 must only be supplied for (BP,BV) type and rnd status in (BTA,VAP,VET).');
ins_err(436, 'True Market Rent/Capital Value must not be supplied if the Property Asset Category is LAND');
ins_err(437, 'Value for Replacement Cost must only be supplied for (AV,BV) Valuation Type.');
ins_err(438, 'Values for columns 26-29 must only be supplied for (AV) Type.');
ins_err(439, 'Adjustment columns 30-36 must only be supplied if status of Valuation Property is (RAP)');
ins_err(440, 'At least one of True Market Rent/Capital Value/Land Value must be supplied if any of the Adj columns 30-36 are supplied.');
ins_err(441, 'Approved By/Date must only be supplied for Property Valuation Status (APR).');
ins_err(442, 'Valuation Date must be earlier than the Target Date.');
--
-- INCOME_DETAIL_DEDUCTIONS 
--
ins_err(443, 'Unable to retrieve Income Detail Reference from DL Table for IHS Unique Income Id');
ins_err(444, 'Income Detail record does not exist for INDT_REFNO/INDT_INCO_CODE combination');
ins_err(445, 'Income Deduction Code not found in domain DEDUCTCODE');
ins_err(446, 'Income Code Deduction record does not exist for Income Code/Deduction Code combination');
ins_err(447, 'Income Detail Deduction already exists Income Detail Reference/Income Code/Deduction Code combination');
--
ins_err(448, 'Record already exists in HVA_ROUND_PROPERTY_VAL_HISTS for Valuation Round Refno/Prop Refno/Created Date combination');
ins_err(449, 'At least one of True Market Rent/Capital Value/Land Value must be supplied if any of the Adj columns 32-38 are supplied.');
ins_err(450, 'Adjustment columns 32-38 must only be supplied if status of Valuation Property is (RAP)');
ins_err(451, 'Values for columns 28-31 must only be supplied for (AV) Type.');
ins_err(452, 'Columns marked (*extrap*) 12-20 must only be supplied for (BP,BV) type and status in (ETP,ERA,EAP,BTA,VAP,VET).');
ins_err(453, 'Record does not exist in HVA_ROUND_PROPERTY_VALUATIONS for Valuation Round Refno/Prop Refno combination');
ins_err(454, 'Record already exists in HVA_ROUND_PROPERTY_VALUATIONS for Valuation Round Refno/Prop Refno combination');
--
-- Subsidy Debt Assessment
--
ins_err(455, 'Subsidy Debt Assessment record already exists for Legacy Reference Supplied');
ins_err(456, 'Invalid Status Code');
ins_err(457, 'Total Debt must be supplied if status code is one of DIS, CAD, CAC or COM');
ins_err(458, 'No matching Revenue Account found for Payment Reference supplied');
ins_err(459, 'Revenue Account Type does not match that of parameter SUB_DEBT_ACCT_TYPE');
--
-- Subsidy Applications
--
ins_err(460, 'One of Tenancy or Application Reference must be supplied');
ins_err(461, 'Only one of Tenancy or Application Reference must be supplied, not both');
ins_err(462, 'Subsidy Application Start Date must not be before Application Received Date');
ins_err(463, 'Next Scheduled Review Date must be in the future');
--
-- Subsidy Reviews errors 493, 494, 495 missed on this version
-- so added at the bottom
--
ins_err(464, 'For Private Subsidy Reviews the Subsidy Application Ref must be null');
ins_err(465, 'Subsidy Stage Code does not exist in domain SUBSIDY STAGE');
ins_err(466, 'Invalid combination of Subsidy Stage and Subsidy Reason codes');
ins_err(467, 'Invalid Household Type code');
--
-- Income Headers
--
ins_err(468, 'Income Header Record already exists for Income Header Reference supplied');
ins_err(469, 'Income Header Status code must be one of RAI, PEN, TBV, VER, CAN');
ins_err(470, 'Income Detail Request Reference does not exists on INCOME DETAIL REQUESTS table');
ins_err(471, 'Overide Income Header Reference must be supplied if Header Override Reason is supplied');
ins_err(472, 'Override Income Header Record does not exist for Override Income Header Reference supplied');
ins_err(473, 'Income Header Record does not exist for Income Header Reference supplied');
ins_err(474, 'Subsidy Review Record does not exist in SUBSIDY REVIEWS table for Subsidy Review Reference supplied');
ins_err(475, 'Subsidy Application Reference does not exist in SUBSIDY APPLICATIONS Table for Subsidy Application reference supplied');
ins_err(476, 'Income Header Usage Record already exists for combination of data supplied');
ins_err(477, 'Boarder Party Reference does not exist on Parties Table');
ins_err(478, 'Income Code = SYS DEEMED_INCOME_CODE param. Inc Detail may only be loaded where the assoc Inc Header has a status of VER');
ins_err(479, 'Multiples Allowed set to N. Only one instance of Income Det, for that Income Code, must be loaded against an Inc Header');
ins_err(480, 'Wages Ind set to Y. Employment Start Date and Employer must be supplied');
ins_err(481, 'Wages Ind set to N. Employment Start Date and Employer must not be supplied');
ins_err(482, 'Calc Avg Income set to M or O. Inc Start/Inc End/Gross Amt/Allowance Amt/No of Days without Pay may be supplied');
ins_err(483, 'Calc Avg Income not set to M or O. Inc Start/Inc End/Gross Amt/Allowance Amt/No of Days without Pay may not be supplied');
ins_err(484, 'If Alt Amt is supplied then Wkly Amount should be set to Alternative Amount');
ins_err(485, 'If Alt Amt is supplied but a Calc Wage has been supplied and Calc Wage Ind is set to Y then Wkly Amt should = Calc Wage');
ins_err(486, 'Asset Code Asset Value Required Ind set to Y. Asset Value and Asset Income must be supplied');
ins_err(487, 'Asset Code Asset Value Required Ind set to N. Asset Value and Asset Income must not be supplied');
ins_err(488, 'Asset Value or Asset Income must be supplied');
ins_err(489, 'If Asset Value is supplied then Percentage Owned must be supplied');
ins_err(490, 'Income Detail Reference does not exist on INCOME DETAILS for reference supplied');
--
-- Non Access Events
--
ins_err(491, 'Works Order Legacy reference must be supplied');
ins_err(492, 'No matching Works Order found for Works Order Legacy reference');
ins_err(493, 'Appointment DateTime cannot be greater than today');
ins_err(494, 'Non Access Reason Code must be supplied');
ins_err(495, 'Non Access Reason Code supplied does not match any Non Access Reason Code on Northgate Housing');
ins_err(496, 'Notify occupant indicator mst be Y or N');
ins_err(497, 'Non Access Notified Date cannot be greater than today');
ins_err(498, 'Appointment DateTime must be supplied');
ins_err(499, 'Non Access Notified Date must be supplied');
--
-- Defect Types
--
ins_err(500, 'Defect Type Code must be supplied');
ins_err(501, 'Defect Type Code already exists on defect_types table');
ins_err(502, 'Right to Repair Indicator must be Y or N');
ins_err(503, 'Current Indicator must be Y or N');
ins_err(504, 'Rechargable Indicator must be Y or N');
ins_err(505, 'Priority Code must be supplied');
ins_err(506, 'Priority Code supplied not found on Northgate Housing');
ins_err(507, 'Repeat Period Unit must be D, W or M');
ins_err(508, 'Defect Type Code must exist on Northgate Housing');
ins_err(509, 'Warranty Period Unit must be D, W or M');
ins_err(510, 'Schedule of Rates Code must be supplied');
ins_err(511, 'Repeat Warning Indicator must be Y or N');
ins_err(512, 'Defect Type Description must be supplied');
ins_err(513, 'Repeat Period Number must be numeric');
ins_err(514, 'Warranty Period Number must be numeric');
ins_err(515, 'Defect Type Code must exist on Northgate Housing');
ins_err(516, 'Default Order Sequence must be numeric');
ins_err(517, 'Quantity must be numeric');
--
-- lbg gsc letters
--
ins_err(518, 'Letter Name must be supplied');
ins_err(519, 'Rundate must be supplied');
ins_err(520, 'Rundate cannot be greater than today');
ins_err(521, 'User must exist on Northgate Housing');
ins_err(522, 'Letter Name must exist as a module on Northgate Housing');
--
-- Tenders
--
ins_err(523, 'Contract Reference must be supplied');
ins_err(524, 'Record already exists for supplied Contract Reference and Sequence on tenders table');
ins_err(525, 'Description must be supplied');
ins_err(526, 'Current Indicator must be Y or N');
ins_err(527, 'Cancelled Indicator must be Y or N');
ins_err(528, 'User must exist on Northgate Housing');
ins_err(529, 'Date cannot be greater than today');
ins_err(530, 'Interested Party must exist on Northgate Housing');
ins_err(531, 'Purchase Method Code not found in domain PURCHMTH');
ins_err(532, 'Procurement Region Code not found in domain PROCREGN');
--
-- Tender_contractor_sites
--
ins_err(533, 'Contract Reference, Sequence and Cos Code must be supplied');
ins_err(534, 'Record already exists for supplied Contract Reference, Sequence and Cos Code on tenders table');
ins_err(535, 'Parent tender record must exists for supplied Contract Reference, Sequence');
ins_err(536, 'Parent contractor_site record must exists for supplied Cos_code');
ins_err(537, 'Sco_code must be one of AWD, CAN, EOI, ITT, LIS, RCD, REG, REJ, RFD, RFL or WDR');
ins_err(538, 'Date cannot be greater than today');
ins_err(539, 'User must exist on Northgate Housing');
--
-- re-indexed errors for Person Attributes
--
ins_err(540, 'Invalid Class Code Supplied. Must be on (TEXT, YESNO, CODED, NUMERIC, DATE)');
ins_err(541, 'Person Attribute Code does not exists on PEOPLE_ATTRIBUTES table for code supplied');
ins_err(542, 'Record does not exists on PEOPLE_ATTRIB_ALLOWED_VALS table for Person Allowed Vals/Person Attribute Code combination');
ins_err(543, 'The Numeric value must be less than or equal to the Person Attribute Max Numeric Value');
ins_err(544, 'The Numeric value must be greater than or equal to the Person Attribute Min Numeric Value');
ins_err(545, 'The Date value must be less than or equal to the Person Attribute Max Date');
ins_err(546, 'The Date value must be greater than or equal to the Person Attribute Min Date');
ins_err(547, 'Yes/No Value must be Y or N');
ins_err(548, 'Invalid Reference Type supplied. Must be one of (PAR or PRF)');
ins_err(549, 'Name Change Reason Code not found in domain FNREAS');
--
-- Additional ICS errors
--
ins_err(550, 'ICS Request Statuses cannot be loaded for Income Detail Request Record at RAI status');
ins_err(551, 'ICS Request Status Error Code not found in domain ICSERRORCODE');
ins_err(552, 'ICS Request Status Record already exists for Income Detail Req Ref/ICS Request Status Code/Created Date combination');
ins_err(553, 'ICS Request Statuses Current Indicator must be Supplied');
ins_err(554, 'ICS Request Statuses Code must be Supplied');
ins_err(555, 'ICS Income Type Indicator must be Supplied');
ins_err(556, 'ICS Income Type must be Supplied');
ins_err(557, 'ICS Income Amount must be Supplied');
ins_err(558, 'ICS Income Date must be Supplied');
--
ins_err(559, 'Benefit Code must be Supplied');
ins_err(560, 'Benefit Payment Status Code must be Supplied');
ins_err(561, 'Benefit Payment Type Code must be Supplied');
ins_err(562, 'Benefit Payment Type Indicator must be Supplied');
ins_err(563, 'Benefit Payment Actual Amount must be Supplied');
ins_err(564, 'Benefit Payment Cancelled Indicator must be Supplied');
ins_err(565, 'Income Frequency must be supplied for DIBP Benefit Payment Type');
ins_err(566, 'Payment Component Amount must be Supplied');
ins_err(567, 'Payment Component Type Code must be Supplied');
ins_err(568, 'Deduction Type Indicator must be Supplied');
ins_err(569, 'Deduction Benefit Code must be Supplied');
ins_err(570, 'Deduction Amount must be Supplied');
ins_err(571, 'Deduction Type Code must be Supplied');
--
-- Service Birmingham HB Details and Under Occupation
--
ins_err(572, 'HB Claim No is Required');
ins_err(573, 'HB Claim No supplied is not valid');
ins_err(574, 'Reduction Perc. is Required');
ins_err(575, 'Reduction Perc. supplied is not valid');
--
--
-- Glasgow CC BESPOKE Cash Sales Historic Tenancy Loads
--
ins_err(576, 'Problem encountered creating HOUSEHOLD record');
ins_err(577, 'Problem encountered creating HOUSEHOLD_PERSONS record');
ins_err(578, 'Problem encountered creating TENANCY_INSTANCES record');
ins_err(579, 'Application Reference must be supplied');
--
-- Additional ICS errors
--
ins_err(580, 'Party Reference Type must be Supplied');
ins_err(581, 'Invalid Party Reference Type Supplied. Must be one of (PAR or PRF)');
ins_err(582, 'Person Reference must be Supplied');
ins_err(583, 'Person does not exists on PARTIES table for person reference supplied');
ins_err(584, 'Granted By Person Reference must be Supplied');
ins_err(585, 'Consent Type Code must be Supplied');
ins_err(586, 'Consent Source Code must be Supplied');
ins_err(587, 'Income Detail Request Current Status Code must be Supplied');
ins_err(588, 'Partner Indicator must only be supplied if Request Status is COM');
ins_err(589, 'Number of Children must only be supplied if Request Status is COM');

ins_err(590, 'Request Date must only be supplied if Request Type is PIT');

ins_err(591, 'Financial Period Code must only be supplied if Request Type is FYR/QTR');
ins_err(592, 'Income Detail Request Type must be Supplied');
ins_err(593, 'Income Detail Request should be DREQ if Request Typs is PIT or CUR');
ins_err(594, 'Income Detail Request should be PREQ if Request Typs is QTR or FYR');
--
--
-- Warranties Data load
--
ins_err(600, 'Warranties Reference must be supplied');
ins_err(601, 'Schedule of Rates type code must be supplied');
ins_err(602, 'The Warranties Reference supplied does not exist in the WARRANTIES table');
ins_err(603, 'The Schedule of Rates type code supplied does not exist in the SCHEDULE_OF_RATES table');
ins_err(604, 'The Schedule of Rates type code already exists against the Warranties reference supplied');
ins_err(605, 'If the warranty reference is supplied the SOR code must also be supplied');
ins_err(606, 'If the SOR is supplied the warranty reference must also be supplied');
ins_err(607, 'The Standard Repair Type already exists against the Warranties reference supplied');
ins_err(608, 'Standard Repair Types code must be supplied');
ins_err(609, 'The Standard Repair Types code supplied does not exist in the DEFECT_TYPES table');
ins_err(610, 'If the warranty reference is supplied the Standrad Repair Type code must also be supplied');
ins_err(611, 'If the Standard Repair Type is supplied the warranty reference must also be supplied');
ins_err(612, 'Property Reference OR Admin Unit Code must be supplied');
ins_err(613, 'Property Reference OR Admin Unit Type Indicator must be supplied');
ins_err(614, 'Admin unit does not exist in ADMIN_UNITS Table');
ins_err(615, 'Property reference does not exist on PROPERTIES');
ins_err(616, 'Invalid Record Type : P - Property Ref, A - Admin Unit');
ins_err(617, 'The Warranty Address already exists against the Warranties reference supplied');
ins_err(618, 'The Warranty Address Start/End Date is Before the Warranty Start Date');
ins_err(619, 'The Warranty Address Start/End Date is After the Warranty Expiry Date');
ins_err(620, 'The HPM Sys Parameter is set to ERROR so the address supplied must be linked to the contract reference');
ins_err(621, 'Warranty Description must be supplied');
ins_err(622, 'Warranty Type Code must be supplied');
ins_err(623, 'Warranty Type Code not found in domain WARNTYTYPE');
ins_err(624, 'Warranty Status Code must be supplied');
ins_err(625, 'Warranty Status code must be one of RAI, AUT, CAN');
ins_err(626, 'Warranty Full or Partial Indicator must be supplied');
ins_err(627, 'Warranty Full or Partial Indicator must be one of F or P');
ins_err(628, 'Invalid Warranty Contract Reference supplied');
ins_err(629, 'Invalid Warranty Contractor Site Code supplied');
ins_err(630, 'Warranty Contractor Site not assigned to a Contract Management Area');
ins_err(631, 'Warranty Effective Date is in the Future');
ins_err(632, 'Warranty Expiry Date is Before the Warranty Effective Date');
ins_err(633, 'The HPM Sys Parameter WACOSADD is set to ERROR so Contractor Site must be linked to the Contract Reference supplied');
ins_err(634, 'Warranty Status Date must be supplied');
--
-- Household_persons 
--
ins_err(635, 'Invalid tenancy ref indicator - must be Y or N');
ins_err(636, 'Invalid create party indicator - must by Y or N');
ins_err(637, 'Invalid merge party indicator - must by Y or N');
ins_err(638, 'Invalid at risk indicator - must be Y of N');
ins_err(639, 'Supplied per_alt_ref does not exist on PARTIES table and no permission to create');
ins_err(640, 'Supplied per_alt_ref relates to a party with a different surname, fornames or initials from those supplied');
ins_err(641, 'Overlapping household person record already exists for this party/tenancy');
--
-- Additional Warranties Data loads originally 635 to 637
--
ins_err(642, 'The Warranties Reference supplied already exists in the WARRANTIES table');
ins_err(643, 'Property reference does not exist in PROPERTIES table with a pro_type of HOU or BOTH');
ins_err(644, 'Admin unit does not exist in ADMIN_UNITS ADMIN_UNIT_TYPES tables with a Admin Unit Type of HOU');
--
-- Budget_Admin_Unit_Security Part 1
--
ins_err(645, 'Budget Calander Year (lmab_bca_year) must be supplied');
ins_err(646, 'Invalid Budget Head (lmab_bhe_code) Code Supplied');
ins_err(647, 'Budget Reference not found for budget head code (lmab_bhe_code) and budget calendar year (lmab_bca_year)');
ins_err(648, 'Budget Head Code (lmab_bhe_code) must be supplied');
ins_err(649, 'Invalid Budget Calander Year (lmab_bca_year) supplied');
--
-- Other_Fields 
--
ins_err(650, 'Admin unit does not exist in ADMIN_UNITS ADMIN_UNIT_TYPES tables with a Admin Unit Type of HOU');
ins_err(651, 'Element Type / Other Field Combination is not valid');
ins_err(652, 'Record overlaps an existing record');
--
-- Party_Admin_Unit_Security Part 1
--
ins_err(653, 'The Supplied End Date is before the Supplied Start Date');
ins_err(654, 'The Record Type (loau_rec_type) in field 4 must be supplied');
ins_err(655, 'The Record Type supplied (loau_rec_type) in field 4 is NOT VALID');
ins_err(656, 'The Object Reference (loau_obj_ref) in field 5 must be supplied');
ins_err(657, 'The Party Alternative Reference supplied is not valid for the Record Type of PAR_ALT');
ins_err(658, 'The Party Type for the Party Alternative Reference supplied must be HOUP');
ins_err(659, 'The Organisation Full Name supplied is not valid for the Record Type of ORG_FULL');
ins_err(660, 'The Party Type for the Organisation Full Name supplied must be ORG');
ins_err(661, 'The Organisation Short Name supplied is not valid for the Record Type of ORG_SHORT');
ins_err(662, 'The Party Type for the Organisation Short Name supplied must be ORG');
ins_err(663, 'The Internal Party Reference supplied is not valid for the Record Type of PAR_REFNO');
ins_err(664, 'The Internal Party Reference supplied must be ORG OR HOUP');
ins_err(665, 'The OBJECT NAME (luoa_obj_name) in field 3 must be supplied');
ins_err(666, 'The OBJECT NAME supplied (luoa_obj_name) in field 3 is NOT VALID');
ins_err(667, 'The ACCESS SECURITY LEVEL (luoa_access_level) in field 4 must be supplied');
ins_err(668, 'The ACCESS SECURITY LEVEL supplied (luoa_access_level) in field 4 is NOT VALID');
ins_err(669, 'The USERNAME (luoa_usr_username) in field 1 must be supplied');
ins_err(670, 'The USERNAME (luoa_usr_username) in field 1 must exist on Northgate Housing');
ins_err(671, 'The OBJECT NAME supplied (luoa_obj_name) does not Exist in the OBJECTS Table');
--
-- Budget_Admin_Unit_Security Part 2
--
ins_err(672, 'A Record already exists for this Admin Unit and Budget combination');
--
-- Party_Admin_Unit_Security Part 2
--
ins_err(673, 'The Param must be set to Y for PARTIES(AU_SEC_FOR_PAR) ORGANISATIONS(AU_SEC_FOR_ORG) INTERESTED_PARTIES(AU_SEC_FOR_IPP)');
ins_err(674, 'The Admin Unit supplied is not current');
ins_err(675, 'The Interested Parties Short Name supplied is not valid for the Record Type of IPP_SHORT');
ins_err(676, 'Check the start and end dates as an overlapping earlier record already exists');
ins_err(677, 'Check the start and end dates as an overlapping future record already exists');
--
-- Arrears Arrangements (also in HDL and HD1)
--
ins_err(678, 'DD Extracted date must not be before the instalment start date');
--
-- HPM Budget GL CLassifications
-- 
ins_err(679, 'Budget Head Code is not valid');
ins_err(680, 'No Valid HPM Budget found for budget head and year');
ins_err(681, 'GL Code 1 must be supplied');
ins_err(682, 'GL Code 1 is not valid');
ins_err(683, 'GL Code 2 is not valid');
ins_err(684, 'GL Code 3 is not valid');
ins_err(685, 'GL Code 4 is not valid');
ins_err(686, 'GL Code 5 is not valid');
ins_err(687, 'GL Code 6 is not valid');
ins_err(688, 'GL Code 1 already exists for this budget');
ins_err(689, 'GL Code 2 already exists for this budget');
ins_err(690, 'GL Code 3 already exists for this budget');
ins_err(691, 'GL Code 4 already exists for this budget');
ins_err(692, 'GL Code 5 already exists for this budget');
ins_err(693, 'GL Code 6 already exists for this budget');
--
-- My Portal Customer_Activity
-- 
ins_err(694, 'The Stored Datetime (lcuac_stored_datetime) must be supplied');
ins_err(695, 'The Username (lcuac_usr_username) must be supplied');
ins_err(696, 'You must supply either the lcuac_par_refno (field 2) or lcuac_par_per_alt_ref (field 1)');
ins_err(697, 'The Party Reference supplied does not exist');
ins_err(698, 'The Party Reference supplied is not of type HOUP or BOTP');
ins_err(699, 'The Action Indicator (lcuac_action_ind) must be supplied');
ins_err(700, 'The Action Indicator (lcuac_action_ind) is not valid it must either D R U or C');
ins_err(701, 'The Customer Activity Type Code (lcuac_cuat_code) does not exist');
ins_err(702, 'The License CUST_MANAGE for this functionality must have a valid key');
ins_err(703, 'The lcuac_par_per_alt_ref supplied must not exceed 20 Characters');
ins_err(704, 'The lcuac_par_refno supplied must not exceed 8 Characters');
ins_err(705, 'The lcuac_usr_username supplied must not exceed 30 Characters');
ins_err(706, 'The lcuac_action_ind supplied must not exceed 1 Characters');
ins_err(707, 'The lcuac_cuat_code supplied must not exceed 10 Characters');
ins_err(708, 'The lcuac_object_identifier supplied must not exceed 255 Characters');
ins_err(709, 'The lcuac_object_reference supplied must not exceed 20 Characters');
ins_err(710, 'The lcuac_sco_code supplied must not exceed 10 Characters');
ins_err(711, 'The llcuac_comments supplied must not exceed 2000 Characters');
ins_err(712, 'The lcuac_action_text supplied must not exceed 255 Characters');
ins_err(713, 'The lcuac_osa_code supplied must not exceed 8 Characters');
ins_err(714, 'The lcuac_letter_type supplied must not exceed 4 Characters');
ins_err(715, 'The lcuac_object_description supplied must not exceed 2000 Characters');
ins_err(716, 'The Alt Party Ref (lcuac_par_per_alt_ref) supplied does not exist');
ins_err(717, 'The Alt Party Ref (lcuac_par_per_alt_ref) supplied is not of type HOUP or BOTP');
ins_err(718, 'The Alt Party Ref (lcuac_par_per_alt_ref) supplied is linked to more that one party');
--
-- More HPM GL Codes
--
ins_err(719, 'GL Code 7 is not valid');
ins_err(720, 'GL Code 8 is not valid');
ins_err(721, 'GL Code 9 is not valid');
ins_err(722, 'GL Code 10 is not valid');
ins_err(723, 'GL Code 7 already exists for this budget');
ins_err(724, 'GL Code 8 already exists for this budget');
ins_err(725, 'GL Code 9 already exists for this budget');
ins_err(726, 'GL Code 10 already exists for this budget');
--
-- Some additional Prevention Payment Error Codes
--
ins_err(730, 'The relevant type of Reference has not been supplied for this Payee Type');
ins_err(731, 'A reference has been supplied that is not relevant for this Payee Type');
--
-- More Other_Fields Codes for Organisations
--
ins_err(732, 'The Legacy Reference(lpva_legacy_ref) must be supplied');
ins_err(733, 'The Other Field Name(lpva_pdf_name) must be supplied');
ins_err(734, 'The Object Name(lpva_pdu_pob_table_name)must be supplied');
ins_err(735, 'The Object Name(lpva_pdu_pob_table_name)is invalid for the Other Field Name(lpva_pdf_name)supplied');
ins_err(736, 'A Parameter Definition Usages record of type OTHER FIELDS does not exist');
ins_err(737, 'The Legacy Reference(lpva_legacy_ref)supplied does not exist');
ins_err(738, 'Party supplied (lpva_legacy_ref)is not an Organisation');
ins_err(739, 'You must supply one of either a Date Number or Character (text) Value');
ins_err(740, 'A lpva_char_value must be provided when the datatype is YN TEXT or CODED');
ins_err(741, 'A lpva_number_value must be provided when the datatype is NUMERIC');
ins_err(742, 'A lpva_number_value must be provided when the datatype is DATE');
ins_err(743, 'The datatype and value do not match');
ins_err(744, 'A record already exits for the Other Field Name and Object Type');
--
-- More Other_Fields Codes for Revenue Accounts
--
ins_err(745, 'The Account Type(lpva_secondary_ref)must be supplied');
ins_err(746, 'The Account Type(lpva_secondary_ref)does not exist in the Other Fields Account Types table');
ins_err(747, 'The Pay Reference (lpva_legacy_ref) does not exist');
ins_err(748, 'The Revenue Account and Account Type combination does not exist');
ins_err(749, 'A record already exits for the Other Field Name and Revenue Account');
--
-- Interested_Party_Usages
--
ins_err(750, 'The dataload needs amending before Registered Addresses Records (REGA) can be used');
ins_err(751, 'Interested Party Shortname you wish to end must be supplied');
ins_err(752, 'More than one Interested Party of this type is not allowed');
ins_err(753, 'More than one Interested Party with this Shortname exists so this dataloader cannot be used');
ins_err(754, 'The Interested Party Supplied must be current (ipp_current_ind = Y)');
ins_err(755, 'The Interested Party to be Replaced was not found or has an end date or starts within 2 days of the new record');
ins_err(756, 'The Interested Party to be Replaced does not exist');
ins_err(757, 'The Interested Party to be Created already exists for all or part of the period');
ins_err(758, 'The Interested Party to be Created and Replaced cannot be the same');
--
-- Landlord_Payment_details
--
ins_err(759, 'The Payment Reason Code must be supplied');
ins_err(760, 'No Property Landlord exists for the payment date supplied');
ins_err(761, 'More than 1 Property Landlord exists for the payment date supplied');
ins_err(762, 'The Payment Reason Code supplied does not exist');
ins_err(763, 'The Payment Reason Code supplied is not current');
ins_err(764, 'The Payment Header for this period does not exist');
ins_err(765, 'Payment Date cannot be earlier than TODAY');
ins_err(766, 'Payment has already been extracted as pyh_passed_date exists');
--
-- Service Charge Rates Bespoke for Genesis
--
ins_err(767, 'The Service Charge Code(lscr_att_ele_code)does not exist');
ins_err(768, 'The Property Reference or Admin Unit Code(lscr_propref_auncode)must be supplied');
ins_err(769, 'The Service Charge Period Code(lscr_scb_scp_code)must be supplied');
ins_err(770, 'The Service Charge Code(lscr_att_ele_code)must be supplied');
ins_err(771, 'The Estimated Service Charge Rate(lscr_estimated_amount)must be supplied');
ins_err(772, 'Property reference does not exist on PROPERTIES');
ins_err(773, 'Admin unit does not exist on ADMIN_UNITS');
ins_err(774, 'This is a New Year file so the Start Date(lscr_scb_scp_start_date)must be supplied');
ins_err(775, 'The Service Charge Period(lscr_scb_scp_code)does not exist');
ins_err(776, 'The Service Charge Period must be Active if not a New Year File');
ins_err(777, 'The Service Charge Period must be Inactive if a New Year File');
ins_err(778, 'Service Charge Basis does not exist for the Start Date, Period and Element/Attribute');
ins_err(779, 'Record does not exists on Service Charge Rates');
ins_err(780, 'Service Charge Rate has an Actual Amount recorded against it so cannot be updated');
ins_err(781, 'Service Charge Rate has already been reconciled so cannot be updated');
--
-- Payment Arrangements - replacing previous SAS error codes
--
ins_err(782, 'Percentage Value required on this record type');
ins_err(783, 'Percentage or Fixed value supplied is inconsistent with the record type');
ins_err(784, 'Account Charge required if Requested Percentage supplied.');
ins_err(785, 'DDI Indictator Required.');
ins_err(786, 'Invalid Party/Organisation Indicator.');
--
-- LCC Project Ids
--
ins_err(787, 'Project ID must be supplied');
ins_err(788, 'Cost Centre must be supplied');
ins_err(789, 'Resource No must be supplied');
ins_err(790, 'Project ID must be unique');
--
-- LCC elig_criterias
--
ins_err(791, 'The apt_code must be supplied');
ins_err(792, 'The hty_code must be supplied');
ins_err(793, 'The apt_code supplied does not exist in alloc_prop_types table or is not current');
ins_err(794, 'The combination of apt_code and hty_code already exist in elig_criterias table for MAIN');
ins_err(795, 'The hty_code does not exist in hhold_types table or is is not current');
ins_err(796, 'The combination of hty_code and MAIN does not exist in hhold_type_elig_schemes table or is not current');
--
--Work_descriptions Multi Language errors
--
ins_err(797, 'Multi Language Work description Code already exists must be unique.');
ins_err(798, 'Multi Language Work description Description already exists must be unique.');
ins_err(799, 'Duplicate Work Description Code exists.');
ins_err(800, 'Duplicate Multi Language Work Description Code exists.');
ins_err(801, 'Multi Language Work Description Description already exists in dataload must be unique.');
ins_err(802, 'Work Description Code or Description cannot be NULL.');
ins_err(803, 'Multi Language Code and/or Description cannot be NULL.');

--
--MAD Dataload Other Field Values bit
--
ins_err(804, 'The lettings Benchmark manual created ind(lpva_further_ref2)is mandatory for ADVICE_CASE_HOUSING_OPTIONS (Y/N)');
ins_err(805, 'The lettings Benchmark auto generated ind(lpva_further_ref)is mandatory for ADVICE_CASE_HOUSING_OPTIONS (Y/N)');
ins_err(806, 'The lettings Benchmark auto generated and manual created indicators must have one set to Y and the Other set to N ');
--
--Work_descriptions Multi Language errors continued
--
ins_err(807, 'Multi Language SOR code already exists.');
--
--Admin Units Dataload Multi Language errors
--
ins_err(808, 'Multi Language Admin Unit Code(laun_code_mlang)already exists in admin_units table');
ins_err(809, 'Batch contains duplicate Multi Language Admin Unit Codes(laun_code_mlang)of this type');
ins_err(810, 'If Multi Language Admin Unit Code is supplied then Name(laun_name_mlang) must also be supplied');
ins_err(811, 'If Multi Language Admin Unit Name is supplied then Code(laun_code_mlang) must also be supplied');
ins_err(812, 'If Multi Language Admin Unit Code contains Invalid Characters only A-Z, 0-9 and hyphens allowed');
ins_err(813, 'Multi Language Bank Name / Branch combination already exits');
--
--
-- Additional Subsidy Reviews were 493 494 and 495 in bespoke version of hd2_errs_in
--
ins_err(814, 'Cap Type is required if Cap Amount is provided');  -- 493
ins_err(815, 'Cap Amount is required if Cap Type is provided');  -- 494
ins_err(816, 'Cap Type must be the value ABATING or NON ABATING'); -- 495
--
-- Additional Advice Case errors
--
ins_err(817, 'invalid Advice Case Reason Stages and Advice Case Reason combination supplied');
--
-- Additional Income Data Load errors (also 873 - 881)
--
ins_err(818, 'Invalid income liability reason supplied');
ins_err(819, 'Income Header Reference must be supplied');
ins_err(820, 'Income liability reason must be supplied');
ins_err(821, 'Combination of ilr_code and hrv_vety_code does not exist on the INCOME LIABILITY VERIFICATIONS table');
--
-- label extract for New Brunswick Data Load errors (also 867 - 872)
--
ins_err(822, 'Module Name(lmod_name)must be supplied');
ins_err(823, 'Module Label Used(lmod_label used)must be supplied');
ins_err(824, 'Module Label Used(lmod_label used)must be either Y or N');
ins_err(825, 'Module Name supplied does not exist in the Modules table');
--
-- GNB elig_criterias (follow on from 787 to 790)
--
ins_err(826, 'The hrv_els_code must be supplied');
ins_err(827, 'The hrv_els_code supplied does not exist in first ref values table or is not current');
--
-- new contact details checks multiple data loads
--
ins_err(828, 'The Contact Method of TELEPHONE specified is not current');
ins_err(829, 'The Contact Method supplied does not exist in the contact_methods table');
ins_err(830, 'The Contact Value should only contain digits for this contact method');
ins_err(831, 'The Contact Value is not long enough for the Contact Method supplied');
ins_err(832, 'The Contact Value is too long for the Contact Method supplied');
ins_err(833, 'The Contact Value supplied must not contain spaces');
ins_err(834, 'The Contact Method specified is not current');
--
-- new checks for Allocations Config data loads
--
ins_err(835, 'The mlang attribute code already exisits for a different attribute in the attributes table');
ins_err(836, 'The mlang attribute description is mandatory if the mlang attribute code has been supplied');
ins_err(837, 'The mlang attribute code already exisits for a different attribute in dl_hat_attributes for this batch_id');
ins_err(838, 'The mlang attribute/lettings areas code already exisits for a different attribute in the attributes table');
ins_err(839, 'The mlang lettings area description is mandatory if the mlang lettings area code has been supplied');
ins_err(840, 'The mlang lettings areas code already exisits for a different attribute in dl_hat_lettings_areas for this batch_id');
ins_err(841, 'The mlang lettings areas code already exisits for a different lettings areas code in the lettings areas table');
ins_err(842, 'The APT Code must be supplied');
ins_err(843, 'The APT Name must be supplied');
ins_err(844, 'The Multi Language APT Code already exists on the alloc_prop_types table');
ins_err(845, 'The mlang APT Name must be supplied');
ins_err(846, 'The mlang APT Code already exisits for a different APT Code in dl_hat_alloc_prop_types for this batch_id');
--
-- new checks for Housing Advice data loads
--
ins_err(847, 'The Household Start Reason does not exist in HLD_START domain');
ins_err(848, 'The Household End Reason does not exist in HLD_END domain');
ins_err(849, 'The Person Alt Ref Indicator(field 29) is needed when Person Alt Ref supplied (field 12)');
ins_err(850, 'Indicator must be either a P or A');
ins_err(851, 'The Landlord Alt Ref Indicator(field 30) is needed when Landlord Alt Ref supplied (field 15)');
ins_err(852, 'More than 1 lrega_ins_rega_refno found in dl_had_registered_addresses for this combination');
ins_err(853, 'No lrega_ins_rega_refno can be found in dl_had_registered_addresses for this combination');
ins_err(854, 'The lrega_ins_rega_refno found does not exist in the registered_addresses table');
ins_err(855, 'The Registered Address Lettings start date is before the Registered Address start date');
ins_err(856, 'The Registered Address Lettings end date is before the Registered Address start date');
ins_err(857, 'The Registered Address Lettings end date is after the Registered Address end date');
ins_err(858, 'The Registered Address Lettings proposed end date is before the Registered Address start date');
ins_err(859, 'The Registered Address Lettings proposed end date is after the Registered Address end date');
--
-- new checks for Allocations data loads
--
ins_err(860, 'The Application Offer Flag is mandatory');
ins_err(861, 'The Application Nomination Flag is mandatory');
ins_err(862, 'The Application Received Date is mandatory');
ins_err(863, 'The Application Correspondence Name is mandatory');
ins_err(864, 'The Application Status Code is mandatory');
ins_err(865, 'The Application Status Date is mandatory');
ins_err(866, 'The Application Legacy Reference is mandatory');
--
-- additional label extract for New Brunswick Data Load errors (822-825)
--
ins_err(867, 'The Errors Shortname(lerr_object_shortname)must be supplied');
ins_err(868, 'The Errors Reference(lerr_refno)must be supplied');
ins_err(869, 'Errors Label Used(lerr_label used)must be supplied');
ins_err(870, 'Errors Label Used(lerr_label used)must be either Y or N');
ins_err(871, 'Error Name and Reference Combination supplied does not exist in the Errors table');
ins_err(872, 'The Errors Label Used(lerr_label used)supplied already exists against the Error Combination');
--
-- Additional Income Data Load errors (also 818 - 821)
--
ins_err(873, 'Percentage Liable must be supplied');
ins_err(874, 'Percentage Liable must be a whole number between 1 and 100');
ins_err(875, 'Income Liability Payment Amount must be supplied');
ins_err(876, 'Verification Type must be supplied as Income Header has been verified');
ins_err(877, 'The Creditors name must be supplied');
ins_err(878, 'The Secured Indicator must be supplied');
ins_err(879, 'The Secured Indicator must be either Y or N');
ins_err(880, 'Income Liability Legacy Reference must be supplied');
ins_err(881, 'The Income Liability record already exists against the income header');
--
-- additional label extract for New Brunswick Data Load substitute errors (822-825) and (867-872)
--
ins_err(882, 'Substitute error reference(lser_refno)must be supplied');
ins_err(883, 'The Oracle Errors Reference(lser_oracle_error)must be supplied');
ins_err(884, 'Substitute error Label Used(lser_label_used)must be supplied');
ins_err(885, 'Substitute error Label Used(lser_label_used)must be either Y or N');
ins_err(886, 'Substitute Error Combination supplied does not exist in the Substitute Errors table');
ins_err(887, 'The Substitute Error Label Used supplied already exists against the Error Combination');
ins_err(888, 'Capture String(lser_capture_string)must be supplied');
--
-- additional property elements data load
--
ins_err(889, 'The numeric value supplied for the admin unit element is to long the max is (8,2)');
--
-- Property status code error
ins_err(890, 'Invalid status code supplied Must be OCC(upied) or VOI(d) or CLO(sed)');
--
-- additional Rents transactions DL errors
ins_err(891, 'Invoice ref can only be supplied for DRS ADC PAY ADJ trans types');
ins_err(892, 'ltra_allocate_to_clin must be Y or N when Invoice ref supplied');
ins_err(893, 'No matching Customer Invoices found');
--
--ins_err(894,'Work Description Code not found in WORK_DESCRIPTIONS.'); possible duplicate added on
--version in v613 dl_load not on master maybe done around 16-SEP-2016
--
-- additional MAD area DL errors
ins_err(894, 'The Notepad Text must be supplied');
ins_err(895, 'More than 1 Organisation or Organisation Contact matches the combination Supplied');
--
-- additional Rent Deduction 
ins_err(896, 'Benefit Source not found in domain RDS_BEN_SOURCE');
--
--
-- additional void instances DLerrors  
ins_err(897, 'APT Code must be supplied and exist in the ALLOC_PROP_TYPES table');
ins_err(898, 'Void Instance SCO code must be FIN,COM,CUR,CAN,PRO,NEW');
ins_err(899, 'No matching Void Path');
ins_err(900, 'Void Instance Effective Date must be supplied');
ins_err(901, 'Void Instance Created Date must be supplied');
ins_err(902, 'Void Instance Status Start must be supplied');
ins_err(903, 'Void Reason Code not found or not current');
ins_err(904, 'Void Class Code must be supplied');
ins_err(905, 'Void Status Code must be supplied');
ins_err(906, 'Void Allocations Property Type Code must be supplied');
ins_err(907, 'Void Group Code must be supplied');
ins_err(908, 'Void Reason Code must be supplied');
ins_err(909, 'Text field is more than the maximum 240 Char allowed');
--
-- HNZ - Consents (transferred from old bespoke HD2 errors sql)
--
ins_err(910, 'Invalid Reference Type supplied. Must be one of (PAR or PRF)');
--
-- HNZ - Name Change History (transferred from old bespoke HD2 errors sql)
--
ins_err(911, 'Name Change Reason Code not found in domain FNREAS');
--
-- HNZ - Person People Attributes (transferred from old bespoke HD2 errors sql)
--
ins_err(912, 'Invalid Class Code Supplied. Must be on (TEXT, YESNO, CODED, NUMERIC, DATE)');
ins_err(913, 'Person Attribute Code does not exists on PEOPLE_ATTRIBUTES table for code supplied');
ins_err(914, 'Record does not exists on PEOPLE_ATTRIB_ALLOWED_VALS table for Person Allowed Vals/Person Attribute Code combination');
ins_err(915, 'The Numeric value must be less than or equal to the Person Attribute Max Numeric Value');
ins_err(916, 'The Numeric value must be greater than or equal to the Person Attribute Min Numeric Value');
ins_err(917, 'The Date value must be less than or equal to the Person Attribute Max Date');
ins_err(918, 'The Date value must be greater than or equal to the Person Attribute Min Date');
ins_err(919, 'Yes/No Value must be Y or N');
--
-- additional MAD area DL errors
ins_err(920, 'For type ORG both the Legacy and Secondary Reference must be supplied');
ins_err(921, 'Legacy Reference must be supplied');
ins_err(922, 'Contact Value must be supplied');
ins_err(923, 'Contact Method Code must be supplied');
ins_err(924, 'For type OCC the Secondary Reference Forename and Surname must be supplied');
ins_err(925, 'A lpva_char_value of Y or N must be provided when the datatype is YN');
ins_err(926, 'The Secondary Reference(lpva_secondary_ref)must be supplied');
--
-- additional for HRM Contractors
ins_err(927, 'The Job Role Code must be supplied');
ins_err(928, 'The Read Write Indicator must be supplied');
ins_err(929, 'The Contractor Site Code must be supplied');
ins_err(930, 'The Read Write Indicator must be either a Y or N');
ins_err(931, 'The Contractor Site Code supplied does not exist');
ins_err(932, 'The Job Role Code supplied does not exist');
ins_err(933, 'The Job Role and Con Site Combination already exists in Job Role Object Rows');
ins_err(934, 'The Job Role Object name of CONTRACTOR_SITES must be supplied');
ins_err(935, 'The Job Role Con Site Code must be the same as the Contractor Con Site Code');
--
-- additional MAD area DL errors
ins_err(936, 'For type OC2 the Forename and Surname must be supplied');
ins_err(937, 'No Matching Organisation Contact found');
ins_err(938, 'No Matching Organisation found');
--
-- 6.18 Queensland Additional errors 
ins_err(939, 'Authorised Deduction Refno does not exist in RDS_AUTHORISED_DEDUCTIONS');
ins_err(940, 'Invalid RDS Account Deduction Type Code Supplied');
ins_err(941, '1 or more Required Columns to link Payment Expectation to RDS Account Deduction not Supplied');
ins_err(942, 'RDS Acct Deduction Record does not exists for, RAUD_Refno, Account No, Deduction Type, Start Date and Benefit Group');
ins_err(943, 'RDS Account Deduction Type Code Supplied not setup for use with this Payment Expectation Type');
ins_err(944, 'The RDS Account Deductions Non Related Indicator must be either a Y or N');
ins_err(945, 'The Link Payment Expectation to RDS Account Deductions Indicator must be either a Y or N');

--
END;
/


