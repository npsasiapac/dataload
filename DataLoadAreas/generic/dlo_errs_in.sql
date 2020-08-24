-- Script Name = hco_errs_in.sql
---------------------------------------------------------------------- 
--
-- Script to insert Contractor dataload errors
--
--   Ver   DB Ver  Who    Date     	Reason
--   1.0                                Initial Creation
--   1.1   5.10.0  PH     28-JUL-2006   Added missing error messages
--   1.2   5.10.0  PH     15-AUG-2006   Added 160 error, corrected text lines
--                                      that had too many sigle quotes
--   1.3   5.11    PJD	  09-Jan-2007   Changed Object Shortcode to DLO
--                                      (from HCO)
--   1.4   5.11    PH     30-MAR-2007   Added error for lead time on products.
--   1.5   5.12.0  PH     09-AUG-2007   Corrected spelling mistake on 374
--   1.6   5.15.1  PH     25-SEP-2009   Amended text for error 225
--   1.7   5.15.1  PH     27-NOV-2009   Added new code for SOR Product 
--                                      Specifications (375)
--   1.8   6.11    AJ     17-AUG-2015   Added new code 279 (lprod_code_mlang)check
--                                      Added new code 280 (lprod_description_mlang)check
--                                      Added new code 281 (lprod_code_mlang/lprod_code)check
--   1.9   6.11    AJ     18-AUG-2015   Added new code 282 (ldep_code_mlang) check
--                                      Added new code 283 (ldep_description_mlang)check
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
AND    err_object_shortname = 'DLO';
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
  values('DLO',p_err_refno,         
  p_err_text,        
  'V','N');
dbms_output.put_line('Error number '||p_err_refno||' inserted.');
ELSIF
   l_err_message != p_err_text       
   THEN
     UPDATE errors
     SET    err_message = p_err_text    
     WHERE  err_object_shortname = 'DLO'
       AND   err_refno = p_err_refno;
dbms_output.put_line('Error number '||p_err_refno||' updated.');
END IF;
--
END ins_err;
--
--
BEGIN
ins_err(100,'No matching Transaction found');
ins_err(101,'No matching Credit Memo found');
ins_err(102,'No matching Customer Invoices found');
ins_err(103,'No matching Source Transaction found');
ins_err(104,'No matching Source Credit Memo found');
ins_err(105,'No matching Source Customer Invoices found');
ins_err(106,'No matching Target Transaction found');
ins_err(107,'No matching Target Customer Invoices found');
ins_err(108,'Invalid Class Code for an Invoice Adjustment');
ins_err(109,'Invalid Invoicable Ind');
ins_err(110,'Invalid Class Code for an Service Charge Item');
ins_err(111,'Invalid Post Issue Ind');
ins_err(112,'No matching Target Service Charge Rate found');
ins_err(113,'No matching Target Service Usage found');
ins_err(114,'No matching Management Cost Group found');
ins_err(115,'Invalid Status Code for a Credit Memo');
ins_err(116,'No matching Revenue Account found');
ins_err(117,'Invalid Class Code for a Customer Invoice');
ins_err(118,'Invalid Status Code for a Customer Invoice');
ins_err(119,'No matching Invoice Category found');
ins_err(120,'No matching Property found');
ins_err(121,'No matching Previous Status Code found');
ins_err(122,'Invalid Arrears Possible Ind');
ins_err(123,'Duplicate Invoice Reference used');
ins_err(124,'A valid Property must be assigned to the Invoice');
ins_err(125,'The Revenue Account for the Invoice must be a Leasehold Invoice Account type');
ins_err(126,'A valid Invoice Point must be assigned for a Scheduled Invoice type');
ins_err(127,'An Invoice Point assignment is only valid for a Scheduled Invoice type');
ins_err(128,'The Invoiced Period start and end dates must be assigned for a Scheduled Invoice type');
ins_err(129,'The Invoiced Period start and end dates are only valid for a Scheduled Invoice type');
ins_err(130,'A source Invoice Refno must be assigned for a Reconciliation Invoice type');
ins_err(131,'A source Invoice Refno can only be assigned for a Reconciliation Invoice type');
ins_err(132,'Duplicate Credit Memo Reference used');
ins_err(133,'No matching Customer Invoice/Seqno');
ins_err(134,'A Service Specific type of Invoice Item must be assigned to a Service Adjustment');
ins_err(135,'A Management Cost type of Invoice Item must be assigned to a Management Adjustment');
ins_err(136,'Start date must be before end date');
ins_err(137,'If Effort Driven Ind is Y then Max Operative must be equal to or greater than 1');
ins_err(138,'Max Operatives must be null if Effort Driven ind is N');
ins_err(139,'If Job Delay Time supplied, Job Delay Time Unit must not be null');
ins_err(140,'SOR Code does not exist');
ins_err(141,'A record already exists for this SOR Code and Start Date');
ins_err(143,'SOR Code must be supplied');
ins_err(144,'Effort Start Date must be supplied');
ins_err(145,'Estimated Effort must be supplied');
ins_err(146,'Estimated Effort Unit must be supplied');
ins_err(147,'Effort Driven Indicator must be supplied');
ins_err(148,'Min Operatives must be supplied');
ins_err(149,'Estimated Effort Unit must be one of M, H or D');
ins_err(150,'Effort Driven Ind must be one of Y or N');
ins_err(151,'Invalid created_by');
ins_err(152,'Invalid modified_by');
ins_err(153,'Invalid interested party type');
ins_err(154,'Invalid grade code');
ins_err(155,'Invalid max_wkly_std_working_time');
ins_err(156,'Invalid max_wkly_overtime');
ins_err(157,'Invalid created_date');
ins_err(158,'Invlaid modified_date');
ins_err(159,'Invalid current ind');
ins_err(160,'Operative Type Grade does not exist');
ins_err(172,'Next Job Delay Time Unit must be one of M, H or D');
ins_err(173,'Max Operatives must not be less than 1');
ins_err(174,'Min Operatives must not be less than 1');
ins_err(175,'Next Job Delay Time must not be less than 1');
ins_err(177,'Modified Date must not be before created date');
ins_err(178,'Contractor Site does not exist');
ins_err(179,'Pricing Policy Group does not exist');
ins_err(180,'Work Programme does not exist');
ins_err(181,'Work Programme must be supplied');
ins_err(182,'Pricing Policy Group Code must be supplied');
ins_err(183,'Contractor Site Code must be supplied');
ins_err(184,'Pricing Policy Start Date must be supplied');
ins_err(185,'Price Group not found for Group Code/Work Programme/Con Site and Start Date');
ins_err(186,'A record already exists for this Price Group/Sor Code and Start Date');
ins_err(187,'Contractor Site Price does not exist for Price Group/SOR');
ins_err(188,'Invalid cos code');
ins_err(189,'Invalid fit code');
ins_err(190,'Invalid shortname/interested party type combination');
ins_err(192,'Proficiency Pct must be between 0 and 100');
ins_err(194,'Store id cannot be null');
ins_err(195,'Invalid store type');
ins_err(196,'Team code cannot be null');
ins_err(197,'Team name cannot be null');
ins_err(198,'Invalid team type');
ins_err(199,'Invalid team level');
ins_err(200,'Invalid contractor site / depot combination');
ins_err(201,'Default Utilisation Pct must be between 0 and 100');
ins_err(202,'Invalid depot code');
ins_err(203,'Description cannot be null');
ins_err(204,'Invalid start date/end date combination');
ins_err(205,'Invalid store id');
ins_err(208,'Staff id cannot be null for this interested party type');
ins_err(209,'Username is not allowed for this interested party type');
ins_err(210,'Party may not be a person for this interested party type');
ins_err(211,'Party may not be an organisation for this interested party type');
ins_err(212,'Termination reason may not be assigned to a current interested party');
ins_err(213,'Contractor site must be assigned for this interested party type');
ins_err(215,'Store ID must be for a vehicle store');
ins_err(216,'The default team utilisation percentage can only be set for a service delivery team');
ins_err(217,'The default team utilisation percentage must be provided for a repairs service delivery team');
ins_err(218,'Team must have the Organisational Level Indicator set');
ins_err(219,'Parent team type must be the same as child team type');
ins_err(220,'Operative Type Grade already exists');
ins_err(221,'Duplicate primary key found');
ins_err(222,'Parent Team does not exist');
ins_err(223,'Duplicate record found within the temporary table');
ins_err(224,'Product type must be either Consumable or Non-consumable');
ins_err(225,'Invalid Product Group/Sub Group Combination');
ins_err(226,'Invalid Manufacturer Code');
ins_err(227,'Invalid Unit of Measure');
ins_err(228,'Invalid Time Unit');
ins_err(229,'Hazardous Indicator must either be Y or N');
ins_err(230,'Manufactured Indicator must either be Y or N');
ins_err(231,'Recyclable Indicator must either be Y or N');
ins_err(232,'Recyclable Indicator can only be entered for consumable products');
ins_err(233,'Inspection Indicator must either be Y or N');
ins_err(234,'Service Indicator must either be Y or N');
ins_err(235,'Safety Clothing Indicator must either be Y or N');
ins_err(236,'Estimated Indicator must either be Y or N');
ins_err(237,'Fraction Allowed Indicator must either be Y or N');
ins_err(238,'Stocked Indicator must either be Y or N');
ins_err(239,'Invoiced Cost must be between 0 and 999999999.99');
ins_err(240,'Contract Price must be between 0 and 999999999.99');
ins_err(241,'Handling Amount must be between 0 and 999999999.99');
ins_err(242,'Handling Percentage must be between 0 and 999');
ins_err(243,'Handling Amount and Handling Percentage are mutually exclusive');
ins_err(244,'Shelf life unit and shelf life are associated, therefore both fields must be populated');
ins_err(245,'Shelf life can only be entered for consumable products');
ins_err(246,'Special Instructions can only be entered for non-consumable products');
ins_err(247,'Store type must either be Depot or Vehicle');
ins_err(248,'Suspended Indicator must either be Y or N');
ins_err(249,'Invalid Store Type');
ins_err(250,'Start date must not be greater than the end date');
ins_err(251,'For Depot type, ensure both Depot code and Depot Cos code are populated');
ins_err(252,'Depot Cos code and Cos code are mutually exclusive');
ins_err(253,'Ensure no vehicle details are entered for type Depot');
ins_err(254,'Invalid Fuel Type');
ins_err(255,'Invalid Vehicle Insurance');
ins_err(256,'Invalid Make/Model');
ins_err(257,'Invalid Product Code');
ins_err(258,'Invalid Status Code');
ins_err(259,'Quantity must be between 0 and 9999.99');
ins_err(260,'Maximum quantity must be greater than the minimum quantity');
ins_err(261,'Reorder Level must be between 0 and 9999.99');
ins_err(262,'Reorder Quantity must be between 0 and 9999.99');
ins_err(263,'On-Order Quantity must be between 0 and 9999.99');
ins_err(264,'Minimum Quantity must be between 0 and 9999.99');
ins_err(265,'Maximum Quantity must be between 0 and 9999.99');
ins_err(266,'Ideal Quantity must be between 0 and 9999.99');
ins_err(267,'Reserved Quantity must be between 0 and 9999.99');
ins_err(268,'Reorder level must be between minimum quantity and maximum quantity');
ins_err(269,'Reorder quantity must be between minimum quantity and maximum quantity');
ins_err(270,'Ideal quantity must be between minimum quantity and maximum quantity');
ins_err(271,'Invalid Start date');
ins_err(272,'Invalid End date');
ins_err(273,'Invalid Tax Due date');
ins_err(274,'Invalid Insurance Due date');
ins_err(275,'Invalid First Registered date');
ins_err(276,'Invalid MOT Due date');
ins_err(277,'Invalid Service Due date');
ins_err(278,'Invalid Lead Unit Supplied');
ins_err(279,'Multi Language Product Code already exists');
ins_err(280,'Multi Language Product Description MUST BE supplied as Multi Language Product Code has been supplied');
ins_err(281,'Multi Language Product Code already exists against a Different Product Code');
ins_err(282,'Multi Language Depot Code already exists');
ins_err(283,'Multi Language Depot Description MUST BE supplied as Multi Language Depot Code has been supplied');
--
ins_err(374,'Invoice Undisputed Balance is Mandatory');
--
ins_err(375,'Product Code does not exist on Products');
--
END;
/

