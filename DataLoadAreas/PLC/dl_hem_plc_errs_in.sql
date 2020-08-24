-- Script Name = PLC_errs_in.sql
---------------------------------------------------------------------- 
--
-- Script to insert HDX Error messages
--
--   Ver   DB Ver  Who    Date        Reason
--
--   1.0   5.16.0  VRS    21-JUL-2009 PLC Error Codes for Bespoke NSW
--                                    Dataloads
--
--   2.0   5.16.0  VRS    20-NOV-2009 PLC Error Codes 29 added
--
--   3.0   5.16.0  VRS    24-NOV-2009 PLC Error Codes for PLC_REQUEST_PROPERTY_LINKS
--                                    added (30-34)
--   4.0   6.12.0  AJ     03-SEP-2015 PLC Error Code for LPLPR_AUN_CODE added to
--                                    PLC_PROPERTY_REQUESTS (35)
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
AND    err_object_shortname = 'PLC';
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
  values('PLC',p_err_refno,         
  p_err_text,        
  'V','N');
dbms_output.put_line('Error number '||p_err_refno||' inserted.');
ELSIF
   l_err_message != p_err_text       
   THEN
     UPDATE errors
     SET    err_message = p_err_text    
     WHERE  err_object_shortname = 'PLC'
       AND   err_refno = p_err_refno;
dbms_output.put_line('Error number '||p_err_refno||' updated.');
END IF;
--
END ins_err;
--
--
BEGIN
--
ins_err(1, 'Property Request Reference already exists in PLC_PROPERTY_REQUESTS');
ins_err(2, 'Create Indicator must be Y or N');
ins_err(3, 'Invalid Property Request Type Code Supplied');
ins_err(4, 'Invalid Status Code Supplied');
ins_err(5, 'PLC Milestone Code not found in domain PLCMILESTONE');
ins_err(6, 'Parent Property Request Reference does not exists in PLC_PROPERTY_REQUESTS');
--
--
ins_err(7, 'Property Request Reference does not exists in PLC_PROPERTY_REQUESTS');
ins_err(8, 'Pending Property Indicator must be Y or N');
ins_err(9, 'Sequence must be supplied and not the Property Reference if Pending Property Indicator = Y');
ins_err(10, 'Sequence must be supplied if Pending Property Indicator = Y');
ins_err(11, 'Property Reference must not be supplied if Pending Property Indicator = Y');
ins_err(12, 'Property Reference must be supplied and not Sequence if Pending Property Indicator = N');
ins_err(13, 'Property Reference must be supplied if Pending Property Indicator = N');
ins_err(14, 'Sequence must not be supplied if Pending Indicator = N');
--
--
ins_err(15, 'Requested Data Item Type must be either PRO or REQ');
ins_err(16, 'Action Data Item Reference does not exists in PLC DATA ITEMS table');
ins_err(17, 'Invalid Property Reference supplied Requested Data Item Type PRO');
ins_err(18, 'Pending Property Indicator must be Y or N');
ins_err(19, 'Either a CHAR or NUMBER or DATE value must be supplied');
ins_err(20, 'Estates Updated Indicator must be Y or N');
ins_err(21, 'The Supplied Sequence does not exist on PLC_REQUEST_PROPERTIES if Pending Property Indicator = Y');
ins_err(22, 'The Supplied Property Reference does not exist on PLC_REQUEST_PROPERTIES if Pending Property Indicator = N');
--
--
ins_err(23, 'Request Property Reference does not exist in PLC_REQUEST_PROPERTIES table');
ins_err(24, 'Invalid Action Code Supplied');
ins_err(25, 'Completed Indicator must be Y or N');
ins_err(26, 'Completed By and Date must be supplied if Completed Indicator = Y');
ins_err(27, 'Invalid Status Code From Supplied');
ins_err(28, 'Invalid Status Code To Supplied');
--
ins_err(29, 'Property Reference does not exist in PROPERTIES table');
--
ins_err(30, 'FROM Property Request Reference does not exists in PLC_PROPERTY_REQUESTS');
ins_err(31, 'TO Property Request Reference does not exists in PLC_PROPERTY_REQUESTS');
ins_err(32, 'FROM Property Reference does not exist in PROPERTIES table');
ins_err(33, 'TO Property Reference does not exist in PROPERTIES table');
ins_err(34, 'PLC_REQUEST_PROPERTY_LINKS record already exists for FROM/TO PLRP_REFNO Combination');
ins_err(35, 'Admin Unit code does not exist in ADMIN_UNITS table');
--
END;
/













