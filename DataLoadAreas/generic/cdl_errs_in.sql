-- Script Name = cdl_errs_in.sql
---------------------------------------------------------------------- 
--
-- Script to insert Contractor dataload errors
--
--   Ver   DB Ver  Who    Date     	Reason
--   1.0   5.10.0  VRS    28/06/2006    Contractor STORES Dataloads
--   
--   2.0   5.10.0  VRS    03/07/2006    Contractor STORES STOCK ITEMS Dataload
--
--   3.0   5.10.0  VRS    17/07/2006    Personnel OPERATIVE SKILLS Dataload
--   3.1   5.10.0  PH     16/08/2006    Replaced Percentage sign with wording
--                                      in error 024.
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
AND    err_object_shortname = 'CDL';
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
  values('CDL',p_err_refno,         
  p_err_text,        
  'V','N');
dbms_output.put_line('Error number '||p_err_refno||' inserted.');
ELSIF
   l_err_message != p_err_text       
   THEN
     UPDATE errors
     SET    err_message = p_err_text    
     WHERE  err_object_shortname = 'CDL'
       AND   err_refno = p_err_refno;
dbms_output.put_line('Error number '||p_err_refno||' updated.');
END IF;
--
END ins_err;
--
--
BEGIN
ins_err(001,'Store Location is Mandatory');
ins_err(002,'Store Location Already Exists');
ins_err(003,'Store Type is Mandatory');
ins_err(004,'Store Description is Mandatory');
ins_err(005,'Store Start Date is Mandatory');
ins_err(006,'Store Type Code is Mandatory');
ins_err(007,'No Corresponding Parameter Definition for Store Type Code');
ins_err(008,'Invalid Contractor Site');
ins_err(009,'Vehicle Registration Must be Unique. Vehicle already Exists in Stores');
--
ins_err(010,'Store StocK Items Product Code is Mandatory');
ins_err(011,'Store Stock Items Quantity is Mandatory');
ins_err(012,'Store Stock Items Re-Order Level is Mandatory');
ins_err(013,'Store Stock Items Re-Order Quantity is Mandatory');
ins_err(014,'Store Stock Items On-Order Quantity is Mandatory');
ins_err(015,'Store Stock Items Minimum Quantity is Mandatory');
ins_err(016,'Store Stock Items Maximum Quantity is Mandatory');
ins_err(017,'Store Stock Items Ideal Quantity is Mandatory');
ins_err(018,'Store Stock Items Reserved Quantity is Mandatory');
ins_err(019,'Store Stock Items Status Code is Mandatory');
ins_err(020,'Store Stock Items Location not setup in STORES');
--
ins_err(021,'Interested Party Type Code is Mandatory');
ins_err(022,'Interested Party Shortname is Mandatory');
ins_err(023,'Shedule of Rates Code is Mandatory');
ins_err(024,'Proficiency Percentage is Mandatory');
ins_err(025,'Record already exists in Operative skills for party/SOR combination');

END;
/