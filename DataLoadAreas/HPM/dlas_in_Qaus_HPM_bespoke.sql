-- Script Name = dlas_in_Qaus_HPM_bespoke.sql
---------------------------------------------------------------------- 
--
-- This script replaces all other dlas scripts
--
-- Script to insert Housing DL Load Area records
--
--   Ver   DB Ver  Who    Date          Reason
--   1.0   6.15    AJ     06-MAR-3017   Created for QAus bespoke as two new DL's24/05/04
--                                      added just for them task_payments and deliverable
--                                      valuations so cannot reply on standard insert now
--                                      based on v3.5(r614)14-FEB-2017 add QAus tag to
--                                      name of script as well
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
--ins_dla('FSC','OTHER_FIELD_VALUES','Y','','','');
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
ins_dla('HPM','TASK_PAYMENTS','Y','','','');
--
ins_dla('HPM','DELIVERABLE_VAL','Y','','','');
--
--
END;
/

