-- Script Name = dl_hra_arrs_dlas_in.sql
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
--   2.3   6.9.0   AJ     12/11/13 Script name changed when re done for Cannock
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
CURSOR c1 (p_dataload_area VARCHAR2) IS
SELECT 
DLA_PRODUCT_AREA  
,DLA_LOAD_ALLOWED  
,DLA_QUESTION1     
,DLA_QUESTION2     
,DLA_PACKAGE_NAME  
FROM   dl_load_areas
WHERE  dla_dataload_area = p_dataload_area
AND    dla_product_area  like 'H%';
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
OPEN c1(p_dataload_area);
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
     AND   dla_product_area like 'H%';
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
ins_dla('HRA','ARREARS_ARRANGEMENTS','Y','End Existing Arrangement?','','');
--
ins_dla('HRA','ARREARS_INSTALMENTS','Y','','','');
--
ins_dla('HRA','PAYMENT_EXPECTATIONS','Y','','','');
--
ins_dla('HRA','EXPECTED_PAYMENTS','Y','','','');
--
END;
/
