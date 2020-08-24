-- Script Name = dlas_in_hlo.sql
---------------------------------------------------------------------- 
--
-- This script replaces all other dlas scripts
--
-- Script to insert Housing DL Load Area records
--
--   Ver   DB Ver  Who    Date     Reason
--   1.0   6.16    MJK    28/03/18 Standard Dataload Install Script
--   1.1   6.16    AJ     24/05/18 script name changed from dlas_in_hat
--                                 to dlas_in_hat_org_offers and added
--                                 to new gen create script
--
------------------------------------------------------------------------------- 
--
--
set serverout on size 1000000
--
DECLARE
PROCEDURE ins_dla
  (p_product_area VARCHAR2
  ,p_dataload_area VARCHAR2
  ,p_load_allowed VARCHAR2
  ,p_question1     VARCHAR2
  ,p_question2    VARCHAR2
  ,p_package_name  VARCHAR2
  ) 
IS
CURSOR c1 
  (p_dataload_area VARCHAR2
  ,p_product_area VARCHAR2
  ) 
IS
  SELECT dla_product_area  
  ,      dla_load_allowed  
  ,      dla_question1     
  ,      dla_question2     
  ,      dla_package_name  
  FROM   dl_load_areas
  WHERE  dla_dataload_area = p_dataload_area
  AND    dla_product_area = p_product_area;
l_product_area VARCHAR2(3);
l_load_allowed VARCHAR2(1);
l_question1    VARCHAR2(50);
l_question2    VARCHAR2(50);
l_package_name VARCHAR2(30);
BEGIN
  l_product_area := NULL;
  l_load_allowed := NULL;
  l_question1    := NULL;
  l_question2    := NULL;
  l_package_name := NULL;
  OPEN c1(p_dataload_area, p_product_area);
  FETCH c1 INTO l_product_area,l_load_allowed,l_question1,l_question2,l_package_name;
  CLOSE c1;
  BEGIN
    IF l_product_area IS NULL 
    THEN
      dbms_output.put_line('Dataload Area '||p_product_area||' '||p_dataload_area||' will be inserted.'); 
      INSERT INTO dl_load_areas 
      (dla_product_area  
      ,dla_dataload_area 
      ,dla_load_allowed 
      ,dla_question1 
      ,dla_question2 
      ,dla_package_name
      )
      VALUES 
      (p_product_area
      ,p_dataload_area
      ,p_load_allowed
      ,p_question1
      ,p_question2
      ,p_package_name
      );
    ELSIF NVL(p_product_area,'~') != NVL(l_product_area,'~')
    OR NVL(p_load_allowed,'~') != NVL(l_load_allowed,'~')
    OR NVL(p_question1,'~') != NVL(l_question1,'~')
    OR NVL(p_question2,'~') != NVL(l_question2,'~')
    OR NVL(p_package_name,'~') != NVL(l_package_name,'~')
    THEN
      dbms_output.put_line('Dataload Area '||p_product_area||' '||p_dataload_area||' will be updated');
      UPDATE dl_load_areas
      SET    dla_product_area = p_product_area  
      ,      dla_load_allowed = p_load_allowed
      ,      dla_question1 = p_question1
      ,      dla_question2 = p_question2
      ,      dla_package_name = p_package_name
      WHERE  dla_dataload_area = p_dataload_area
      AND    dla_product_area = p_product_area;
      dbms_output.put_line('Dataload Area '||p_product_area||' '||p_dataload_area||' updated.');
    END IF;
  EXCEPTION
  WHEN OTHERS 
  THEN
    dbms_output.put_line('Error encountered with above action...continuing with next record');
  END;
END ins_dla;
--
--
BEGIN
  ins_dla('HAT','ORGANISATION_OFFERS','Y','','','');
END;
/

