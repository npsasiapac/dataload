--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   14-DEC-2015  Control comments section added
--                                      REGISTERED_ADDRESS CHANGED TO REGISTERED_ADDRESSES
--                                      Delete for advice case reason added as changed to
--                                      advice_case_reasons and dlas left separate
--
--
--
--***********************************************************************
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                   SELECT  'HAD',
                            'ADVICE_CASES',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                      AND DLA.DLA_DATALOAD_AREA = 'ADVICE_CASES'); 

--
--PACKAGE NAME ADVICE_CASE_REASONS
--
DELETE FROM DL_LOAD_AREAS DLA
WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
AND DLA.DLA_DATALOAD_AREA = 'ADVICE_CASE_REASON';									  
									  
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,
                           DLA_LOAD_ALLOWED,
                           DLA_QUESTION1,
                           DLA_QUESTION2,
                           DLA_PACKAGE_NAME)
                   SELECT  'HAD',
                            'ADVICE_CASE_REASONS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                      AND DLA.DLA_DATALOAD_AREA = 'ADVICE_CASE_REASONS');



INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                   SELECT  'HAD',
                            'ADVICE_CASE_PEOPLE',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                      AND DLA.DLA_DATALOAD_AREA = 'ADVICE_CASE_PEOPLE'); 




INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'ADVICE_CASE_HSG_OPTN',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'ADVICE_CASE_HSG_OPTN'); 

                    


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'ADV_RSN_CASEWRK_EVTS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'ADV_RSN_CASEWRK_EVTS'); 

                    


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'ADV_CASE_HSG_OPTN_HIS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'ADV_CASE_HSG_OPTN_HIS'); 




INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'BONDS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'BONDS');


                    


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'ADV_CASE_QUESTN_RESP',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'ADV_CASE_QUESTN_RESP'); 

                    


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'PREVENTION_PAYMENTS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'PREVENTION_PAYMENTS'); 

                    

DELETE 
FROM DL_LOAD_AREAS 
WHERE dla_product_area = 'HAD'
  AND dla_dataload_area = 'REGISTERED_ADDRESS'
;


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'REGISTERED_ADDRESSES',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'REGISTERED_ADDRESSES'); 



                    


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'REG_ADDRESS_LETTINGS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'REG_ADDRESS_LETTINGS'); 

                    


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'HOUSEHOLDS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'HOUSEHOLDS'); 

                    


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HAD',
                           'HOUSEHOLD_PEOPLE',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HAD'
                                    AND DLA.DLA_DATALOAD_AREA = 'HOUSEHOLD_PEOPLE'); 







 