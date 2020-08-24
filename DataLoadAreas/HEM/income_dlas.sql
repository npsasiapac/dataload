--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0               VS   2008         Initial Creation
--  1.0     6.11      AJ   22-Dec-2015  Added new functionality
--                                      released at v611 income liabilities
--                                      and change control
--
--
--***********************************************************************
--
-- Bespoke data load Areas (dlas) script for Income Data Load NonICS v6.11
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,
                           DLA_LOAD_ALLOWED,
                           DLA_QUESTION1,
                           DLA_QUESTION2,
                           DLA_PACKAGE_NAME)
                   SELECT  'HEM',
                            'INCOME_HEADERS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                      AND DLA.DLA_DATALOAD_AREA = 'INCOME_HEADERS');


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,
                           DLA_LOAD_ALLOWED,
                           DLA_QUESTION1,
                           DLA_QUESTION2,
                           DLA_PACKAGE_NAME)
                   SELECT  'HEM',
                            'INCOME_HEADER_USAGES',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                      AND DLA.DLA_DATALOAD_AREA = 'INCOME_HEADER_USAGES');


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                   SELECT  'HEM',
                            'INCOME_DETAILS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                      AND DLA.DLA_DATALOAD_AREA = 'INCOME_DETAILS'); 


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'ASSETS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'ASSETS'); 


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'INC_DET_DEDUCTIONS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'INC_DET_DEDUCTIONS');

INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'INCOME_LIABILITIES',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'INCOME_LIABILITIES');
									
									
									
									
