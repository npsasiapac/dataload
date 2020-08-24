INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                   SELECT  'HEM',
                            'CONSENTS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                      AND DLA.DLA_DATALOAD_AREA = 'CONSENTS'); 


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'INCOME_DETAIL_REQS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'INCOME_DETAIL_REQS'); 


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'ICS_REQUEST_STATUSES',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'ICS_REQUEST_STATUSES'); 


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'ICS_INCOMES',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'ICS_INCOMES'); 


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'ICS_BENEFIT_PAYMENTS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'ICS_BENEFIT_PAYMENTS'); 


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'ICS_PAYMENT_CMPTS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'ICS_PAYMENT_CMPTS'); 


INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'ICS_DEDUCTIONS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'ICS_DEDUCTIONS');