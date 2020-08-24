INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                   SELECT  'HEM',
                            'PERSON_NAME_HISTORY',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                     FROM   SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                        FROM DL_LOAD_AREAS DLA
                                       WHERE DLA.DLA_PRODUCT_AREA  = 'HEM'
                                         AND DLA.DLA_DATALOAD_AREA = 'PERSON_NAME_HISTORY');
