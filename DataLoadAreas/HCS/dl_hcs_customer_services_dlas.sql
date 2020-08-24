--
-- dl_hcs_customer_services_dlas.sql
--------------------------------- Comment -------------------------------------
-- Script to load Customer Services data load area
-------------------------------------------------------------------------------
-- Date          Version  DB Version  Name  Amendment(s)
-- ----          ---      ------      ----  ------------
--    OCT-2009   1.0      v5.16.0     VRS   Initial Creation
-- 02-SEP-2015   1.1      v6.12.0     AJ    Change Control Added
--
-------------------------------------------------------------------------------
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                   SELECT  'HCS',
                            'BUSINESS_ACTIONS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HCS'
                                      AND DLA.DLA_DATALOAD_AREA = 'BUSINESS_ACTIONS'); 
--
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,
                           DLA_LOAD_ALLOWED,
                           DLA_QUESTION1,
                           DLA_QUESTION2,
                           DLA_PACKAGE_NAME)
                   SELECT  'HCS',
                            'BUSINESS_ACT_EVENTS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HCS'
                                      AND DLA.DLA_DATALOAD_AREA = 'BUSINESS_ACT_EVENTS');
