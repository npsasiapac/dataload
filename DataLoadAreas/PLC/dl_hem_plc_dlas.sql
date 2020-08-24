--
-- dl_hem_plc_dlas.sql
--------------------------------- Comment -------------------------------------
-- Script to load Property Life Cycle data Load Areas
-------------------------------------------------------------------------------
-- Date          Version  DB Version  Name  Amendment(s)
-- ----          ---      ------      ----  ------------
-- 01-MAR-2010   1.0      v5.16.0     VRS   Initial Creation
-- 02-SEP-2015   1.1      v6.12.0     AJ    Change Control added and checked
--                                          PLC_PROP_REQ_HIST no longer available
--                                          so left commented out
--
--
-------------------------------------------------------------------------------
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                   SELECT  'HEM',
                            'PLC_PROP_ACT_HIST',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                    FROM    SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                      AND DLA.DLA_DATALOAD_AREA = 'PLC_PROP_ACT_HIST'); 
--
--INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
--                           DLA_DATALOAD_AREA,
--                           DLA_LOAD_ALLOWED,
--                           DLA_QUESTION1,
--                           DLA_QUESTION2,
--                           DLA_PACKAGE_NAME)
--                   SELECT  'HEM',
--                            'PLC_PROP_REQ_HIST',
--                            'Y',
--                            NULL,
--                            NULL,
--                            NULL
--                    FROM    SYS.DUAL
--                    WHERE NOT EXISTS (SELECT NULL
--                                      FROM DL_LOAD_AREAS DLA
--                                      WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
--                                      AND DLA.DLA_DATALOAD_AREA = 'PLC_PROP_REQ_HIST');
--
--
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'PLC_PROP_REQUESTS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'PLC_PROP_REQUESTS'); 
--
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'PLC_REQ_ACT_HIST',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'PLC_REQ_ACT_HIST'); 
--
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'PLC_REQ_DATA_ITEMS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'PLC_REQ_DATA_ITEMS');
--
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'PLC_REQUEST_PROPS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'PLC_REQUEST_PROPS');
--
--
INSERT INTO DL_LOAD_AREAS (DLA_PRODUCT_AREA,
                           DLA_DATALOAD_AREA,               
                           DLA_LOAD_ALLOWED,             
                           DLA_QUESTION1,                           
                           DLA_QUESTION2,                            
                           DLA_PACKAGE_NAME)
                  SELECT   'HEM',
                           'PLC_REQ_PROP_LINKS',
                            'Y',
                            NULL,
                            NULL,
                            NULL
                  FROM SYS.DUAL
                  WHERE NOT EXISTS (SELECT NULL
                                    FROM DL_LOAD_AREAS DLA
                                    WHERE DLA.DLA_PRODUCT_AREA = 'HEM'
                                    AND DLA.DLA_DATALOAD_AREA = 'PLC_REQ_PROP_LINKS');
--
--
