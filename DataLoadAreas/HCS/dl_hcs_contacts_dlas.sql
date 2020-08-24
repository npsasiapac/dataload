-- dl_hcs_contacts_dlas.sql
--------------------------------- Comment -------------------------------------
-- Script to load Customer Services data load area
-------------------------------------------------------------------------------
-- Date          Version  DB Version  Name  Amendment(s)
-- ----          ---      ------      ----  ------------
-- 18-SEP-2018   1.0                  MOK    Initial Creation
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
                           'CONTACTS',
                           'Y',
                           NULL,
                           NULL,
                           NULL
                    FROM  SYS.DUAL
                    WHERE NOT EXISTS (SELECT NULL
                                      FROM DL_LOAD_AREAS DLA
                                      WHERE DLA.DLA_PRODUCT_AREA = 'HCS'
                                      AND DLA.DLA_DATALOAD_AREA = 'CONTACTS'); 
--
