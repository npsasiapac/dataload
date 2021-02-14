SPOO SecurityScriptClone5.txt
set echo on
SET DEFINE OFF;
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('HSGONLINE_CUST','Housing Online Customer','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('CEOFFICE','CE Office','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('AE_CHP_MAN','Assets - Estates - CHP Management','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('ORG_FIELDHIDE','Field Hide Org Portal','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('ORG_AMEH','Amelie Housing','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('ORG_UNSA','UnitingSA Housing Ltd','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('ORG_UNCH','Uniting Country Housing SA','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('ORG_YOUR','YourPlace Housing Pty Ltd','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('BA_NOACCESS','Business Reason only - No CR Access','Y','USR');
Insert into JOB_ROLES (JRO_CODE,JRO_DESCRIPTION,JRO_CURRENT_IND,JRO_USAGE) values ('DOC_TYPES','SAHA User Document Types','Y','USR');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_DOCUPL','Upload Client Attachment','USR','SYS','FSC');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_HOPAPP_V','Org Portal View Applications','USR','HOP','HOU');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_HOPCLI_V','Org Portal Client View','USR','HOP','HOU');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_HOP_APPMNT','Create Organisation','USR','SYS','FSC');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_ORG_MAINT','Create Organisation','USR','SYS','FSC');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_HOP_CLIDP','Org Portal Client Details','USR','SYS','FSC');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_HOPASSM_V','View Assessments','USR','SYS','FSC');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_HOPCCT_V','Org Portal Contact View','USR','SYS','FSC');
Insert into ACTION_GROUPS (AGP_NAME,AGP_DESCRIPTION,AGP_USAGE,AGP_ARC_CODE,AGP_ARC_SYS_CODE) values ('HSA_HOP_CXTRPT','Org Portal Context Reports','USR','SYS','FSC');
delete from NAVIGATION_ACTION_GROUPS where NAG_NAC_ACT_CODE = 'PEGAUS1$' and NAG_AGP_NAME = 'HSA_ADDRESS_V';
delete from NAVIGATION_ACTION_GROUPS where NAG_NAC_ACT_CODE = 'ASSHS' and NAG_AGP_NAME = 'HSA_ASSM_VIEW';
delete from NAVIGATION_ACTION_GROUPS where NAG_NAC_ACT_CODE = 'CCTINSA' and NAG_AGP_NAME = 'HSA_CONTACT_CRE';
DELETE FROM NAVIGATION_ACTION_GROUPS WHERE NAG_NAC_ACT_CODE = 'INCDTL$' AND NAG_NAC_MODULE_FROM = 'APP053' AND NAG_AGP_NAME = 'HSA_VIEW_HAD';
DELETE FROM NAVIGATION_ACTION_GROUPS WHERE NAG_NAC_ACT_CODE = 'INCHDR$' AND NAG_NAC_MODULE_FROM = 'APP053' AND NAG_AGP_NAME = 'HSA_VIEW_HAD';
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_ORG_MAINT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ORGINS'
    and nac_module_from = 'IPP001'
    and nac_module_to = 'IPP017'
    and nac_block_from = 'TAB3'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_ORG_MAINT'
                    AND NAG_NAC_ACT_CODE = 'ORGINS'
                    AND NAG_NAC_MODULE_FROM = 'IPP001'
                    AND NAG_NAC_MODULE_TO = 'IPP017'
                    AND NAG_NAC_BLOCK_FROM = 'TAB3'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ORGINS already exists in action group HSA_ORG_MAINT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ORGINS into action group HSA_ORG_MAINT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_ORG_MAINT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ORGUP'
    and nac_module_from = 'IPP001'
    and nac_module_to = 'IPP018'
    and nac_block_from = 'TAB3'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_ORG_MAINT'
                    AND NAG_NAC_ACT_CODE = 'ORGUP'
                    AND NAG_NAC_MODULE_FROM = 'IPP001'
                    AND NAG_NAC_MODULE_TO = 'IPP018'
                    AND NAG_NAC_BLOCK_FROM = 'TAB3'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ORGUP already exists in action group HSA_ORG_MAINT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ORGUP into action group HSA_ORG_MAINT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_ORG_MAINT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ORGUP'
    and nac_module_from = 'IPP020'
    and nac_module_to = 'IPP018'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_ORG_MAINT'
                    AND NAG_NAC_ACT_CODE = 'ORGUP'
                    AND NAG_NAC_MODULE_FROM = 'IPP020'
                    AND NAG_NAC_MODULE_TO = 'IPP018'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ORGUP already exists in action group HSA_ORG_MAINT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ORGUP into action group HSA_ORG_MAINT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_ACTION_CRE'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ACTINS1'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS100'
    and nac_block_from = 'ORG'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_ACTION_CRE'
                    AND NAG_NAC_ACT_CODE = 'ACTINS1'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS100'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ACTINS1 already exists in action group HSA_ACTION_CRE');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ACTINS1 into action group HSA_ACTION_CRE');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_ACTION_VIEW'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'SCNBAN'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'HCS001'
    and nac_block_from = 'ORG'
    and nac_block_to = 'TAB2'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_ACTION_VIEW'
                    AND NAG_NAC_ACT_CODE = 'SCNBAN'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'HCS001'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'TAB2');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action SCNBAN already exists in action group HSA_ACTION_VIEW');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert SCNBAN into action group HSA_ACTION_VIEW');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_CRE'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTINS'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS080'
    and nac_block_from = 'ORG'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_CRE'
                    AND NAG_NAC_ACT_CODE = 'CCTINS'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS080'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTINS already exists in action group HSA_CONTACT_CRE');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTINS into action group HSA_CONTACT_CRE');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_CRE'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTINS2'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS080'
    and nac_block_from = 'ORG'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_CRE'
                    AND NAG_NAC_ACT_CODE = 'CCTINS2'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS080'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTINS2 already exists in action group HSA_CONTACT_CRE');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTINS2 into action group HSA_CONTACT_CRE');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_SYS'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTDEL'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS088'
    and nac_block_from = 'ORG'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_SYS'
                    AND NAG_NAC_ACT_CODE = 'CCTDEL'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS088'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTDEL already exists in action group HSA_CONTACT_SYS');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTDEL into action group HSA_CONTACT_SYS');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_SYS'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTDEL'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS088'
    and nac_block_from = 'ORG'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_SYS'
                    AND NAG_NAC_ACT_CODE = 'CCTDEL'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS088'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTDEL already exists in action group HSA_CONTACT_SYS');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTDEL into action group HSA_CONTACT_SYS');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_UPD'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTUPD'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS083'
    and nac_block_from = 'ORG'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_UPD'
                    AND NAG_NAC_ACT_CODE = 'CCTUPD'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS083'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTUPD already exists in action group HSA_CONTACT_UPD');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTUPD into action group HSA_CONTACT_UPD');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_UPD'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTUPSR'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS085'
    and nac_block_from = 'ORG'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_UPD'
                    AND NAG_NAC_ACT_CODE = 'CCTUPSR'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS085'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTUPSR already exists in action group HSA_CONTACT_UPD');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTUPSR into action group HSA_CONTACT_UPD');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_SYS'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTCAN'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS086'
    and nac_block_from = 'ORG'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_SYS'
                    AND NAG_NAC_ACT_CODE = 'CCTCAN'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS086'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTCAN already exists in action group HSA_CONTACT_SYS');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTCAN into action group HSA_CONTACT_SYS');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_SYS'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTREIN'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS087'
    and nac_block_from = 'ORG'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_SYS'
                    AND NAG_NAC_ACT_CODE = 'CCTREIN'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS087'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTREIN already exists in action group HSA_CONTACT_SYS');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTREIN into action group HSA_CONTACT_SYS');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_UPD'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTANS'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS089'
    and nac_block_from = 'ORG'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_UPD'
                    AND NAG_NAC_ACT_CODE = 'CCTANS'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS089'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTANS already exists in action group HSA_CONTACT_UPD');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTANS into action group HSA_CONTACT_UPD');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CONTACT_UPD'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTANS'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS089'
    and nac_block_from = 'ORG'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CONTACT_UPD'
                    AND NAG_NAC_ACT_CODE = 'CCTANS'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS089'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTANS already exists in action group HSA_CONTACT_UPD');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTANS into action group HSA_CONTACT_UPD');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CCT_VIEW'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTDV'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS082'
    and nac_block_from = 'ORG'
    and nac_block_to = 'TAB1A'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CCT_VIEW'
                    AND NAG_NAC_ACT_CODE = 'CCTDV'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS082'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'TAB1A');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTDV already exists in action group HSA_CCT_VIEW');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTDV into action group HSA_CCT_VIEW');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_CCT_VIEW'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CONQRY'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS084'
    and nac_block_from = 'ORG'
    and nac_block_to = 'QUERY_CONTROL1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_CCT_VIEW'
                    AND NAG_NAC_ACT_CODE = 'CONQRY'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS084'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'QUERY_CONTROL1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CONQRY already exists in action group HSA_CCT_VIEW');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CONQRY into action group HSA_CCT_VIEW');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_PPL_VIEW'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARDV7'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'ORG'
    and nac_block_to = 'CONTACT_DETAILS_DV'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_PPL_VIEW'
                    AND NAG_NAC_ACT_CODE = 'PARDV7'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'CONTACT_DETAILS_DV');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARDV7 already exists in action group HSA_PPL_VIEW');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARDV7 into action group HSA_PPL_VIEW');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AALMAIN'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP191'
    and nac_block_from = 'TAB1A'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'AALMAIN'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP191'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1A'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AALMAIN already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AALMAIN into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPADV$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP053'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB2'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'IPADV$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP053'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB2');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPADV$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPADV$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPADV'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP197'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'IPADV'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP197'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPADV already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPADV into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPLLA$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP190'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB2'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPLLA$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP190'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB2');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPLLA$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPLLA$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPNOP$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'HOU-COM-NOP'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPNOP$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-NOP'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPNOP$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPNOP$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPASS'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP081'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'POINTS_TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPASS'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP081'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'POINTS_TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPASS already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPASS into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPIPU$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'TCY028'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'INTERESTED_PARTY_USAGES_TAB'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPIPU$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'TCY028'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'INTERESTED_PARTY_USAGES_TAB');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPIPU$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPIPU$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPQUES$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP191'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPQUES$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP191'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPQUES$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPQUES$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPUNQU$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP191'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB3'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPUNQU$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP191'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB3');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPUNQU$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPUNQU$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPHIST$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'HAT-APP-HIST-DP'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPHIST$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-HIST-DP'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPHIST$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPHIST$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'DOCGEN$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'HOU-COM-GEN'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'NUL'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'DOCGEN$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-GEN'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'NUL');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action DOCGEN$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert DOCGEN$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'NOMDV1'
    and nac_module_from = 'APP081'
    and nac_module_to = 'NOM013'
    and nac_block_from = 'TAB3'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'NOMDV1'
                    AND NAG_NAC_MODULE_FROM = 'APP081'
                    AND NAG_NAC_MODULE_TO = 'NOM013'
                    AND NAG_NAC_BLOCK_FROM = 'TAB3'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action NOMDV1 already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert NOMDV1 into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPAHS$'
    and nac_module_from = 'APP191'
    and nac_module_to = 'APP077'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPAHS$'
                    AND NAG_NAC_MODULE_FROM = 'APP191'
                    AND NAG_NAC_MODULE_TO = 'APP077'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPAHS$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPAHS$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPAHS$'
    and nac_module_from = 'APP191'
    and nac_module_to = 'APP077'
    and nac_block_from = 'TAB3'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPAHS$'
                    AND NAG_NAC_MODULE_FROM = 'APP191'
                    AND NAG_NAC_MODULE_TO = 'APP077'
                    AND NAG_NAC_BLOCK_FROM = 'TAB3'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPAHS$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPAHS$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPAHS$'
    and nac_module_from = 'APP197'
    and nac_module_to = 'APP077'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPAHS$'
                    AND NAG_NAC_MODULE_FROM = 'APP197'
                    AND NAG_NAC_MODULE_TO = 'APP077'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPAHS$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPAHS$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AALQRY'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP052'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'QUERY_CONTROL'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'AALQRY'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP052'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'QUERY_CONTROL');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AALQRY already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AALQRY into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AALDV'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP053'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1A'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'AALDV'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP053'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1A');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AALDV already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AALDV into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPAUTH'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP177'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPAUTH'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP177'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPAUTH already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPAUTH into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPHIST$'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'HAT-APP-HIST-DP'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPHIST$'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-HIST-DP'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPHIST$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPHIST$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPDTLS'
    and nac_module_from = 'HAT-APP-HIST-DP'
    and nac_module_to = 'APP053'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPDTLS'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-HIST-DP'
                    AND NAG_NAC_MODULE_TO = 'APP053'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPDTLS already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPDTLS into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPAHS$'
    and nac_module_from = 'HAT-APP-HIST-DP'
    and nac_module_to = 'APP077'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPAHS$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-HIST-DP'
                    AND NAG_NAC_MODULE_TO = 'APP077'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPAHS$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPAHS$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPIPH$'
    and nac_module_from = 'HAT-APP-HIST-DP'
    and nac_module_to = 'APP077'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB5'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPIPH$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-HIST-DP'
                    AND NAG_NAC_MODULE_TO = 'APP077'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB5');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPIPH$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPIPH$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPLEH$'
    and nac_module_from = 'HAT-APP-HIST-DP'
    and nac_module_to = 'APP077'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB2'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPLEH$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-HIST-DP'
                    AND NAG_NAC_MODULE_TO = 'APP077'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB2');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPLEH$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPLEH$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPLEHS$'
    and nac_module_from = 'HAT-APP-HIST-DP'
    and nac_module_to = 'APP077'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB3'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPLEHS$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-HIST-DP'
                    AND NAG_NAC_MODULE_TO = 'APP077'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB3');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPLEHS$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPLEHS$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPANS$'
    and nac_module_from = 'HAT-APP-HIST-DP'
    and nac_module_to = 'APP088'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB2'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPANS$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-HIST-DP'
                    AND NAG_NAC_MODULE_TO = 'APP088'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB2');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPANS$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPANS$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'SDDV$'
    and nac_module_from = 'HAT-APP-SDV'
    and nac_module_to = 'HAT-SDV-DP'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'SDDV$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-SDV'
                    AND NAG_NAC_MODULE_TO = 'HAT-SDV-DP'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action SDDV$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert SDDV$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPDV$'
    and nac_module_from = 'HEM-INC-IHU'
    and nac_module_to = 'APP053'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1A'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPDV$'
                    AND NAG_NAC_MODULE_FROM = 'HEM-INC-IHU'
                    AND NAG_NAC_MODULE_TO = 'APP053'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1A');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPDV$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPDV$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AALDV'
    and nac_module_from = 'HSS-CLI-APPLS'
    and nac_module_to = 'APP053'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'AALDV'
                    AND NAG_NAC_MODULE_FROM = 'HSS-CLI-APPLS'
                    AND NAG_NAC_MODULE_TO = 'APP053'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AALDV already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AALDV into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARAPP$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HSS-CLI-APPLS'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'PARAPP$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HSS-CLI-APPLS'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARAPP$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARAPP$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'SDAQ$'
    and nac_module_from = 'HAT-SDV-DP'
    and nac_module_to = 'HAT-APP-SDAQ'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'SDAQ$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-SDV-DP'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-SDAQ'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action SDAQ$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert SDAQ$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPAHS$'
    and nac_module_from = 'APP197'
    and nac_module_to = 'APP077'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPAHS$'
                    AND NAG_NAC_MODULE_FROM = 'APP197'
                    AND NAG_NAC_MODULE_TO = 'APP077'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPAHS$ already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPAHS$ into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPRIN'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP083'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPRIN'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP083'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPRIN already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPRIN into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPAPP_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPRIN'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP083'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPAPP_V'
                    AND NAG_NAC_ACT_CODE = 'APPRIN'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP083'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPRIN already exists in action group HSA_HOPAPP_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPRIN into action group HSA_HOPAPP_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARDV3'
    and nac_module_from = 'APP053'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARDV3'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARDV3 already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARDV3 into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARDV'
    and nac_module_from = 'HOP-CLI-SRS'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARDV'
                    AND NAG_NAC_MODULE_FROM = 'HOP-CLI-SRS'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARDV already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARDV into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARDV$'
    and nac_module_from = 'HEM-INC-INHU'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARDV$'
                    AND NAG_NAC_MODULE_FROM = 'HEM-INC-INHU'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARDV$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARDV$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARCDE$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HOU-COM-CON'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARCDE$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-CON'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARCDE$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARCDE$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CLIATTR$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HSS-CLI-ATTRS'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'CLIATTR$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HSS-CLI-ATTRS'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CLIATTR$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CLIATTR$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'OTHF$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HOU-COM-OTHF'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'OTHF$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-OTHF'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action OTHF$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert OTHF$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARNAMH$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HSS-CLI-NAMHI'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARNAMH$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HSS-CLI-NAMHI'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARNAMH$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARNAMH$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'DOCGEN$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HOU-COM-GEN'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'NUL'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'DOCGEN$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-GEN'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'NUL');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action DOCGEN$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert DOCGEN$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CLIATTH$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HSS-CLI-ATTRS-H'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'CLIATTH$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HSS-CLI-ATTRS-H'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CLIATTH$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CLIATTH$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARNOP$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'PAR_NOTE'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARNOP$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'PAR_NOTE');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARNOP$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARNOP$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PAROTHF$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'OTHER_FIELDS_DV'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PAROTHF$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'OTHER_FIELDS_DV');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PAROTHF$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PAROTHF$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARAUS1$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'AUN007'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB_AUS'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARAUS1$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'AUN007'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB_AUS');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARAUS1$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARAUS1$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARINH$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HEM-INC-INH'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARINH$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HEM-INC-INH'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARINH$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARINH$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARINC$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HEM-INC-INDT'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARINC$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HEM-INC-INDT'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARINC$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARINC$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARASST$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HEM-ICS-ASSE'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARASST$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HEM-ICS-ASSE'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARASST$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARASST$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCLI_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARAUS2$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HOU-COM-AUS'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB_AUS'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCLI_V'
                    AND NAG_NAC_ACT_CODE = 'PARAUS2$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-AUS'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB_AUS');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARAUS2$ already exists in action group HSA_HOPCLI_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARAUS2$ into action group HSA_HOPCLI_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_DOCUPL'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CLIDOCUP'
    and nac_module_from = 'HOU-COM-GEN'
    and nac_module_to = 'HOU-COM-ATT-UPL'
    and nac_block_from = 'NUL'
    and nac_block_to = 'NUL'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_DOCUPL'
                    AND NAG_NAC_ACT_CODE = 'CLIDOCUP'
                    AND NAG_NAC_MODULE_FROM = 'HOU-COM-GEN'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-ATT-UPL'
                    AND NAG_NAC_BLOCK_FROM = 'NUL'
                    AND NAG_NAC_BLOCK_TO = 'NUL');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CLIDOCUP already exists in action group HSA_DOCUPL');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CLIDOCUP into action group HSA_DOCUPL');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'NOPINS'
    and nac_module_from = 'HOU-COM-NOP'
    and nac_module_to = 'HOU-COM-NOP-C'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'NOPINS'
                    AND NAG_NAC_MODULE_FROM = 'HOU-COM-NOP'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-NOP-C'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action NOPINS already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert NOPINS into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'NOPUPD'
    and nac_module_from = 'HOU-COM-NOP'
    and nac_module_to = 'HOU-COM-NOP-U'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'NOPUPD'
                    AND NAG_NAC_MODULE_FROM = 'HOU-COM-NOP'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-NOP-U'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action NOPUPD already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert NOPUPD into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'NOPDEL'
    and nac_module_from = 'HOU-COM-NOP'
    and nac_module_to = 'HOU-COM-NOP-D'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'NOPDEL'
                    AND NAG_NAC_MODULE_FROM = 'HOU-COM-NOP'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-NOP-D'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action NOPDEL already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert NOPDEL into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPALE$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP053'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1A'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'APPALE$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP053'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1A');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPALE$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPALE$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ALECPY'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP063'
    and nac_block_from = 'TAB1A'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ALECPY'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP063'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1A'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ALECPY already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ALECPY into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ALECPY'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP063'
    and nac_block_from = 'TAB1A'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ALECPY'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP063'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1A'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ALECPY already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ALECPY into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ALEINS'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP062'
    and nac_block_from = 'TAB1A'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ALEINS'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP062'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1A'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ALEINS already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ALEINS into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ALEINS'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP062'
    and nac_block_from = 'TAB1A'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ALEINS'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP062'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1A'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ALEINS already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ALEINS into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ALEUPD2'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP064'
    and nac_block_from = 'TAB1A'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ALEUPD2'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP064'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1A'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ALEUPD2 already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ALEUPD2 into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ALEUPD2'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP064'
    and nac_block_from = 'TAB1A'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ALEUPD2'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP064'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1A'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ALEUPD2 already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ALEUPD2 into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPRIN'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP083'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'APPRIN'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP083'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPRIN already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPRIN into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPRIN'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP083'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'APPRIN'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP083'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPRIN already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPRIN into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CLIAPINS'
    and nac_module_from = 'HSS-CLI-APPLS'
    and nac_module_to = 'APP051'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'CLIAPINS'
                    AND NAG_NAC_MODULE_FROM = 'HSS-CLI-APPLS'
                    AND NAG_NAC_MODULE_TO = 'APP051'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CLIAPINS already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CLIAPINS into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'LARDTL$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP190'
    and nac_block_from = 'TAB1A'
    and nac_block_to = 'TAB2'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'LARDTL$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP190'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1A'
                    AND NAG_NAC_BLOCK_TO = 'TAB2');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action LARDTL$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert LARDTL$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAINS'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP195'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'TAB2'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAINS'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP195'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'TAB2');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAINS already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAINS into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAINS'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP195'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'TAB2'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAINS'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP195'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'TAB2');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAINS already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAINS into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAUPD'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP067'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAUPD'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP067'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAUPD already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAUPD into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAUPD'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP067'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAUPD'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP067'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAUPD already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAUPD into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAUPD2'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP070'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAUPD2'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP070'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAUPD2 already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAUPD2 into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAUPD2'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP070'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAUPD2'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP070'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAUPD2 already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAUPD2 into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'GANUPD'
    and nac_module_from = 'APP190'
    and nac_module_to = 'APP099'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'QUESTION'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'GANUPD'
                    AND NAG_NAC_MODULE_FROM = 'APP190'
                    AND NAG_NAC_MODULE_TO = 'APP099'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'QUESTION');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action GANUPD already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert GANUPD into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP191'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP191'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP191'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP191'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP191'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP191'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP191'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB3'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP191'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB3'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP191'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB3'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP191'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB3'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP191'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB3'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP191'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB3'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP197'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP197'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP197'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP197'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP197'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP197'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPINS'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP051'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'APPINS'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP051'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPINS already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPINS into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPUPD'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP056'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'APPUPD'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP056'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPUPD already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPUPD into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPUPD'
    and nac_module_from = 'HAT001'
    and nac_module_to = 'APP056'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'APPUPD'
                    AND NAG_NAC_MODULE_FROM = 'HAT001'
                    AND NAG_NAC_MODULE_TO = 'APP056'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPUPD already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPUPD into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'SDVC$'
    and nac_module_from = 'HAT-APP-SDV'
    and nac_module_to = 'HAT-APP-SDV-C'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'SDVC$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-SDV'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-SDV-C'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action SDVC$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert SDVC$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'SDVD$'
    and nac_module_from = 'HAT-APP-SDV'
    and nac_module_to = 'HAT-APP-SDV-D'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'SDVD$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-SDV'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-SDV-D'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action SDVD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert SDVD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'SDVU$'
    and nac_module_from = 'HAT-APP-SDV'
    and nac_module_to = 'HAT-APP-SDV-U'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'SDVU$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-SDV'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-SDV-U'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action SDVU$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert SDVU$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'NOPINE$'
    and nac_module_from = 'HOU-COM-NOP'
    and nac_module_to = 'HOU-COM-NOP-C'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'NOPINE$'
                    AND NAG_NAC_MODULE_FROM = 'HOU-COM-NOP'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-NOP-C'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action NOPINE$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert NOPINE$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'NOPUPE$'
    and nac_module_from = 'HOU-COM-NOP'
    and nac_module_to = 'HOU-COM-NOP-U'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'NOPUPE$'
                    AND NAG_NAC_MODULE_FROM = 'HOU-COM-NOP'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-NOP-U'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action NOPUPE$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert NOPUPE$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'NOPDEE$'
    and nac_module_from = 'HOU-COM-NOP'
    and nac_module_to = 'HOU-COM-NOP-D'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'NOPDEE$'
                    AND NAG_NAC_MODULE_FROM = 'HOU-COM-NOP'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-NOP-D'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action NOPDEE$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert NOPDEE$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAEND'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP069'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAEND'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP069'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAEND already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAEND into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAEND'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP069'
    and nac_block_from = 'TAB2'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAEND'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP069'
                    AND NAG_NAC_BLOCK_FROM = 'TAB2'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAEND already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAEND into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAEND'
    and nac_module_from = 'HOU100'
    and nac_module_to = 'APP069'
    and nac_block_from = 'TAB6_2'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAEND'
                    AND NAG_NAC_MODULE_FROM = 'HOU100'
                    AND NAG_NAC_MODULE_TO = 'APP069'
                    AND NAG_NAC_BLOCK_FROM = 'TAB6_2'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAEND already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAEND into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAEND'
    and nac_module_from = 'HOU100'
    and nac_module_to = 'APP069'
    and nac_block_from = 'TAB6_2'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAEND'
                    AND NAG_NAC_MODULE_FROM = 'HOU100'
                    AND NAG_NAC_MODULE_TO = 'APP069'
                    AND NAG_NAC_BLOCK_FROM = 'TAB6_2'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAEND already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAEND into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAEND'
    and nac_module_from = 'APP055'
    and nac_module_to = 'APP069'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAEND'
                    AND NAG_NAC_MODULE_FROM = 'APP055'
                    AND NAG_NAC_MODULE_TO = 'APP069'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAEND already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAEND into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPAEND'
    and nac_module_from = 'APP055'
    and nac_module_to = 'APP069'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPAEND'
                    AND NAG_NAC_MODULE_FROM = 'APP055'
                    AND NAG_NAC_MODULE_TO = 'APP069'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPAEND already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPAEND into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPUSINS$'
    and nac_module_from = 'TCY028'
    and nac_module_to = 'IPP015'
    and nac_block_from = 'INTERESTED_PARTY_USAGES_TAB'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPUSINS$'
                    AND NAG_NAC_MODULE_FROM = 'TCY028'
                    AND NAG_NAC_MODULE_TO = 'IPP015'
                    AND NAG_NAC_BLOCK_FROM = 'INTERESTED_PARTY_USAGES_TAB'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPUSINS$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPUSINS$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPUSUPD$'
    and nac_module_from = 'TCY028'
    and nac_module_to = 'IPP016'
    and nac_block_from = 'INTERESTED_PARTY_USAGES_TAB'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPUSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'TCY028'
                    AND NAG_NAC_MODULE_TO = 'IPP016'
                    AND NAG_NAC_BLOCK_FROM = 'INTERESTED_PARTY_USAGES_TAB'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPUSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPUSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPPDV$'
    and nac_module_from = 'TCY028'
    and nac_module_to = 'IPP012'
    and nac_block_from = 'INTERESTED_PARTY_USAGES_TAB'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPPDV$'
                    AND NAG_NAC_MODULE_FROM = 'TCY028'
                    AND NAG_NAC_MODULE_TO = 'IPP012'
                    AND NAG_NAC_BLOCK_FROM = 'INTERESTED_PARTY_USAGES_TAB'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPPDV$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPPDV$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'HAT-APP-SDAQ'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'HAT-APP-SDAQ'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP197'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP197'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP197'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP197'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ANSUPD$'
    and nac_module_from = 'APP197'
    and nac_module_to = 'HAT-APP-ANS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'ANSUPD$'
                    AND NAG_NAC_MODULE_FROM = 'APP197'
                    AND NAG_NAC_MODULE_TO = 'HAT-APP-ANS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ANSUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ANSUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPUPD2'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP056'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'APPUPD2'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP056'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPUPD2 already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPUPD2 into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPUPD2'
    and nac_module_from = 'APP053'
    and nac_module_to = 'APP056'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'APPUPD2'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'APP056'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPUPD2 already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPUPD2 into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPPUPD$'
    and nac_module_from = 'IPP012'
    and nac_module_to = 'IPP010'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPPUPD$'
                    AND NAG_NAC_MODULE_FROM = 'IPP012'
                    AND NAG_NAC_MODULE_TO = 'IPP010'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPPUPD$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPPUPD$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AUSDV$'
    and nac_module_from = 'IPP012'
    and nac_module_to = 'AUN007'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB_AUS'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'AUSDV$'
                    AND NAG_NAC_MODULE_FROM = 'IPP012'
                    AND NAG_NAC_MODULE_TO = 'AUN007'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB_AUS');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AUSDV$ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AUSDV$ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPPINSQ'
    and nac_module_from = 'IPP008'
    and nac_module_to = 'IPP009'
    and nac_block_from = 'QUERY_RESULTS'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPPINSQ'
                    AND NAG_NAC_MODULE_FROM = 'IPP008'
                    AND NAG_NAC_MODULE_TO = 'IPP009'
                    AND NAG_NAC_BLOCK_FROM = 'QUERY_RESULTS'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPPINSQ already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPPINSQ into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AUSCRE'
    and nac_module_from = 'APP053'
    and nac_module_to = 'ADR007'
    and nac_block_from = 'TAB_AUS'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'AUSCRE'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'ADR007'
                    AND NAG_NAC_BLOCK_FROM = 'TAB_AUS'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AUSCRE already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AUSCRE into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AUSUPD'
    and nac_module_from = 'APP053'
    and nac_module_to = 'ADR008'
    and nac_block_from = 'TAB_AUS'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'AUSUPD'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'ADR008'
                    AND NAG_NAC_BLOCK_FROM = 'TAB_AUS'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AUSUPD already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AUSUPD into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPUSINS'
    and nac_module_from = 'TCY028'
    and nac_module_to = 'IPP015'
    and nac_block_from = 'INTERESTED_PARTY_USAGES_TAB'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPUSINS'
                    AND NAG_NAC_MODULE_FROM = 'TCY028'
                    AND NAG_NAC_MODULE_TO = 'IPP015'
                    AND NAG_NAC_BLOCK_FROM = 'INTERESTED_PARTY_USAGES_TAB'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPUSINS already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPUSINS into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_APPMNT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'IPUSUPD'
    and nac_module_from = 'TCY028'
    and nac_module_to = 'IPP016'
    and nac_block_from = 'INTERESTED_PARTY_USAGES_TAB'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_APPMNT'
                    AND NAG_NAC_ACT_CODE = 'IPUSUPD'
                    AND NAG_NAC_MODULE_FROM = 'TCY028'
                    AND NAG_NAC_MODULE_TO = 'IPP016'
                    AND NAG_NAC_BLOCK_FROM = 'INTERESTED_PARTY_USAGES_TAB'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action IPUSUPD already exists in action group HSA_HOP_APPMNT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert IPUSUPD into action group HSA_HOP_APPMNT');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOP_CLIDP'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARDV11$'
    and nac_module_from = 'HSS-ASM-DP'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOP_CLIDP'
                    AND NAG_NAC_ACT_CODE = 'PARDV11$'
                    AND NAG_NAC_MODULE_FROM = 'HSS-ASM-DP'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARDV11$ already exists in action group HSA_HOP_CLIDP');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARDV11$ into action group HSA_HOP_CLIDP');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ASSCRE$'
    and nac_module_from = 'MYP-CUS-PRF'
    and nac_module_to = 'HSS-ASM-C'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'ASSCRE$'
                    AND NAG_NAC_MODULE_FROM = 'MYP-CUS-PRF'
                    AND NAG_NAC_MODULE_TO = 'HSS-ASM-C'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ASSCRE$ already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ASSCRE$ into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARASS$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HSS-CLI-ASM'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'PARASS$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HSS-CLI-ASM'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARASS$ already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARASS$ into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ASSMQRY'
    and nac_module_from = 'HSS-ASM-SP'
    and nac_module_to = 'HSS-ASM-AS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'ASSMQRY'
                    AND NAG_NAC_MODULE_FROM = 'HSS-ASM-SP'
                    AND NAG_NAC_MODULE_TO = 'HSS-ASM-AS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ASSMQRY already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ASSMQRY into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ASSQRY'
    and nac_module_from = 'HSS-ASM-SP'
    and nac_module_to = 'HSS-ASM-AS'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'ASSQRY'
                    AND NAG_NAC_MODULE_FROM = 'HSS-ASM-SP'
                    AND NAG_NAC_MODULE_TO = 'HSS-ASM-AS'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ASSQRY already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ASSQRY into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'ASSDV'
    and nac_module_from = 'HSS-ASM-SP'
    and nac_module_to = 'HSS-ASM-DP'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'ASSDV'
                    AND NAG_NAC_MODULE_FROM = 'HSS-ASM-SP'
                    AND NAG_NAC_MODULE_TO = 'HSS-ASM-DP'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action ASSDV already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert ASSDV into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'GQUEVW'
    and nac_module_from = 'HSS-ASM-DP'
    and nac_module_to = 'HSS-COM-GQUE'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'GQUEVW'
                    AND NAG_NAC_MODULE_FROM = 'HSS-ASM-DP'
                    AND NAG_NAC_MODULE_TO = 'HSS-COM-GQUE'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action GQUEVW already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert GQUEVW into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'GQREVW'
    and nac_module_from = 'HSS-ASM-DP'
    and nac_module_to = 'HSS-COM-GQRE'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'GQREVW'
                    AND NAG_NAC_MODULE_FROM = 'HSS-ASM-DP'
                    AND NAG_NAC_MODULE_TO = 'HSS-COM-GQRE'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action GQREVW already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert GQREVW into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'GQRHVW'
    and nac_module_from = 'HSS-COM-GQRE'
    and nac_module_to = 'HSS-COM-GQRH'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'GQRHVW'
                    AND NAG_NAC_MODULE_FROM = 'HSS-COM-GQRE'
                    AND NAG_NAC_MODULE_TO = 'HSS-COM-GQRH'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action GQRHVW already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert GQRHVW into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPASSM_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'GQREVW'
    and nac_module_from = 'HSS-ASM-ITEMR'
    and nac_module_to = 'HSS-COM-GQRE'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPASSM_V'
                    AND NAG_NAC_ACT_CODE = 'GQREVW'
                    AND NAG_NAC_MODULE_FROM = 'HSS-ASM-ITEMR'
                    AND NAG_NAC_MODULE_TO = 'HSS-COM-GQRE'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action GQREVW already exists in action group HSA_HOPASSM_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert GQREVW into action group HSA_HOPASSM_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_PPL_VIEW'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARASS$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HSS-CLI-ASM'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_PPL_VIEW'
                    AND NAG_NAC_ACT_CODE = 'PARASS$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HSS-CLI-ASM'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARASS$ already exists in action group HSA_PPL_VIEW');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARASS$ into action group HSA_PPL_VIEW');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_ASSM_VIEW'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARASS$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HSS-CLI-ASM'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_ASSM_VIEW'
                    AND NAG_NAC_ACT_CODE = 'PARASS$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HSS-CLI-ASM'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARASS$ already exists in action group HSA_ASSM_VIEW');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARASS$ into action group HSA_ASSM_VIEW');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'APPCCT$'
    and nac_module_from = 'APP053'
    and nac_module_to = 'HOU-COM-CCT'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'APPCCT$'
                    AND NAG_NAC_MODULE_FROM = 'APP053'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-CCT'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action APPCCT$ already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert APPCCT$ into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTCNS$'
    and nac_module_from = 'CUS082'
    and nac_module_to = 'CUS082'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1B'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'CCTCNS$'
                    AND NAG_NAC_MODULE_FROM = 'CUS082'
                    AND NAG_NAC_MODULE_TO = 'CUS082'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1B');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTCNS$ already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTCNS$ into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTAHIS$'
    and nac_module_from = 'CUS082'
    and nac_module_to = 'HCS-CCT-CCTH-A'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'CCTAHIS$'
                    AND NAG_NAC_MODULE_FROM = 'CUS082'
                    AND NAG_NAC_MODULE_TO = 'HCS-CCT-CCTH-A'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTAHIS$ already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTAHIS$ into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTSHIS$'
    and nac_module_from = 'CUS082'
    and nac_module_to = 'HCS-CCT-CCTH-S'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'CCTSHIS$'
                    AND NAG_NAC_MODULE_FROM = 'CUS082'
                    AND NAG_NAC_MODULE_TO = 'HCS-CCT-CCTH-S'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTSHIS$ already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTSHIS$ into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CONQRY'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS084'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'QUERY_CONTROL1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'CONQRY'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS084'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'QUERY_CONTROL1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CONQRY already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CONQRY into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTDV'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS082'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1A'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'CCTDV'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS082'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1A');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTDV already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTDV into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTDV'
    and nac_module_from = 'HOU-COM-CCT'
    and nac_module_to = 'CUS082'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'MASTER'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'CCTDV'
                    AND NAG_NAC_MODULE_FROM = 'HOU-COM-CCT'
                    AND NAG_NAC_MODULE_TO = 'CUS082'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'MASTER');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTDV already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTDV into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARCCT$'
    and nac_module_from = 'PAR015'
    and nac_module_to = 'HOU-COM-CCT'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'PARCCT$'
                    AND NAG_NAC_MODULE_FROM = 'PAR015'
                    AND NAG_NAC_MODULE_TO = 'HOU-COM-CCT'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARCCT$ already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARCCT$ into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARDV7'
    and nac_module_from = 'CUS082'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'MASTER'
    and nac_block_to = 'CONTACT_DETAILS_DV'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'PARDV7'
                    AND NAG_NAC_MODULE_FROM = 'CUS082'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'MASTER'
                    AND NAG_NAC_BLOCK_TO = 'CONTACT_DETAILS_DV');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARDV7 already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARDV7 into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARDV7'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'CONTACT_DETAILS_DV'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'PARDV7'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'CONTACT_DETAILS_DV');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARDV7 already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARDV7 into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AALDV7'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'APP053'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1A'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'AALDV7'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'APP053'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1A');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AALDV7 already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AALDV7 into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CONQRY'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS084'
    and nac_block_from = 'ORG'
    and nac_block_to = 'QUERY_CONTROL1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'CONQRY'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS084'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'QUERY_CONTROL1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CONQRY already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CONQRY into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CCTDV'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'CUS082'
    and nac_block_from = 'ORG'
    and nac_block_to = 'TAB1A'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'CCTDV'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'CUS082'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'TAB1A');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CCTDV already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CCTDV into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'PARDV7'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'PAR015'
    and nac_block_from = 'ORG'
    and nac_block_to = 'CONTACT_DETAILS_DV'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'PARDV7'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'PAR015'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'CONTACT_DETAILS_DV');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action PARDV7 already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert PARDV7 into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_HOPCCT_V'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSGBURT'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'AALDV7'
    and nac_module_from = 'HCS001'
    and nac_module_to = 'APP053'
    and nac_block_from = 'ORG'
    and nac_block_to = 'TAB1A'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_HOPCCT_V'
                    AND NAG_NAC_ACT_CODE = 'AALDV7'
                    AND NAG_NAC_MODULE_FROM = 'HCS001'
                    AND NAG_NAC_MODULE_TO = 'APP053'
                    AND NAG_NAC_BLOCK_FROM = 'ORG'
                    AND NAG_NAC_BLOCK_TO = 'TAB1A');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action AALDV7 already exists in action group HSA_HOPCCT_V');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert AALDV7 into action group HSA_HOPCCT_V');
END;
/
BEGIN
    insert into navigation_action_groups(NAG_AGP_NAME
                                        ,NAG_NAC_ACT_CODE
                                        ,NAG_NAC_SCO_CODE
                                        ,NAG_NAC_MODULE_FROM
                                        ,NAG_NAC_BLOCK_FROM
                                        ,NAG_NAC_ITEM_FROM
                                        ,NAG_NAC_MODULE_TO
                                        ,NAG_NAC_BLOCK_TO
                                        ,NAG_NAC_ITEM_TO
                                        ,NAG_USAGE
                                        ,NAG_NAC_TYPE_FROM
                                        ,NAG_NAC_TYPE_VALUE_FROM
                                        ,NAG_NAC_TYPE_TO
                                        ,NAG_NAC_TYPE_VALUE_TO
                                        ,NAG_CREATED_DATE
                                        ,NAG_CREATED_BY
                                        ,NAG_RELEASE_VERS)
    select 'HSA_APP_MAINT'
    ,      nac_act_code
    ,      nac_sco_code
    ,      nac_module_from
    ,      nac_block_from
    ,      nac_item_from
    ,      nac_module_to
    ,      nac_block_to
    ,      nac_item_to
    ,      'USR'
    ,     nac_type_from
    ,     nac_type_value_from
    ,     nac_type_to
    ,     nac_type_value_to
    ,     sysdate
    ,     'NPSHPATEL'
    ,    '6.12.0'
    from navigation_actions
    where nac_act_code = 'CLIAPINS'
    and nac_module_from = 'HSS-CLI-APPLS'
    and nac_module_to = 'APP051'
    and nac_block_from = 'TAB1'
    and nac_block_to = 'TAB1'
    AND NOT EXISTS (SELECT NULL
                    FROM navigation_action_groups
                    WHERE NAG_AGP_NAME = 'HSA_APP_MAINT'
                    AND NAG_NAC_ACT_CODE = 'CLIAPINS'
                    AND NAG_NAC_MODULE_FROM = 'HSS-CLI-APPLS'
                    AND NAG_NAC_MODULE_TO = 'APP051'
                    AND NAG_NAC_BLOCK_FROM = 'TAB1'
                    AND NAG_NAC_BLOCK_TO = 'TAB1');
    
    if sql%rowcount = 0 then
      dbms_output.put_line('Action CLIAPINS already exists in action group HSA_APP_MAINT');
    end if;
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('Failed to insert CLIAPINS into action group HSA_APP_MAINT');
END;
/
commit;
SPOO OFF