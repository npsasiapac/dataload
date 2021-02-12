SPOO hna_migration_VSTS46125_delete.log
SET PAGESIZE 0

DELETE 
  FROM applic_list_entry_history 
 WHERE leh_app_refno IN (SELECT DISTINCT ale_app_refno
                           FROM applic_list_entries
                          WHERE ale_rli_code IN ('HNA', 'TRHNA'))
   AND leh_rli_code IN ('HNA', 'TRHNA')
   AND NOT EXISTS (SELECT 'x'
                     FROM applications
                    WHERE app_refno = leh_app_refno
                      AND app_sco_code IN ('HSD', 'CLD'));
   
DELETE 
  FROM slist_entries 
 WHERE sle_ale_app_refno IN (SELECT DISTINCT ale_app_refno
                               FROM applic_list_entries
                              WHERE ale_rli_code IN ('HNA', 'TRHNA')) 
   AND sle_ale_rli_code IN ('TRHNA', 'HNA')
   AND NOT EXISTS (SELECT 'x'
                     FROM applications
                    WHERE app_refno = sle_ale_app_refno
                      AND app_sco_code IN ('HSD', 'CLD'));

DELETE
  FROM applic_stage_decision_hist
 WHERE sdh_als_ale_app_refno IN (SELECT DISTINCT ale_app_refno
                                   FROM applic_list_entries
                                  WHERE ale_rli_code IN ('HNA', 'TRHNA'))
   AND sdh_als_ale_rli_code IN ('TRHNA', 'HNA')
   AND NOT EXISTS (SELECT 'x'
                     FROM applications
                    WHERE app_refno = sdh_als_ale_app_refno
                      AND app_sco_code IN ('HSD', 'CLD'));
   
DELETE
  FROM applic_list_stage_decisions
 WHERE als_ale_app_refno IN (SELECT DISTINCT ale_app_refno
                               FROM applic_list_entries
                              WHERE ale_rli_code IN ('HNA', 'TRHNA'))
   AND als_ale_rli_code IN ('TRHNA', 'HNA')
   AND NOT EXISTS (SELECT 'x'
                     FROM applications
                    WHERE app_refno = als_ale_app_refno
                      AND app_sco_code IN ('HSD', 'CLD'));

DELETE
  FROM applic_rule_points
 WHERE arp_app_refno IN (SELECT DISTINCT ale_app_refno
                               FROM applic_list_entries
                              WHERE ale_rli_code IN ('HNA', 'TRHNA'))
   AND arp_ale_rli_code IN ('HNA', 'TRHNA')
   AND NOT EXISTS (SELECT 'x'
                     FROM applications
                    WHERE app_refno = arp_app_refno
                      AND app_sco_code IN ('HSD', 'CLD'));
   
DELETE 
  FROM applic_list_entries 
 WHERE ale_rli_code IN ('TRHNA', 'HNA')
   AND NOT EXISTS (SELECT 'x'
                     FROM applications
                    WHERE app_refno = ale_app_refno
                      AND app_sco_code IN ('HSD', 'CLD'));

COMMIT;
   
SPOO OFF