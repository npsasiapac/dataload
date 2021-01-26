SPOO delete_ale_hna_trhna.log
SET PAGESIZE 0

DELETE 
  FROM applic_list_entry_history 
 WHERE leh_app_refno IN (SELECT DISTINCT ale_app_refno
                           FROM applic_list_entries
                          WHERE ale_rli_code IN ('HNA', 'TRHNA'))
   AND leh_rli_code IN ('HNA', 'TRHNA');
   
DELETE 
  FROM slist_entries 
 WHERE sle_ale_app_refno IN (SELECT DISTINCT ale_app_refno
                               FROM applic_list_entries
                              WHERE ale_rli_code IN ('HNA', 'TRHNA')) 
   AND sle_ale_rli_code IN ('TRHNA', 'HNA');

DELETE
  FROM applic_stage_decision_hist
 WHERE sdh_als_ale_app_refno IN (SELECT DISTINCT ale_app_refno
                                   FROM applic_list_entries
                                  WHERE ale_rli_code IN ('HNA', 'TRHNA'))
   AND sdh_als_ale_rli_code IN ('TRHNA', 'HNA');
   
DELETE
  FROM applic_list_stage_decisions
 WHERE als_ale_app_refno IN (SELECT DISTINCT ale_app_refno
                               FROM applic_list_entries
                              WHERE ale_rli_code IN ('HNA', 'TRHNA'))
   AND als_ale_rli_code IN ('TRHNA', 'HNA');

DELETE
  FROM applic_rule_points
 WHERE arp_app_refno IN (SELECT DISTINCT ale_app_refno
                               FROM applic_list_entries
                              WHERE ale_rli_code IN ('HNA', 'TRHNA'))
   AND arp_ale_rli_code IN ('HNA', 'TRHNA');
   
DELETE 
  FROM applic_list_entries 
 WHERE ale_rli_code IN ('TRHNA', 'HNA');

COMMIT;
   
SPOO OFF