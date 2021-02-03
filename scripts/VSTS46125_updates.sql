-------------------------------------------------------------------------------------------------
--- VSTS46125 - Updates to applications to be applied after DM
--- 26/11/2020 v1.0 P Nguyen
-------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE assign_current_ipa_aun(p_legacy_ref   IN   VARCHAR2,
                                                p_aun_code     IN   VARCHAR2)
IS
   l_exists   VARCHAR2(1);
   
   CURSOR C_AUN_ALREADY_ASSIGNED IS
      SELECT 'Y'
        FROM object_admin_units oau
       INNER JOIN involved_parties ipa ON ipa.ipa_par_refno = oau.oau_par_refno
                                      AND TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1)
       INNER JOIN applications app ON app.app_refno = ipa.ipa_par_refno
                                  AND app.app_legacy_ref = p_legacy_ref
       WHERE oau.oau_aun_code = p_aun_code
         AND TRUNC(SYSDATE) BETWEEN TRUNC(oau.oau_start_date) AND NVL(TRUNC(oau.oau_end_date), SYSDATE + 1);
      
BEGIN
   OPEN C_AUN_ALREADY_ASSIGNED;
   FETCH C_AUN_ALREADY_ASSIGNED INTO l_exists;
   CLOSE C_AUN_ALREADY_ASSIGNED;
   
   IF NVL(l_exists, 'N') = 'N'
   THEN
      DELETE
        FROM object_admin_units
       WHERE oau_par_refno IN (SELECT ipa.ipa_par_refno
                                 FROM involved_parties ipa
                                INNER JOIN applications app ON app.app_refno = ipa.ipa_app_refno
                                                           AND app.app_legacy_ref = p_legacy_ref
                                WHERE TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1));
                                 
      INSERT INTO object_admin_units(oau_par_refno,
                                     oau_aun_code,
                                     oau_start_date)
                                     SELECT ipa.ipa_par_refno,
                                            p_aun_code,
                                            SYSDATE
                                       FROM involved_parties ipa
                                      INNER JOIN applications app ON app.app_refno = ipa.ipa_app_refno
                                                                 AND app.app_legacy_ref = p_legacy_ref
                                      WHERE TRUNC(SYSDATE) BETWEEN TRUNC(ipa.ipa_start_date) AND NVL(TRUNC(ipa.ipa_end_date), SYSDATE + 1);
   END IF;
END assign_current_ipa_aun;
/

CREATE OR REPLACE PROCEDURE create_ipro_sup(p_ipp_shortname   IN   VARCHAR2)
IS
   l_ipp_refno   interested_parties.ipp_refno%TYPE;
   
   CURSOR C_IPP_REFNO IS
      SELECT ipp_refno
        FROM interested_parties
       WHERE ipp_shortname = p_ipp_shortname;
BEGIN
   OPEN C_IPP_REFNO;
   FETCH C_IPP_REFNO INTO l_ipp_refno;
   CLOSE C_IPP_REFNO;
   
   IF l_ipp_refno IS NOT NULL
   THEN   
      MERGE INTO interested_party_roles tgt
      USING (SELECT l_ipp_refno ipro_ipp_refno,
                    'UNITYSUP' ipro_iprt_code,
                    'Y' ipro_current_ind
               FROM dual) src
         ON (    src.ipro_ipp_refno = tgt.ipro_ipp_refno
             AND src.ipro_iprt_code = tgt.ipro_iprt_code)
       WHEN MATCHED THEN
          UPDATE
             SET tgt.ipro_current_ind = src.ipro_current_ind
       WHEN NOT MATCHED THEN
          INSERT(ipro_ipp_refno,
                 ipro_iprt_code,
                 ipro_current_ind)
          VALUES(src.ipro_ipp_refno,
                 src.ipro_iprt_code,
                 src.ipro_current_ind);
   END IF;
END create_ipro_sup;
/
                     
-----------------------------------------------------------------------------------   
-- 1. Admin Unit on application
-----------------------------------------------------------------------------------   
-- Applications have GEN, CHP or COOP list entry
MERGE INTO applic_list_entries tgt
USING (SELECT *
         FROM (SELECT ale_app_refno,
                      ale_rli_code,
                      ale_aun_code,
                      ROW_NUMBER() OVER (PARTITION BY ale_app_refno ORDER BY ale_modified_date DESC) rn
                 FROM applic_list_entries ale
                WHERE EXISTS (SELECT 'x'
                                FROM applic_list_entries
                               WHERE ale_app_refno = ale.ale_app_refno
                                 AND ale_rli_code IN ('GEN', 'CHP', 'COOP')
                                 AND ale_lst_code IN ('UE', 'ACT', 'REVA', 'DEF')))
        WHERE rn = 1) src
   ON (    tgt.ale_app_refno = src.ale_app_refno)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.ale_aun_code = src.ale_aun_code;

MERGE INTO applic_list_entries tgt
USING (SELECT ale_app_refno,
              ale_rli_code,
              DECODE(SUBSTR(ale_aun_code, 1, 2), 'A-', ale_aun_code, DECODE(ale_aun_code, 'CONTACT', 'A-ADE6301', 'FMRTCYOFF', 'A-ADE6301', 'A-' || ale_aun_code)) ale_aun_code,
              ROW_NUMBER() OVER (PARTITION BY ale_app_refno ORDER BY ale_modified_date DESC) rn
         FROM applic_list_entries ale
        WHERE EXISTS (SELECT 'x'
                        FROM applic_list_entries
                       WHERE ale_app_refno = ale.ale_app_refno
                         AND ale_rli_code IN ('GEN', 'CHP', 'COOP')
                         AND ale_lst_code IN ('UE', 'ACT', 'REVA', 'DEF'))) src
   ON (    tgt.ale_app_refno = src.ale_app_refno
       AND tgt.ale_rli_code = src.ale_rli_code)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.ale_aun_code = src.ale_aun_code;

MERGE INTO applications tgt
USING (SELECT ale_app_refno app_refno,
              ale_aun_code app_aun_code,
              ROW_NUMBER() OVER (PARTITION BY ale_app_refno ORDER BY ale_modified_date DESC) rn
         FROM applic_list_entries ale
        WHERE EXISTS (SELECT 'x'
                        FROM applic_list_entries
                       WHERE ale_app_refno = ale.ale_app_refno
                         AND ale_rli_code IN ('GEN', 'CHP', 'COOP')
                         AND ale_lst_code IN ('UE', 'ACT', 'REVA', 'DEF'))) src
   ON (    src.app_refno = tgt.app_refno
       AND src.rn = 1)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.app_aun_code = src.app_aun_code
     WHERE tgt.app_aun_code != src.app_aun_code;

-- Applications do not have GEN, CHP or COOP list entry
MERGE INTO applic_list_entries tgt
USING (SELECT ale_app_refno,
              ale_rli_code,
              DECODE(ale_aun_code, 'CONTACT', 'ADE6301', 'FMRTCYOFF', 'ADE6301', ale_aun_code) ale_aun_code,
              ROW_NUMBER() OVER (PARTITION BY ale_app_refno ORDER BY ale_modified_date DESC) rn
         FROM applic_list_entries ale
        WHERE NOT EXISTS (SELECT 'x'
                            FROM applic_list_entries
                           WHERE ale_app_refno = ale.ale_app_refno
                             AND ale_rli_code IN ('GEN', 'CHP', 'COOP')
                             AND ale_lst_code IN ('UE', 'ACT', 'REVA', 'DEF'))) src
   ON (    tgt.ale_app_refno = src.ale_app_refno
       AND tgt.ale_rli_code = src.ale_rli_code
       AND src.rn = 1)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.ale_aun_code = src.ale_aun_code;  

MERGE INTO applications tgt
USING (SELECT ale_app_refno app_refno,
              ale_aun_code app_aun_code,
              ROW_NUMBER() OVER (PARTITION BY ale_app_refno ORDER BY ale_modified_date DESC) rn
         FROM applic_list_entries ale
        WHERE NOT EXISTS (SELECT 'x'
                            FROM applic_list_entries
                           WHERE ale_app_refno = ale.ale_app_refno
                             AND ale_rli_code IN ('GEN', 'CHP', 'COOP')
                             AND ale_lst_code IN ('UE', 'ACT', 'REVA', 'DEF'))) src
   ON (    src.app_refno = tgt.app_refno
       AND src.rn = 1)
 WHEN MATCHED THEN
    UPDATE
       SET tgt.app_aun_code = src.app_aun_code
     WHERE tgt.app_aun_code != src.app_aun_code;      

-----------------------------------------------------------------------------------   
-- 1. Admin Unit on restricted party
-----------------------------------------------------------------------------------   
            
exec assign_current_ipa_aun('CHCRREG61794', 'AMELIEOFF');
exec assign_current_ipa_aun('CHCRREG60346', 'AMELIEOFF');
exec assign_current_ipa_aun('CHCRREG70459', 'CORSTOOFF');
exec assign_current_ipa_aun('CHCRREG99588', 'ANGSAHOFF');
exec assign_current_ipa_aun('CHCRREG102985', 'ANGSAHOFF');
exec assign_current_ipa_aun('CHCRREG66492', 'UNICOUOFF');
exec assign_current_ipa_aun('CHCRREG97235', 'UNITYOFF');
exec assign_current_ipa_aun('CHCRREG103311', 'ANGSAHOFF');
exec assign_current_ipa_aun('CHCRREG60895', 'ADE6301');
exec assign_current_ipa_aun('CHCRREG63703', 'CORSTOOFF');
exec assign_current_ipa_aun('CHCRREG59002', 'UNITSAOFF');
exec assign_current_ipa_aun('CHCRREG114403', 'UNITSAOFF');
exec assign_current_ipa_aun('CHCRREG62145', 'AMELIEOFF');
exec assign_current_ipa_aun('CHCRREG64848', 'CORSTOOFF');
exec assign_current_ipa_aun('CHCRREG60993', 'AMELIEOFF');
exec assign_current_ipa_aun('CHCRREG61617', 'ANGSAHOFF');
exec assign_current_ipa_aun('CHCRREG85768', 'ACC2PLCOFF');
exec assign_current_ipa_aun('CHCRREG64725', 'CORSTOOFF');
exec assign_current_ipa_aun('CHCRREG64370', 'SALVOHOFF');
exec assign_current_ipa_aun('CHCRREG104646', 'ANGSAHOFF');
exec assign_current_ipa_aun('CHCRREG63870', 'CORSTOOFF');
exec assign_current_ipa_aun('CHCRREG63595', 'ADE6301');
exec assign_current_ipa_aun('CHCRREG97562', 'UNITSAOFF');
exec assign_current_ipa_aun('CHCRREG105857', 'UNITYOFF');
exec assign_current_ipa_aun('CHCRREG97427', 'COMHOUOFF');
exec assign_current_ipa_aun('CHCRREG97698', 'COMHOUOFF');



-----------------------------------------------------------------------------------   
-- 3. Core Property Record
-----------------------------------------------------------------------------------   

DECLARE
   CURSOR C_PRO_STATUS_TRAN IS
      SELECT hps.hps_pro_refno
        FROM hou_prop_statuses hps
       WHERE hps.hps_hpc_code = 'TRAN'
         AND TRUNC(SYSDATE) BETWEEN TRUNC(hps.hps_start_date) AND NVL(TRUNC(hps.hps_end_date), SYSDATE + 1);
BEGIN
   FOR l_curr_pro IN C_PRO_STATUS_TRAN
   LOOP
      UPDATE hou_prop_statuses
         SET hps_end_date = SYSDATE - 1
       WHERE hps_pro_refno = l_curr_pro.hps_pro_refno
         AND hps_hpc_code = 'TRAN'
         AND TRUNC(SYSDATE) BETWEEN TRUNC(hps_start_date) AND NVL(TRUNC(hps_end_date), SYSDATE + 1);
      
      INSERT INTO hou_prop_statuses(hps_pro_refno,
                                    hps_hpc_code,
                                    hps_hpc_type,
                                    hps_start_date)
                             VALUES(l_curr_pro.hps_pro_refno,
                                    'EXTM',
                                    'C',
                                    SYSDATE);
   END LOOP;
END;
/

-----------------------------------------------------------------------------------   
-- 4. Other fields for contact details -> SEPERATE SCRIPT
-----------------------------------------------------------------------------------   

-----------------------------------------------------------------------------------   
-- 5. Write null to annual income on customer report
-----------------------------------------------------------------------------------     
UPDATE assets
   SET asse_annual_income = NULL
 WHERE asse_annual_income = 0
   AND asse_created_by = 'MIGRATION2';

-----------------------------------------------------------------------------------   
-- 6. SACHA Property ID -> SEPERATE SCRIPT
-----------------------------------------------------------------------------------   

-----------------------------------------------------------------------------------   
-- 7. Script to make ineligible applications eligible -> SEPERATE SCRIPT
-----------------------------------------------------------------------------------   
/*INSERT INTO general_answers(gan_app_refno, 
                            gan_que_refno, 
                            gan_char_value)
                            SELECT ale_app_refno, 
                                   que_refno, 
                                   'Y'
                              FROM applic_list_entries ale 
                             INNER JOIN questions ON que_refno IN (61, 68)
                             INNER JOIN general_answers gan1 ON gan1.gan_app_refno = ale.ale_app_refno
                                                            AND gan1.gan_que_refno = 619
                                                            AND gan1.gan_char_value = 'N'
                              LEFT JOIN general_answers gan2 ON gan2.gan_app_refno = ale.ale_app_refno
                                                            AND gan2.gan_que_refno = 61
                              LEFT JOIN general_answers gan3 ON gan3.gan_app_refno = ale.ale_app_refno
                                                            AND gan3.gan_que_refno = 68
                             WHERE ale.ale_rli_code = 'GEN' 
                               AND gan2.gan_app_refno IS NULL
                               AND gan3.gan_app_refno IS NULL*/

-----------------------------------------------------------------------------------   
-- 8. Interested Party Role
-----------------------------------------------------------------------------------   
exec create_ipro_sup('SUPANGCOM');
exec create_ipro_sup('SUPANGSA');
exec create_ipro_sup('SUPAMRC');
exec create_ipro_sup('SUPAUSREF');
exec create_ipro_sup('SUPBAPCAR');
exec create_ipro_sup('SUPBARINC');
exec create_ipro_sup('SUPCAMPAG');
exec create_ipro_sup('SUPCARA');
exec create_ipro_sup('SUPCATHOU');
exec create_ipro_sup('SUPCENCFS');
exec create_ipro_sup('SUPCTYSAL');
exec create_ipro_sup('SUPCOMAUS');
exec create_ipro_sup('SUPCOMLIV');
exec create_ipro_sup('SUPDEACAN');
exec create_ipro_sup('SUPEBLDIS');
exec create_ipro_sup('SUPHLSI');
exec create_ipro_sup('SUPHUTCEN');
exec create_ipro_sup('SUPLIFEWB');
exec create_ipro_sup('SUPLUCOMM');
exec create_ipro_sup('SUPMINAUS');
exec create_ipro_sup('SUPMINHOM');
exec create_ipro_sup('SUPNEAMI');
exec create_ipro_sup('SUPNORDOM');
exec create_ipro_sup('SUPOARS');
exec create_ipro_sup('SUPORANA');
exec create_ipro_sup('SUPSALVTI');
exec create_ipro_sup('SUPSALVIF');
exec create_ipro_sup('SUPUNICOM');
exec create_ipro_sup('SUPUNICOU');
exec create_ipro_sup('SUPUNITSA');


-----------------------------------------------------------------------------------   
-- 9. Interested Party Usage on Application -> SEPERATE SCRIPT
----------------------------------------------------------------------------------- 

-----------------------------------------------------------------------------------   
-- 10. End property element -> SEPERATE SCRIPT
----------------------------------------------------------------------------------- 

COMMIT;

DROP PROCEDURE assign_current_ipa_aun;
DROP PROCEDURE create_ipro_sup;
