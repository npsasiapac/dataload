SET SERVEROUTPUT ON

DECLARE

   CURSOR c_bri IS
   SELECT DISTINCT bri_bre_mod_name mod_name
   FROM   batch_request_instances
   WHERE  bri_status                   = 'S';

   l_bri          batch_request_instances%ROWTYPE;

   PROCEDURE delete_gpi (p_module     VARCHAR2
                        ,p_start_date DATE
                        ,p_end_date   DATE) IS

   BEGIN

      DELETE FROM parameter_values_history
      WHERE  EXISTS (SELECT 'x'
                     FROM   batch_request_instances
                     WHERE  bri_reusable_refno           = pvh_pva_reusable_refno
                     AND    bri_bre_mod_name             = p_module
                     AND    bri_status                   = 'S');

      DELETE FROM parameter_values
      WHERE  EXISTS (SELECT 'x'
                     FROM   batch_request_instances
                     WHERE  bri_reusable_refno           = pva_reusable_refno
                     AND    bri_bre_mod_name             = p_module
                     AND    bri_status                   = 'S');

      DELETE FROM batch_request_instances
      WHERE bri_bre_mod_name             = p_module
      AND   bri_status                   = 'S';

      DELETE FROM batch_requests
      WHERE bre_mod_name     = p_module
      AND   bre_action       = 'S'
      AND   NOT EXISTS       (SELECT 'x'
                              FROM   batch_request_instances
                              WHERE  bri_bre_mod_name     = bre_mod_name
                              AND    bri_bre_usr_username = bre_usr_username
                              AND    bri_bre_created_date = bre_created_date);

   END delete_gpi;

BEGIN

-- ** Warning **
-- This will delete all scheduled jobs for the module
--

   FOR l_bri IN c_bri
   LOOP
      dbms_output.put_line('Deleting Module '||l_bri.mod_name);
      delete_gpi(l_bri.mod_name,NULL,NULL);
   END LOOP;

END;
/

--COMMIT
--/

