ALTER TRIGGER PAR_CRN_BR_IU DISABLE;

UPDATE first_ref_values
   SET frv_current_ind = 'Y'
 WHERE frv_frd_domain = 'VERIFIEDTYPE'
   AND frv_code = 'MIGRATION';
   
COMMIT;