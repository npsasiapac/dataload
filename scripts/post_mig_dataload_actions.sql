ALTER TRIGGER PAR_CRN_BR_IU ENABLE;

UPDATE first_ref_values
   SET frv_current_ind = 'N'
 WHERE frv_frd_domain = 'VERIFIEDTYPE'
   AND frv_code = 'MIGRATION';
   
COMMIT;

ALTER TRIGGER ASSE_BR_I ENABLE;