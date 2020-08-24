UPDATE first_ref_values
SET    frv_default_ind = 'N'
WHERE  frv_frd_domain = 'DATEPREF'
AND    frv_default_ind = 'Y'
/
UPDATE first_ref_values
SET    frv_default_ind = 'Y'
WHERE  frv_frd_domain = 'DATEPREF'
AND    frv_code = 'DD-MO-RRRR'
/

