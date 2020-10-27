--
-- This script will enable the setting of the PAR_PER_ALT_REF to the desired value
-- that has been loaded against the CRN OTHER FIELD. The PAR_PER_ALT_REF column
-- by default is set to the system legacy number, to enable successful dataload.
-- However, the business need this field to display the Centrelink Reference
-- Number once the dataload has been complete.
--  
-- Ensure triggers are disabled prior to running update, so that 
-- History record is not created unnecessarily and CRN is not validated.

UPDATE parties
SET    par_per_alt_ref = (SELECT pva_char_value
                          FROM   parameter_values p1
                          WHERE  pva_reusable_refno = par_reusable_refno
                          AND    pva_pdu_pdf_name   = 'CRN'
                          AND    pva_pdu_pob_table_name = 'PARTIES'
                          AND NOT EXISTS (SELECT NULL
                                          FROM parameter_values p2
                                          WHERE  p2.pva_pdu_pdf_name   = 'CRN'
                                          AND    p2.pva_pdu_pob_table_name = 'PARTIES'
                                          AND    p2.pva_reusable_refno > p1.pva_reusable_refno
                                          AND    p2.pva_char_value = p1.pva_char_value )
                          AND NOT EXISTS (SELECT NULL FROM parties j WHERE j.par_per_alt_ref = pva_char_value))
WHERE  par_type        = 'HOUP'
AND    par_per_alt_ref LIKE 'CHCR%';

DELETE FROM parameter_values
WHERE pva_pdu_pdf_name   = 'CRN'
AND pva_pdu_pob_table_name = 'PARTIES'
AND EXISTS (SELECT NULL FROM parties
            WHERE par_reusable_refno = pva_reusable_refno
            AND par_per_alt_ref = pva_char_value);
