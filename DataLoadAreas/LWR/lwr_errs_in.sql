/************************************************************************
   Version  Who           Date          Why
   -------  ------------  -----------   ---------------------------------
   0.1      Northgate     26-Sept-2018  Initial Version
 ************************************************************************/

SET FEEDBACK OFF
CREATE OR REPLACE PROCEDURE insert_errors(p_err_refno       IN   errors.err_refno%TYPE,
                                          p_err_shortname   IN   errors.err_object_shortname%TYPE,
                                          p_err_message     IN   errors.err_message%TYPE,
                                          p_err_type        IN   errors.err_type%TYPE,
                                          p_err_popup_ind   IN   errors.err_popup%TYPE,
                                          p_required_ind    IN   VARCHAR2)
IS

BEGIN
   IF p_required_ind = 'Y'
   THEN
      MERGE INTO errors tgt
      USING (SELECT p_err_shortname err_object_shortname,
                    p_err_refno err_refno,
                    p_err_message err_message,
                    p_err_type err_type,
                    p_err_popup_ind err_popup                 
               FROM dual) src
         ON (    tgt.err_object_shortname = src.err_object_shortname
             AND tgt.err_refno = src.err_refno)
       WHEN MATCHED THEN
          UPDATE
             SET tgt.err_message = src.err_message,
                 tgt.err_message_mlang = src.err_message,
                 tgt.err_type = src.err_type,
                 tgt.err_popup = src.err_popup
       WHEN NOT MATCHED THEN
          INSERT(err_object_shortname,
                 err_refno,
                 err_message,
                 err_type,
                 err_popup,
                 err_message_mlang,
                 err_extracted)
          VALUES(src.err_object_shortname,
                 src.err_refno,
                 src.err_message,
                 src.err_type,
                 src.err_popup,
                 src.err_message,
                 'Y');
   ELSE
      DELETE
        FROM errors
       WHERE err_object_shortname = p_err_shortname
         AND err_refno = p_err_refno;
   END IF;
END insert_errors;
/
exec insert_errors(340, 'HD2', 'Batch Type not found in domain LWR_BATCH_TYPE', 'V','N','Y');
exec insert_errors(343, 'HD2', 'Interested Party for shortname/ipt_code does not exist on INTERESTED_PARTIES table', 'V','N','Y');
exec insert_errors(329, 'HD2', 'Interested Party Type Code must be supplied if interested party shortname is supplied', 'V','N','Y');
exec insert_errors(362, 'HD2', 'Category End date cannot be before Category Start Date', 'V','N','Y');
exec insert_errors(363, 'HD2', 'Rate Category Code does not exist on RATE_CATEGORIES table', 'V','N','Y');
exec insert_errors(370, 'HD2', 'Record already exists in LWR_ASSESSMENT_VAL_ERRORS for Assessment Ref, Validation Error Code combination', 'V','N','Y');
exec insert_errors(371, 'HD2', 'Water Usage Details Period End date cannot be before Period Start Date', 'V','N','Y');
exec insert_errors(591, 'HD1', 'Property Reference cannot be found on the PROPERTIES table', 'V','N','Y');
exec insert_errors(344, 'HD2', 'Record does not exist in LWR_ANNUAL_RATES_SCHEDULES for lars_flrs_code, lars_year combination', 'V','N','Y');
exec insert_errors(346, 'HD2', 'Annual Batch Instalment Reference does not exist on LWR_BATCHES table', 'V','N','Y');
exec insert_errors(352, 'HD2', 'Batch ID does not exist in LWR_BATCHES', 'V','N','Y');
exec insert_errors(330, 'HD2', 'Invalid Interested Party Type Code supplied', 'V','N','Y');
exec insert_errors(355, 'HD2', 'Assessment Rate Period End date cannot be before Assessment Rate Period Start Date', 'V','N','Y');
exec insert_errors(365, 'HD2', 'Rate Category Start/End Date does not fall bewteen on LWR_ASSESSMENTS Rate Period Start/End Date', 'V','N','Y');
exec insert_errors(341, 'HD2', 'Invalid Batch Status supplied. Must be one of (NEW, LOD, LVF, LVA, CLO, APP, CAN, PAD)', 'V','N','Y');
exec insert_errors(351, 'HD2', 'Cancelled Reason not found in domain LWR_BATCH_CAN_RSN', 'V','N','Y');
exec insert_errors(364, 'HD2', 'Record does not exist on LWR_ASSESSMENTS table for Batch Id, Current Assessment Ref, Rate Period Start/End Date', 'V','N','Y');
exec insert_errors(372, 'HD2', 'Record already exists in LWR_APPORTIONED_ASSESSMENTS for lwra_refno/pro_refno combination', 'V','N','Y');
exec insert_errors(347, 'HD2', 'Cancelled Date must be supplied if Cancelled By is supplied', 'V','N','Y');
exec insert_errors(353, 'HD2', 'Invalid Assessment Type supplied. Must be one of (SURI, WAAS, ANRH)', 'V','N','Y');
exec insert_errors(407, 'HD2', 'Unable to derive the LWR_RATE_ASSESSMENT_DETAILS refno for details supplied', 'V','N','Y');
exec insert_errors(358, 'HD2', 'One or More mandatory values missing for Instalment Assessment Batch Type SURI', 'V','N','Y');
exec insert_errors(359, 'HD2', 'One or More mandatory values missing for Water Assessment Batch Type WAAS', 'V','N','Y');
exec insert_errors(369, 'HD2', 'Validation Error Code not found in domain LWR_VAL_ERR', 'V','N','Y');
exec insert_errors(339, 'HD2', 'Batch ID Already exist in LWR_BATCHES', 'V','N','Y');
exec insert_errors(349, 'HD2', 'Cancelled Reason must be supplied if Cancelled By/Date is supplied', 'V','N','Y');
exec insert_errors(350, 'HD2', 'Cancelled By/Date must be supplied if Cancelled Reason is supplied', 'V','N','Y');
exec insert_errors(342, 'HD2', 'Load From File Indicator must be Y or N', 'V','N','Y');
exec insert_errors(345, 'HD2', 'Rates Schedule Code/Year, Instalment Number, Annual Batch Instalment Ref must be supplied for ANN/INS Batch Types', 'V','N','Y');
exec insert_errors(354, 'HD2', 'Invalid Assessment Status supplied. Must be one of (NEW, PAY, DNP, INV)', 'V','N','Y');
exec insert_errors(356, 'HD2', 'Assessment Override Reason not found in domain LWR_ASS_ORRIDE_RSN', 'V','N','Y');
exec insert_errors(357, 'HD2', 'One or More mandatory values missing for Annual Assessment Batch Type ANRH', 'V','N','Y');
exec insert_errors(360, 'HD2', 'Invalid Credit/Debit Indicator supplied. Must be one of (CR, DR)', 'V','N','Y');
exec insert_errors(366, 'HD2', 'One or More mandatory values missing for Annual Assessment Detail Batch ANRD', 'V','N','Y');
exec insert_errors(368, 'HD2', 'One or More mandatory values missing for Water Assessment Batch Type WFRD', 'V','N','Y');
exec insert_errors(348, 'HD2', 'Cancelled By must be supplied if Cancelled Date is supplied', 'V','N','Y');
exec insert_errors(406, 'HD2', 'Unable to derive the WATER_USAGE_DETAILS refno for details supplied', 'V','N','Y');
exec insert_errors(361, 'HD2', 'Invalid Assessment Detail Class Code supplied. Must be one of (ANRD, WFRD, IOAC)', 'V','N','Y');
exec insert_errors(459, 'HD1', 'Land Title Reference does not exist on Land Titles', 'V','N','Y');
exec insert_errors(367, 'HD2', 'One or More mandatory values missing for Instalment Assessment Batch Type IOAC', 'V','N','Y');
exec insert_errors(373, 'HD2', 'Invalid Water Usage Details Status supplied. Must be one of (APR, REJ, RAP)', 'V','N','Y');
exec insert_errors(403, 'HD2', 'Billing Period Start Date is later than Billing Period End date', 'V','N','Y');
exec insert_errors(404, 'HD2', 'Invalid Water Charge Status supplied. Must be one of (RAP, CHG, REJ, APR)', 'V','N','Y');
exec insert_errors(405, 'HD2', 'Invalid Calc Type supplied. Must be one of (RECON, RECALC, OVERRIDE)', 'V','N','Y');
exec insert_errors(323, 'HD2', 'Invalid Revenue Account Payment Reference', 'V','N','Y');