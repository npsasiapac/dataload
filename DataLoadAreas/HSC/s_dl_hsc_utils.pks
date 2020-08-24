CREATE OR REPLACE PACKAGE s_dl_hsc_utils
AS
   FUNCTION valid_service_period
         (p_scp_code IN VARCHAR2,
		  p_start_date IN DATE ) RETURN BOOLEAN;
		  
   FUNCTION valid_service
         (p_att_ele_code IN VARCHAR2,
		  p_att_code IN VARCHAR2 ) RETURN BOOLEAN;
		  
   FUNCTION map_admin_unit
         (p_prorefno_auncode IN VARCHAR2) RETURN VARCHAR2;
		  
   FUNCTION map_element
         (p_svc_att_ele_code IN VARCHAR2) RETURN VARCHAR2;
		 		  
   FUNCTION map_attribute
         (p_svc_att_code IN VARCHAR2) RETURN VARCHAR2;
--
END  s_dl_hsc_utils;
/
