CREATE OR REPLACE PACKAGE BODY s_dl_hsc_utils              
AS                    
--                    
FUNCTION valid_service_period
         (p_scp_code IN VARCHAR2,
		  p_start_date IN DATE ) RETURN BOOLEAN                  
 IS           
  CURSOR c_exists(cp_scp_code IN VARCHAR2, cp_start_date IN DATE ) IS    
   SELECT 'x'         
    FROM  service_charge_periods                            
   WHERE  scp_code       = cp_scp_code
     AND  scp_start_date = cp_start_date;           
   l_exists VARCHAR2(1) := NULL;                
   l_result BOOLEAN := FALSE;                   
BEGIN                 
    OPEN  c_exists(p_scp_code, p_start_date );                  
    FETCH c_exists INTO l_exists;               
    IF c_exists%FOUND THEN                      
       l_result := TRUE;                        
    END IF;           
    CLOSE c_exists;   
    RETURN( l_result );                         
  EXCEPTION           
   WHEN   Others THEN                           
     fsc_utils.handle_exception;                
END valid_service_period;   
--
FUNCTION valid_service
         (p_att_ele_code IN VARCHAR2,
		  p_att_code IN VARCHAR2 ) RETURN BOOLEAN                  
 IS           
  CURSOR c_exists(cp_att_ele_code IN VARCHAR2, cp_att_code IN VARCHAR2 ) IS    
   SELECT 'x'         
    FROM attributes                            
   WHERE  att_ele_code   = cp_att_ele_code
     AND  att_code       = cp_att_code;           
   l_exists VARCHAR2(1) := NULL;                
   l_result BOOLEAN := FALSE;                   
BEGIN                 
    OPEN  c_exists(p_att_ele_code, p_att_code );                  
    FETCH c_exists INTO l_exists;               
    IF c_exists%FOUND THEN                      
       l_result := TRUE;                        
    END IF;           
    CLOSE c_exists;   
    RETURN( l_result );                         
  EXCEPTION           
   WHEN   Others THEN                           
     fsc_utils.handle_exception;                
END valid_service; 
--
FUNCTION map_admin_unit(p_prorefno_auncode VARCHAR2 ) 
RETURN VARCHAR2					  
IS
CURSOR c_hfi (cp_prorefno_auncode VARCHAR2) is
SELECT hmv_map_value
  FROM hfi_segments hs,
       hfi_mappings hm,
       hfi_segment_codes hsc,
       hfi_segment_elements hse,
       hfi_segment_details hsd,
       hfi_rents_extract_types hre,
       hfi_mapping_values
 WHERE hs.hse_no                  = hsd.sgd_hse_no
   AND hsd.sgd_rxt_refno          = hre.rxt_refno
   AND hsd.sgd_hsc_control_code   = hsc.hsc_code -- control
   AND hsc.hsc_code               = hse.hsl_hsc_code
   AND hse.hsl_hmp_code           = hm.hmp_code (+)
   AND hre.rxt_hdt_code           = 'SERVICE'
   AND hre.rxt_hrv_ate_code       = 'SER'
   AND hmv_hmp_code               = hmp_code
   AND hmp_hmt_obj_name           = 'ADMIN_UNITS'
   AND hmp_code                   = hmv_hmp_code
   AND hmv_aun_code               = cp_prorefno_auncode;
   
   l_mapped_value hfi_mapping_values.hmv_aun_code%TYPE := NULL;    
BEGIN

  OPEN  c_hfi (p_prorefno_auncode);
  FETCH c_hfi INTO l_mapped_value;
  CLOSE c_hfi;
  
  RETURN( l_mapped_value );                         
EXCEPTION           
 WHEN   Others THEN                           
   fsc_utils.handle_exception; 
END map_admin_unit;   

--
FUNCTION map_element(p_svc_att_ele_code VARCHAR2 ) 
RETURN VARCHAR2					  
IS
CURSOR c_hfi2 (p_svc_att_ele_code VARCHAR2) is
SELECT hmv_map_value
  FROM hfi_segments hs,
       hfi_mappings hm,
       hfi_segment_codes hsc,
       hfi_segment_elements hse,
       hfi_segment_details hsd,
       hfi_rents_extract_types hre,
       hfi_mapping_values
 WHERE hs.hse_no                  = hsd.sgd_hse_no
   AND hsd.sgd_rxt_refno          = hre.rxt_refno
   AND hsd.sgd_hsc_control_code   = hsc.hsc_code -- control
   AND hsc.hsc_code               = hse.hsl_hsc_code
   AND hse.hsl_hmp_code           = hm.hmp_code (+)
   AND hre.rxt_hdt_code           = 'SERVICE'
   AND hre.rxt_hrv_ate_code       = 'SER'
   AND hmv_hmp_code               = hmp_code
   AND hmp_hmt_obj_name           = 'ELEMENT'
   AND hmp_code                   = hmv_hmp_code
   AND hmv_ele_code               = p_svc_att_ele_code;
   
   l_mapped_value hfi_mapping_values.hmv_ele_code%TYPE := NULL;    
BEGIN

  OPEN  c_hfi2(p_svc_att_ele_code);
  FETCH c_hfi2 INTO l_mapped_value ;
  CLOSE c_hfi2;
  
  RETURN( l_mapped_value );                         
EXCEPTION           
 WHEN   Others THEN                           
   fsc_utils.handle_exception; 
END map_element;   
                    
--
FUNCTION map_attribute(p_svc_att_code VARCHAR2 ) 
RETURN VARCHAR2					  
IS
CURSOR c_hfi3 (p_svc_att_code VARCHAR2) is
SELECT hmv_map_value
  FROM hfi_segments hs,
       hfi_mappings hm,
       hfi_segment_codes hsc,
       hfi_segment_elements hse,
       hfi_segment_details hsd,
       hfi_rents_extract_types hre,
       hfi_mapping_values
 WHERE hs.hse_no                  = hsd.sgd_hse_no
   AND hsd.sgd_rxt_refno          = hre.rxt_refno
   AND hsd.sgd_hsc_control_code   = hsc.hsc_code -- control
   AND hsc.hsc_code               = hse.hsl_hsc_code
   AND hse.hsl_hmp_code           = hm.hmp_code (+)
   AND hre.rxt_hdt_code           = 'SERVICE'
   AND hre.rxt_hrv_ate_code       = 'SER'
   AND hmv_hmp_code               = hmp_code
   AND hmp_hmt_obj_name           = 'ATTRIBUTES'
   AND hmp_code                   = hmv_hmp_code
   AND hmv_att_code               = p_svc_att_code;   

   
   l_mapped_value hfi_mapping_values.hmv_att_code%TYPE := NULL;    
BEGIN

  OPEN  c_hfi3(p_svc_att_code);
  FETCH c_hfi3 INTO l_mapped_value;	
  CLOSE c_hfi3;
  
  RETURN( l_mapped_value );                         
EXCEPTION           
 WHEN   Others THEN                           
   fsc_utils.handle_exception; 
END map_attribute; 
				      
END s_dl_hsc_utils;                       
/
