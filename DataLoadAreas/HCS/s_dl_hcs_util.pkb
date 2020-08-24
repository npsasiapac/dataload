create or replace PACKAGE BODY s_dl_hcs_utils
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--
--     1.0             MOK  18/09/18     Initial Version
--     1.1             AJ   10/10/18     Change control added and blank
--                                       and slash at bottom
--
-- ***********************************************************************
--  
PROCEDURE get_address (p_refno     IN  NUMBER,
                          p_type      IN  VARCHAR2,
                          l_adr_refno OUT NUMBER,
                          l_addr      OUT VARCHAR2)
   IS
      -- Local Variables
      l_aun_code     admin_units.aun_code%TYPE;
      l_par_refno    parties.par_refno%TYPE;
      l_pro_refno    properties.pro_refno%TYPE;
      l_main_tin_par parties.par_refno%TYPE;
   BEGIN
      fsc_utils.proc_start('s_dl_hsc_utils.get_address');
      fsc_utils.debug_message('s_dl_hsc_utils.get_address - Type  '||p_type||'   Refno  '||p_refno, 3);

      IF p_type = 'APP'
      THEN
         l_main_tin_par := s_involved_parties.get_par_refno_for_app_refno(p_refno);
         s_address_usages.adr_refno_for_par('PAR','CONTACT',l_main_tin_par,l_adr_refno);
      ELSIF p_type = 'IPP'
      THEN
         l_par_refno := s_interested_parties.get_ipp_par_refno(p_refno);
         s_address_usages.adr_refno_for_par('PAR','CONTACT',l_par_refno,l_adr_refno);
      ELSIF p_type = 'TCY'
      THEN
         l_main_tin_par := s_household_persons.get_hop_par_refno_for_tcy(p_refno);
         s_address_usages.adr_refno_for_par('PAR','CONTACT',l_main_tin_par,l_adr_refno);
      ELSIF p_type = 'PAR'
      THEN
         s_address_usages.adr_refno_for_par('PAR','CONTACT',p_refno,l_adr_refno);
      ELSIF p_type IN ('COS','CON')
      THEN
         s_address_usages.adr_refno_for_cos('CON','PHYSICAL',p_refno,l_adr_refno);
      ELSIF p_type = 'PRO'
      THEN
         s_address_usages.adr_refno_for_pro('PRO','PHYSICAL',p_refno,l_adr_refno);
      ELSIF p_type = 'AUN'
      THEN
         s_address_usages.adr_refno_for_aun('AUN','PHYSICAL',p_refno,l_adr_refno);
      ELSIF p_type = 'LAS'
      THEN
         s_address_usages.adr_refno_for_pro('PRO','PHYSICAL',p_refno,l_adr_refno);
      ELSIF p_type = 'SRQ'
      THEN
         s_service_requests.get_pro_refno_aun_code (p_refno, l_pro_refno, l_aun_code);
         IF l_pro_refno IS NOT NULL
         THEN
            s_address_usages.adr_refno_for_pro('PRO','PHYSICAL',l_pro_refno,l_adr_refno);
         ELSIF l_aun_code IS NOT NULL
         THEN
            s_address_usages.adr_refno_for_aun('AUN','PHYSICAL',l_aun_code,l_adr_refno);
         END IF;
      END IF;

      l_addr := s_addresses.get_address(l_adr_refno);
      fsc_utils.proc_end;
      
   EXCEPTION
      WHEN OTHERS
      THEN
         fsc_utils.handle_exception;
   END get_address;
   
--

   PROCEDURE get_address (p_code      IN  VARCHAR2,
                                           p_type      IN  VARCHAR2,
                                           l_adr_refno OUT NUMBER,
                                           l_addr      OUT VARCHAR2)
   IS
      -- Local Variables
      l_aun_code  admin_units.aun_code%TYPE;
      l_par_refno parties.par_refno%TYPE;
   BEGIN
      fsc_utils.proc_start('s_contacts.get_address');
      fsc_utils.debug_message('s_contacts.get_address - Type  '||p_type||'   Code  '||p_code, 3);

      IF  p_type = 'PEG'
      THEN
         s_address_usages.adr_refno_for_peg('PEG','CORRESPOND',p_code,l_adr_refno);
      ELSIF p_type IN ('COS','CON')
      THEN
         s_address_usages.adr_refno_for_cos('CON','CORRESPOND',p_code,l_adr_refno);
         l_addr := s_addresses.get_address(l_adr_refno);
         IF l_addr IS NULL
         THEN
            s_address_usages.adr_refno_for_cos('CON','PHYSICAL',p_code,l_adr_refno);
         END IF;
      ELSIF p_type = 'AUN'
      THEN
         s_address_usages.adr_refno_for_aun('AUN','PHYSICAL',p_code,l_adr_refno);
      END IF;

      l_addr := s_addresses.get_address(l_adr_refno);
      fsc_utils.proc_end;
      
   EXCEPTION
      WHEN OTHERS
      THEN
         fsc_utils.handle_exception;
   END get_address;

--
 
   FUNCTION get_adr_refno (p_type      IN VARCHAR2,
                                           p_code      IN VARCHAR2 DEFAULT NULL,
                                           p_refno     IN NUMBER DEFAULT NULL)
    RETURN addresses.adr_refno%TYPE
  IS
    l_adr_refno addresses.adr_refno%TYPE;
    l_adr_refno1 addresses.adr_refno%TYPE;
    l_address VARCHAR2(500);
  BEGIN
      fsc_utils.proc_start('s_dl_hcs_utils.get_adr_refno');
      fsc_utils.debug_message('s_dl_hcs_utils.get_adr_refno'  ||' p_type='||p_type||',p_code='||p_code||',p_refno='||p_refno ,3);

    IF p_code IS NOT NULL
    THEN
        get_address(p_code,p_type, l_adr_refno1, l_address);
    ELSE
        get_address(p_refno,p_type,l_adr_refno1, l_address);
    END IF;
    l_adr_refno := l_adr_refno1;
   RETURN (l_adr_refno);

   EXCEPTION
      WHEN OTHERS
      THEN
         fsc_utils.handle_exception;
   END get_adr_refno;

--
END s_dl_hcs_utils;
/

