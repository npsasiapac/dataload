create or replace PACKAGE s_dl_hcs_utils
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--
--     1.0             MOK  18/09/18     Initial Version
--     1.1             AJ   10/10/18     Change control added plus blank
--                                       lines and slash at bottom
--
-- ***********************************************************************
--
 PROCEDURE get_address (p_refno     IN NUMBER,
                                         p_type      IN VARCHAR2,
                                         l_adr_refno OUT NUMBER,
                                         l_addr      OUT VARCHAR2);
--
  PROCEDURE get_address (p_code     IN VARCHAR2,
                                          p_type      IN VARCHAR2,
                                          l_adr_refno OUT NUMBER,
                                          l_addr      OUT VARCHAR2);
--                         
  FUNCTION get_adr_refno ( p_type      IN VARCHAR2,
                                           p_code      IN VARCHAR2 DEFAULT NULL,
                                           p_refno     IN NUMBER DEFAULT NULL)
  RETURN addresses.adr_refno%TYPE;                         
--
END  s_dl_hcs_utils;
/
