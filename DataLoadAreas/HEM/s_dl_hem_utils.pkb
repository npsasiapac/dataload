CREATE OR REPLACE PACKAGE BODY s_dl_hem_utils
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN       WHY
--      1.0          PJD  Sep 2000   Product Dataload
--      1.1  5.1.4   PJD  02/02/2002 NVL on Free Format Address Insert
--      1.2  5.1.6   PJD  07/06/2002 Speeded up address insert by creating 3
--                                   different cursors for sia_addresses
--      1.3  5.1.6   PJD  11/06/2002 Added aus_papp_refno to address_insert
--      2.0  5.2.0   PJD  15/07/2002 Minor Corrections to address_insert
--      2.1  5.2.0   MH   21/11/2002 Details added for bankaccount addresses
--      3.0  5.3.0   PH   17/06/2003 Changed comp_stats to ESTIMATE rather
--                                   than COMPUTE statistics
--      3.1  5.3.0   PJD  20/10/2003 Another version of comp_stats that
--                                   accepts original No of rows param
--                                   Latest version of Insert_addresses
--      3.2  5.4.0   PJD  13/11/2003 Removed references to aus_tcy_refno
--                                   New function added dl_orig_rows
--                                   New function added hps_hpc_code_for_date
--      3.3  5.4.0   MH   13/02/2004 P_AUN_CODE insert missing from
--                                   insert_address - added to allow
--                                   admin unit address creation.
--      3.4  5.5.0   MH   09/03/2004 Added AND FRV_CURRENT_IND = 'Y'
--                                   as non-current FRV's were passing
--                                   validation.
--      3.5  5.5.0   PJD  21/06/2004 Added extra cursor c_ael_no_pcode into
--                                   insert_address
--      3.6  5.5.0   PJD  12/10/2004 Added extra cursors c_ael_no_street and
--                                   c_ael_no_street_no_pcode into
--                                   insert_address
--      3.7  5.8.0   PH   19/07/2005 Added new fields on insert address
--                                   adr_eastings and adr_northings
--      3.8  5.9.0   PH   03/03/2006 Amended Insert Address cursors
--                                   c_existing_sia_adr2 and ...adr3 to include
--                                   postcode to prevent picking up the wrong
--                                   address record
--      3.9  5.9.0   PH   07/03/2006 Amended  AEL cursors for the street index
--                                   being null. (insert_address)
--      4.0  5.10.0  PH   16/03/2007 Amended insert_address to cater for no street
--                                   and no street index, added variable
--                                   l_street_or_area if p_street is null
--      4.1  5.12.0  PH   17/07/07   Amended code to allow for Housing Advice
--                                   Cases, these are REG PHYSICAL Addresses.
--      5.0  5.13.1  PH   08-MAY-2008 Added new field on insert address
--                                    adr_uprn
--      5.1  5.15.1  PH   06-APR-2009 Added additional fields for Landlord
--                                    information to insert_address
--      5.2  6.1.1   PH   09-MAY-2010 Added UPRN, eastings and northings into
--                                    FFA insert section
--      6.0  6.6     PJD  06-NOV-2013 Changed (both versions of) Comp Stats to
--                                    use newer syntax.
--      6.1  6.9     PJD  31-JAN-2014 Add l_create_aus variable to
--                                    insert_address proc
--      6.1  6.11    AJ   18-JUN-2015 Added procedure insert_bank_details_mlang
--                                    used in s_dl_adim_units.pkb
--      6.2  6.11    PJD  30-JUN-2015 Added in chk_is_numeric and chk_is integer
--                                    functions 
--                                    plus first version of mlang_street
--      6.3  6.11    PJD  01-JUL-2015 Added licence check for bilingual
--                                    into insert_address and added
--                                    the mlang address_element fields 
--      6.4  6.15    DLB  07-AUG-2017 Changed c_ffa_format to allow for NULL
--                                    address components so the commas are only used
--                                    if needed.
--      6.5  6.17.1  PLL  14-DEC-2018 Changed to allow for more than 1000 Addresses 
--                                    for different streets starting with the same 5 
--                                    characters.
-- ***********************************************************************
--
--
FUNCTION exists_propref
       (p_propref IN VARCHAR2 ) RETURN BOOLEAN
 IS
  CURSOR c_exists(cp_propref IN VARCHAR2) IS
   SELECT 'x'
    FROM  properties
   WHERE  pro_propref   = cp_propref;
   l_exists VARCHAR2(1) := NULL;
   l_result BOOLEAN := FALSE;
BEGIN
--    fsc_utils.proc_start('s_dl_hem_utils.exists_propref',p_ele_code);
--    fsc_utils.debug_message('s_dl_hem_utils.exists_propref',3);
    OPEN  c_exists(p_propref);
    FETCH c_exists INTO l_exists;
    IF c_exists%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_exists;
--    fsc_utils.proc_END;
    RETURN( l_result );
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
END exists_propref;
--
FUNCTION pro_refno_for_propref
       (p_propref IN VARCHAR2 ) RETURN NUMBER
 IS
  CURSOR c_pro_refno(cp_propref IN VARCHAR2) IS
   SELECT pro_refno
    FROM  properties
   WHERE  pro_propref   = cp_propref;
   l_pro_refno NUMBER := NULL;
BEGIN
--    fsc_utils.proc_start('s_dl_hem_utils.exists_propref',p_ele_code);
--    fsc_utils.debug_message('s_dl_hem_utils.exists_propref',3);
    OPEN  c_pro_refno(p_propref);
    FETCH c_pro_refno INTO l_pro_refno;
    CLOSE c_pro_refno;
--    fsc_utils.proc_END;
    RETURN( l_pro_refno );
--
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
--
END pro_refno_for_propref;
--
FUNCTION exists_frv
       (p_domain     IN VARCHAR2,
        p_code       IN VARCHAR2,
        p_allow_null IN VARCHAR2 default 'N' )
        RETURN BOOLEAN
  IS
  CURSOR c_exists(p_domain IN VARCHAR2,p_code IN VARCHAR2) IS
   SELECT  'x'
     FROM  first_ref_values
    WHERE  frv_frd_domain   = p_domain
      AND  frv_code         = p_code
      AND  frv_current_ind  = 'Y';
--
   l_exists VARCHAR2(1) := NULL;
   l_RETURN BOOLEAN := FALSE;
BEGIN
-- fsc_utils.proc_start('s_dl_hem_utils.exists_frv',p_domain);
-- fsc_utils.debug_message('s_dl_hem_utils.exists_frv',3);
--
IF p_code IS null
  THEN
  IF p_allow_null = 'N'
     THEN
     l_RETURN := FALSE;
  ELSE
     l_RETURN := TRUE;
  END IF;
ELSE
  OPEN  c_exists(p_domain,p_code);
  FETCH c_exists INTO l_exists;
  IF c_exists%FOUND THEN
     l_RETURN := TRUE;
  END IF;
  CLOSE c_exists;
--
END IF;
--
-- fsc_utils.proc_END;
--
    RETURN( l_RETURN );
--
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
--
END exists_frv;
--
FUNCTION exists_hrv
         (p_table_name IN VARCHAR2, p_code IN VARCHAR2)
        RETURN BOOLEAN
IS
l_value     varchar2(100);
l_exists    number;
l_result    BOOLEAN := FALSE;
--
BEGIN
l_value :=
'SELECT 1 FROM '||p_table_name||' WHERE frv_code = '''||p_code||'''';
--     fsc_utils.debug_message('F : exists_hrv('||l_value||')',3);
-- run sql
-- dbms_output.put_line(l_value);
EXECUTE IMMEDIATE l_value
INTO l_exists;
IF l_exists IS not null THEN l_result := TRUE; END IF;
--
--    fsc_utils.proc_END;
RETURN (l_result );
--
EXCEPTION
WHEN   Others THEN
fsc_utils.handle_exception;
--
END exists_hrv;
--
 FUNCTION yornornull
          (p_value in VARCHAR2)
           RETURN BOOLEAN
IS
l_result BOOLEAN := FALSE;
--
BEGIN
IF p_value in ('Y','N')
THEN l_result := TRUE;
elsif p_value IS null
THEN l_result := TRUE;
END IF;
--
RETURN (l_result);
--
END yornornull;
--
--
FUNCTION yorn
          (p_value in VARCHAR2)
          RETURN BOOLEAN
IS
l_result BOOLEAN := FALSE;
--
BEGIN
IF p_value in ('Y','N')
THEN l_result := TRUE;
END IF;
--
RETURN (l_result);
--
END yorn;
--
FUNCTION dateorder
          (p_date1 in DATE,
           p_date2 in DATE)
           RETURN BOOLEAN
IS
l_result BOOLEAN := FALSE;
--
BEGIN
IF p_date1 < p_date2
THEN l_result := TRUE;
END IF;
--
RETURN (l_result);
--
END dateorder;
--
FUNCTION exists_aun_code
       (p_aun_code IN VARCHAR2 ) RETURN BOOLEAN
 IS
  CURSOR c_exists(cp_aun_code  VARCHAR2) IS
   SELECT 'x'
    FROM  admin_units
   WHERE  aun_code   = cp_aun_code;
   l_exists VARCHAR2(1) := NULL;
   l_result BOOLEAN := FALSE;
BEGIN
--    fsc_utils.proc_start('s_dl_hem_utils.exists_aun_code',p_aun_code);
--    fsc_utils.debug_message('s_dl_hem_utils.exists_aun_code',p_aun_code);
    OPEN  c_exists(p_aun_code);
    FETCH c_exists INTO l_exists;
    IF c_exists%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_exists;
--    fsc_utils.proc_END;
    RETURN( l_result );
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
END exists_aun_code;
--
FUNCTION tcy_refno_for_alt_ref
       (p_alt_ref  IN VARCHAR2 ) RETURN INTEGER
IS
--
CURSOR c_tcy_refno IS
SELECT tcy_refno
FROM tenancies
WHERE tcy_alt_ref = p_alt_ref;
--
l_tcy_refno integer;
--
BEGIN
--
OPEN  c_tcy_refno;
FETCH c_tcy_refno INTO l_tcy_refno;
CLOSE c_tcy_refno;
--
RETURN( l_tcy_refno);
--
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
--
END tcy_refno_for_alt_ref;
--
FUNCTION tcy_start_date_for_alt_ref
       (p_alt_ref  IN VARCHAR2 ) RETURN DATE
IS
--
CURSOR c_tcy_refno IS
SELECT tcy_act_start_date
FROM tenancies
WHERE tcy_alt_ref = p_alt_ref;
--
l_tcy_start_date DATE;
--
BEGIN
--
OPEN  c_tcy_refno;
FETCH c_tcy_refno INTO l_tcy_start_date;
CLOSE c_tcy_refno;
--
RETURN( l_tcy_start_date);
--
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
--
END tcy_start_date_for_alt_ref;
--
FUNCTION tho_tcy_refno_for_date
         (p_pro_propref IN VARCHAR2,
          p_start_date  IN DATE    ,
          p_end_date    IN DATE    ) RETURN INTEGER
IS
--
CURSOR c_tcy_refno IS
SELECT tho_tcy_refno
FROM tenancy_holdings,properties
WHERE pro_propref = p_pro_propref
  and tho_pro_refno    = pro_refno
  and (   p_start_date between tho_start_date and nvl(tho_end_date,sysdate)
       or
          p_end_date   between tho_start_date and nvl(tho_end_date,sysdate)
      );
--
l_tcy_refno integer;
--
BEGIN
--
OPEN  c_tcy_refno;
FETCH c_tcy_refno INTO l_tcy_refno;
CLOSE c_tcy_refno;
--
RETURN( l_tcy_refno);
--
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
--
END tho_tcy_refno_for_date;
--
--
PROCEDURE get_address
         (p_mode                  IN OUT varchar2
         ,p_street_index_code IN OUT varchar2
         ,p_adr_refno             IN OUT integer
         ,p_flat                  IN varchar2
         ,p_building              IN varchar2
         ,p_street_number         IN varchar2
         ,p_sub_street1           IN varchar2
         ,p_sub_street2           IN varchar2
         ,p_sub_street3           IN varchar2
         ,p_street                IN varchar2
         ,p_area                  IN varchar2
         ,p_town                  IN varchar2
         ,p_county                IN varchar2
         ,p_pcode                 IN varchar2
         ) AS
--
CURSOR c_ael IS
SELECT ael_street_index_code
FROM address_elements
WHERE ael_street_index_code          = nvl(p_street_index_code,ael_street_index_code)
  and nvl(ael_sub_street1,'~')       = nvl(p_sub_street1,'~')
  and nvl(ael_sub_street2,'~')       = nvl(p_sub_street2,'~')
  and nvl(ael_sub_street3,'~')       = nvl(p_sub_street3,'~')
  and nvl(ael_street,'~')            = nvl(p_street,'~')
  and nvl(ael_area,'~')              = nvl(p_area,'~')
  and nvl(ael_town,'~')              = nvl(p_town,'~')
  and nvl(ael_postcode,'~')          = nvl(p_pcode,'~')
  and nvl(ael_county,'~')            = nvl(p_county,'~');
--
CURSOR c_adr IS
SELECT adr_refno
FROM addresses
WHERE adr_refno                      = nvl(p_adr_refno,adr_refno)
  and nvl(adr_flat,'~')              = nvl(p_flat,'~')
  and nvl(adr_building,'~')          = nvl(p_building,'~')
  and nvl(adr_street_number,'~')     = nvl(p_street_number,'~');
--
l_street_index_code varchar2(12);
l_adr_refno             integer;
--
-- 2 modes
-- Mode L = look and see IF an entry exists for this address
-- Mode C = check that the address IS correct for that code/refno
BEGIN
--
IF p_mode = 'L'
THEN
  OPEN  c_ael;
  FETCH c_ael INTO l_street_index_code;
  IF c_ael%found THEN
    p_mode := 'I';
    p_street_index_code := l_street_index_code;
  END IF;
  CLOSE c_ael;
--
  IF p_mode = 'I' THEN
    OPEN  c_adr;
    FETCH c_adr INTO l_adr_refno;
    IF c_adr%found THEN
      p_mode := 'A';
      p_adr_refno := l_adr_refno;
    END IF;
    CLOSE c_adr;
  END IF;
--
elsif p_mode = 'C'
THEN
   OPEN  c_ael;
   FETCH c_ael INTO l_street_index_code;
   IF c_ael%found THEN
     IF l_street_index_code = p_street_index_code
        THEN p_mode := 'V';
        OPEN  c_adr;
        FETCH c_adr INTO l_adr_refno;
        IF c_adr%found THEN p_mode := 'W'; p_adr_refno := l_adr_refno; END IF;
        CLOSE c_adr;
     END IF;
   ELSE p_mode := 'X';
   CLOSE c_ael;
   END IF;
END IF;
--
-- So the p_mode could now be;-
-- L = Looked but could not find a similar existing street index code
-- I = Found a suitable street index code but not an existing adr_refno
-- A = Found a suitable street index code and adr_refno
-- C = street index code IS wrong for the address supplied.
-- V = street index code IS correct but no entry in addresses
-- W = street index code IS correct and an entry exists in addresses
-- X = street index code does not exist.
--
END get_address;
--
PROCEDURE insert_address
         (p_fao_code              IN varchar2 DEFAULT NULL
         ,p_far_code              IN varchar2 DEFAULT NULL
         ,p_street_index_code     IN OUT varchar2
         ,p_adr_refno             IN OUT integer
         ,p_flat                  IN varchar2
         ,p_building              IN varchar2
         ,p_street_number         IN varchar2
         ,p_sub_street1           IN varchar2 DEFAULT NULL
         ,p_sub_street2           IN varchar2 DEFAULT NULL
         ,p_sub_street3           IN varchar2 DEFAULT NULL
         ,p_street                IN varchar2
         ,p_area                  IN varchar2
         ,p_town                  IN varchar2
         ,p_county                IN varchar2
         ,p_pcode                 IN varchar2
         ,p_country               IN varchar2
         ,p_aun_code              IN varchar2 DEFAULT NULL
         ,p_pro_refno             IN number DEFAULT NULL
         ,p_par_refno             IN number DEFAULT NULL
         ,p_tcy_refno             IN number DEFAULT NULL
         ,p_bde_refno             IN number DEFAULT NULL
         ,p_pof_refno             IN number DEFAULT NULL
         ,p_start_date            IN date
         ,p_end_date              IN date
         ,p_local_ind             IN varchar2 default 'N'
         ,p_abroad_ind            IN varchar2 default 'Y'
         ,p_app_refno             IN number   default NULL
         ,p_cos_code              IN varchar2 default NULL
         ,p_cou_code              IN varchar2 default NULL
         ,p_aer_id                IN NUMBER   default NULL
         ,p_nom_refno             IN NUMBER   default NULL
         ,p_papp_refno            IN NUMBER   default NULL
         ,p_aus_contact_name      IN VARCHAR2 default NULL
         ,p_eastings              IN VARCHAR2
         ,p_northings             IN VARCHAR2
         ,p_acas_reference        IN NUMBER   default NULL
         ,p_uprn                  IN VARCHAR2
         ,p_landlord_par_refno    IN NUMBER DEFAULT NULL
         ,p_llt_code              IN VARCHAR2 DEFAULT NULL
         ,p_aat_code              IN VARCHAR2 DEFAULT NULL
         ,p_pty_code              IN VARCHAR2 DEFAULT NULL
         ,p_property_size         IN VARCHAR2 DEFAULT NULL
         ,p_floor_level           IN NUMBER DEFAULT NULL
         ,p_hrv_alr_code          IN VARCHAR2 DEFAULT NULL
         ,p_tenancy_leave_date    IN DATE DEFAULT NULL
         ,p_arrears_amount        IN NUMBER DEFAULT NULL
         ,p_storage_ind           IN VARCHAR2 DEFAULT NULL
         ,p_storage_unit_cost     IN NUMBER DEFAULT NULL
         ,p_storage_cost          IN NUMBER DEFAULT NULL
) AS
--
CURSOR c_ael IS
SELECT ael_street_index_code
FROM address_elements
WHERE ael_street_index_code          = nvl(p_street_index_code,ael_street_index_code)
  and nvl(ael_sub_street1,'~')       = nvl(p_sub_street1,'~')
  and nvl(ael_sub_street2,'~')       = nvl(p_sub_street2,'~')
  and nvl(ael_sub_street3,'~')       = nvl(p_sub_street3,'~')
  and nvl(ael_street,'~')            = nvl(p_street,'~')
  and nvl(ael_area,'~')              = nvl(p_area,'~')
  and nvl(ael_town,'~')              = nvl(p_town,'~')
  and nvl(ael_county,'~')            = nvl(p_county,'~')
  and nvl(ael_postcode,'~')          = nvl(p_pcode,'~')
  and nvl(ael_country,'~')           = nvl(p_country,'~');
--
CURSOR c_ael_no_street IS
SELECT ael_street_index_code
FROM address_elements
WHERE ael_street_index_code          = nvl(p_street_index_code,ael_street_index_code)
  and nvl(ael_sub_street1,'~')       = nvl(p_sub_street1,'~')
  and nvl(ael_sub_street2,'~')       = nvl(p_sub_street2,'~')
  and nvl(ael_sub_street3,'~')       = nvl(p_sub_street3,'~')
  and nvl(ael_street,'~')            = nvl(p_street,'~')
  and nvl(ael_area,'~')              = nvl(p_area,'~')
  and nvl(ael_town,'~')              = nvl(p_town,'~')
  and nvl(ael_county,'~')            = nvl(p_county,'~')
  and nvl(ael_postcode,'~')          = nvl(p_pcode,'~')
  and nvl(ael_country,'~')           = nvl(p_country,'~');
--
CURSOR c_ael_no_pcode IS
SELECT ael_street_index_code
FROM address_elements
WHERE ael_street_index_code          = nvl(p_street_index_code,ael_street_index_code)
  and nvl(ael_sub_street1,'~')       = nvl(p_sub_street1,'~')
  and nvl(ael_sub_street2,'~')       = nvl(p_sub_street2,'~')
  and nvl(ael_sub_street3,'~')       = nvl(p_sub_street3,'~')
  and nvl(ael_street,'~')            = nvl(p_street,'~')
  and nvl(ael_area,'~')              = nvl(p_area,'~')
  and nvl(ael_town,'~')              = nvl(p_town,'~')
  and nvl(ael_county,'~')            = nvl(p_county,'~')
  and nvl(ael_country,'~')           = nvl(p_country,'~');
--
CURSOR c_ael_no_street_no_pcode IS
SELECT ael_street_index_code
FROM address_elements
WHERE ael_street_index_code          = nvl(p_street_index_code,ael_street_index_code)
  and nvl(ael_sub_street1,'~')       = nvl(p_sub_street1,'~')
  and nvl(ael_sub_street2,'~')       = nvl(p_sub_street2,'~')
  and nvl(ael_sub_street3,'~')       = nvl(p_sub_street3,'~')
  and nvl(ael_street,'~')            = nvl(p_street,'~')
  and nvl(ael_area,'~')              = nvl(p_area,'~')
  and nvl(ael_town,'~')              = nvl(p_town,'~')
  and nvl(ael_county,'~')            = nvl(p_county,'~')
  and nvl(ael_country,'~')           = nvl(p_country,'~');
--
CURSOR c_adr (p_adr_refno number   ,p_flat          varchar2
             ,p_building  varchar2 ,p_street_number varchar2
             ,p_ael_street varchar2)
IS
SELECT adr_refno
FROM addresses
WHERE adr_refno                      = nvl(p_adr_refno,adr_refno)
  and nvl(adr_flat,'~')              = nvl(p_flat,'~')
  and nvl(adr_building,'~')          = nvl(p_building,'~')
  and nvl(adr_street_number,'~')     = nvl(p_street_number,'~');
--
CURSOR c_existing_sia_adr1
             (p_adr_refno number   ,p_flat          varchar2
             ,p_building  varchar2 ,p_street_number varchar2
             ,p_ael_street_index_code varchar2)
IS
SELECT adr_refno
FROM addresses
WHERE adr_refno                      = p_adr_refno
  and nvl(adr_flat,'~')              = nvl(p_flat,'~')
  and nvl(adr_building,'~')          = nvl(p_building,'~')
  and nvl(adr_street_number,'~')     = nvl(p_street_number,'~')
  and nvl(adr_ael_street_index_code,'~')
                                     = nvl(p_ael_street_index_code,'~');
--
CURSOR c_existing_sia_adr2
             (p_adr_refno number   ,p_flat          varchar2
             ,p_building  varchar2 ,p_street_number varchar2
             ,p_ael_street_index_code varchar2, p_pcode varchar2)
IS
SELECT adr_refno
FROM addresses
WHERE adr_refno                      = nvl(p_adr_refno,adr_refno)
  and nvl(adr_flat,'~')              = nvl(p_flat,'~')
  and nvl(adr_building,'~')          = nvl(p_building,'~')
  and nvl(adr_street_number,'~')     = nvl(p_street_number,'~')
  and adr_ael_street_index_code
                                     = p_ael_street_index_code
  and nvl(adr_postcode,'~')          = nvl(p_pcode,'~');
--
CURSOR c_existing_sia_adr3
             (p_adr_refno number   ,p_flat          varchar2
             ,p_building  varchar2 ,p_street_number varchar2
             ,p_ael_street_index_code varchar2, p_pcode varchar2)
IS
SELECT adr_refno
FROM addresses
WHERE adr_refno                      = nvl(p_adr_refno,adr_refno)
  and nvl(adr_flat,'~')              = nvl(p_flat,'~')
  and nvl(adr_building,'~')          = nvl(p_building,'~')
  and nvl(adr_street_number,'~')     = nvl(p_street_number,'~')
  and nvl(adr_ael_street_index_code,'~')
                                     = nvl(p_ael_street_index_code,'~')
  and nvl(adr_postcode,'~')          = nvl(p_pcode,'~');
--
CURSOR c_street_index(p_street varchar2) IS
SELECT to_char(max(to_number(substr(ael_street_index_code,6,4)))+1) st_suffix -- change for 6.5
FROM address_elements
WHERE substr(ael_street_index_code,1,5) = rpad(substr(replace(p_street,' ','_'),1,5),5,'_')
  and substr(ael_street_index_code,6,1) in
       ('0','1','2','3','4','5','6','7','8','9')
  and nvl(substr(ael_street_index_code,7,1),'0') in
       ('0','1','2','3','4','5','6','7','8','9')
  and nvl(substr(ael_street_index_code,8,1),'0')
                      in ('0','1','2','3','4','5','6','7','8','9')
  and nvl(substr(ael_street_index_code,9,1),'0')
                      in ('0','1','2','3','4','5','6','7','8','9');  -- change for 6.5
--
CURSOR c_adr_refno IS
SELECT adr_refno_seq.nextval FROM dual;
--
CURSOR c_existing_adr_refno(p_add_line1 VARCHAR2, p_add_line2 VARCHAR2
 ,p_add_line3 VARCHAR2) is
SELECT adr_refno from addresses
where adr_free_text1 = p_add_line1
and   nvl(adr_free_text2,'X') = nvl(p_add_line2,'X')
and   nvl(adr_free_text3,'X') = nvl(p_add_line3,'X');
--
CURSOR c_aut_format IS
SELECT aut_format_ind FROM address_usage_types
WHERE aut_fao_code = p_fao_code
  and aut_far_code = p_far_code;
--
CURSOR c_ffa_format is
SELECT substr(
              decode(p_flat,null,null,decode(p_building,null,p_flat,p_flat||', '))||
              decode(p_building,null,null,decode(p_street_number,null,p_building,p_building||', '))||
              decode(p_street_number,null,null,decode(p_sub_street1,null,p_street_number,p_street_number||', '))||
              decode(p_sub_street1,null,null,decode(p_sub_street2,null,p_sub_street1,p_sub_street1||','))||
              decode(p_sub_street2,null,null,decode(p_sub_street3,null,p_sub_street2,p_sub_street2||','))||
              decode(p_sub_street3,null,null,decode(p_street,null,p_sub_street3,p_sub_street3||','))||
              p_street,
              1,240),
              p_area,
              substr(
              decode(p_town,null,null,decode(p_county,null,p_town,p_town||', '))||
              decode(p_county,null,null,decode(p_country,null,p_county,p_county||','))||
              p_country,
        1,240)
FROM dual;

--
l_street_index_code varchar2(12);
l_street_index_suff varchar2(4);
l_adr_refno         integer;
l_adr_type          varchar2(3);
l_adr_free_text1    varchar2(240);
l_adr_free_text2    varchar2(240);
l_adr_free_text3    varchar2(240);
l_post_code         varchar2(10);
l_street_or_area    varchar2(100);
l_create_aus        VARCHAR2(1) := 'N';
l_ael_street_mlang    VARCHAR2(100);
l_ael_area_mlang      VARCHAR2(50);
l_ael_town_mlang      VARCHAR2(50);
l_ael_county_mlang    VARCHAR2(50);
l_ael_postcode_mlang  VARCHAR2(10);
l_ael_country_mlang   VARCHAR2(50);
--
--
BEGIN
--  note that PDI PEF, RAC and DEP addres types are mentioned in
--  the trigger aus_br_iu2 but are not currently included here
--
IF  (  p_aun_code              IS NOT NULL
    OR p_pro_refno             IS NOT NULL
    OR p_par_refno             IS NOT NULL
    OR p_tcy_refno             IS NOT NULL
    OR p_bde_refno             IS NOT NULL
    OR p_pof_refno             IS NOT NULL
    OR p_app_refno             IS NOT NULL
    OR p_cos_code              IS NOT NULL
    OR p_cou_code              IS NOT NULL
    OR p_aer_id                IS NOT NULL
    OR p_nom_refno             IS NOT NULL
    OR p_papp_refno            IS NOT NULL
    OR p_acas_reference        IS NOT NULL
    )
    THEN l_create_aus := 'Y';
END IF;

-- The first Question is whether the target address format is structured or
-- free format.
open  c_aut_format;
fetch c_aut_format into l_adr_type;
close c_aut_format;
--
IF l_adr_type IS NULL
THEN
  l_adr_type := 'SIA';
END IF;

--
-- IF P_STREET_INDEX_CODE IS NOT NULL THEN
-- DBMS_OUTPUT.PUT_LINE('P_STREET_INDEX_CODE SUPPLIED');
-- END IF;
--
-- DBMS_OUTPUT.PUT_LINE('P_STREET = '||p_street);
-- DBMS_OUTPUT.PUT_LINE('P_PRO_REFNO = '||p_pro_refno);
-- DBMS_OUTPUT.PUT_LINE('P_tcy_refno = '||p_tcy_refno);
-- DBMS_OUTPUT.PUT_LINE('P_app_refno = '||p_app_refno);
--
IF l_adr_type = 'FFA' THEN
-- need to sort into 3 lines
-- DBMS_OUTPUT.PUT_LINE('l_adr_rtpe in FFA');
  open  c_ffa_format;
  fetch c_ffa_format into l_adr_free_text1,l_adr_free_text2,l_adr_free_text3;
  close c_ffa_format;
--
-- Do we need to see if there is an address that matches this one?
l_adr_refno := null;
--
  OPEN c_existing_adr_refno(l_adr_free_text1,l_adr_free_text2,
  l_adr_free_text3);
  FETCH c_existing_adr_refno into l_adr_refno;
  CLOSE c_existing_adr_refno;
--
  IF l_adr_refno IS NULL THEN
    OPEN  c_adr_refno;
    FETCH c_adr_refno into l_adr_refno;
    CLOSE c_adr_refno;
--
    INSERT INTO addresses
    (adr_refno
    ,adr_type
    ,adr_local_ind
    ,adr_abroad_ind
    ,adr_postcode
    ,adr_free_text1
    ,adr_free_text2
    ,adr_free_text3
    ,adr_eastings
    ,adr_northings
    ,adr_uprn)
    values
    (l_adr_refno
    ,'FFA'
    ,nvl(p_local_ind,'Y')
    ,nvl(p_abroad_ind,'N')
    ,p_pcode
    ,l_adr_free_text1
    ,l_adr_free_text2
    ,l_adr_free_text3
    ,p_eastings
    ,p_northings
    ,p_uprn);
--
  END IF;
--
  IF l_create_aus = 'Y'
  THEN
    INSERT INTO address_usages
    (aus_aut_fao_code
    ,aus_aut_far_code
    ,aus_start_date
    ,aus_adr_refno
    ,aus_aun_code
    ,aus_pro_refno
    ,aus_par_refno
    ,aus_bde_refno
    ,aus_end_date
    ,aus_app_refno
    ,aus_cos_code
    ,aus_cou_code
    ,aus_aer_id
    ,aus_nom_refno
    ,aus_papp_refno
    ,aus_contact_name
    ,aus_acas_reference
    ,aus_landlord_par_refno
    ,aus_hrv_llt_code
    ,aus_hrv_aat_code
    ,aus_pty_code
    ,aus_property_size
    ,aus_floor_level
    ,aus_hrv_alr_code
    ,aus_tenancy_leave_date
    ,aus_arrears_amount
    ,aus_storage_ind
    ,aus_storage_unit_cost
    ,aus_storage_cost
    )
    values
    (p_fao_code
    ,p_far_code
    ,p_start_date
    ,l_adr_refno
    ,p_aun_code
    ,p_pro_refno
    ,p_par_refno
    ,p_bde_refno
    ,p_end_date
    ,p_app_refno
    ,p_cos_code
    ,p_cou_code
    ,p_aer_id
    ,p_nom_refno
    ,p_papp_refno
    ,p_aus_contact_name
    ,p_acas_reference
    ,p_landlord_par_refno
    ,p_llt_code
    ,p_aat_code
    ,p_pty_code
    ,p_property_size
    ,p_floor_level
    ,p_hrv_alr_code
    ,p_tenancy_leave_date
    ,p_arrears_amount
    ,p_storage_ind
    ,p_storage_unit_cost
    ,p_storage_cost);
  END IF;
END IF;
--
-- thats it for free format
-- now for structured
--
IF (l_adr_type = 'SIA' OR l_adr_type = 'VAD')
 THEN
--
-- DBMS_OUTPUT.PUT_LINE('l_adr_rtpe in SIA VAD');
-- Look for a matching street index
--
-- Changed PJD 12 Oct 2004
-- As Street is no longer mandatory we will need to go down 2 possible routes
-- first one - street supplied is...
  IF p_street IS NOT NULL
  THEN
    -- DBMS_OUTPUT.PUT_LINE('p_street is not null ');
    l_street_index_code := null;
    OPEN c_ael;
    FETCH c_ael INTO l_street_index_code;
    CLOSE c_ael;
    -- Now it could be just that the post code doen't match - so
    IF l_street_index_code IS NULL
    THEN
      OPEN c_ael_no_pcode;
      FETCH c_ael_no_pcode INTO l_street_index_code;
      CLOSE c_ael_no_pcode;
    END IF;
  --
  -- Else p_street is blank
  --
  ELSE
    -- DBMS_OUTPUT.PUT_LINE('p_street is null ');
    l_street_index_code := null;
    OPEN c_ael_no_street;
    FETCH c_ael_no_street INTO l_street_index_code;
    CLOSE c_ael_no_street;
    -- again it could be just that the post code doen't match - so
    IF l_street_index_code IS NULL
    THEN
      OPEN c_ael_no_street_no_pcode;
      FETCH c_ael_no_street_no_pcode INTO l_street_index_code;
      CLOSE c_ael_no_street_no_pcode;
    END IF;
  END IF;
--
-- Do we have a valid street index code - if not insert into street_indexes
--
  IF l_street_index_code IS NULL
    THEN
      -- DBMS_OUTPUT.PUT_LINE('l_street_index_code IS NULL');
    IF p_street_index_code IS NOT NULL THEN
       l_street_index_code := p_street_index_code;
    ELSE
--
l_street_or_area := nvl(p_street, nvl(p_area,p_town));
--
      OPEN c_street_index(l_street_or_area);
      FETCH c_street_index INTO l_street_index_suff;
      CLOSE c_street_index;
      IF l_street_index_suff is null
         THEN l_street_index_suff := '1';
      END IF;
--
--    was replace(substr(p_street,1,5),' ','_')
      l_street_index_code :=  rpad(substr(replace(l_street_or_area,' ','_'),1,5),5,'_')
                              ||l_street_index_suff;
      p_street_index_code := l_street_index_code;
   END IF; -- p_street_index_code is not null
  -- dbms_output.put_line('l street_index = '||l_street_index_code);
  -- dbms_output.put_line('p street_index = '||p_street_index_code);
  -- dbms_output.put_line(p_sub_street1||'-'||p_sub_street2||'-'||p_sub_street3);
  -- dbms_output.put_line(p_street||'-'||p_area||'-'||p_town||'-'||p_county);
  --
    IF s_licences.f_verify_licence2('BILINGUAL')
    THEN
      l_ael_street_mlang    := mlang_street(p_street);
      l_ael_area_mlang      := p_area;
      l_ael_town_mlang      := p_town;
      l_ael_county_mlang    := p_county;
      l_ael_postcode_mlang  := p_pcode;
      l_ael_country_mlang   := p_country;
    END IF;

    INSERT INTO address_elements
    (ael_street_index_code
    ,ael_creation_date
    ,ael_created_by
    ,ael_local_ind
    ,ael_abroad_ind
    ,ael_sub_street1
    ,ael_sub_street2
    ,ael_sub_street3
    ,ael_street
    ,ael_area
    ,ael_town
    ,ael_county
    ,ael_postcode
    ,ael_country
    ,ael_street_mlang
    ,ael_area_mlang
    ,ael_town_mlang
    ,ael_county_mlang
    ,ael_postcode_mlang
    ,ael_country_mlang)
    VALUES
    (p_street_index_code
    ,sysdate
    ,user
    ,nvl(p_local_ind,'Y')
    ,nvl(p_abroad_ind,'N')
    ,p_sub_street1
    ,p_sub_street2
    ,p_sub_street3
    ,p_street
    ,p_area
    ,p_town
    ,p_county
    ,p_pcode
    ,p_country
    ,l_ael_street_mlang
    ,l_ael_area_mlang
    ,l_ael_town_mlang
    ,l_ael_county_mlang  
    ,l_ael_postcode_mlang
    ,l_ael_country_mlang);
--
  END IF;
--
-- Do we have a valid address refno
-- Added PD 171001
  IF (    p_street_index_code IS NULL
      AND l_street_index_code IS NOT NULL) THEN
  p_street_index_code := l_street_index_code;
  END IF;
-- End of Added Section
--
IF p_adr_refno IS NOT NULL
THEN
  OPEN c_existing_sia_adr1 (p_adr_refno,p_flat,p_building,p_street_number
                        ,p_street_index_code);
  FETCH c_existing_sia_adr1 INTO l_adr_refno;
  CLOSE c_existing_sia_adr1;
ELSIF p_street_index_code IS NOT NULL
THEN
  OPEN c_existing_sia_adr2 (p_adr_refno,p_flat,p_building,p_street_number
                        ,p_street_index_code, p_pcode);
  FETCH c_existing_sia_adr2 INTO l_adr_refno;
  CLOSE c_existing_sia_adr2;
ELSE
  OPEN c_existing_sia_adr3 (p_adr_refno,p_flat,p_building,p_street_number
                        ,p_street_index_code, p_pcode);
  FETCH c_existing_sia_adr3 INTO l_adr_refno;
  CLOSE c_existing_sia_adr3;
END IF;
--
-- DBMS_OUTPUT.PUT_LINE('Adr Refno (1)      '||l_adr_Refno);
--
    p_adr_refno := l_adr_refno;
--
  IF l_adr_refno is null then
    OPEN  c_adr_refno;
    FETCH c_adr_refno into l_adr_refno;
    CLOSE c_adr_refno;
--
    p_adr_refno := l_adr_refno;
--
-- DBMS_OUTPUT.PUT_LINE('Inserting address '||l_adr_Refno);

    INSERT INTO addresses
    (adr_refno
    ,adr_type
    ,adr_ael_street_index_code
    ,adr_local_ind
    ,adr_abroad_ind
    ,adr_postcode
    ,adr_flat
    ,adr_building
    ,adr_street_number
    ,adr_eastings
    ,adr_northings
    ,adr_uprn)
    values
    (p_adr_refno
    ,l_adr_type
    ,p_street_index_code
    ,nvl(p_local_ind,'Y')
    ,nvl(p_abroad_ind,'N')
    ,p_pcode
    ,p_flat
    ,p_building
    ,p_street_number
    ,p_eastings
    ,p_northings
    ,p_uprn);
--
  END IF;
--
--  Now we can insert into Address Usages
--
  IF l_create_aus = 'Y'
  THEN
    INSERT INTO address_usages
    (aus_aut_fao_code
    ,aus_aut_far_code
    ,aus_start_date
    ,aus_adr_refno
    ,aus_pro_refno
    ,aus_aun_code
    ,aus_par_refno
    ,aus_bde_refno
    ,aus_end_date
    ,aus_app_refno
    ,aus_cos_code
    ,aus_cou_code
    ,aus_aer_id
    ,aus_nom_refno
    ,aus_papp_refno
    ,aus_contact_name
    ,aus_acas_reference
    ,aus_landlord_par_refno
    ,aus_hrv_llt_code
    ,aus_hrv_aat_code
    ,aus_pty_code
    ,aus_property_size
    ,aus_floor_level
    ,aus_hrv_alr_code
    ,aus_tenancy_leave_date
    ,aus_arrears_amount
    ,aus_storage_ind
    ,aus_storage_unit_cost
    ,aus_storage_cost
    )
    values
    (p_fao_code
    ,p_far_code
    ,p_start_date
    ,l_adr_refno
    ,p_pro_refno
    ,p_aun_code
    ,p_par_refno
    ,p_bde_refno
    ,p_end_date
    ,p_app_refno
    ,p_cos_code
    ,p_cou_code
    ,p_aer_id
    ,p_nom_refno
    ,p_papp_refno
    ,p_aus_contact_name
    ,p_acas_reference
    ,p_landlord_par_refno
    ,p_llt_code
    ,p_aat_code
    ,p_pty_code
    ,p_property_size
    ,p_floor_level
    ,p_hrv_alr_code
    ,p_tenancy_leave_date
    ,p_arrears_amount
    ,p_storage_ind
    ,p_storage_unit_cost
    ,p_storage_cost
    );
  --
  END IF;
END IF;
END insert_address;
--
--
PROCEDURE display_error
         (p_batch              IN varchar2
         ,p_process            IN varchar2
         ,p_date               IN date
         ,p_table              IN varchar2
         ,p_sequence           IN number
         ,p_hdl                IN varchar2
         ,p_code               IN number
         ) IS
begin
dbms_output.put_line(p_batch||' '||p_process||' '||p_date||' '||p_table||
                     p_sequence||' '||p_hdl||' '||p_code);
end display_error;
--
-- Second Version of dl_comp_stats
--
FUNCTION dl_comp_stats
         (p_table_name IN VARCHAR2
         ,p_orig_rows  IN INTEGER
         ,p_proc_rows  IN INTEGER)
        RETURN VARCHAR2
IS
--
CURSOR c1 IS
SELECT owner
FROM all_tables
WHERE table_name = p_table_name;
--
l_value       VARCHAR2(100);
l_exists      NUMBER;
l_result      VARCHAR2(1);
l_table_owner VARCHAR2(30);
l_orig_rows   INTEGER;
--
BEGIN
--
  l_result := 'Y';
  --
  l_orig_rows := greatest(p_orig_rows,1);
  --
  IF p_proc_rows < (l_orig_rows *10)
  THEN
    l_result := 'N';
  --
  ELSE
  --
  BEGIN
    OPEN  c1;
    FETCH c1 INTO l_table_owner;
    CLOSE c1;
    --
    --
    dbms_stats.gather_table_stats(ownname => l_table_owner
                               ,tabname=> p_table_name
                               ,cascade=> true);
    --
    EXCEPTION
    WHEN OTHERS THEN
     l_result := 'N';
  END;
--
 END IF;
--
RETURN (l_result);
--
-- fsc_utils.handle_exception;
--
END dl_comp_stats;
--
FUNCTION dl_comp_stats
         (p_table_name IN VARCHAR2)
        RETURN VARCHAR2
IS
CURSOR c1 IS
SELECT owner
FROM all_tables
WHERE table_name = p_table_name;
--
l_value       VARCHAR2(300);
l_exists      NUMBER;
l_result      VARCHAR2(1);
l_table_owner VARCHAR2(30);
--
BEGIN
--
l_result := 'Y';
--
BEGIN
  OPEN  c1;
  FETCH c1 INTO l_table_owner;
  CLOSE c1;
  --
  dbms_stats.gather_table_stats(ownname => l_table_owner
                               ,tabname=> p_table_name
                               ,cascade=> true);
  --
  --
  EXCEPTION
  WHEN OTHERS
  THEN
    l_result := 'N';
  END;
--
RETURN (l_result );
--
-- fsc_utils.handle_exception;
--
END dl_comp_stats;
--
FUNCTION dl_orig_rows
         (p_table_name IN VARCHAR2)
          RETURN INTEGER
IS
--
l_value       VARCHAR2(100);
l_count       INTEGER;
--
BEGIN
--
  l_value := 'select count(*) from '||p_table_name;
--
  BEGIN
    EXECUTE IMMEDIATE l_value
    INTO l_count;
    --
    EXCEPTION
    WHEN OTHERS THEN
    l_count := 0;
    END;
--
  RETURN (l_count);
--
-- fsc_utils.handle_exception;
--
END dl_orig_rows;
--
FUNCTION f_bru_run_no
(
 p_bru_aun_code VARCHAR2
,p_effective_date DATE
)
RETURN NUMBER IS
--
l_bru_run_no number default null;
--
CURSOR c_bru is
  select bru_run_no
  from   batch_runs
  where  bru_mod_name  = 'HRA069'
  and    bru_aun_code  = p_bru_aun_code
  and    p_effective_date  between bru_period_start_date
  and     bru_period_end_date;
--
BEGIN
--
open  c_bru;
fetch c_bru
into  l_bru_run_no;
close c_bru;
--
return (l_bru_run_no);
--
END f_bru_run_no;
--
-- Create Bank Details for VALID Bank/Account information
--
PROCEDURE insert_bank_details
    (p_bde_bank_name         IN varchar2,
     p_bde_branch_name       IN varchar2,
     p_bad_account_no        IN varchar2,
     p_bad_account_name      IN varchar2,
     p_bad_sort_code         IN varchar2,
     p_bad_start_date        IN DATE,
     p_bde_refno             OUT integer,
     p_bad_refno             OUT integer
     )
IS
--
CURSOR c_bank_details IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name = p_bde_bank_name;
--
CURSOR c_branch_details IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name = p_bde_bank_name
   AND bde_branch_name = p_bde_branch_name;
--
--
l_bank_exists     number(10);
l_branch_exists   number(10);
l_bde_refno       number(10);
l_bad_refno       number(10);
quit_insert exception;
--
BEGIN
  -- Check Parent AND Child details supplied
  IF p_bde_bank_name IS NULL
  THEN
    raise quit_insert;
  ELSE
    OPEN  c_bank_details;
    FETCH c_bank_details INTO l_bank_exists;
    CLOSE c_bank_details;
    --
    IF p_bde_branch_name IS NOT NULL
    THEN
      OPEN  c_branch_details;
      FETCH c_branch_details INTO l_branch_exists;
      CLOSE c_branch_details;
    END IF;
  END IF;
  --
  -- IF Bank Details do NOT exist CREATE record.
  IF (l_bank_exists IS NULL
  OR  l_branch_exists IS NULL)
  THEN
    -- Get Bank Details refno
    SELECT bde_refno_seq.nextval INTO l_bde_refno FROM dual;
    p_bde_refno := l_bde_refno;
    --
    INSERT into bank_details(
      bde_refno,
      bde_bank_name,
      bde_created_by,
      bde_created_date,
      bde_branch_name )
    VALUES(
      l_bde_refno,
      p_bde_bank_name,
      user,
      TRUNC(sysdate),
      p_bde_branch_name );
  END IF;
 --
 -- IF Bank Account Details do NOT exist CREATE record.
 IF p_bad_account_no IS NOT NULL
 THEN
   IF p_bad_sort_code IS NOT NULL
   AND p_bad_account_name IS NOT NULL
   AND p_bad_start_date IS NOT NULL
   THEN
     -- Get Bank Account Details refno
     SELECT bad_refno_seq.nextval INTO l_bad_refno FROM dual;
     p_bad_refno := l_bad_refno;
     --
     INSERT into bank_account_details(
       bad_refno,
       bad_type,
       bad_sort_code,
       bad_bde_refno,
       bad_account_no,
       bad_account_name,
       bad_start_date,
       bad_created_by,
       bad_created_date )
     VALUES(
       l_bad_refno,
       'ORG',
       p_bad_sort_code,
       l_bde_refno,
       p_bad_account_no,
       p_bad_account_name,
       p_bad_start_date,
       user,
       TRUNC(sysdate) );
   ELSE
       raise quit_insert;
   END IF;
 END IF;
--
EXCEPTION
 WHEN quit_insert THEN
   NULL;
 WHEN   Others THEN
   fsc_utils.handle_exception;
--
END insert_bank_details;
--
FUNCTION hps_hpc_code_for_date
         (p_pro_propref IN VARCHAR2,
          p_date        IN DATE    ) RETURN VARCHAR2
IS
--
CURSOR c_hps_hpc IS
SELECT hps_hpc_code
FROM   hou_prop_statuses,properties
WHERE pro_propref      = p_pro_propref
AND   hps_pro_refno    = pro_refno
AND   p_date BETWEEN hps_start_date AND NVL(hps_end_date,p_date);
--
l_hps_hpc VARCHAR2(4);
--
BEGIN
--
OPEN  c_hps_hpc;
FETCH c_hps_hpc INTO l_hps_hpc;
CLOSE c_hps_hpc;
--
RETURN( l_hps_hpc);
--
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
--
END hps_hpc_code_for_date;
--
-- Create Bank Details for VALID Bank/Account information
-- where multi language name and branch supplied
--
PROCEDURE insert_bank_details_mlang
    (p_bde_bank_name         IN varchar2,
     p_bde_branch_name       IN varchar2,
     p_bde_bank_name_mlang   IN varchar2,
     p_bde_branch_name_mlang IN varchar2,
     p_bad_account_no        IN varchar2,
     p_bad_account_name      IN varchar2,
     p_bad_sort_code         IN varchar2,
     p_bad_start_date        IN DATE,
     p_bde_refno             OUT integer,
     p_bad_refno             OUT integer
     )
IS
--
CURSOR c_bank_details IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name = p_bde_bank_name;
--
CURSOR c_branch_details IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name = p_bde_bank_name
   AND bde_branch_name = p_bde_branch_name;
--
--
l_bank_exists     number(10);
l_branch_exists   number(10);
l_bde_refno       number(10);
l_bad_refno       number(10);
quit_insert exception;
--
BEGIN
--
-- Check Parent AND Child details supplied
--
  IF p_bde_bank_name IS NULL 
  THEN
    raise quit_insert;
  ELSE
    OPEN  c_bank_details;
    FETCH c_bank_details INTO l_bank_exists;
    CLOSE c_bank_details;
    --
    IF p_bde_branch_name IS NOT NULL
    THEN
      OPEN  c_branch_details;
      FETCH c_branch_details INTO l_branch_exists;
      CLOSE c_branch_details;
    END IF;
  END IF;
--
-- IF Bank Details do NOT exist CREATE record
--
  IF (l_bank_exists IS NULL
  OR  l_branch_exists IS NULL)
  THEN
--
-- Get Bank Details refno
--
    SELECT bde_refno_seq.nextval INTO l_bde_refno FROM dual;
    p_bde_refno := l_bde_refno;
--
    INSERT into bank_details(
      bde_refno,
      bde_bank_name,
      bde_created_by,
      bde_created_date,
      bde_branch_name,
      bde_bty_code,
      bde_bank_name_mlang,
      bde_branch_name_mlang)
    VALUES(
      l_bde_refno,
      p_bde_bank_name,
      user,
      sysdate,
      'DEF',            -- should always exist sys default
      p_bde_branch_name,
      p_bde_bank_name_mlang,
      p_bde_branch_name_mlang);
  END IF;
--
-- IF Bank Account Details do NOT exist CREATE record
--
  IF p_bad_account_no IS NOT NULL
   THEN
    IF (    p_bad_sort_code IS NOT NULL
        AND p_bad_account_name IS NOT NULL
        AND p_bad_start_date IS NOT NULL  )
     THEN
--
-- Get Bank Account Details refno
--
      SELECT bad_refno_seq.nextval INTO l_bad_refno FROM dual;
      p_bad_refno := l_bad_refno;
--
      INSERT into bank_account_details(
        bad_refno,
        bad_type,
        bad_sort_code,
        bad_bde_refno,
        bad_account_no,
        bad_account_name,
        bad_start_date,
        bad_created_by,
        bad_created_date )
      VALUES(
        l_bad_refno,
        'ORG',
        p_bad_sort_code,
        l_bde_refno,
        p_bad_account_no,
        p_bad_account_name,
        p_bad_start_date,
        user,
        sysdate );
    ELSE
       raise quit_insert;
    END IF;
  END IF;
--
EXCEPTION
 WHEN quit_insert THEN
   NULL;
 WHEN   Others THEN
   fsc_utils.handle_exception;
--
END insert_bank_details_mlang;
--
--
FUNCTION chk_is_numeric (p_char VARCHAR2)
RETURN BOOLEAN Is
--
l_number number;
l_return BOOLEAN := TRUE;
--
BEGIN
--
BEGIN
--
l_number:= TO_NUMBER(p_char);
--
EXCEPTION
WHEN OTHERS
THEN
  l_return:=FALSE;
--
END;
return l_return;
END chk_is_numeric;
--
FUNCTION chk_is_integer (p_char VARCHAR2)
RETURN BOOLEAN Is
--
l_number PLS_INTEGER;
l_return BOOLEAN := TRUE;
--
BEGIN
--
BEGIN
--
l_number:= TO_NUMBER(p_char);
--
EXCEPTION
WHEN OTHERS
THEN
  l_return:=FALSE;
--
END;
--
IF TO_CHAR(l_number) != p_char
THEN
  l_return:=FALSE;
END IF;
--
return l_return;
END chk_is_integer;
--
FUNCTION mlang_street  (p_street VARCHAR2)
RETURN VARCHAR2 IS
--
l_dummy VARCHAR2(1) := 'X';
--
l_orig   VARCHAR2(100);
l_street VARCHAR2(100);
l_desc   VARCHAR2(20);
l_char   VARCHAR2(1);
l_length PLS_INTEGER;
--
BEGIN
--
l_orig   := LTRIM(RTRIM(p_street));
l_street := LTRIM(RTRIM(p_street));
--
l_length := length(l_street);
l_char   := SUBSTR(l_street,l_length,1);
--
WHILE (l_char != ' '
       AND l_length > 1) LOOP
--
l_length := l_length -1;
--
l_char   := SUBSTR(l_street,l_length,1);
--
END LOOP;
--
IF l_length > 1
THEN
  l_desc := LTRIM(RTRIM(SUBSTR(l_orig,l_length,20)));
  l_street := LTRIM(RTRIM(SUBSTR(l_orig,1,l_length-1)));
  IF l_desc IN ('ST','STREET')
  THEN
    l_street := 'RUE '||l_street;
  ELSIF l_desc = 'BLVD'
  THEN
    l_street := 'BOUL '||l_street;
  ELSIF l_desc = 'BOULEVARD'
  THEN
    l_street := 'BOULEVARD '||l_street;
  ELSIF l_desc = 'AVENUE'
  THEN
    l_street := 'AVENUE '||l_street;
  ELSIF l_desc = 'AVE'
  THEN
    l_street := 'AV '||l_street;
  ELSE
    l_street := l_orig;

  END IF;
--
END IF;
--
RETURN l_street;
--
END mlang_street;
--
END s_dl_hem_utils;
/

