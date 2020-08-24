CREATE OR REPLACE PACKAGE BODY s_dl_hem_property_elements
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VER  DB Ver  WHO  WHEN       WHY
--  1.0          PJD  05/9/2000  Dataload
--  1.1          PJD  20/12/2000 Error Handling changes
--  1.2  5.1.4   PJD  28/02/02   Removed superfluous update from delete 
--                               proc. 
--  1.3  5.1.4   PJD  07/03/03   Minor changes to c_check_dupp 
--                               and c_check_dupa
--  1.4  5.1.6   PJD  16/05/02   Chnges c_fat_code to use 'NUL'
--  1.5  5.1.6   PJD  19/06/02   Added clause to update 'created' info to 
--                               allow delete proc to work even if elements 
--                               more that 1hr old
--  1.6  5.2.0   SB   22/11/02   Added Validation of Element Type
--  1.7  5.3.0   PH   01/09/03   Added validation on Element Start Date
--  1.8  5.4.0   PJD  20/11/03   Moved Update Record Status and Record Count 
--                               sections
--  1.9  5.5.0   PJD  02/03/04   Errors on Create now get a status of O
--  2.0  5.7.0   PH   18/02/05   Amended Validate on Location. Now uses
--                               domain LOCATION not ELELOC.
--  2.1  5.8.0   PH   19/07/05   Added New field Repair Condition
--  2.2  5.8.0   VST  30/11/05   Added pel_start_date to delete process to allow
--                               unique identification if value is same
--  2.3  5.10.0  PH   05/09/06   Added Batch Question to enable the ending
--                               of existing elements. Changes to Create, 
--                               Validate and Delete processes
--  2.4  5.10.0  PH   10/05/07   Amended Create and Delete processes where
--                               end elements question is Y. Now check
--                               to see if Multi Value or not. This will
--                               prevent duplicates
--  2.5 5.13.0   PH  06-FEB-2008 Now includes its own 
--                               set_record_status_flag procedure.
--  2.6 5.13.0   PH  08-AUG-2008 Amended validate for end elements = Y
--                               to check if there is an existsing record
--                               that starts after the one being loaded
--  2.7 5.13.0   PH  21-APR-2009 Amended code for end elements, previously
--                               only applied to Property Elements now
--                               applies to Admin Unit Elements too
--  2.8 5.15.1   PH  10-JAN-2010 New field for Quantity added
--  2.9 6.1.1    PH  31-MAY-2011 Amended validate for end elements = Y
--                               to check if there is an existsing record
--                               that starts ON or after the one being 
--                               loaded. This is because we could end it the
--                               day before it starts. Cursors
--                               c_check_dupp2 and c_check_dupa2
--  3.0 6.9      PJD 09-Dec-2013 Changes to Overlapping Element Validation
--                               including extra validation in the 'Create'.
--                               This uses a new function called is_current.
--                               Various other amendments to bits of code
--                               around deleting/updating existing 
--                               overlapping elements.
--  3.1 6.9      PJD 07-Jan-2014 Correction to variables passed into
--                               c_check_dupa2 cursor
--  3.2 6.9      PJD 27-Feb-2014 Further refinement of dupp2 and dupa2 cursors
--                               where date check needed to be > rather then >=
--  3.3 6.12     MJK 17-AUG-2015 No changes for 6.12
--  3.4 6.13     AJ  24-MAR-2016 Added check on length of lpel_value as admin unit element
--                               is only (8,2) where property element is (11,2)
--  3.5 6.13     AJ  04-OCT-2016 Condition on update for fat code  "AND aue_att_code  = NVL(p1.lpel_fat_code,'NUL')"
--                               amended to "AND aue_fat_code  = NVL(p1.lpel_fat_code,'NUL')" 
--  3.6 6.13     AJ  20-OCT-2016 Condition is admin units elements hrv_elo_code check amended from
--                               PRO to AUN to correct 
--                            
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hem_property_elements
  SET lpel_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_property_elements');
     RAISE;
  --
END set_record_status_flag;
--
FUNCTION is_current_element
      (  p_pel_pro_refno  IN NUMBER,
         p_pel_aun_code   IN VARCHAR2,
         p_pel_ele_code   IN VARCHAR2,
         p_pel_att_code   IN VARCHAR2,
         p_pel_fat_Code   IN VARCHAR2,
         p_pel_start_date IN DATE,
         p_pel_end_Date   IN DATE,
         p_hrv_elo_code   IN VARCHAR2,
         p_ele_value_type IN VARCHAR2 DEFAULT 'M'
       )
RETURN VARCHAR2
AS

CURSOR c_get_pel1
IS
SELECT 'Exists'
FROM property_elements
WHERE pel_pro_refno = p_pel_pro_refno
AND   pel_ele_code  = p_pel_ele_code
AND   pel_start_date <= NVL( p_pel_end_date, pel_start_date )
AND   NVL( pel_end_date, p_pel_start_date ) >= p_pel_start_date
AND   p_ele_value_type IN ( 'D', 'N', 'C' )
UNION
SELECT 'Exists'
FROM property_elements
WHERE pel_pro_refno = p_pel_pro_refno
AND pel_ele_code    = p_pel_ele_code
AND pel_att_code    = NVL( p_pel_att_code, 'NUL' )
AND pel_fat_code    = NVL( p_pel_fat_code, 'NUL' )
AND pel_start_date <= NVL( p_pel_end_date, pel_start_date )
AND NVL(pel_hrv_elo_code,'PRO') = NVL(p_hrv_elo_code,'PRO')
AND NVL( pel_end_date, p_pel_start_date ) >= p_pel_start_date
AND p_ele_value_type IN ( 'M' )
;
--
CURSOR c_get_pel2
IS
SELECT 'Exists'
FROM property_elements
WHERE pel_pro_refno = p_pel_pro_refno
AND   pel_ele_code  = p_pel_ele_code
AND   pel_start_date >=  p_pel_start_date
AND   pel_start_date <=  NVL(p_pel_end_date,pel_start_date)
AND   p_ele_value_type IN ( 'D', 'N', 'C' )
UNION
SELECT 'Exists'
FROM property_elements
WHERE pel_pro_refno = p_pel_pro_refno
AND   pel_ele_code    = p_pel_ele_code
AND   pel_att_code    = NVL( p_pel_att_code, 'NUL' )
AND   pel_fat_code    = NVL( p_pel_fat_code, 'NUL' )
AND   pel_start_date >=  p_pel_start_date
AND   pel_start_date <=  NVL(p_pel_end_date,pel_start_date)
AND   NVL(pel_hrv_elo_code,'PRO') = NVL(p_hrv_elo_code,'PRO')
AND   p_ele_value_type IN ( 'M' );
--
CURSOR c_get_aue1
IS
SELECT 'Exists'
FROM admin_unit_elements
WHERE aue_aun_code  = p_pel_aun_code     
AND   aue_ele_code  = p_pel_ele_code
AND   aue_start_date <= NVL( p_pel_end_date, aue_start_date )
AND   NVL( aue_end_date, p_pel_start_date ) >= p_pel_start_date
AND   p_ele_value_type IN ( 'D', 'N', 'C' )
UNION
SELECT 'Exists'
FROM admin_unit_elements
WHERE aue_aun_code  = p_pel_aun_code     
AND aue_ele_code    = p_pel_ele_code
AND aue_att_code    = NVL( p_pel_att_code, 'NUL' )
AND aue_fat_code    = NVL( p_pel_fat_code, 'NUL' )
AND aue_start_date <= NVL( p_pel_end_date, aue_start_date )
AND NVL(aue_hrv_elo_code,'AUN') = NVL(p_hrv_elo_code,'AUN')
AND NVL( aue_end_date, p_pel_start_date ) >= p_pel_start_date
AND p_ele_value_type IN ( 'M' )
;

CURSOR c_get_aue2
IS
SELECT 'Exists'
FROM admin_unit_elements
WHERE aue_aun_code  = p_pel_aun_code     
AND   aue_ele_code  = p_pel_ele_code
AND   aue_start_date >=  p_pel_start_date
AND   aue_start_date <=  NVL(p_pel_end_date,aue_start_date)
AND   p_ele_value_type IN ( 'D', 'N', 'C' )
UNION
SELECT 'Exists'
FROM admin_unit_elements
WHERE aue_aun_code  = p_pel_aun_code  
AND   aue_ele_code    = p_pel_ele_code
AND   aue_att_code    = NVL( p_pel_att_code, 'NUL' )
AND   aue_fat_code    = NVL( p_pel_fat_code, 'NUL' )
AND   aue_start_date >=  p_pel_start_date
AND   aue_start_date <=  NVL(p_pel_end_date,aue_start_date)
AND   NVL(aue_hrv_elo_code,'AUN') = NVL(p_hrv_elo_code,'AUN')
AND   p_ele_value_type IN ( 'M' )
;

l_Dumm VARCHAR2(10);
l_ret VARCHAR2(1):= 'N';    

BEGIN
fsc_utils.proc_start('s_dl_hem_property_elements.is_current_element');
fsc_utils.debug_message( 's_dl_hem_property_elements.is_Current_element'||
                         ' Pro Refno : '||p_pel_pro_refno||' '||
                         ' Aun Code  : '||p_pel_aun_code||' '||
                         ' Pel Ele   : '||p_pel_ele_code||' '||
                         ' Pel Att   : '||p_pel_att_code||' '||
                         ' Pel Fat   : '||p_pel_fat_code||' '||
                         ' Elo Type  : '||p_hrv_elo_code||' '||
                         ' Pel Type  : '||p_ele_value_type||' '||
                         ' Pel Start : '||
                               to_char(p_pel_start_date,'DD-MON-YYYY')||' '||
                         ' Pel End   : '||
                               to_char(p_pel_end_date,'DD-MON-YYYY'),3 );

IF p_pel_pro_Refno IS NOT NULL
THEN

  OPEN c_get_pel1;
  FETCH c_get_pel1    INTO l_dumm;
  IF c_get_pel1%FOUND
  THEN
    l_Ret := 'Y';  
  END IF;

  CLOSE c_get_pel1;
  
  IF l_ret = 'N'     
  THEN
    OPEN c_get_pel2;
    FETCH c_get_pel2    INTO l_dumm;
    IF c_get_pel2%FOUND
    THEN
      l_Ret := 'Y';  
    END IF;
    CLOSE c_get_pel2;
  END IF;

END IF;
--
IF p_pel_aun_code  IS NOT NULL
THEN

  OPEN c_get_aue1;
  FETCH c_get_aue1    INTO l_dumm;
  IF c_get_aue1%FOUND
  THEN
    l_Ret := 'Y';  
  END IF;

  CLOSE c_get_aue1;
  IF l_ret = 'N'    
  THEN
    OPEN c_get_aue2;
    FETCH c_get_aue2    INTO l_dumm;
    IF c_get_aue2%FOUND
    THEN
      l_Ret := 'Y';  
    END IF;
    CLOSE c_get_aue2;
  END IF;

END IF;


fsc_utils.proc_end;
IF l_ret = 'Y'  
THEN
fsc_utils.debug_message( 's_dl_hem_property_elements.is_Current_element'||
                         ' l_ret returned as TRUE',3);
ELSE
fsc_utils.debug_message( 's_dl_hem_property_elements.is_Current_element'||
                         ' l_ret returned as FALSE',3);
END IF;

RETURN l_ret;

EXCEPTION

WHEN OTHERS 
THEN
  fsc_utils.handle_exception;
END is_current_element;
--
-- ***********************************************************************
--
--  declare package variables AND constants
--
PROCEDURE dataload_create
      (p_batch_id          IN VARCHAR2
      ,p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) IS
SELECT
rowid rec_rowid
,lpel_dlb_batch_id
,lpel_dl_seqno
,lpel_dl_load_status
,lpel_pro_propref  -- PRO_REFNO OR AUN_CODE
,lpel_ety_code
,lpel_attr_type
,lpel_atty_code
,lpel_hrv_elo_code  -- PRO OR AUN
,lpel_fat_code
,lpel_date
,lpel_value
,lpel_hrv_repcat
,lpel_end
,lpel_start
,lpel_text
,nvl(lpel_aun_ind,'P') lpel_aun_ind  -- A if admin_unit
,lpel_hrv_rco_code
,lpel_quantity
FROM dl_hem_property_elements
WHERE lpel_dlb_batch_id = p_batch_id
AND   lpel_dl_load_status = 'V';
--
CURSOR c_ele_type(p_ele_code  VARCHAR2) IS
SELECT ele_value_type
FROM   elements
WHERE  ele_code = p_ele_code;
--
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_PROPERTY_ELEMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_pro_refno NUMBER(10);
i           INTEGER := 0;
l_an_tab    VARCHAR2(1);
l_answer    VARCHAR2(1);
l_ele_type  VARCHAR2(1);
l_errors    VARCHAR2(1);
--
e_dup_pgr   EXCEPTION;
--
--
BEGIN
fsc_utils.proc_start('s_dl_hem_property_elements.dataload_create');
fsc_utils.debug_message( 's_dl_hem_property_elements.dataload_create',3);
--
cb := p_batch_id;
cd := p_DATE;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
-- Get the answer to the 'End existing Charges'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
IF l_answer IS NULL
THEN
  l_answer := 'N';
END IF;
--
FOR p1 in c1(p_batch_id) LOOP
--
BEGIN
--
cs := p1.lpel_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
IF p1.lpel_aun_ind != 'A' 
THEN
  --
  -- get the pro_refno
  --
  l_pro_refno := NULL;
  l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lpel_pro_propref);
  l_ele_type  := NULL;
  --
  -- we need to find out if it's a multi value element
  -- if it is we match on attribute, if not we just
  -- match on element code
  --
  OPEN c_ele_type(p1.lpel_ety_code);
  FETCH c_ele_type into l_ele_type;
  CLOSE c_ele_type;
  --
  -- If a current element exists then end it prior to loading a new one
  --
  IF l_answer = 'Y'
  THEN
    --
    IF l_ele_type = 'M'
    THEN
      DELETE FROM property_elements
      WHERE pel_pro_refno = l_pro_refno
      AND pel_ele_code  = p1.lpel_ety_code
      AND pel_att_code  = NVL(p1.lpel_atty_code,'NUL')
      AND pel_fat_code  = NVL(p1.lpel_fat_code,'NUL')
      AND pel_hrv_elo_code  = NVL(p1.lpel_hrv_elo_code,'PRO')
      AND pel_start_date    =   p1.lpel_start 
      ; 

      UPDATE property_elements
      SET pel_end_date  = p1.lpel_start-1
      WHERE pel_pro_refno = l_pro_refno
      AND pel_ele_code  = p1.lpel_ety_code
      AND pel_att_code  = NVL(p1.lpel_atty_code,'NUL')
      AND pel_fat_code  = NVL(p1.lpel_fat_code,'NUL')
      AND pel_hrv_elo_code  = NVL(p1.lpel_hrv_elo_code,'PRO')
      AND p1.lpel_start  BETWEEN  pel_start_date
                             AND NVL(pel_end_date,p1.lpel_start +1)
      ;
      --
    ELSE  -- l_ele_type must be C N or D therefore just end
      --
      DELETE FROM property_elements
      WHERE pel_pro_refno  = l_pro_refno
        AND pel_ele_code   = p1.lpel_ety_code
        AND pel_start_date =  p1.lpel_start 
      ;

      UPDATE property_elements
        SET pel_end_date  = p1.lpel_start-1
      WHERE pel_pro_refno = l_pro_refno
        AND pel_ele_code  = p1.lpel_ety_code
        AND p1.lpel_start BETWEEN pel_start_date 
                              AND NVL(pel_end_date, p1.lpel_start +1)
      ;
      --
    END IF; -- l_ele_type = 'M'
    --
  END IF; -- l_answer = 'Y'
  --
  --
  IF  is_current_element
   (  p_pel_pro_refno  => l_pro_refno,
      p_pel_aun_code   => NULL,
      p_pel_ele_code   => p1.lpel_ety_code,
      p_pel_att_code   => p1.lpel_atty_code,
      p_pel_fat_Code   => p1.lpel_fat_code,
      p_pel_start_date => p1.lpel_start,
      p_pel_end_Date   => p1.lpel_end,
      p_hrv_elo_code   => NVL(p1.lpel_hrv_elo_code,'PRO'),
      p_ele_value_type => l_ele_type
   ) = 'Y' 
  THEN
    ROLLBACK TO SP1;
    fsc_utils.debug_message( 'Current Check returned True',3);
    RAISE e_dup_pgr;
  ELSE   
    fsc_utils.debug_message( 'Current Check returned False',3);
    INSERT INTO PROPERTY_ELEMENTS
    (pel_pro_refno
    ,pel_ele_code
    ,pel_start_DATE
    ,pel_att_code
    ,pel_hrv_elo_code
    ,pel_fat_code
    ,pel_created_by
    ,pel_created_date
    ,pel_modified_by
    ,pel_modified_date
    ,pel_hrv_rco_code
    ,pel_hrv_rca_code
    ,pel_date_value
    ,pel_numeric_value
    ,pel_quantity
    ,pel_end_date
    ,pel_comment
    ,pel_source
    ,pel_source_refno
    ) VALUES
    (l_pro_refno       ,
    p1.lpel_ety_code  ,
    p1.lpel_start     ,
    NVL(p1.lpel_atty_code,'NUL') ,
    NVL(p1.lpel_hrv_elo_code,'PRO'),
    NVL(p1.lpel_fat_code,'NUL'),
    'DATALOAD'        ,
    sysdate           ,
    NULL              ,
    NULL              ,
    p1.lpel_hrv_rco_code,
    p1.lpel_hrv_repcat,
    p1.lpel_date      ,
    p1.lpel_value     ,
    p1.lpel_quantity  ,
    p1.lpel_end       ,
    p1.lpel_text      ,
    NULL              ,
    NULL);
    --
  END IF;  -- not a dup 
-- 
ELSE -- Admin Unit ind
  --
  -- New code to check if we should end existing ones
  --
  -- If a current element exists then end it prior to loading a new one
  --
  IF l_answer = 'Y'
  THEN
    --
    -- we need to find out if it's a multi value element
    -- if it is we match on attribute, if not we just
    -- match on element code
    --
    OPEN  c_ele_type(p1.lpel_ety_code);
    FETCH c_ele_type into l_ele_type;
    CLOSE c_ele_type;
    --
    IF l_ele_type = 'M'
    THEN
      DELETE FROM admin_unit_elements
      WHERE aue_aun_code  = substr(p1.lpel_pro_propref,1,20)
      AND aue_ele_code  = p1.lpel_ety_code
      AND aue_att_code  = NVL(p1.lpel_atty_code,'NUL')
      AND aue_fat_code  = NVL(p1.lpel_fat_code,'NUL')
      AND aue_hrv_elo_code  = NVL(p1.lpel_hrv_elo_code,'AUN')
      AND aue_start_date    =   p1.lpel_start 
      ; 

      UPDATE admin_unit_elements
      SET aue_end_date  = p1.lpel_start-1
      WHERE aue_aun_code  = substr(p1.lpel_pro_propref,1,20)
      AND aue_ele_code  = p1.lpel_ety_code
      AND aue_att_code  = NVL(p1.lpel_atty_code,'NUL')
      AND aue_fat_code  = NVL(p1.lpel_fat_code,'NUL')
      AND aue_hrv_elo_code  = NVL(p1.lpel_hrv_elo_code,'AUN')
      AND p1.lpel_start BETWEEN aue_start_date 
                            AND NVL(aue_end_date,p1.lpel_start +1)
      ;
      --
    ELSE  -- l_ele_type must be C N or D therefore just end
      --
      DELETE FROM admin_unit_elements
      WHERE aue_aun_code  = substr(p1.lpel_pro_propref,1,20)
      AND aue_ele_code    = p1.lpel_ety_code
      AND aue_start_date  =   p1.lpel_start 
      ;
 
     UPDATE admin_unit_elements
        SET aue_end_date  = p1.lpel_start-1
      WHERE aue_aun_code  = substr(p1.lpel_pro_propref,1,20)
        AND aue_ele_code  = p1.lpel_ety_code
        AND p1.lpel_start BETWEEN aue_start_date 
                              AND NVL(aue_end_date,p1.lpel_start +1)
      ;
      --
    END IF; -- l_ele_type = 'M'
    --
  END IF; -- l_answer = 'Y'
  --
  --
  IF  is_current_element
      (  p_pel_pro_refno  => NULL,
         p_pel_aun_code   => p1.lpel_pro_propref,
         p_pel_ele_code   => p1.lpel_ety_code,
         p_pel_att_code   => p1.lpel_atty_code,
         p_pel_fat_Code   => p1.lpel_fat_code,
         p_pel_start_date => p1.lpel_start,
         p_pel_end_Date   => p1.lpel_end,
         p_hrv_elo_code   => NVL(p1.lpel_hrv_elo_code,'AUN'),
         p_ele_value_type => l_ele_type
      ) = 'Y'
  THEN
    ROLLBACK TO SP1;
    RAISE e_dup_pgr;
  ELSE
    INSERT INTO admin_unit_elements
    (aue_aun_code
    ,aue_ele_code
    ,aue_start_date
    ,aue_att_code
    ,aue_hrv_elo_code
    ,aue_fat_code
    ,aue_created_by
    ,aue_created_DATE
    ,aue_modified_by
    ,aue_modified_DATE
    ,aue_hrv_rco_code
    ,aue_hrv_rca_code
    ,aue_DATE_value
    ,aue_numeric_value
    ,aue_quantity
    ,aue_end_date
    ,aue_comment
    ,aue_source
    ,aue_source_refno
    ) VALUES
    (
    SUBSTR(p1.lpel_pro_propref,1,20),
    p1.lpel_ety_code  ,
    p1.lpel_start     ,
    NVL(p1.lpel_atty_code,'NUL') ,
    NVL(p1.lpel_hrv_elo_code,'AUN'),
    NVL(p1.lpel_fat_code,'NUL'),
    'DATALOAD'        ,
    TRUNC(SYSDATE)    ,
    NULL,
    NULL,
    p1.lpel_hrv_rco_code,
    p1.lpel_hrv_repcat,
    p1.lpel_date      ,
    p1.lpel_value     ,
    p1.lpel_quantity  ,
    p1.lpel_end       ,
    p1.lpel_text      ,
    NULL,
    NULL);
    --
  END IF;  -- Not a Dup        
  --
END IF;  -- admin unit ind
--
-- Update Record Status and Record Count
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'C');
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
 EXCEPTION
   WHEN e_dup_pgr 
   THEN
     ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA','1',
          'DUPLICATE VALUE IN PROP/ADMIN UNIT ELEMENTS');
     set_record_status_flag(l_id,'O');
     s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
     fsc_utils.debug_message( 's_dl_hem_property_elements.dataload_create'||
                              ' raising e_dup_pgr' ,3);
   --
   WHEN OTHERS 
   THEN
     ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
     set_record_status_flag(l_id,'O');
     s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
END;
--
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PROPERTY_ELEMENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADMIN_UNITS_ELEMENTS');
--
fsc_utils.proc_end;
COMMIT;
--
   EXCEPTION
   WHEN OTHERS 
   THEN
     s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
--
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN date)
AS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lpel_dlb_batch_id
,lpel_dl_seqno
,lpel_dl_load_status
,lpel_pro_propref
,lpel_ety_code
,lpel_attr_type
,lpel_atty_code
,lpel_hrv_elo_code
,lpel_fat_code
,lpel_date
,lpel_value
,lpel_hrv_repcat
,lpel_end
,lpel_start
,lpel_text
,nvl(lpel_aun_ind,'P') lpel_aun_ind
,lpel_hrv_rco_code
,lpel_quantity
FROM dl_hem_property_elements
WHERE lpel_dlb_batch_id      = p_batch_id
AND   lpel_dl_load_status   IN ('L','F','O');
--
CURSOR c_ele_code(p_ele_code VARCHAR2) IS
SELECT ele_value_type,ele_type
FROM elements
WHERE ele_code     = p_ele_code;
--
CURSOR c_atty_code(p_ele_code VARCHAR2,p_att_code VARCHAR2) IS
SELECT 'X'
FROM attributes
WHERE att_ele_code = p_ele_code
  AND att_code     = p_att_code;
--
CURSOR c_fat_code(p_ele_code VARCHAR2,p_att_code VARCHAR2,
                  p_fat_code VARCHAR2)                      IS
SELECT NULL
FROM further_attributes
WHERE fat_ele_code = p_ele_code
  AND nvl(fat_att_code,'NUL') 
                   = nvl(p_att_code,'NUL') 
  AND fat_code     = p_fat_code;
--
--
CURSOR c_check_dupp2(p_pro_refno number   ,p_ety_code VARCHAR2,
                     p_atty_code VARCHAR2 ,p_start date,
                     p_end date) IS
SELECT NULL 
FROM   property_elements
WHERE  pel_pro_refno   = p_pro_refno
AND    pel_ele_code    = p_ety_code
AND    pel_att_code    = NVL(p_atty_code,pel_att_code)
AND    pel_start_date  > p_start;
--
CURSOR c_check_dupa2(p_aun_code VARCHAR2  ,p_ety_code VARCHAR2,
                     p_atty_code VARCHAR2 ,p_start date,
                     p_end date) IS
SELECT NULL 
FROM   admin_unit_elements
WHERE  aue_aun_code    = p_aun_code
AND    aue_ele_code    = p_ety_code
AND    aue_att_code    = NVL(p_atty_code,aue_att_code)
AND    aue_start_date  > p_start;
--
CURSOR c_check_len(p_lpel_value NUMBER) IS
select 
length(regexp_substr(replace(p_lpel_value,',',''), '[^.]+', 1, 1)) as whole_part_count,
length(trim(regexp_replace(p_lpel_value, '[^.]+\.(.*)$', '.\1')))-1 as fract_part_count 
from dual;
--
-- Constants FOR summary reporting
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HEM_PROPERTY_ELEMENTS';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_ele_value_type   VARCHAR2(1);
l_ele_type         VARCHAR2(2);
l_attr_type        VARCHAR2(3);
l_exists           VARCHAR2(1);
l_pro_refno        NUMBER(10);
l_errors           VARCHAR2(1);
l_error_ind        VARCHAR2(1);
i                  INTEGER :=0;
l_answer           VARCHAR2(1);
l_whole_part_count NUMBER(10); 
l_fract_part_count NUMBER(10); 
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_property_elements.dataload_validate');
fsc_utils.debug_message('s_dl_hem_property_elements.dataload_validate',3 );
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'End existing Charges'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lpel_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors    := 'V';
l_error_ind := 'N';
--
-- Check the property exists on PROPERTIES
--
IF p1.lpel_aun_ind != 'A' 
THEN
--
  l_pro_refno := NULL;
  l_pro_refno := s_dl_hem_utils.pro_refno_FOR_propref(p1.lpel_pro_propref);
--
  IF l_pro_refno is NULL 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',030);
  END IF;
ELSE
  IF (NOT s_dl_hem_utils.exists_aun_code(p1.lpel_pro_propref)) 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',050);
  END IF;
--
END IF;
--
-- Get the element_value_type FROM ELEMENTS
--
l_ele_value_type := NULL;
l_ele_type       := NULL;
--
OPEN c_ele_code(p1.lpel_ety_code);
FETCH c_ele_code INTO l_ele_value_type, l_ele_type;
CLOSE c_ele_code;
--
IF l_ele_type != 'PR'
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',981);
END IF;
--
IF l_ele_value_type is NULL
THEN 
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',031);
ELSE
  --
   l_attr_type := NULL;
  --
  -- set the l_attr_type value based on the info supplied
  --
  IF p1.lpel_atty_code is NOT NULL
  THEN 
    l_attr_type := l_attr_type||'A';
  END IF;
  --
  IF p1.lpel_date is NOT NULL
  THEN 
    l_attr_type := l_attr_type||'D';
  END IF;
  --
  IF p1.lpel_value is NOT NULL
  THEN 
    l_attr_type := l_attr_type||'V';
  END IF;
  --
  -- so the l_attr type should be A,D OR V
  --
  -- first check that at least one field has been supplied
  IF l_attr_type is NULL 
  THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',039);
  END IF;
  --
  -- check that NOT more than one field has been supplied
  --
  IF l_attr_type NOT in ('A','D','V') 
  THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',040);
  END IF;
  --
  -- check that the l_attr_type is consistant with the l_ele_value_type
  --
  IF l_attr_type||l_ele_value_type NOT in ('AM','AC','DD','VN') 
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',032);
  END IF;
  --
  -- FOR a coded OR multi-value element check that the attribute code
  -- entered is valid
  --
  IF p1.lpel_atty_code is NOT NULL
  THEN
    l_exists := null;
    --
    OPEN c_atty_code(p1.lpel_ety_code,p1.lpel_atty_code);
    FETCH c_atty_code INTO l_exists;
    CLOSE c_atty_code;
    IF l_exists is null
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',041);
    END IF;
    --
  END IF;
  --
END IF; -- attr type is NOT NULL
--
-- Check the Further Attribute Code
--
IF (    p1.lpel_fat_code is NOT NULL
    and p1.lpel_fat_code != 'NUL')
THEN
    OPEN  c_fat_code(p1.lpel_ety_code,p1.lpel_atty_code,p1.lpel_fat_code);
    FETCH c_fat_code INTO l_exists;
    IF c_fat_code%notfound 
    THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',328);
    END IF;
    CLOSE c_fat_code;
END IF;
--
-- Check the FRV reference values
--
-- Check the repair category is valid
--
IF (p1.lpel_hrv_repcat is NOT NULL)
THEN
  IF (NOT s_dl_hem_utils.exists_frv('REPCAT',p1.lpel_hrv_repcat))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',042);
  END IF;
END IF;
--
-- Check the location code is valid
--
IF (    p1.lpel_hrv_elo_code is NOT NULL
    and p1.lpel_hrv_elo_code != 'PRO'
    and p1.lpel_hrv_elo_code != 'AUN')
THEN
  IF (NOT s_dl_hem_utils.exists_frv('LOCATION',p1.lpel_hrv_elo_code,'Y'))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',329);
  END IF;
END IF;
--
-- Check the Element Start date has been supplied
--
IF p1.lpel_start is null
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',046);
END IF;
--
-- Check END date later than start date
--
IF (p1.lpel_end < p1.lpel_start)
THEN
  l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',043);
END IF;
--
-- check FOR overlaps
-- only where Answer is N to end Elements
--
IF nvl(l_answer, 'N') = 'N'
THEN
  --
  IF p1.lpel_aun_ind != 'A' 
  THEN
    IF  is_current_element
      (  p_pel_pro_refno  => l_pro_refno,
         p_pel_aun_code   => NULL,   
         p_pel_ele_code   => p1.lpel_ety_code,
         p_pel_att_code   => p1.lpel_atty_code,
         p_pel_fat_Code   => p1.lpel_fat_code,
         p_pel_start_date => p1.lpel_start,   
         p_pel_end_Date   => p1.lpel_end,
         p_hrv_elo_code   => NVL(p1.lpel_hrv_elo_code,'PRO'),
         p_ele_value_type => l_ele_value_type 
      ) = 'Y'
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',325);
    END IF;
  ELSE  -- it's and admin unit
    --
    IF  is_current_element
      (  p_pel_pro_refno  => NULL,        
         p_pel_aun_code   => p1.lpel_pro_propref,
         p_pel_ele_code   => p1.lpel_ety_code,
         p_pel_att_code   => p1.lpel_atty_code,
         p_pel_fat_Code   => p1.lpel_fat_code,
         p_pel_start_date => p1.lpel_start,   
         p_pel_end_Date   => p1.lpel_end,
         p_hrv_elo_code   => NVL(p1.lpel_hrv_elo_code,'AUN'),
         p_ele_value_type => l_ele_value_type 
      ) = 'Y'
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',325);
    END IF; 
  END IF;  -- Property or Admin Unit
--
END IF; -- l_answer 
--
-- check that there is not an element
-- starting after this one
-- only where Answer is Y to end Elements
--
IF nvl(l_answer, 'N') = 'Y'
THEN
  --
  IF p1.lpel_aun_ind != 'A' 
  THEN
    --
    IF l_ele_value_type = 'M' 
    THEN
      OPEN c_check_dupp2(l_pro_refno          ,p1.lpel_ety_code
                       ,p1.lpel_atty_code    ,p1.lpel_start
                       ,p1.lpel_end);
      FETCH c_check_dupp2 INTO l_exists;
      IF    c_check_dupp2%found 
      THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',540);
      END IF;
      CLOSE c_check_dupp2;
    --
    END IF;
    --
    IF l_ele_value_type != 'M' 
    THEN
      OPEN c_check_dupp2(l_pro_refno       ,p1.lpel_ety_code
                        ,NULL             ,p1.lpel_start
                        ,p1.lpel_end);
      FETCH c_check_dupp2 INTO l_exists;
      IF    c_check_dupp2%found 
      THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',540);
      END IF;
      CLOSE c_check_dupp2;
    END IF;
    --
  END IF;
  --
END IF;
--
IF nvl(l_answer, 'N') = 'Y'
THEN
  IF p1.lpel_aun_ind = 'A' 
  THEN
  --
    IF l_ele_value_type = 'M' 
    THEN
      OPEN  c_check_dupa2(p1.lpel_pro_propref ,p1.lpel_ety_code
                       ,p1.lpel_atty_code     ,p1.lpel_start
                       ,p1.lpel_end);
      FETCH c_check_dupa2 INTO l_exists;
      IF    c_check_dupa2%found 
      THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',540);
      END IF;
      CLOSE c_check_dupa2;
    END IF;
    --
    IF l_ele_value_type != 'M' 
    THEN
      OPEN  c_check_dupa2(p1.lpel_pro_propref ,p1.lpel_ety_code
                         ,NULL                ,p1.lpel_start
                         ,p1.lpel_end);
      FETCH c_check_dupa2 INTO l_exists;
      IF    c_check_dupa2%found 
      THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',540);
      END IF;
      CLOSE c_check_dupa2;
    END IF;
  END IF;
  --
END IF;
--
-- Check the Repair Condition
--
IF (p1.lpel_hrv_rco_code is NOT NULL)
THEN
  IF (NOT s_dl_hem_utils.exists_frv('REPCOND',p1.lpel_hrv_rco_code))
  THEN
    l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',353);
  END IF;
END IF;
--
-- If its a admin unit element lpel_value limited to (8,2)
-- If its a property element lpel_value limited to (11,2) size of DL field
--
l_whole_part_count := 0;
l_fract_part_count := 0;
--
IF p1.lpel_value IS NOT NULL 
THEN  
  OPEN  c_check_len(p1.lpel_value);
  FETCH c_check_len INTO l_whole_part_count, l_fract_part_count;
  CLOSE c_check_len;
--  
  IF p1.lpel_aun_ind = 'A' 
   THEN     
    IF l_whole_part_count > 8
      THEN     
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',889);
    END IF;
  END IF;
END IF;
--
-- Now UPDATE the record count AND error code
IF l_errors = 'F' 
THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
EXCEPTION
WHEN OTHERS 
THEN
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  set_record_status_flag(l_id,'O');
END;
--
END LOOP;
COMMIT;
--
fsc_utils.proc_end;
--
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
PROCEDURE dataload_delete (p_batch_id       IN VARCHAR2
                          ,p_date           IN date) IS
--
CURSOR c1 is
SELECT
rowid rec_rowid
,lpel_dlb_batch_id
,lpel_dl_seqno
,lpel_dl_load_status
,lpel_pro_propref
,lpel_ety_code
,lpel_attr_type
,lpel_atty_code lpel_att_code
,lpel_date
,lpel_value
,lpel_hrv_repcat
,lpel_end
,lpel_start
,lpel_text
,nvl(lpel_aun_ind,'P') lpel_aun_ind
FROM dl_hem_property_elements
WHERE lpel_dlb_batch_id      = p_batch_id
AND   lpel_dl_load_status    = 'C';
--
CURSOR c_ele_type(p_ele_code  VARCHAR2) IS
SELECT ele_value_type
FROM   elements
WHERE  ele_code = p_ele_code;
--
i           INTEGER := 0;
l_pro_refno INTEGER;
l_an_tab    VARCHAR2(1);
l_answer    VARCHAR2(1);
l_ele_type  VARCHAR2(1);
--
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_PROPERTY_ELEMENTS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--
BEGIN
--
fsc_utils.proc_start('s_dl_hem_property_elements.dataload_delete');
fsc_utils.debug_message('s_dl_hem_property_elements.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'End existing Charges'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lpel_dl_seqno;
i  := i +1;
l_id := p1.rec_rowid;
--
IF p1.lpel_aun_ind = 'A' 
THEN
  -- next clause added to get around the trigger that prevents 
  -- deletion of elements
  --
  UPDATE admin_unit_elements
  SET    aue_created_by   = user
  ,      aue_created_date = sysdate
  WHERE  aue_aun_code          = p1.lpel_pro_propref
    AND  aue_ele_code          = p1.lpel_ety_code
    AND  aue_start_date	       = p1.lpel_start
    AND  (   aue_att_code      = p1.lpel_att_code
          OR aue_numeric_value = p1.lpel_value
          OR aue_date_value    = p1.lpel_date);
  --
  DELETE FROM admin_unit_elements
  WHERE aue_aun_code          = p1.lpel_pro_propref
    AND aue_ele_code          = p1.lpel_ety_code
    AND  aue_start_date	      = p1.lpel_start
    AND (   aue_att_code      = p1.lpel_att_code
         OR aue_numeric_value = p1.lpel_value
         OR aue_date_value    = p1.lpel_date);
  --
  -- If batch Question is Y then remove end date from
  -- previous element to make it current again
  --
  IF l_answer = 'Y'
  THEN
    --
    -- Check to see if it's a multi value element
    --
    l_ele_type := NULL;
    --
    OPEN  c_ele_type(p1.lpel_ety_code);
    FETCH c_ele_type into l_ele_type;
    CLOSE c_ele_type;
    --
    IF l_ele_type = 'M'
    THEN
      UPDATE admin_unit_elements
      SET aue_end_date  = null
      WHERE aue_aun_code  = p1.lpel_pro_propref
      AND aue_ele_code  = p1.lpel_ety_code
      AND aue_att_code  = NVL(p1.lpel_att_code,aue_att_code)
      AND aue_end_date  = p1.lpel_start-1;
      --
    ELSE -- l_ele_type must be C N or D
      --
      UPDATE admin_unit_elements
      SET aue_end_date  = null
      WHERE aue_aun_code  = p1.lpel_pro_propref
      AND aue_ele_code  = p1.lpel_ety_code
      AND aue_end_date  = p1.lpel_start-1;
      --
    END IF;  -- l_ele_type = 'M'
    --
  END IF;  -- IF l_answer = 'Y'
  --
ELSE
  --
  l_pro_refno := NULL;
  l_pro_refno := s_dl_hem_utils.pro_refno_FOR_propref(p1.lpel_pro_propref);
  --
  -- next clause added to get around the trigger that prevents deletion 
  -- of elements
  --
  UPDATE property_elements
   SET    pel_created_by   = user
   ,      pel_created_date = sysdate
  WHERE    pel_pro_refno        = l_pro_refno
   AND    pel_ele_code          = p1.lpel_ety_code
   AND    pel_start_date        = p1.lpel_start
   AND    (pel_att_code         = p1.lpel_att_code
           OR pel_numeric_value = p1.lpel_value
           OR pel_date_value    = p1.lpel_date);
  --
  DELETE FROM property_elements
   WHERE pel_pro_refno         = l_pro_refno
     AND pel_ele_code          = p1.lpel_ety_code
     AND pel_start_date	       = p1.lpel_start
     AND (   pel_att_code      = p1.lpel_att_code
          OR pel_numeric_value = p1.lpel_value
          OR pel_date_value    = p1.lpel_date);
  --
  -- If batch Question is Y then remove end date from
  -- previous element to make it current again
  --
  IF l_answer = 'Y'
  THEN
    --
    -- Check to see if it's a multi value element
    --
    l_ele_type := NULL;
    --
    OPEN  c_ele_type(p1.lpel_ety_code);
    FETCH c_ele_type into l_ele_type;
    CLOSE c_ele_type;
    --
    IF l_ele_type = 'M'
    THEN
      UPDATE property_elements
      SET pel_end_date  = null
      WHERE pel_pro_refno = l_pro_refno
      AND pel_ele_code  = p1.lpel_ety_code
      AND pel_att_code  = NVL(p1.lpel_att_code,pel_att_code)
      AND pel_end_date  = p1.lpel_start-1;
      --
    ELSE -- l_ele_type must be C N or D
      --
      UPDATE property_elements
      SET pel_end_date  = null
      WHERE pel_pro_refno = l_pro_refno
      AND pel_ele_code  = p1.lpel_ety_code
      AND pel_end_date  = p1.lpel_start-1;
      --
    END IF;  -- l_ele_type = 'M'
    --
  END IF;
  --
END IF;
--
-- Update record status and record count
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
IF mod(i,5000) = 0 THEN commit; END IF;
--
EXCEPTION
WHEN OTHERS 
THEN
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
  set_record_status_flag(l_id,'C');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
  --
END;
--
END LOOP;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('PROPERTY_ELEMENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADMIN_UNITS_ELEMENTS');
--
fsc_utils.proc_end;
COMMIT;
--
EXCEPTION
WHEN OTHERS 
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
RAISE;
--
END dataload_delete;
--
--
END s_dl_hem_property_elements;
--
/
SHOW ERRORS


