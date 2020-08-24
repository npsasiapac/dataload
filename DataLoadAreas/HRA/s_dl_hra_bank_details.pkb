create or replace
PACKAGE BODY s_dl_hra_bank_details
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.16.1    PH   07-MAR-2011  Initial Creation
--                                      Bespoke Dataload for Cardiff CC
--  1.1     5.16.1    PH   14-MAR-2011  Changed table name from
--                                      dl_hra_bank_details_02
--                                      to dl_hra_bank_details
--  1.2     5.16.1    PH   29-MAR-2011  Commented out the ending of
--                                      address_usages as there is a trigger
--                                      that does this. Also only do inserts
--                                      ends on contact details where the
--                                      record is different. Also added
--                                      max to aus start date in cursor.
--                                      Further changes to contact details,
--                                      seems the forms expect only 1
--                                      therefore just update.
--
--  1.3     5.16.1    VS   29-JAN-2013  Bug Fix, Call ref 1629220
--                                      break sort code into 2 digit Bank
--                                      4 digit branch--
--  1.4     6.10      JS   05-MAY-2016  Correct error with delete proc
--  1.5     6.14      AJ   19-DEC-2016  New version for Manitoba
--  1.5     6.14      AJ   21-DEC-2016  Further changes done during testing
--                                      including new errors in hd3 error file
--
--  declare package variables AND constants
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag
        ( p_rowid  IN ROWID
        , p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_hra_bank_details
  SET    lbde_dl_load_status  = p_status
  WHERE  rowid                = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hra_bank_details');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
-- ***********************************************************************
--
PROCEDURE dataload_create
        ( p_batch_id          IN VARCHAR2
        , p_date              IN DATE)
AS
--
CURSOR  c1 IS
SELECT  rowid rec_rowid
      , lbde_dlb_batch_id
      , lbde_dl_seqno
      , lbde_dl_load_status
      , upper(lbde_type)                       lbde_type
      , lbde_branch
      , upper(lbde_branch_code)                lbde_branch_code
      , upper(lbde_adr1)                       lbde_adr1
      , upper(lbde_adr2)                       lbde_adr2
      , upper(lbde_adr3)                       lbde_adr3
      , upper(lbde_pcode)                      lbde_pcode
      , upper(lbde_phone)                      lbde_phone
      , upper(lbde_bank_code)                  lbde_bank_code
      , lbde_bank_name
      , lbde_amended
      , lbde_bde_refno
      , lbde_adr_refno
FROM    dl_hra_bank_details
WHERE   lbde_dlb_batch_id    =  p_batch_id
AND     lbde_dl_load_status  = 'V'
ORDER by lbde_bank_name
      ,  lbde_branch
      ,  lbde_amended;
--
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_aut_chk IS
SELECT aut_format_ind
FROM   address_usage_types
WHERE  aut_fao_code = 'BDE'
AND    aut_far_code = 'PHYSICAL';
--
CURSOR  c_bde_refno  IS
SELECT  bde_refno_seq.nextval
FROM    dual;
--
CURSOR  get_adr (p_bde_refno NUMBER) IS
SELECT aus_adr_refno
FROM   address_usages
WHERE aus_bde_refno = p_bde_refno;
--
-- ***********************************************************************
--
-- Constants for process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'CREATE';
ct                   VARCHAR2(30) := 'DL_HRA_BANK_DETAILS';
cs                   INTEGER;
ce	                 VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
i                    INTEGER := 0;
l_bde_refno          bank_details.bde_refno%type;
l_insert_aus         VARCHAR2(1);
l_adr_refno          addresses.adr_refno%type;
l_adr_refno_b        addresses.adr_refno%type;
l_street_index       VARCHAR2(10);
l_insert_cde         VARCHAR2(1);
l_aut_format         VARCHAR2(3);
--
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_bank_details.dataload_create');
fsc_utils.debug_message('s_dl_hra_bank_details.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- First change the address type of BDE PHYSICAL from SIA to FFA
-- as that's what we are receiving, at the need we can change it back
-- again
--
l_aut_format := NULL;
--
OPEN c_aut_chk;
FETCH c_aut_chk INTO l_aut_format;
CLOSE c_aut_chk;
--
IF l_aut_format = 'SIA'
 THEN
   UPDATE  address_usage_types
      SET  aut_format_ind  =  'FFA'
    WHERE  aut_fao_code    =  'BDE'
      AND  aut_far_code    =  'PHYSICAL'
      AND  aut_format_ind  =  'SIA';
END IF;
--
 FOR p1 in c1 LOOP
--
  BEGIN
--
  cs   := p1.lbde_dl_seqno;
  l_id := p1.rec_rowid;
--
  SAVEPOINT SP1;
--
-- Main processing
--
--
-- So does the bank detail already exist
--
  l_bde_refno  := NULL;
--
  IF l_bde_refno       IS NULL 
   THEN
    OPEN c_bde_refno;
    FETCH c_bde_refno into l_bde_refno;
    CLOSE c_bde_refno;
--
-- Insert into relevant table
--
    INSERT into  bank_details
               ( bde_refno
               , bde_bank_name
               , bde_created_by
               , bde_created_date
               , bde_branch_name
               , bde_bty_code
               , bde_bank_code
               , bde_branch_code
               )
        VALUES
               ( l_bde_refno
               , p1.lbde_bank_name
               , 'DATALOAD'
               , p1.lbde_amended
               , p1.lbde_branch
               , p1.lbde_type
               , TRIM(p1.lbde_bank_code)
               , TRIM(p1.lbde_branch_code)
               );
--
    UPDATE dl_hra_bank_details
    SET    lbde_bde_refno  = l_bde_refno
    WHERE  rowid = p1.rec_rowid;
--
  END IF;   /*  IF l_bde_refno IS NULL  */
--
-- Now deal with addresses
-- If address then set the indicator to create a new one
--
  l_insert_aus    := 'N';
  l_adr_refno     := NULL;
  l_street_index  := NULL;
  l_adr_refno_b   := NULL;
--
  IF p1.lbde_adr1 IS NOT NULL
   THEN
    l_insert_aus := 'Y';
  END IF;
--
-- Now do address insert
--
  IF l_insert_aus = 'Y'
   THEN
    s_dl_hem_utils.insert_address
           ( 'BDE'               -- p_fao_code
           , 'PHYSICAL'          -- p_far_code
           , l_street_index      -- p_street_index_code
           , l_adr_refno         -- p_adr_refno
           , null                -- p_flat
           , null                -- p_building
           , null                -- p_street_number
           , null                -- p_sub_street1
           , null                -- p_sub_street2
           , null                -- p_sub_street3
           , p1.lbde_adr1        -- p_street
           , p1.lbde_adr2        -- p_area
           , p1.lbde_adr3        -- p_town
           , null                -- p_county
           , p1.lbde_pcode       -- p_pcode
           , null                -- p_country
           , null                -- p_aun_code
           , null                -- p_pro_refno
           , null                -- p_par_refno
           , null                -- p_tcy_refno
           , l_bde_refno         -- p_bde_refno
           , NULL                -- p_pof_refno
           , p1.lbde_amended     -- p_start_date
           , null                -- p_end_date
           , null                -- p_local_ind
           , null                -- p_abroad_ind
           , null                -- p_app_refno
           , null                -- p_cos_code
           , NULL                -- p_cou_code
           , NULL                -- p_aer_id
           , null                -- p_nom_refno
           , NULL                -- p_papp_refno
           , null                -- p_aus_contact_name
           , null                -- p_eastings
           , null                -- p_northings
           , null                -- p_acas_reference
           , null                -- p_uprn
           , null                -- p_landlord_par_refno
           , null                -- p_llt_code
           , null                -- p_aat_code
           , null                -- p_pty_code
           , null                -- p_property_size
           , null                -- p_floor_level
           , null                -- p_hrv_alr_code
           , null                -- p_tenancy_leave_date
           , null                -- p_arrears_amount
           , null                -- p_storage_ind
           , null                -- p_storage_unit_cost
           , null                -- p_storage_cost
           );
--
    OPEN get_adr(l_bde_refno);
    FETCH get_adr into l_adr_refno_b;
    CLOSE get_adr;
--	
    UPDATE dl_hra_bank_details
    SET    lbde_adr_refno  = l_adr_refno_b
    WHERE  rowid = p1.rec_rowid;
--
  END IF;    /*       IF p1.lbde_adr1 is not null  */
--
-- Now insert into Contact Details using same principles
--
  l_insert_cde    := 'N';
--
  IF p1.lbde_phone is not null
   THEN
    l_insert_cde := 'Y';
  END IF;   /*   F p1.lbde_phone is not null   */
--
-- Now do the insert
--
  IF l_insert_cde = 'Y'
   THEN
    INSERT INTO contact_details
            ( cde_refno
            , cde_start_date
            , cde_created_date
            , cde_created_by
            , cde_contact_value
            , cde_frv_cme_code
            , cde_contact_name
            , cde_end_date
            , cde_pro_refno
            , cde_aun_code
            , cde_par_refno
            , cde_bde_refno
            , cde_cos_code
            , cde_cse_contact
            , cde_srq_no)
         VALUES
            ( cde_refno.nextval
            , p1.lbde_amended
            , sysdate
            , 'DATALOAD'
            , p1.lbde_phone
            , 'TEL'
            , null
            , null
            , null
            , null
            , null
            , l_bde_refno
            , null
            , null
            , null
            );
--
  END IF;    /*   IF l_insert_cde = 'Y'   */
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
  i := i+1;
--
  IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'C');
--
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO SP1;
        ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
        set_record_status_flag(l_id,'O');
        s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
 END LOOP;
--
-- Now change the address type back again
--
IF l_aut_format = 'SIA'
 THEN
   UPDATE  address_usage_types
      SET  aut_format_ind  =  'SIA'
    WHERE  aut_fao_code    =  'BDE'
      AND  aut_far_code    =  'PHYSICAL'
      AND  aut_format_ind  =  'FFA';
END IF;
--
COMMIT;
--
-- ***********************************************************************
--
-- Section to analyse the table(s) populated by this dataload
--
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESSES');
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_USAGES');
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_ELEMENTS');
 l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
--
fsc_utils.proc_END;
--
  EXCEPTION
    WHEN OTHERS THEN
     s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
       RAISE;
--
END dataload_create;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_validate
        ( p_batch_id          IN VARCHAR2
        , p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT  rowid rec_rowid
      , lbde_dlb_batch_id
      , lbde_dl_seqno
      , lbde_dl_load_status
      , upper(lbde_type)                       lbde_type
      , lbde_branch
      , upper(lbde_branch_code)                lbde_branch_code
      , upper(lbde_adr1)                       lbde_adr1
      , upper(lbde_adr2)                       lbde_adr2
      , upper(lbde_adr3)                       lbde_adr3
      , upper(lbde_pcode)                      lbde_pcode
      , upper(lbde_phone)                      lbde_phone
      , upper(lbde_bank_code)                  lbde_bank_code
      , lbde_bank_name
      , lbde_amended
      , lbde_bde_refno
      , lbde_adr_refno
FROM    dl_hra_bank_details
WHERE   lbde_dlb_batch_id    = p_batch_id
AND     lbde_dl_load_status in ('L','F','O');
--
--************************
-- Additional Cursors
--
CURSOR  c_bde_exists(p_bank_name    VARCHAR2) IS
SELECT  'X'
FROM    bank_details
WHERE   UPPER(bde_bank_name)   = UPPER(p_bank_name);
--
CURSOR  c_bde2_exists(p_bank_name    VARCHAR2
                     ,p_branch_name  VARCHAR2 ) IS
SELECT  'X'
FROM    bank_details
WHERE   UPPER(bde_bank_name)   = UPPER(p_bank_name)
AND     UPPER(bde_branch_name) = UPPER(p_branch_name);
--
CURSOR  c_bty_exists(p_bty_code    VARCHAR2) IS
SELECT  'X'
FROM    bank_types
WHERE   bty_code   = p_bty_code;
--
--************************
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HRA_BANK_DETAILS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
--************************
-- Other variables
--
l_exists         VARCHAR2(1);
l_bty_exists     VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_pro_aun        VARCHAR2(20);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
--
--************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_bank_details.dataload_validate');
fsc_utils.debug_message( 's_dl_hra_bank_details.dataload_validate',3);
--
cb := p_batch_id;
cd := p_DATE;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 IN c1 LOOP
--
  BEGIN
--
   cs           := p1.lbde_dl_seqno;
   l_id         := p1.rec_rowid;
   l_errors     := 'V';
   l_error_ind  := 'N';
   l_exists     := NULL;
   l_bty_exists := NULL;
--
-- Check The mandatory Fields
-- Bank Name
--
   IF p1.lbde_bank_name IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',079);
   END IF;
--
-- Bank Type
--
   IF p1.lbde_type IS NULL
    THEN
     l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',080);
   END IF;
--
-- Check the bank record doesn't already exist the
-- Bank/Branch name combination are unique
--
   IF ( p1.lbde_bank_name IS NOT NULL
    AND p1.lbde_branch    IS NULL    )
    THEN
     OPEN c_bde_exists(p1.lbde_bank_name);
     FETCH c_bde_exists INTO l_exists;
      IF c_bde_exists%FOUND
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',081);
      END IF;
    CLOSE c_bde_exists;
   END IF;
--
   IF ( p1.lbde_bank_name IS NOT NULL
    AND p1.lbde_branch    IS NOT NULL    )
    THEN
     OPEN c_bde2_exists(p1.lbde_bank_name, p1.lbde_branch);
     FETCH c_bde2_exists INTO l_exists;
      IF c_bde2_exists%FOUND
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',081);
      END IF;
    CLOSE c_bde2_exists;
   END IF;
--
-- Check the bank type exist
--
   IF p1.lbde_type IS NOT NULL
    THEN
     OPEN c_bty_exists(p1.lbde_type);
     FETCH c_bty_exists INTO l_bty_exists;
      IF c_bty_exists%NOTFOUND
       THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',082);
      END IF;
    CLOSE c_bty_exists;
   END IF;
--
-- If the address details have been supplied
-- make sure it's in the correct combination
--
  IF (   p1.lbde_adr1            IS NOT NULL
      OR p1.lbde_adr2            IS NOT NULL
      OR p1.lbde_adr3            IS NOT NULL
      OR p1.lbde_pcode           IS NOT NULL
     )
   THEN 
    IF( p1.lbde_adr1             IS NULL
       OR p1.lbde_adr2           IS NULL
       OR p1.lbde_adr3           IS NULL
       OR p1.lbde_pcode          IS NULL
       )
	 THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',083);
    END IF;
  END IF;
--
--************************
-- Now UPDATE the record status and process count
--
  IF (l_errors = 'F') THEN
   l_error_ind := 'Y';
  ELSE
   l_error_ind := 'N';
  END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
   i := i+1;
--
   IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
   s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
   set_record_status_flag(l_id,l_errors);
--
   EXCEPTION
    WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    set_record_status_flag(l_id,'O');
--
  END;
--
 END LOOP;
 COMMIT;
--
fsc_utils.proc_END;
--
COMMIT;
--
EXCEPTION
WHEN OTHERS THEN
s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- ***********************************************************************
--
PROCEDURE dataload_delete
        ( p_batch_id          IN VARCHAR2
        , p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT  rowid rec_rowid
      , lbde_dlb_batch_id
      , lbde_dl_seqno
      , lbde_dl_load_status
      , upper(lbde_type)                       lbde_type
      , lbde_branch
      , upper(lbde_branch_code)                lbde_branch_code
      , upper(lbde_adr1)                       lbde_adr1
      , upper(lbde_adr2)                       lbde_adr2
      , upper(lbde_adr3)                       lbde_adr3
      , upper(lbde_pcode)                      lbde_pcode
      , upper(lbde_phone)                      lbde_phone
      , upper(lbde_bank_code)                  lbde_bank_code
      , lbde_bank_name
      , lbde_amended
      , lbde_bde_refno
      , lbde_adr_refno
FROM    dl_hra_bank_details
WHERE   lbde_dlb_batch_id    = p_batch_id
AND     lbde_dl_load_status  = 'C';
--
-- ***********************************************************************
--
-- Additional Cursors
--
CURSOR  c_bde_exists( p_bank_name    VARCHAR2
                    , p_branch_name  VARCHAR2 ) IS
SELECT  bde_refno
      , trunc(bde_created_date)   bde_created_date
FROM    bank_details
WHERE   bde_bank_name           = p_bank_name
AND     NVL(bde_branch_name,1)  = NVL(p_branch_name,1)
--
--      1.4 bank and branch name together are unique so if record has not been modified
--          this must be the one created by the dataload if select returns a row (JS)
--
AND     bde_modified_date IS NULL;
--
CURSOR  c_aus( p_bde_refno    NUMBER
             , p_start_date   DATE ) IS
SELECT  'Y'
FROM    address_usages
WHERE   aus_aut_fao_code  = 'BDE'
AND     aus_aut_far_code  = 'PHYSICAL'
AND     aus_end_date      IS NULL
AND     aus_bde_refno     = p_bde_refno
AND     aus_start_date    = p_start_date;
--
CURSOR  c_cde( p_bde_refno    NUMBER
             , p_start_date   DATE ) IS
SELECT  'Y'
FROM    contact_details
WHERE   cde_frv_cme_code  = 'TEL'
AND     cde_end_date      IS NULL
AND     cde_bde_refno     = p_bde_refno;
--
CURSOR  chk_adr_refno( p_adr_refno NUMBER) IS
SELECT  count(*)
FROM    address_usages
WHERE   aus_adr_refno = p_adr_refno;
--
-- ***********************************************************************
--
-- Constants FOR process_summary
--
cb                   VARCHAR2(30);
cd                   DATE;
cp                   VARCHAR2(30) := 'DELETE';
ct                   VARCHAR2(30) := 'DL_HRA_BANK_DETAILS';
cs                   INTEGER;
ce                   VARCHAR2(200);
l_id                 ROWID;
l_an_tab             VARCHAR2(1);
--
--
-- ***********************************************************************
--
-- Other variables
--
l_exists             VARCHAR2(1);
i                    INTEGER :=0;
l_bde_refno          bank_details.bde_refno%type;
l_bde_created_date   bank_details.bde_created_date%type;
l_chk_adr_refno      INTEGER :=0;
--
-- ***********************************************************************
--
BEGIN
--
fsc_utils.proc_start('s_dl_hra_bank_details.dataload_delete');
fsc_utils.debug_message('s_dl_hra_bank_details.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
 FOR p1 in c1 LOOP
--
  BEGIN
--
  cs   := p1.lbde_dl_seqno;
  l_id := p1.rec_rowid;
  SAVEPOINT SP1;
--
-- Check if we have created this bank detail
--
  l_exists           := NULL;
  l_bde_refno        := NULL;
  l_bde_created_date := NULL;
  l_chk_adr_refno    :=0;
--
  OPEN c_bde_exists ( p1.lbde_bank_name
                    , p1.lbde_branch);
  FETCH c_bde_exists INTO l_bde_refno, l_bde_created_date;
  CLOSE c_bde_exists;
--
-- We should first deal with Addresses and Contact details
--
  IF p1.lbde_adr1 is not null
   THEN
    OPEN c_aus(l_bde_refno, p1.lbde_amended);
    FETCH c_aus into l_exists;
    CLOSE c_aus;
--
    IF nvl(l_exists, 'N') = 'Y'
     THEN  -- delete from address usages
--
      DELETE FROM address_usages
       WHERE  aus_bde_refno     = l_bde_refno
         AND  aus_aut_fao_code  = 'BDE'
         AND  aus_aut_far_code  = 'PHYSICAL'
         AND  aus_end_date      IS NULL
         AND  aus_start_date    = p1.lbde_amended;
--
-- Take the end date out of previous entry
--
      UPDATE  address_usages
         SET  aus_end_date      = NULL
       WHERE  aus_bde_refno     = l_bde_refno
         AND  aus_aut_fao_code  = 'BDE'
         AND  aus_aut_far_code  = 'PHYSICAL'
         AND  aus_end_date      = p1.lbde_amended-1;
--
    END IF;   /*   IF nvl(l_exists, 'N') = 'Y'   */
  END IF;   /*   IF p1.lbde_adr1 is not null   */
--
--
-- Now check Contact details
-- Revised code as should only hold 1 number
-- therefore just delete the record
--
-- 1.4 was originally IF l_bde_created_date = p1.lbde_amended -- created by this process so delete
-- but this does not take account of the fact that database trigger BDE_BRI overwrites the created date supplied with sysdate
-- Therefore check now needs to be on the bank and branch name matching the original if the record has not been changed (JS)
--
  IF l_bde_refno IS NOT NULL
   THEN
--
    DELETE FROM  contact_details
          WHERE  cde_bde_refno     = l_bde_refno
            AND  cde_frv_cme_code  = 'TEL'
            AND  cde_end_date      IS NULL;
--
    DELETE FROM bank_details
          WHERE bde_refno = l_bde_refno;
--
  END IF;   /*   IF l.bde_refno IS NOT NULL   */
--
-- Section to cleanse address details
-- Now that address usages have been removed
--
-- Remove the addresses if address reference is no longer
-- associated with an address usage as can be reasonable confident
-- that this process created it as no created date on addresses table
--
  IF p1.lbde_adr_refno IS NOT NULL
   THEN
    OPEN chk_adr_refno (p1.lbde_adr_refno);
    FETCH chk_adr_refno INTO l_chk_adr_refno;
    CLOSE chk_adr_refno;
--
    IF l_chk_adr_refno = 0
     THEN
       DELETE FROM addresses
       WHERE adr_refno = p1.lbde_adr_refno;
    END IF;
  END IF;
--
-- Now remove lbde_bde_refno and lbde_adr_refno from data load record
--
      UPDATE  dl_hra_bank_details
         SET  lbde_bde_refno = NULL
       WHERE  rowid = p1.rec_rowid
	     AND  lbde_bde_refno IS NOT NULL;
--
      UPDATE  dl_hra_bank_details
         SET  lbde_adr_refno = NULL
       WHERE  rowid = p1.rec_rowid
	     AND  lbde_adr_refno IS NOT NULL;
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
--
  s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
  set_record_status_flag(l_id,'V');
--
    i := i +1; IF mod(i,5000) = 0 THEN commit; END IF;
--
   EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
--
-- Section to analyse the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('BANK_DETAILS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_USAGES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('CONTACT_DETAILS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESSES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_ELEMENTS');
--
fsc_utils.proc_end;
COMMIT;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
END s_dl_hra_bank_details;
/

SHOW ERRORS
