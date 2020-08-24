CREATE OR REPLACE PACKAGE BODY s_dl_had_registered_addresses
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION DB Ver WHO  WHEN       WHY
--    1.0          MK   08/08/13   Initial Creation
--    1.1   6.9    PJD  31/01/14   Proper Cursors to replace direct
--                                 Select statememt in Validate proc
--    1.2   6.13   AJ   25/02/16   Altered validate to allow for the
--                                 lrega_ael_street_index_code not being 
--                                 mandatory as created and returned by the
--                                 create function in s_dl_hem_utils if not
--                                 supplied  
--
-- ***********************************************************************
--
--
PROCEDURE set_record_status_flag
 (p_rowid  IN ROWID
 ,p_status IN VARCHAR2
 )
AS
BEGIN
  UPDATE dl_had_registered_addresses
  SET    lrega_dl_load_status = p_status
  WHERE  ROWID = p_rowid;
EXCEPTION
WHEN OTHERS
THEN
  dbms_output.put_line('Error updating status of dl_hem_addresses');
  RAISE;
END set_record_status_flag;
--
-- ***********************************************************************
--
PROCEDURE dataload_create
  (p_batch_id          IN VARCHAR2
  ,p_date              IN DATE
  )
AS
--
CURSOR c1
IS
  SELECT ROWID rec_rowid
  ,      lrega_dlb_batch_id
  ,      lrega_dl_seqno
  ,      lrega_dl_load_status
  ,      lrega_legacy_ref
  ,      lrega_adre_code
  ,      lrega_start_date
  ,      lrega_end_date
  ,      lrega_hrv_rae_code
  ,      lrega_proposed_end_date
  ,      lrega_aun_code
  ,      lrega_comments
  ,      lrega_created_by
  ,      lrega_created_date
  ,      lrega_adr_flat
  ,      lrega_adr_building
  ,      lrega_adr_street_number
  ,      lrega_ael_street_index_code
  ,      lrega_ael_street
  ,      lrega_ael_area
  ,      lrega_ael_town
  ,      lrega_ael_county
  ,      lrega_ael_country
  ,      lrega_ael_postcode
  ,      lrega_ael_local_ind
  ,      lrega_ael_abroad_ind
  ,      lrega_adr_eastings
  ,      lrega_adr_northings
  ,      lrega_adr_uprn
  FROM   dl_had_registered_addresses
  WHERE  lrega_dlb_batch_id    = p_batch_id
  AND    lrega_dl_load_status = 'V';
--
-- Constants FOR process_summary
--
cb           VARCHAR2(30);
cd           DATE;
cp           VARCHAR2(30) := 'CREATE';
ct           VARCHAR2(30) := 'DL_HAD_REGISTERED_ADDRESSES';
cs           INTEGER;
ce           VARCHAR2(200);
l_id         ROWID;
--
-- Other variables
--
i            INTEGER := 0;
l_adr_refno  addresses.adr_refno%TYPE;
l_street_index_code VARCHAR2(20);
l_rega_refno registered_addresses.rega_refno%TYPE;
l_an_tab     VARCHAR2(30);
BEGIN
fsc_utils.proc_start('s_dl_had_registered_addresses.dataload_create');
fsc_utils.debug_message('s_dl_had_registered_addresses.dataload_create',3);
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
FOR p1 IN c1 LOOP
BEGIN
cs := p1.lrega_dl_seqno;
l_id := p1.rec_rowid;
SAVEPOINT SP1;
--
IF p1.lrega_ael_street_index_code IS NULL
  THEN
   l_street_index_code := NULL;
  ELSE 
   l_street_index_code := p1.lrega_ael_street_index_code;
END IF;
l_adr_refno         := NULL;
--
s_dl_hem_utils.insert_address
       (p_street_index_code     => l_street_index_code
       ,p_adr_refno             => l_adr_refno
       ,p_flat                  => p1.lrega_adr_flat
       ,p_building              => p1.lrega_adr_building
       ,p_street_number         => p1.lrega_adr_street_number
       ,p_street                => p1.lrega_ael_street
       ,p_area                  => p1.lrega_ael_area
       ,p_town                  => p1.lrega_ael_town
       ,p_county                => p1.lrega_ael_county
       ,p_pcode                 => p1.lrega_ael_postcode
       ,p_country               => p1.lrega_ael_country
       ,p_start_date            => p1.lrega_start_date
       ,p_end_date              => p1.lrega_end_date
       ,p_local_ind             => p1.lrega_ael_local_ind
       ,p_abroad_ind            => p1.lrega_ael_abroad_ind
       ,p_eastings              => p1.lrega_adr_eastings
       ,p_northings             => p1.lrega_adr_northings
       ,p_uprn                  => p1.lrega_adr_uprn
       );
-- DBMS_OUTPUT.PUT_LINE('adr_Refno '||l_adr_Refno);

-- Insert the registered address
--
l_rega_refno := NULL;
SELECT rega_refno_seq.NEXTVAL
INTO   l_rega_refno
FROM   dual;
INSERT INTO registered_addresses
      (rega_refno
      ,rega_adre_code
      ,rega_hrv_rae_code
      ,rega_start_date
      ,rega_reusable_refno
      ,rega_created_by
      ,rega_created_date
      ,rega_adr_refno
      ,rega_end_date
      ,rega_comments
      ,rega_proposed_end_date
      ,rega_aun_code
      )
      VALUES
      (l_rega_refno
      ,p1.lrega_adre_code
      ,p1.lrega_hrv_rae_code
      ,p1.lrega_start_date
      ,reusable_refno_seq.NEXTVAL
      ,NVL(p1.lrega_created_by,USER)
      ,NVL(p1.lrega_created_date,SYSDATE)
      ,l_adr_refno
      ,p1.lrega_end_date
      ,p1.lrega_comments
      ,p1.lrega_proposed_end_date
      ,p1.lrega_aun_code
      );
--
-- Update the datalaoad record with the inserted street index so that we can delete it if necessary
--
UPDATE dl_had_registered_addresses
SET    lrega_ins_rega_refno        = l_rega_refno
,      lrega_ins_adr_refno         = l_adr_refno
,      lrega_ins_street_index_code = l_street_index_code
WHERE  ROWID = p1.rec_rowid;
--
-- keep a count of the rows processed and commit after every 1000
--
i := i + 1;
IF MOD(i,1000) = 0
THEN
  COMMIT;
END IF;
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
--
-- Set the dataload statuses
--
set_record_status_flag(l_id,'C');
EXCEPTION
WHEN OTHERS
THEN
  ROLLBACK TO SP1;
  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
  set_record_status_flag(l_id,'O');
  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
END;
END LOOP;
--
-- Section to analyze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESSES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADDRESS_ELEMENTS');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('REGISTERED_ADDRESSES');
fsc_utils.proc_end;
COMMIT;
--
EXCEPTION
WHEN OTHERS
THEN
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
END dataload_create;
--
PROCEDURE dataload_validate
  (p_batch_id          IN VARCHAR2
  ,p_date              IN DATE
  )
AS
--
CURSOR c1
IS
  SELECT ROWID rec_rowid
  ,      lrega_dlb_batch_id
  ,      lrega_dl_seqno
  ,      lrega_dl_load_status
  ,      lrega_legacy_ref
  ,      lrega_adre_code
  ,      lrega_start_date
  ,      lrega_end_date
  ,      lrega_hrv_rae_code
  ,      lrega_proposed_end_date
  ,      lrega_aun_code
  ,      lrega_comments
  ,      lrega_created_by
  ,      lrega_created_date
  ,      lrega_adr_flat
  ,      lrega_adr_building
  ,      lrega_adr_street_number
  ,      lrega_ael_street_index_code
  ,      lrega_ael_street
  ,      lrega_ael_area
  ,      lrega_ael_town
  ,      lrega_ael_county
  ,      lrega_ael_country
  ,      lrega_ael_postcode
  ,      lrega_ael_local_ind
  ,      lrega_ael_abroad_ind
  ,      lrega_adr_eastings
  ,      lrega_adr_northings
  ,      lrega_adr_uprn
  FROM   dl_had_registered_addresses
  WHERE  lrega_dlb_batch_id    = p_batch_id
  AND    lrega_dl_load_status IN ('L','F','O');
--
CURSOR c_adre_code (p_adre_code VARCHAR2) IS
SELECT 'Y'
FROM   address_registers
WHERE  adre_code = p_adre_code;
--
CURSOR c_aun_code (p_aun_code VARCHAR2) IS
SELECT 'Y'
FROM   admin_units
WHERE  aun_code = p_aun_code;
--
CURSOR c_ex_with_other_sc (p_lrega_ael_street_index_code VARCHAR2
                        ,p_lrega_ael_street VARCHAR2
                        ,p_lrega_ael_area VARCHAR2
                        ,p_lrega_ael_town VARCHAR2
                        ,p_lrega_ael_county VARCHAR2
                        ,p_lrega_ael_postcode VARCHAR2
                        ,p_lrega_ael_country VARCHAR2
                        ) IS
SELECT 'Y'
FROM   address_elements
WHERE  ael_street_index_code !=  p_lrega_ael_street_index_code
AND    NVL(ael_street,'XYZ')  =  NVL(p_lrega_ael_street,'XYZ')
AND    NVL(ael_area,'XYZ')    =  NVL(p_lrega_ael_area,'XYZ')
AND    NVL(ael_town,'XYZ')    =  NVL(p_lrega_ael_town,'XYZ')
AND    NVL(ael_county,'XYZ')  =  NVL(p_lrega_ael_county,'XYZ')
AND    NVL(ael_postcode,'XYZ') =  NVL(p_lrega_ael_postcode,'XYZ')
AND    NVL(ael_country,'XYZ') =  NVL(p_lrega_ael_country,'XYZ')
;
--
-- constants FOR error process
--
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HAD_REGISTERED_ADDRESSES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- Other Variables
i                    INTEGER:=0;
l_errors             VARCHAR2(10);
l_error_ind          VARCHAR2(10);
l_dummy              VARCHAR2(50);
l_exists             VARCHAR2(1);
BEGIN
  fsc_utils.proc_start('s_dl_had_registered_addresses.dataload_validate');
  fsc_utils.debug_message('s_dl_had_registered_addresses.dataload_validate',3);
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR p1 IN c1
  LOOP
    BEGIN
      cs := p1.lrega_dl_seqno;
      l_id := p1.rec_rowid;
      SAVEPOINT SP1;
      l_errors := 'V';
      l_error_ind := 'N';
      --
      -- Check Address Register Code is Supplied
      --
      IF p1.lrega_adre_code IS NULL
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',263);
      ELSE
        --
        -- Check that the adre_code exists
        --
        l_exists := NULL;
        OPEN c_adre_code (p1.lrega_adre_code);
        FETCH c_adre_code INTO l_exists;
        CLOSE c_adre_code;
        IF NVL(l_exists,'N') = 'N'
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',264);
        END IF;
      END IF;
      --
      -- Check Start Date is supplied
      --
      IF p1.lrega_start_date IS NULL
      THEN
        l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',12);
      END IF;
      --
      -- Check end date is not before start date where supplied
      --
      IF p1.lrega_end_date IS NOT NULL
      THEN
        IF p1.lrega_end_date < p1.lrega_start_date
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',3);
        END IF;
      END IF;
      --
      -- Check End Reason supplied where End Date is supplied
      --
      IF p1.lrega_end_date IS NOT NULL
      THEN
        IF p1.lrega_hrv_rae_code IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',5);
        END IF;
      END IF;
      --
      -- Check End Reason Exists
      --
      IF p1.lrega_hrv_rae_code IS NOT NULL
      THEN
        IF NOT s_dl_hem_utils.exists_frv('REG_ADDR_END_REASON',p1.lrega_hrv_rae_code,'Y')
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',262);
        END IF ;
      END IF;
      --
      -- Check that the aun_code exists
      --
      IF p1.lrega_aun_code IS NOT NULL
      THEN
        l_exists := NULL;
        OPEN c_aun_code( p1.lrega_aun_code);
        FETCH c_aun_code INTO l_exists;
        CLOSE c_aun_Code;
        IF l_exists IS NULL
        THEN
          l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD2',127);
        END IF;
      END IF;
      --
      -- Check that the same street is not already present under a different street index code
      -- Only required if Street Index Code supplied as address will be checked and code will
      -- be returned by create which would have either found a current one or created one
      --
      IF (p1.lrega_ael_street_index_code IS NOT NULL)
        THEN
        l_exists := NULL;
        OPEN   c_ex_with_other_sc (p1.lrega_ael_street_index_code
                                  ,p1.lrega_ael_street
                                  ,p1.lrega_ael_area
                                  ,p1.lrega_ael_town
                                  ,p1.lrega_ael_county
                                  ,p1.lrega_ael_postcode
                                  ,p1.lrega_ael_country);
        FETCH  c_ex_with_other_sc INTO l_exists;
        CLOSE  c_ex_with_other_sc;
        IF NVL(l_exists,'N') = 'Y'
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',48);
        END IF;
      END IF;
      --
      -- Check that a valid combination of address columns have been supplied
      --
      IF p1.lrega_ael_street IS NULL
      AND p1.lrega_ael_area IS NULL
      AND p1.lrega_ael_town IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
      ELSIF p1.lrega_ael_street    IS NOT NULL
      AND p1.lrega_adr_flat      IS NOT NULL
      AND p1.lrega_adr_building  IS NULL
      AND p1.lrega_adr_street_number IS NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',962);
      END IF;
      --
      -- Check the Street name has been supplied
      --
      IF p1.lrega_adr_flat IS NOT NULL
      OR p1.lrega_adr_building IS NOT NULL
      OR p1.lrega_adr_street_number IS NOT NULL
      OR p1.lrega_ael_street IS NOT NULL
      OR p1.lrega_ael_area IS NOT NULL
      OR p1.lrega_ael_town IS NOT NULL
      OR p1.lrega_ael_county IS NOT NULL
      OR p1.lrega_ael_postcode IS NOT NULL
      THEN
        IF p1.lrega_ael_street IS NULL
        AND p1.lrega_ael_area IS NULL
        AND p1.lrega_ael_town IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',943);
        END IF;
      END IF;
      --
      -- Check that IF street name supplied that either the flat No,
      -- building or street no has also been supplied.
      --
      IF p1.lrega_ael_street IS NOT NULL
      OR p1.lrega_ael_area IS NOT NULL
      OR p1.lrega_ael_town IS NOT NULL
      THEN
        IF p1.lrega_adr_flat IS NULL
        AND p1.lrega_adr_building IS NULL
        AND p1.lrega_adr_street_number IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',944);
        END IF;
      END IF;
      --
      -- Check that IF street name supplied that either the flat No,
      -- building or street no has also been supplied.
      --
      IF p1.lrega_ael_street IS NOT NULL
      OR p1.lrega_ael_area IS NOT NULL
      OR p1.lrega_ael_town IS NOT NULL
      THEN
        IF p1.lrega_adr_flat IS NOT NULL
        AND p1.lrega_adr_building IS NULL
        AND p1.lrega_adr_street_number IS NULL
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',963);
        END IF;
      END IF;
      --
      -- Check that the address END date is NOT earlier than the address
      -- start date
      --
      IF p1.lrega_end_date IS NOT NULL
      THEN
        IF p1.lrega_end_date < p1.lrega_start_date
        THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',882);
        END IF;
      END IF;
      --
      -- Abroad Ind
      --
      IF NOT s_dl_hem_utils.yornornull(p1.lrega_ael_abroad_ind)
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',972);
      END IF;
      --
      -- Local Ind
      --
      IF NOT s_dl_hem_utils.yornornull(p1.lrega_ael_local_ind)
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',005);
      END IF;
      --
      -- Now UPDATE the record count and error code
      --
      IF l_errors = 'F'
      THEN
        l_error_ind := 'Y';
      ELSE
        l_error_ind := 'N';
      END IF;
      --
      -- keep a count of the rows processed and commit after every 1000
      --
      i := i + 1;
      IF MOD(i,1000) = 0
      THEN
        COMMIT;
      END IF;
      s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
      set_record_status_flag(l_id,l_errors);
    EXCEPTION
    WHEN OTHERS
    THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
    END;
  END LOOP;
  COMMIT;
  fsc_utils.proc_END;
--   EXCEPTION
--   WHEN OTHERS
--   THEN
--     s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--     RAISE;
END dataload_validate;
--
PROCEDURE dataload_delete
  (p_batch_id        IN VARCHAR2
  ,p_date            IN DATE
  )
IS
CURSOR c1
IS
  SELECT ROWID rec_rowid
  ,      lrega_dlb_batch_id
  ,      lrega_dl_seqno
  ,      lrega_ins_street_index_code
  ,      lrega_ins_adr_refno
  ,      lrega_ins_rega_refno
  FROM   dl_had_registered_addresses
  WHERE  lrega_dlb_batch_id = p_batch_id
  AND    lrega_dl_load_status = 'C';
i INTEGER := 0;
-- Constants FOR process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HAD_REGISTERED_ADDRESSES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
BEGIN
  fsc_utils.proc_start('s_dl_had_registered_addresses.dataload_delete');
  fsc_utils.debug_message( 's_dl_had_registered_addresses.dataload_delete',3 );
  cb := p_batch_id;
  cd := p_DATE;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  FOR p1 IN c1
  LOOP
    BEGIN
      cs := p1.lrega_dl_seqno;
      l_id := p1.rec_rowid;
      IF p1.lrega_ins_rega_refno IS NOT NULL
      THEN
        DELETE FROM registered_addresses
        WHERE  rega_refno = p1.lrega_ins_rega_refno;
      END IF;
      IF p1.lrega_ins_adr_refno IS NOT NULL
      THEN
        BEGIN
          DELETE FROM addresses
          WHERE  adr_refno = p1.lrega_ins_adr_refno;
        EXCEPTION
        WHEN OTHERS
        THEN
          NULL;
        END;
      END IF;
      IF p1.lrega_ins_street_index_code IS NOT NULL
      THEN
        BEGIN
          DELETE FROM address_elements
          WHERE  ael_street_index_code = p1.lrega_ins_street_index_code;
        EXCEPTION
        WHEN OTHERS
        THEN
          NULL;
        END;
      END IF;
      i := i + 1;
      IF mod(i,5000) = 0
      THEN
        COMMIT;
      END IF;
      s_dl_process_summary.UPDATE_processed_count(cb,cp,cd,'N');
      set_record_status_flag(l_id,'V');
    EXCEPTION
    WHEN OTHERS
    THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      set_record_status_flag(l_id,'C');
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    END;
  END LOOP;
  COMMIT;
  fsc_utils.proc_end;
EXCEPTION
WHEN OTHERS
THEN
  s_dl_utils.set_record_status_flag(ct,cb,cs,'C');
  s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
  RAISE;
END dataload_delete;
END s_dl_had_registered_addresses;
/
show errors