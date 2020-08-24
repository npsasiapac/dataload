CREATE OR REPLACE PACKAGE BODY s_dl_hem_property_landlords
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB Ver   WHO  WHEN       	WHY
--      1.0           VRS  21-MAY-2006  Initial Dataload
--      1.1           PJD  07-SEP-2011  Change Errors to HD1 range 201 to 216
--      1.2			  MB   29-SEP-2011  Put in own set_record_status_flag procedure
--      1.3			  AJ   28-JUL-2016  Amended validate around Agent details being supplied
--                                      as only mandatory if paying Agent also changed create
--                                      so l_agent_par_refno only gets set if Agent supplied
--                                      otherwise set to NULL
--      1.4			  AJ   29-JUL-2016  Amended to default payment direct and living abroad
--                                      indicators if not supplied and validate corrected for
--                                      lpld_pro_propref check
--      1.5           AJ   02-AUG-2016  slight amendment to correct compile error when assign default
--                                      values with cursor c1 and LPLD_LIVING_ABROAD_IND default in
--                                      validate corrected to 'N' rather than 'Y' to match create  
--
--  declare package variables AND constants
--
-- *******************************************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
  UPDATE dl_hem_property_landlords
  SET lpld_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_property_landlords');
     RAISE;
  --
END set_record_status_flag;
--
PROCEDURE dataload_create(p_batch_id	IN VARCHAR2,
                          p_date	IN DATE)
AS
--
CURSOR c1 is
SELECT LPLD_DL_SEQNO,
       LPLD_PRO_PROPREF,
       LPLD_START_DATE,
       LPLD_PAR_REFNO,
       LPLD_ALT_PAR_REF,
       NVL(LPLD_PAY_LANDLORD_DIRECTLY_IND,'Y') LPLD_PAY_LANDLORD_DIRECTLY_IND,
       NVL(LPLD_LIVING_ABROAD_IND,'N')         LPLD_LIVING_ABROAD_IND,
       LPLD_AGENT_PAR_REFNO,
       LPLD_AGENT_ALT_PAR_REF,
       LPLD_END_DATE,
       LPLD_ALTERNATIVE_REFERENCE,
       LPLD_COMMENTS,
       LPLD_REFNO,
	   rowid rec_rowid
--
  FROM dl_hem_property_landlords
--
 WHERE lpld_dlb_batch_id   = p_batch_id
   AND lpld_dl_load_status = 'V';
--
-- ************************************************************
--
-- Get the par_refno for lpld_alt_par_ref and lpld_agent_alt_par_ref
--
CURSOR get_par_refno(p_alt_par_ref VARCHAR2) IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_alt_par_ref;
--
-- ************************************************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_PROPERTY_LANDLORDS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
i			INTEGER := 0;
l_an_tab		VARCHAR2(1);
l_pro_refno		NUMBER(10);
l_par_refno		NUMBER(10);
l_agent_par_refno	NUMBER(10);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_property_landlords.dataload_create');
    fsc_utils.debug_message('s_dl_hem_property_landlords.dataload_create',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lpld_dl_seqno;
		  l_id := p1.rec_rowid;
--
          l_pro_refno 	    := NULL;
          l_par_refno 	    := NULL;
          l_agent_par_refno := NULL;
--
--
-- get the pro_refno this is mandatory for this load as pld_adr_refno cannot be loaded
-- on this data loader and one or the other must be supplied
--
          IF (p1.lpld_pro_propref IS NOT NULL) THEN
--
           l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lpld_pro_propref);
--
          END IF;
--
--
--
-- get the par_refno
--
          IF (p1.lpld_alt_par_ref IS NOT NULL) THEN
--
            OPEN get_par_refno(p1.lpld_alt_par_ref);
           FETCH get_par_refno INTO l_par_refno;
           CLOSE get_par_refno;
--
          ELSE
--
           l_par_refno := p1.lpld_par_refno;
--
          END IF;
--
--
--
-- get the agent_par_refno
--
          IF (p1.lpld_agent_alt_par_ref IS NOT NULL) THEN
--
            OPEN get_par_refno(p1.lpld_agent_alt_par_ref);
           FETCH get_par_refno INTO l_agent_par_refno;
           CLOSE get_par_refno;
--
          ELSE
--
           IF (p1.lpld_agent_par_refno IS NOT NULL)  THEN
             l_agent_par_refno := p1.lpld_agent_par_refno;
           END IF;
--
          END IF;
--
    INSERT INTO property_landlords(PLD_REFNO, --NN
                                   PLD_PRO_REFNO, --N
                                   PLD_START_DATE, --NN
                                   PLD_PAR_REFNO, --NN
                                   PLD_PAY_LANDLORD_DIRECTLY_IND, --NN
                                   PLD_LIVING_ABROAD_IND, --NN
                                   PLD_AGENT_PAR_REFNO, --N
                                   PLD_END_DATE, --N
                                   PLD_ALTERNATIVE_REFERENCE, --N
                                   PLD_COMMENTS --N
                                   )
--
                                   VALUES(p1.lpld_refno,
                                   l_pro_refno,
                                   p1.lpld_start_date,
                                   l_par_refno,
                                   p1.lpld_pay_landlord_directly_ind,
                                   p1.lpld_living_abroad_ind,
                                   l_agent_par_refno,
                                   p1.lpld_end_date,
                                   p1.lpld_alternative_reference,
                                   p1.lpld_comments
                                   );
--
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'C');
--
          EXCEPTION
               WHEN OTHERS THEN
                    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
                    set_record_status_flag(l_id,'O');
                    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
--
    END LOOP;
--
-- Section to analyse the table populated by this dataload
--
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PROPERTY_LANDLORDS');
--
    fsc_utils.proc_end;
    COMMIT;
--
    EXCEPTION
         WHEN OTHERS THEN
              s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
         RAISE;
--
END dataload_create;
--
-- *******************************************************************************************
--
PROCEDURE dataload_validate (p_batch_id          IN VARCHAR2,
     			     p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT LPLD_DL_SEQNO,
       LPLD_PRO_PROPREF,
       LPLD_START_DATE,
       LPLD_PAR_REFNO,
       LPLD_ALT_PAR_REF,
       NVL(LPLD_PAY_LANDLORD_DIRECTLY_IND,'Y') LPLD_PAY_LANDLORD_DIRECTLY_IND,
       NVL(LPLD_LIVING_ABROAD_IND,'N')         LPLD_LIVING_ABROAD_IND,
       LPLD_AGENT_PAR_REFNO,
       LPLD_AGENT_ALT_PAR_REF,
       LPLD_END_DATE,
       LPLD_ALTERNATIVE_REFERENCE,
       LPLD_COMMENTS,
       LPLD_REFNO,
	   rowid rec_rowid
--
  FROM dl_hem_property_landlords
--
 WHERE lpld_dlb_batch_id    = p_batch_id
   AND lpld_dl_load_status IN ('L','F','O');
--
-- ************************************************************
--
-- Check property_landlord reference doesn't already exist
--
CURSOR chk_pld_ref_exists(p_pld_refno NUMBER) IS
SELECT 'X'
  FROM property_landlords
 WHERE pld_refno = p_pld_refno;
--
-- ************************************************************
--
-- Check property_landlord record doesn't already exist
--
CURSOR chk_pld_exists(p_pro_refno  NUMBER,
                      p_start_date DATE,
                      p_par_refno  NUMBER) IS
SELECT 'X'
  FROM property_landlords
 WHERE pld_pro_refno  = p_pro_refno
   AND pld_start_date = p_start_date
   AND pld_par_refno  = p_par_refno;
--
-- ************************************************************
--
-- Get the par_refno for lpld_alt_par_ref and lpld_agent_alt_par_ref
--
CURSOR get_par_refno(p_alt_par_ref VARCHAR2) IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_alt_par_ref;
--
-- ************************************************************
--
-- Check pro_refno exists
--
CURSOR chk_par_refno_exists(p_pld_par_refno NUMBER) IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_pld_par_refno;
--
-- ************************************************************
--
-- constants FOR error process
--
cb 	VARCHAR2(30);
cd 	DATE;
cp 	VARCHAR2(30) := 'VALIDATE';
ct 	VARCHAR2(30) := 'DL_HEM_PROPERTY_LANDLORDS';
cs 	INTEGER;
ce 	VARCHAR2(200);
l_id     ROWID;
--
-- other variables
--
l_errors		VARCHAR2(10);
l_error_ind		VARCHAR2(10);
i			INTEGER := 0;
--
l_pld_ref_exists		VARCHAR2(1);
l_pld_exists			VARCHAR2(1);
l_par_refno_exists		VARCHAR2(1);
l_agent_par_refno_exists	VARCHAR2(1);
l_pro_refno			NUMBER(10);
l_par_refno			NUMBER(10);
l_agent_par_refno		NUMBER(10);
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_property_landlords.dataload_validate');
    fsc_utils.debug_message('s_dl_hem_property_landlords.dataload_validate',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lpld_dl_seqno;
		  l_id := p1.rec_rowid;
--
          l_errors := 'V';
          l_error_ind := 'N';
--
-- *********************************
--                                 *
-- VALIDATE FIELDS       	   *
--                                 *
-- *********************************
--
--
-- *******************************************************************************
--  
-- Check Property Landlord reference doesn't already exist (lpld_refno)
--
--
          IF (p1.lpld_refno IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',201);
-- 
          ELSE
--
             l_pld_ref_exists := NULL;
--
              OPEN chk_pld_ref_exists(p1.lpld_refno);
             FETCH chk_pld_ref_exists INTO l_pld_ref_exists;
             CLOSE chk_pld_ref_exists;
--
             IF (l_pld_ref_exists IS NOT NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',202);
--
             END IF;
--             
          END IF;
--
-- *******************************************************************************
-- 
-- Check Property Landlord Property Ref is supplied and valid (lpld_pro_propref)
-- This is mandatory for this load as pld_adr_refno is not included on this data
-- loader and one or the other must be supplied in the table
--
          IF (p1.lpld_pro_propref IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',203);
          END IF;
--
          IF (p1.lpld_pro_propref IS NOT NULL) THEN
           l_pro_refno := NULL;
           l_pro_refno := s_dl_hem_utils.pro_refno_FOR_propref(p1.lpld_pro_propref);
--
           IF (l_pro_refno is NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',55);
           END IF;
          END IF;
--
-- *******************************************************************************
-- 
-- Check Property Landlord Start Date is supplied (lpld_start_date)
--
--
          IF (p1.lpld_start_date IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',204);
          END IF;
--
-- *******************************************************************************
-- 
-- Check Property Landlord Par Refno or alt_par_ref, one or the other, not both is 
-- supplied and valid (lpld_par_refno, lpld_alt_par_ref)
--
--
          IF (    p1.lpld_par_refno   IS NULL
              AND p1.lpld_alt_par_ref IS NULL) THEN
--
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',205);
--
          ELSIF (    p1.lpld_par_refno   IS NOT NULL
                 AND p1.lpld_alt_par_ref IS NOT NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',206);
--
          ELSIF (p1.lpld_alt_par_ref IS NOT NULL) THEN
--
              l_par_refno := NULL;
--
               OPEN get_par_refno(p1.lpld_alt_par_ref);
              FETCH get_par_refno INTO l_par_refno;
              CLOSE get_par_refno;
--
              IF (l_par_refno IS NULL) THEN
--
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',207);
--
              END IF;
--
          ELSIF (p1.lpld_par_refno IS NOT NULL) THEN
--
              l_par_refno_exists := NULL;
--
               OPEN chk_par_refno_exists(p1.lpld_par_refno);
              FETCH chk_par_refno_exists INTO l_par_refno_exists;
              CLOSE chk_par_refno_exists;
--
              IF (l_par_refno_exists IS NULL) THEN
--
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',208);
--
              END IF;
--
          END IF;
--
-- *******************************************************************************
--
-- Check Property Landlord Pay Landlord Directly Indicator is Valid (lpld_pay_landlord_directly_ind)
-- if indicator not supplied in data load file will be defaulted to 'Y' by cursor c1
--
          IF (p1.lpld_pay_landlord_directly_ind IS NOT NULL) THEN
--
           IF (p1.lpld_pay_landlord_directly_ind NOT IN ('Y','N')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',209);
           END IF;
--
          END IF;
--
-- *******************************************************************************
--
-- Check Property Landlord Living Abroad Indicator is Valid (lpld_living_abroad_ind)
-- if indicator not supplied in data load file will be defaulted to 'N' by cursor c1
--
          IF (p1.lpld_living_abroad_ind IS NOT NULL) THEN
--
           IF (p1.lpld_living_abroad_ind NOT IN ('Y','N')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',210);
           END IF;
--
          END IF;
--
-- *******************************************************************************
--
-- Check Property Landlord Agent Par Refno or alt_par_ref, one or the other, not both is 
-- supplied and valid (lpld_agent_par_refno, lpld_agent_alt_par_ref)
--
-- Only fail if neither alt or par for agent provided if the payment direct to landlord
-- indicator (lpld_pay_landlord_directly_ind) is set to N as they are indicating they want
-- to pay the agent  (AJ 28Jul2016)
--
          IF (p1.lpld_pay_landlord_directly_ind = 'N')
           THEN	   
            IF (    p1.lpld_agent_par_refno   IS NULL
                AND p1.lpld_agent_alt_par_ref IS NULL) THEN
--
             l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',211);
--
            END IF;
          END IF;
--
-- The following checks are required is agent details are provided does not matter if
-- paying direct to agent or landlord
--
          IF  (   p1.lpld_agent_par_refno   IS NOT NULL
              AND p1.lpld_agent_alt_par_ref IS NOT NULL) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',212);
--
          ELSIF (p1.lpld_agent_alt_par_ref IS NOT NULL) THEN
--
              l_agent_par_refno := NULL;
--
               OPEN get_par_refno(p1.lpld_agent_alt_par_ref);
              FETCH get_par_refno INTO l_agent_par_refno;
              CLOSE get_par_refno;
--
              IF (l_agent_par_refno IS NULL) THEN
--
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',213);
--
              END IF;
--
          ELSIF (p1.lpld_agent_par_refno IS NOT NULL) THEN
--
              l_agent_par_refno_exists := NULL;
--
               OPEN chk_par_refno_exists(p1.lpld_agent_par_refno);
              FETCH chk_par_refno_exists INTO l_agent_par_refno_exists;
              CLOSE chk_par_refno_exists;
--
              IF (l_agent_par_refno_exists IS NULL) THEN
--
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',214);
--
              END IF;
--
          END IF;
--
-- *******************************************************************************
--
-- Check Property Landlord End Date < Property Landlord Start Date (lpld_end_date)
--
--
          IF (p1.lpld_end_date IS NOT NULL) THEN
--
           IF (p1.lpld_end_date < p1.lpld_start_date) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',215);
           END IF;
--
          END IF;
--
-- *******************************************************************************
--
-- Check Property Landlord record doesn't already exist 
-- (lpld_pro_propref, lpld_start_date, lpld_par_refno)
--
--
          IF (    l_pro_refno        IS NOT NULL
              AND p1.lpld_start_date IS NOT NULL
              AND (l_par_refno IS NOT NULL OR l_par_refno_exists IS NOT NULL)) THEN
--
           l_pld_exists := NULL;
--
            OPEN chk_pld_exists(l_pro_refno,p1.lpld_start_date, NVL(l_par_refno,p1.lpld_par_refno));
           FETCH chk_pld_exists INTO l_pld_exists;
           CLOSE chk_pld_exists;
--
           IF (l_pld_exists IS NOT NULL) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',216);
--
           END IF;

--
          END IF;
--
-- *******************************************************************************
--

-- Now UPDATE the record count and error code
--
          IF l_errors = 'F' THEN
           l_error_ind := 'Y';
          ELSE
             l_error_ind := 'N';
          END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
          set_record_status_flag(l_id,l_errors);
--
          EXCEPTION
               WHEN OTHERS THEN
                    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
                    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
                    set_record_status_flag(l_id,ce);
      END;
--
    END LOOP;
--
    fsc_utils.proc_END;
--
    commit;
--
    EXCEPTION
         WHEN OTHERS THEN
              s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
-- *******************************************************************************************
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT LPLD_DL_SEQNO,
       LPLD_PRO_PROPREF,
       LPLD_START_DATE,
       LPLD_PAR_REFNO,
       LPLD_ALT_PAR_REF,
       LPLD_REFNO,
	   rowid rec_rowid
--
  FROM dl_hem_property_landlords
--
 WHERE lpld_dlb_batch_id   = p_batch_id
   AND lpld_dl_load_status = 'C';
--
-- ************************************************************
--
-- Get the par_refno for lpld_alt_par_ref 
--
CURSOR get_par_refno(p_alt_par_ref VARCHAR2) IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_alt_par_ref;
--
-- ************************************************************
--
-- Constants FOR process_summary
--
cb       	VARCHAR2(30);
cd       	DATE;
cp       	VARCHAR2(30) := 'DELETE';
ct       	VARCHAR2(30) := 'DL_HEM_PROPERTY_LANDLORDS';
cs       	INTEGER;
ce       	VARCHAR2(200);
l_id     	ROWID;
--
i 			INTEGER := 0;
l_an_tab 	VARCHAR2(1);
l_pro_refno	NUMBER(10);
l_par_refno	NUMBER(10);
--
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hem_property_landlords.dataload_delete');
    fsc_utils.debug_message('s_dl_hem_property_landlords.dataload_delete',3);
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
    FOR p1 IN c1 LOOP
--
      BEGIN
--
          cs := p1.lpld_dl_seqno;
		  l_id := p1.rec_rowid;
--
          l_pro_refno 	    := NULL;
          l_par_refno 	    := NULL;
--
--
-- get the pro_refno
--
          IF (p1.lpld_pro_propref IS NOT NULL) THEN
--
           l_pro_refno := s_dl_hem_utils.pro_refno_for_propref(p1.lpld_pro_propref);
--
          END IF;
--
--
--
-- get the par_refno
--
          IF (p1.lpld_alt_par_ref IS NOT NULL) THEN
--
            OPEN get_par_refno(p1.lpld_alt_par_ref);
           FETCH get_par_refno INTO l_par_refno;
           CLOSE get_par_refno;
--
          ELSE
--
             l_par_refno := p1.lpld_par_refno;
--
          END IF;
--
--
-- Delete the Property Landlords Record from the Property_Landlords table
--
          DELETE 
            FROM PROPERTY_LANDLORDS
           WHERE pld_refno      = p1.lpld_refno
             AND pld_pro_refno  = l_pro_refno
             AND pld_start_date = p1.lpld_start_date
             AND pld_par_refno  = l_par_refno;
--
--
-- keep a count of the rows processed and commit after every 1000
--        
          i  := i +1;
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          EXCEPTION
               WHEN OTHERS THEN
                    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
                    set_record_status_flag(l_id,'C');
                    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
      END;
--
    END LOOP;
--
-- Section to analyse the table populated by this dataload
--
    l_an_tab := s_dl_hem_utils.dl_comp_stats('PROPERTY_LANDLORDS');
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
--
END s_dl_hem_property_landlords;
/
