CREATE OR REPLACE PACKAGE BODY s_dl_hpp_pp_appln_parties
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--      1.0  5.1.6     PH   14-APR-2002  Initial Creation
--      1.1  5.6.0     PH   13-OCT-2004  New fields added for 
--                                       non_tenant_appln_parties, admitted_ind,
--                                       denied ind and denied reason.
--      1.2  5.7.0     PH   19-JAN-2005  Amended error code on 'Wish to Buy'
--                                       from 825 to 826 (was incorrect)
--                                       Also commented out Islington Bespoke
--                                       which allows par_refno rather than
--                                       par_per_alt_ref.
--                                       Also added validate on Admitted Ind as
--                                       its mandatory for non tenants.
--      1.3 5.7.0      PH   21-JAN-2005  New validation added on TEN to make sure
--                                       they exist in tenancy_instances
--      1.4 5.9.0      PH   12-JUN-2006  Made use of batch question for using
--                                       par_refno or par_alt_ref. Amending Create,
--                                       Validate and Load processes
--      1.5 5.10.0     PH   20-NOV-2006  Amended create for non tenants. If they
--                                       dont exist in household_persons for the
--                                       application date then create a new entry.
--                                       If you create an appln party in the 
--                                       application that's what happens.
--      2.0 5.13.0     PH   06-FEB-2008  Now includes its own 
--                                       set_record_status_flag procedure.
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
  UPDATE dl_hpp_pp_appln_parties
  SET lpap_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hpp_pp_appln_parties');
     RAISE;
  --
END set_record_status_flag;
--
--      
-- ***********************************************************************     
--
--  declare package variables and constants
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lpap_dlb_batch_id        
,lpap_dl_seqno            
,lpap_dl_load_status      
,lpap_papp_displayed_reference 
,lpap_par_per_alt_ref     
,lpap_party_type      
,lpap_principal_home_ind      
,lpap_wish_to_buy_ind         
,lpap_lived_one_year        
,lpap_party_verified_ind      
,lpap_signature_verified_ind    
,lpap_signature_date            
,lpap_comments
,lpap_admitted_ind
,lpap_denied_ind
,lpap_hrv_denied_reason
FROM DL_HPP_PP_APPLN_PARTIES
WHERE lpap_dlb_batch_id    = p_batch_id
AND   lpap_dl_load_status = 'V';
--
-- Cursor to get papp_refno
--
CURSOR c_papp_refno(p_papp_ref varchar2) IS
SELECT papp_refno, papp_application_date, papp_tho_tcy_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_ref;
--
-- Cursor to get par_refno
--
CURSOR c_par_refno(p_par_refno varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_refno;
--
-- Cursor to get tenancy details
--
CURSOR c_tin_start(p_tcy_refno number
                  ,p_par_refno number
                  ,p_app_date  date) IS
SELECT tin_start_date, hop_refno
FROM   tenancy_instances,
       household_persons
WHERE  tin_hop_refno = hop_refno
AND    tin_tcy_refno = p_tcy_refno
AND    hop_par_refno = p_par_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
-- Cursor to get the hou_refno for a tenant so we
-- will then be able to get the right hop_refno for
-- a non_tenant;
--
CURSOR c_hou_refno(p_tcy_refno number
                  ,p_app_date  date) IS
SELECT hop_hou_refno
FROM   household_persons,
       tenancy_instances
WHERE  tin_hop_refno = hop_refno
AND    tin_tcy_refno = p_tcy_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
-- Cursor to get the hop_refno for a non tenant
--
CURSOR c_hop_refno(p_hou_refno number
                  ,p_par_refno number
                  ,p_app_date  date) IS
SELECT hop_refno
FROM   household_persons
WHERE  hop_hou_refno  = p_hou_refno
AND    hop_par_refno  = p_par_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
--
CURSOR c_hop_refno2(p_hou_refno number
                  ,p_par_refno number) IS
SELECT hop_refno
FROM   household_persons
WHERE  hop_hou_refno  = p_hou_refno
AND    hop_par_refno  = p_par_refno
AND    hop_end_date  is null;
--
CURSOR c_hop_refno_seq IS
SELECT hop_refno_seq.nextval
FROM   dual;
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_APPLN_PARTIES';
cs       INTEGER;
ce	   VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
-- Other variables
--
l_papp_refno number;
l_app_date   date;
i            integer := 0;
l_tcy_refno  number;
l_par_refno  number;
l_hou_refno  number;
l_hop_refno  number;
l_tin_start  date;
l_answer     VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_appln_parties.dataload_create');
fsc_utils.debug_message( 's_dl_hpp_pp_appln_parties.dataload_create',3);
--
cb := p_batch_id;
cd := p_date;
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Using Par Refno in place of Alt Ref?'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 in c1 LOOP
--
BEGIN
--
cs := p1.lpap_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
--
-- get the papp_refno
--
l_papp_refno := null;
l_app_date   := null;
l_tcy_refno  := null;
l_par_refno  := null;
l_tin_start  := null;
l_hop_refno  := null;
l_par_refno  := null;
 --
OPEN  c_papp_refno(p1.lpap_papp_displayed_reference);
 FETCH c_papp_refno INTO l_papp_refno, l_app_date, l_tcy_refno;
CLOSE c_papp_refno;
--
-- Use the answer to get the person record
--
  IF l_answer = 'Y'
   THEN l_par_refno := to_number(p1.lpap_par_per_alt_ref);
  ELSE
--
 OPEN  c_par_refno(p1.lpap_par_per_alt_ref);
 FETCH c_par_refno INTO l_par_refno;
 CLOSE c_par_refno;
--
  END IF;
--
OPEN  c_tin_start(l_tcy_refno, l_par_refno, l_app_date);
 FETCH c_tin_start INTO l_tin_start, l_hop_refno;
CLOSE c_tin_start;
--
-- Check to see if Tenant or not
--
 IF p1.lpap_party_type in ('TEN','TENANT')
  THEN
--
      INSERT INTO tenant_application_parties
         (tap_principle_home_ind        
         ,tap_wish_to_buy_ind           
         ,tap_party_verified_ind        
         ,tap_signature_verified_ind   
         ,tap_signature_date            
         ,tap_comments                  
         ,tap_created_date              
         ,tap_created_by             
         ,tap_tin_start_date            
         ,tap_tin_tcy_refno             
         ,tap_tin_hop_refno                 
         ,tap_papp_refno                
         )
      VALUES
         (p1.lpap_principal_home_ind 
         ,p1.lpap_wish_to_buy_ind
         ,p1.lpap_party_verified_ind 
         ,p1.lpap_signature_verified_ind
         ,p1.lpap_signature_date
         ,p1.lpap_comments
         ,trunc(sysdate)
         ,'DATALOAD'
         ,l_tin_start
         ,l_tcy_refno
         ,l_hop_refno
         ,l_papp_refno
         );
--
  ELSE
--
l_hop_refno := null;
--
OPEN  c_hou_refno(l_tcy_refno, l_app_date);
 FETCH c_hou_refno INTO l_hou_refno;
CLOSE c_hou_refno;
--
OPEN  c_hop_refno(l_hou_refno, l_par_refno, l_app_date);
 FETCH c_hop_refno INTO l_hop_refno;
CLOSE c_hop_refno;
--
-- New code added 20-NOV-2006. If the l_hop_refno is null
-- then create an entry into household_persons as this is
-- what happens in the application if the person is not
-- in there for the period. I think we should make sure
-- that there isn't an open ended one first.
-- If we don't do this insert the create will fail with an
-- oracle error as ntap_hop_refno is mandatory
--
  IF l_hop_refno IS NULL
   THEN
-- 
-- Check to see if there is a current one
--
   OPEN c_hop_refno2(l_hou_refno, l_par_refno);
    FETCH c_hop_refno INTO l_hop_refno;
   CLOSE c_hop_refno;
--
   IF l_hop_refno is null
    THEN
--
-- create one
--
    OPEN c_hop_refno_seq;
     FETCH c_hop_refno_seq INTO l_hop_refno;
    CLOSE c_hop_refno_seq;
--
     INSERT INTO household_persons
        (hop_refno
        ,hop_hou_refno
        ,hop_par_refno
        ,hop_start_date
        ,hop_end_date
        ,hop_hrv_rel_code
        ,hop_hrv_hpsr_code
        ,hop_hrv_hper_code
        )
     VALUES
        (l_hop_refno
        ,l_hou_refno
        ,l_par_refno
        ,trunc(sysdate) -- as per front end application
        ,null
        ,null
        ,null
        ,null
        );
   END IF;
--
  END IF;
--
      INSERT INTO non_tenant_appln_parties
         (ntap_lived_one_year_ind        
         ,ntap_principle_home_ind        
         ,ntap_wish_to_buy_ind           
         ,ntap_party_verified_ind        
         ,ntap_signature_verified_ind
         ,ntap_signature_date            
         ,ntap_comments                  
         ,ntap_created_date              
         ,ntap_created_by                
         ,ntap_papp_refno                
         ,ntap_hop_refno
         ,ntap_admitted_ind
         ,ntap_denied_ind
         ,ntap_hrv_denied_reason
         )
      VALUES
         (p1.lpap_lived_one_year
         ,p1.lpap_principal_home_ind 
         ,p1.lpap_wish_to_buy_ind
         ,p1.lpap_party_verified_ind 
         ,p1.lpap_signature_verified_ind
         ,p1.lpap_signature_date
         ,p1.lpap_comments
         ,sysdate
         ,'DATALOAD'
         ,l_papp_refno
         ,l_hop_refno
         ,p1.lpap_admitted_ind
         ,p1.lpap_denied_ind
         ,p1.lpap_hrv_denied_reason
         );
 END IF;
--
-- keep a count of the rows processed and commit after every 5000
--
i := i+1; IF MOD(i,5000)=0 THEN COMMIT; END If;
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
 END;
--
END LOOP;
COMMIT;
--
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANT_APPLICATION_PARTIES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('NON_TENANT_APPLN_PARTIES');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
     RAISE;
--
END dataload_create;
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lpap_dlb_batch_id        
,lpap_dl_seqno            
,lpap_dl_load_status      
,lpap_papp_displayed_reference 
,lpap_par_per_alt_ref     
,lpap_party_type      
,lpap_principal_home_ind      
,lpap_wish_to_buy_ind         
,lpap_lived_one_year        
,lpap_party_verified_ind      
,lpap_signature_verified_ind    
,lpap_signature_date            
,lpap_comments
,lpap_admitted_ind
,lpap_denied_ind
,lpap_hrv_denied_reason
FROM  DL_HPP_PP_APPLN_PARTIES
WHERE lpap_dlb_batch_id      = p_batch_id
AND   lpap_dl_load_status   in ('L','F','O');
--
-- Cursor to see if party exists
--
CURSOR c_par(p_par_refno varchar2) IS
SELECT 'X'
FROM   parties
WHERE  par_per_alt_ref = p_par_refno;
--
CURSOR c_par2(p_par_refno varchar2) IS
SELECT 'X'
FROM   parties
WHERE  par_refno       = to_number(p_par_refno);
--
-- Cursor to see if application exists
--
CURSOR c_papp_refno(p_papp_refno varchar2) IS
SELECT papp_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_refno;
--
-- Cursors for Duplicate Person Applications
-- Tenant
--
CURSOR c_ten_par(p_papp_ref varchar2, p_par_refno number) IS
SELECT 'X'
FROM   tenant_application_parties,
       pp_applications,
       household_persons
WHERE  hop_par_refno            = p_par_refno
AND    papp_refno               = tap_papp_refno
AND    hop_refno                = tap_tin_hop_refno
AND    papp_displayed_reference = p_papp_ref;
--
-- Then Non tenant
--
CURSOR c_nten_par(p_papp_ref varchar2, p_par_refno number) IS
SELECT 'X'
FROM   non_tenant_appln_parties,
       pp_applications,
       household_persons
WHERE  hop_par_refno      = p_par_refno
AND    papp_refno         = ntap_papp_refno
AND    hop_refno          = ntap_hop_refno
AND    papp_displayed_reference = p_papp_ref;
--
-- Cursor to Check they are linked to Tenancy
--
CURSOR c_tin_exists(p_papp_ref varchar2, p_par_refno number) IS
SELECT 'X'
FROM   pp_applications,
       household_persons,
       tenancy_instances
WHERE  hop_par_refno            = p_par_refno
AND    papp_displayed_reference = p_papp_ref
AND    hop_refno                = tin_hop_refno
AND    tin_tcy_refno            = papp_tho_tcy_refno
AND    papp_application_date between hop_start_date
                             and nvl(hop_end_date, sysdate);
--
-- Cursor to get par_refno
--
CURSOR c_par_refno(p_par_refno varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_refno;

--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HPP_PP_APPLN_PARTIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_exists         VARCHAR2(1);
l_pro_refno      NUMBER(10);
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_papp_refno     NUMBER(10);
l_answer         VARCHAR2(1);
l_par_refno      NUMBER(10);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_appln_parties.dataload_validate');
fsc_utils.debug_message( 's_dl_hpp_pp_appln_parties.dataload_validate',3);
--
cb := p_batch_id;
cd := p_date;
--
s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Using Par Refno in place of Alt Ref?'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lpap_dl_seqno;
l_id := p1.rec_rowid;
--
l_errors := 'V';
l_error_ind := 'N';
--
--
-- Check the Party type is valid
--
   IF nvl(p1.lpap_party_type, 'X') NOT IN ('TEN','FAM','TENANT','NON-TENANT')
    THEN l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',824);
   END IF;
--
-- Check Y/N Columns
--
-- Principal Home
--
    IF (NOT s_dl_hem_utils.yorn(p1.lpap_principal_home_ind))
     THEN 
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',825);
    END IF;
--
-- Wish to buy
--
    IF (NOT s_dl_hem_utils.yorn(p1.lpap_wish_to_buy_ind))
     THEN 
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',826);
    END IF;
--
-- Lived One Year
--
    IF p1.lpap_party_type in ('FAM', 'NON-TENANT')
     THEN
      IF (NOT s_dl_hem_utils.yornornull(p1.lpap_lived_one_year))
       THEN 
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',827);
      END IF;
    END IF;
--
-- Party Verified
--
    IF (NOT s_dl_hem_utils.yorn(p1.lpap_party_verified_ind))
     THEN 
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',828);
    END IF;
--
-- Signature Verified
--
    IF (NOT s_dl_hem_utils.yorn(p1.lpap_signature_verified_ind))
     THEN 
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',829);
    END IF;
--
-- Check application party is present on Parties table
--
--
  IF l_answer = 'Y'
   THEN
    OPEN c_par2(p1.lpap_par_per_alt_ref);
     FETCH c_par2 into l_exists;
      IF c_par2%NOTFOUND
       THEN 
         l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',823);
      END IF;
     CLOSE c_par2;
  ELSE
   OPEN c_par(p1.lpap_par_per_alt_ref);
    FETCH c_par into l_exists;
     IF c_par%NOTFOUND
      THEN 
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',823);
     END IF;
    CLOSE c_par;
  END IF;
--
--
-- Check application exists
--
 OPEN c_papp_refno(p1.lpap_papp_displayed_reference);
  FETCH c_papp_refno into l_papp_refno;
   IF c_papp_refno%NOTFOUND
    THEN 
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',804);
   END IF;
 CLOSE c_papp_refno;
--
-- Check that a matching Party record does not already exist
-- Get the par_refno first
--
  IF l_answer = 'Y'
   THEN l_par_refno := to_number(p1.lpap_par_per_alt_ref);
  ELSE
--
   OPEN  c_par_refno(p1.lpap_par_per_alt_ref);
    FETCH c_par_refno INTO l_par_refno;
   CLOSE c_par_refno;
--
  END IF;

  IF p1.lpap_party_type in ('TEN', 'TENANT')
     THEN
      OPEN c_ten_par(p1.lpap_papp_displayed_reference, l_par_refno);
       FETCH c_ten_par INTO l_exists;
        IF c_ten_par%FOUND
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',831);
        END IF;
      CLOSE c_ten_par;
     ELSE
      OPEN c_nten_par(p1.lpap_papp_displayed_reference, l_par_refno);
       FETCH c_nten_par INTO l_exists;
        IF c_nten_par%FOUND
         THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',510);
        END IF;    
      CLOSE c_nten_par;
  END IF;
--
-- Check that if the Denied Reason is supplied
-- it's valid and also that the denied Ind is Y
--
   IF p1.lpap_hrv_denied_reason is not null
    THEN
     IF (NOT s_dl_hem_utils.exists_frv('DENYREAS',p1.lpap_hrv_denied_reason,'N'))
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',196);
     END IF;
--
     IF (p1.lpap_denied_ind != 'Y')
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',197);
     END IF;
   END IF;
--
-- Admitted Indicator, Mandatory for Non Tenants
--
   IF p1.lpap_party_type not in ('TEN','TENANT')
    THEN
     IF (nvl(p1.lpap_admitted_ind, 'X') not in ('Y', 'N'))
      THEN
       l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',181);
     END IF;
   END IF;
--
-- New Check added 21-JAN-05. Check that if a TEN they 
-- exist on Tenancy Instances/Household Persons for the
-- details supplied. As there are Mandatory fields derived from
-- Tenancy Instances on the create process.
--
  IF p1.lpap_party_type in ('TEN', 'TENANT')
     THEN
      OPEN c_tin_exists(p1.lpap_papp_displayed_reference, l_par_refno);
       FETCH c_tin_exists into l_exists;
        IF c_tin_exists%NOTFOUND
         THEN
          l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',182);
        END IF;
      CLOSE c_tin_exists;
   END IF;
--
--
-- Now UPDATE the record count AND error code
IF l_errors = 'F' THEN
  l_error_ind := 'Y';
ELSE
  l_error_ind := 'N';
END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
set_record_status_flag(l_id,l_errors);
--
   EXCEPTION
      WHEN OTHERS THEN
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      set_record_status_flag(l_id,'O');
END;
--
END LOOP;
COMMIT;
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
--
END dataload_validate;
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT
rowid rec_rowid
,lpap_dlb_batch_id        
,lpap_dl_seqno            
,lpap_dl_load_status      
,lpap_papp_displayed_reference 
,lpap_par_per_alt_ref     
,lpap_party_type      
,lpap_principal_home_ind      
,lpap_wish_to_buy_ind         
,lpap_lived_one_year        
,lpap_party_verified_ind      
,lpap_signature_verified_ind    
,lpap_signature_date            
,lpap_comments
FROM DL_HPP_PP_APPLN_PARTIES
WHERE lpap_dlb_batch_id     = p_batch_id
  AND lpap_dl_load_status   = 'C';
--
-- Cursor to get papp_refno
--
CURSOR c_papp_refno(p_papp_ref varchar2) IS
SELECT papp_refno, papp_application_date, papp_tho_tcy_refno
FROM   pp_applications
WHERE  papp_displayed_reference = p_papp_ref;
--
-- Cursor to get par_refno
--
CURSOR c_par_refno(p_par_refno varchar2) IS
SELECT par_refno
FROM   parties
WHERE  par_per_alt_ref = p_par_refno;
--
-- Cursor to get tenancy details
--
CURSOR c_tin_start(p_tcy_refno number
                  ,p_par_refno number
                  ,p_app_date  date) IS
SELECT tin_start_date, hop_refno
FROM   tenancy_instances,
       household_persons
WHERE  tin_hop_refno = hop_refno
AND    tin_tcy_refno = p_tcy_refno
AND    hop_par_refno = p_par_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
-- Cursor to get the hou_refno for a tenant so we
-- will then be able to get the right hop_refno for
-- a non_tenant;
--
CURSOR c_hou_refno(p_tcy_refno number
                  ,p_app_date  date) IS
SELECT hop_hou_refno
FROM   household_persons,
       tenancy_instances
WHERE  tin_hop_refno = hop_refno
AND    tin_tcy_refno = p_tcy_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
-- Cursor to get the hop_refno for a non tenant
--
CURSOR c_hop_refno(p_hou_refno number
            ,p_par_refno number
            ,p_app_date  date) IS
SELECT hop_refno
FROM   household_persons
WHERE  hop_hou_refno  = p_hou_refno
AND    hop_par_refno  = p_par_refno
AND    p_app_date between hop_start_date
                  and nvl(hop_end_date, sysdate);
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HPP_PP_APPLN_PARTIES';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
i integer := 0;
l_papp_refno number;
l_app_date   date;
l_tcy_refno  number;
l_par_refno  number;
l_hou_refno  number;
l_hop_refno  number;
l_tin_start  date;
l_answer     VARCHAR2(1);
--
BEGIN
--
fsc_utils.proc_start('s_dl_hpp_pp_appln_parties.dataload_delete');
fsc_utils.debug_message( 's_dl_hpp_pp_appln_parties.dataload_delete',3 );
--
cb := p_batch_id;
cd := p_date;
--
-- s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
-- Get the answer to the 'Using Par Refno in place of Alt Ref?'
--
l_answer := s_dl_batches.get_answer(p_batch_id, 1);
--
--
FOR p1 IN c1 LOOP
--
BEGIN
--
cs := p1.lpap_dl_seqno;
l_id := p1.rec_rowid;
--
SAVEPOINT SP1;
--
-- Get the papp_refno
--
l_papp_refno := null;
l_tcy_refno  := null;
l_app_date   := null;
l_par_refno  := null;
l_hop_refno  := null;
l_tin_start  := null;
--
  OPEN c_papp_refno(p1.lpap_papp_displayed_reference);
   FETCH c_papp_refno INTO l_papp_refno, l_app_date, l_tcy_refno;
  CLOSE c_papp_refno;
--
-- Use the answer to get the person record
--
  IF l_answer = 'Y'
   THEN l_par_refno := to_number(p1.lpap_par_per_alt_ref);
  ELSE
--
 OPEN  c_par_refno(p1.lpap_par_per_alt_ref);
 FETCH c_par_refno INTO l_par_refno;
 CLOSE c_par_refno;
--
  END IF;
--
--
-- Delete from tenant_application_parties
--
   IF p1.lpap_party_type in ('TEN', 'TENANT')
      THEN
       OPEN c_tin_start(l_tcy_refno, l_par_refno, l_app_date);
        FETCH c_tin_start INTO l_tin_start, l_hop_refno;
       CLOSE c_tin_start;
--
          DELETE FROM tenant_application_parties
          WHERE  tap_papp_refno    = l_papp_refno
          AND    tap_tin_tcy_refno = l_tcy_refno
          AND    tap_tin_hop_refno = l_hop_refno;
--
   ELSE
--
l_hop_refno := null;
l_hou_refno := null;
--
  OPEN c_hou_refno(l_tcy_refno, l_app_date);
   FETCH c_hou_refno INTO l_hou_refno;
  CLOSE c_hou_refno;
--
  OPEN c_hop_refno(l_hou_refno, l_par_refno, l_app_date);
   FETCH c_hop_refno INTO l_hop_refno;
  CLOSE c_hop_refno;
--
          DELETE FROM non_tenant_appln_parties
          WHERE  ntap_papp_refno = l_papp_refno
          AND    ntap_hop_refno   = l_hop_refno;
   END IF;
--
-- keep a count of the rows processed and commit after every 1000
--
i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
--
s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
set_record_status_flag(l_id,'V');
--
EXCEPTION
   WHEN OTHERS THEN
   ROLLBACK TO SP1;
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
   set_record_status_flag(l_id,'C');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END;
--
END LOOP;
--
COMMIT;
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('TENANT_APPLICATION_PARTIES');
l_an_tab:=s_dl_hem_utils.dl_comp_stats('NON_TENANT_APPLN_PARTIES');
--
fsc_utils.proc_end;
--
   EXCEPTION
      WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
      RAISE;
--
END dataload_delete;
--
--
END s_dl_hpp_pp_appln_parties;
/

