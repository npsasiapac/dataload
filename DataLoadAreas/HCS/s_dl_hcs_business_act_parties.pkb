CREATE OR REPLACE PACKAGE BODY s_dl_hcs_business_act_parties
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  VERSION  DB Ver   WHO            WHEN         WHY
--      1.0  6.18     Rob Heath      05-FEB-2019  Initial Creation for SAHT
--                                                New Data Load
--                                    
-- 
-- ***********************************************************************
--
--  declare package variables AND constants
--
--
-- ***********************************************************************
--
PROCEDURE set_record_status_flag(
  p_rowid  IN ROWID,
  p_status IN VARCHAR2)
AS
--
BEGIN
   UPDATE dl_hcs_business_act_parties
   SET    lbpa_dl_load_status = p_status
   WHERE  rowid = p_rowid;
--
EXCEPTION
   WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hcs_business_act_parties');
      RAISE;
--
END set_record_status_flag;
--
--
PROCEDURE dataload_create
   (p_batch_id          IN VARCHAR2
   ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       lbpa_dlb_batch_id,
       lbpa_dl_seqno,
       lbpa_dl_load_status,
       lbpa_ban_reference,       
       lbpa_start_date,
       lbpa_main_party_ind,
       lbpa_hrv_bac_code,
       lbpa_created_by,
       lbpa_created_date,
       lbpa_object_type,
       lbpa_object_ref,
       lbpa_end_date,
       lbpa_comments       
FROM  dl_hcs_business_act_parties
WHERE lbpa_dlb_batch_id   = p_batch_id
AND   lbpa_dl_load_status = 'V';
--
-- ***********************************************************************
-- Get Party
--
CURSOR get_par_refno(p_par_per_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref
   --AND par_type = 'HOUP'
   AND SYSDATE <= NVL(par_per_hou_end_date,SYSDATE);
--
-- ***********************************************************************
-- Get Tenancy
--
CURSOR get_tcy_refno(p_tcy_alt_ref VARCHAR2)
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_alt_ref;
--
-- ***********************************************************************
-- Get Application
--
CURSOR get_app_refno(p_app_legacy_ref VARCHAR2)
IS
SELECT app_refno
  FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;
--
-- *****************************
--
-- Constants for process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HCS_BUSINESS_ACT_PARTIES';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- Other variables
--
l_id            ROWID;
l_an_tab        VARCHAR2(1);
i               INTEGER := 0;
l_bpa_refno     INTEGER;
l_bpa_par_refno INTEGER;
l_bpa_tcy_refno INTEGER;
l_bpa_ipp_refno INTEGER;
l_bpa_app_refno INTEGER;
l_bpa_peg_code  VARCHAR2(10);
l_bpa_cos_code  VARCHAR2(15);
--
-- *****************************
--
BEGIN
--
   fsc_utils.proc_start('s_'||ct||'.dataload_create');
   fsc_utils.debug_message('s_'||ct||'.dataload_create',3);
--
   cb := p_batch_id;
   cd := p_date;
--
   s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
   FOR p1 in c1 LOOP
      BEGIN
--
         cs   := p1.lbpa_dl_seqno;
         l_id := p1.rec_rowid;
--
         SAVEPOINT SP1;
--       
         l_bpa_refno := bpa_refno_seq.nextval;
         l_bpa_par_refno := NULL;
         l_bpa_tcy_refno := NULL;
         l_bpa_ipp_refno := NULL;
         l_bpa_app_refno := NULL;
         l_bpa_peg_code  := NULL;
         l_bpa_cos_code  := NULL;
         
         IF p1.lbpa_object_type = 'PAR'
         THEN
            l_bpa_par_refno := p1.lbpa_object_ref;
         ELSIF p1.lbpa_object_type = 'ALT_PAR'
         THEN
            OPEN get_par_refno(p1.lbpa_object_ref);
            FETCH get_par_refno INTO l_bpa_par_refno;
            CLOSE get_par_refno;
         ELSIF p1.lbpa_object_type = 'TCY'
         THEN
            l_bpa_tcy_refno := p1.lbpa_object_ref;
         ELSIF p1.lbpa_object_type = 'ALT_TCY'
         THEN
            OPEN get_tcy_refno(p1.lbpa_object_ref);
            FETCH get_tcy_refno INTO l_bpa_tcy_refno;
            CLOSE get_tcy_refno;
         ELSIF p1.lbpa_object_type = 'IPP'
         THEN
            l_bpa_ipp_refno := p1.lbpa_object_ref;
         ELSIF p1.lbpa_object_type = 'APP'
         THEN
            l_bpa_app_refno := p1.lbpa_object_ref;
         ELSIF p1.lbpa_object_type = 'LEGACY_APP'
         THEN
            OPEN get_app_refno(p1.lbpa_object_ref);
            FETCH get_app_refno INTO l_bpa_app_refno;
            CLOSE get_app_refno;   
         ELSIF p1.lbpa_object_type = 'PEG'
         THEN
            l_bpa_peg_code := p1.lbpa_object_ref;
         ELSIF p1.lbpa_object_type = 'COS'
         THEN
            l_bpa_cos_code := p1.lbpa_object_ref;
         END IF;   
--
         INSERT INTO business_action_parties
            (bpa_refno,
             bpa_ban_reference,
             bpa_start_date,
             bpa_main_party_ind,
             bpa_hrv_bac_code,
             bpa_par_refno,
             bpa_tcy_refno,
             bpa_ipp_refno,
             bpa_app_refno,
             bpa_peg_code,
             bpa_cos_code,
             bpa_end_date,
             bpa_comments)
         VALUES
            (l_bpa_refno,
             p1.lbpa_ban_reference,
             p1.lbpa_start_date,
             NVL(p1.lbpa_main_party_ind,'N'),
             p1.lbpa_hrv_bac_code,             
             l_bpa_par_refno,
             l_bpa_tcy_refno,
             l_bpa_ipp_refno,
             l_bpa_app_refno,
             l_bpa_peg_code,
             l_bpa_cos_code,
             p1.lbpa_end_date,
             p1.lbpa_comments);
--
-- Now update record with created date as trigger sets it to sysdate
-- only needed if creation date supplied otherwise sysdate is fine   
--
         IF p1.lbpa_created_date IS NOT NULL AND p1.lbpa_created_by IS NOT NULL
         THEN
            UPDATE business_action_parties
            SET    bpa_created_date = p1.lbpa_created_date,
                   bpa_created_by = p1.lbpa_created_by
            WHERE  bpa_refno = l_bpa_refno;
         ELSE
            UPDATE business_action_parties
            SET    bpa_created_by = 'DATALOAD'
            WHERE  bpa_refno = l_bpa_refno;               
         END IF;
--       
         UPDATE dl_hcs_business_act_parties
         SET    lbpa_refno = l_bpa_refno
         WHERE  rowid = p1.rec_rowid;               
--
--       Now UPDATE the record status and process count
--
         i := i + 1;
--
         IF MOD(i,500000)=0 THEN
            COMMIT;
         END IF;
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
--
   COMMIT;
--
-- ***********************************************************************
-- Section to analyze the table(s) populated by this dataload
--
   l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUSINESS_ACTION_PARTIES');
--
   fsc_utils.proc_END;
--
EXCEPTION
   WHEN OTHERS THEN
     s_dl_process_summary.UPDATE_summary(cb,cp,cd,'FAILED');
     RAISE;
END dataload_create;
--
-- ***********************************************************************
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2
     ,p_date              IN DATE)
AS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       lbpa_dlb_batch_id,
       lbpa_dl_seqno,
       lbpa_dl_load_status,
       lbpa_ban_reference,       
       lbpa_start_date,
       lbpa_main_party_ind,
       lbpa_hrv_bac_code,
       lbpa_created_by,
       lbpa_created_date,
       lbpa_object_type,
       lbpa_object_ref,
       lbpa_end_date,
       lbpa_comments
FROM   dl_hcs_business_act_parties
WHERE  lbpa_dlb_batch_id   = p_batch_id
AND    lbpa_dl_load_status IN ('L','F','O');
--
-- ***********************************************************************
-- Check Business Action Reference is valid
--
CURSOR chk_ban_reference(p_ban_reference INTEGER)
IS
SELECT 'X'
  FROM business_actions
 WHERE ban_reference = p_ban_reference;
--
-- ***********************************************************************
-- Check Parties with main
--
CURSOR c_parties_with_main( p_dlb_batch_id  VARCHAR2
                           ,p_dl_seqno      INTEGER
                           ,p_ban_reference INTEGER)
IS
 SELECT  COUNT(*)
   FROM  (SELECT  'X'
            FROM  business_action_parties
           WHERE  bpa_ban_reference = p_ban_reference
             AND  bpa_main_party_ind = 'Y' 
          UNION
          SELECT 'X'
            FROM  dl_hcs_business_act_parties
           WHERE  lbpa_dlb_batch_id = p_dlb_batch_id
             AND  lbpa_ban_reference = p_ban_reference
             AND  lbpa_main_party_ind = 'Y'
             AND  lbpa_dl_seqno < p_dl_seqno
             AND  lbpa_dl_load_status != 'F');        
--
-- ***********************************************************************
-- Check Business Action Role Code is valid
--
CURSOR chk_bap_role(p_hrv_bac_code VARCHAR2)
IS
SELECT 'X'
  FROM first_ref_values
 WHERE frv_frd_domain = 'BAPROLES'
   AND frv_code = p_hrv_bac_code
   AND frv_current_ind = 'Y';
--
-- ***********************************************************************
-- Check Party is valid
--
CURSOR chk_par_refno(p_par_refno INTEGER)
IS
SELECT 'X'
  FROM parties
 WHERE par_refno = p_par_refno
   --AND par_type = 'HOUP'
   AND SYSDATE <= NVL(par_per_hou_end_date,SYSDATE);
--
-- ***********************************************************************
-- Check Alt Party is valid
--
CURSOR get_par_refno(p_par_per_alt_ref VARCHAR2)
IS
SELECT par_refno
  FROM parties
 WHERE par_per_alt_ref = p_par_per_alt_ref
   --AND par_type = 'HOUP'
   AND SYSDATE <= NVL(par_per_hou_end_date,SYSDATE);
--
-- ***********************************************************************
-- Check Tenancy is valid
--
CURSOR chk_tcy_refno(p_tcy_refno VARCHAR2)
IS
SELECT 'X'
  FROM tenancies
 WHERE tcy_refno = p_tcy_refno;
--
-- ***********************************************************************
-- Check Alt Tenancy is valid
--
CURSOR get_tcy_refno(p_tcy_alt_ref VARCHAR2)
IS
SELECT tcy_refno
  FROM tenancies
 WHERE tcy_alt_ref = p_tcy_alt_ref;
--
-- ***********************************************************************
-- Check Interested Party is valid
--
CURSOR chk_ipp_refno(p_ipp_refno VARCHAR2)
IS
SELECT 'X'
  FROM  interested_parties
 WHERE  ipp_refno = p_ipp_refno;
--
-- ***********************************************************************
-- Check Application is valid
--
CURSOR chk_app_refno(p_app_refno VARCHAR2)
IS
SELECT 'X'
  FROM applications
 WHERE app_refno = p_app_refno;
--
-- ***********************************************************************
-- Check Legacy Application is valid
--
CURSOR get_app_refno(p_app_legacy_ref VARCHAR2)
IS
SELECT app_refno
  FROM applications
 WHERE app_legacy_ref = p_app_legacy_ref;
--
-- ***********************************************************************
-- Check People Group Code is valid
--
CURSOR chk_peg_code(p_peg_code VARCHAR2)
IS
SELECT 'X'
  FROM people_groups
 WHERE peg_code = p_peg_code;
--
-- ***********************************************************************
-- Check Contractor Site code is valid
--
CURSOR chk_cos_code(p_cos_code VARCHAR2)
IS
SELECT 'X'
  FROM contractor_sites
 WHERE cos_code = p_cos_code;
--
-- ***********************************************************************
-- Check for overlapping records
--
CURSOR chk_overlap(p_ban_reference INTEGER
                  ,p_par_refno     INTEGER
                  ,p_tcy_refno     INTEGER
                  ,p_ipp_refno     INTEGER
                  ,p_app_refno     INTEGER
                  ,p_peg_code      VARCHAR2
                  ,p_cos_code      VARCHAR2
                  ,p_start_date    DATE
                  ,p_end_date      DATE )
IS
      
SELECT 'X'
FROM   business_action_parties
WHERE  bpa_ban_reference = p_ban_reference
AND    NVL(bpa_par_refno,-1) = NVL(p_par_refno,-1)
AND    NVL(bpa_tcy_refno,-1) = NVL(p_tcy_refno,-1)
AND    NVL(bpa_ipp_refno,-1) = NVL(p_ipp_refno,-1)
AND    NVL(bpa_app_refno,-1) = NVL(p_app_refno,-1)
AND    NVL(bpa_peg_code,'X') = NVL(p_peg_code,'X')
AND    NVL(bpa_cos_code,'X') = NVL(p_cos_code,'X')
AND    p_start_date <= NVL(bpa_end_date,TO_DATE('31129999','DDMMYYYY'))
AND    NVL(p_end_date,TO_DATE('31129999','DDMMYYYY')) >= bpa_start_date;
--
-- ***********************************************************************
-- Check for duplicates/overlapping dates in data load batch
CURSOR chk_dup_dl (p_dlb_batch_id  VARCHAR2
                  ,p_dl_seqno      INTEGER
                  ,p_ban_reference INTEGER
                  ,p_start_date    DATE
                  ,p_object_type   VARCHAR2
                  ,p_object_ref    VARCHAR2
                  ,p_end_date      DATE )
                 
IS
SELECT count(*)
FROM   dl_hcs_business_act_parties
WHERE  lbpa_dlb_batch_id = p_dlb_batch_id
AND    lbpa_ban_reference = p_ban_reference
AND    lbpa_object_type = p_object_type
AND    lbpa_object_ref = p_object_ref
AND    NVL(p_end_date,TO_DATE('31129999','DDMMYYYY')) >= lbpa_start_date
AND    p_start_date <= NVL(lbpa_end_date,TO_DATE('31129999','DDMMYYYY'))
AND    lbpa_dl_seqno < p_dl_seqno
AND    lbpa_dl_load_status != 'F';
--  
-- ***********************************************************************
--
-- constants FOR error process
cb VARCHAR2(30);
cd DATE;
cp VARCHAR2(30) := 'VALIDATE';
ct VARCHAR2(30) := 'DL_HCS_BUSINESS_ACT_PARTIES';
cs INTEGER;
ce VARCHAR2(200);
l_id     ROWID;
--
-- other variables
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER := 0;
l_chk        	  VARCHAR2(1);
l_chk_dup_dl     INTEGER;
l_main_cnt       INTEGER;
l_par_refno      INTEGER;
l_tcy_refno      INTEGER;
l_ipp_refno      INTEGER;
l_app_refno      INTEGER;
l_peg_code       VARCHAR2(10);
l_cos_code       VARCHAR2(15);
--
-- ***********************************************************************
--
BEGIN
--
   fsc_utils.proc_start('s_'||ct||'.dataload_validate');
   fsc_utils.debug_message('s_'||ct||'.dataload_validate',3 );
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
         cs := p1.lbpa_dl_seqno;
         l_id := p1.rec_rowid;
--
         l_errors := 'V';
         l_error_ind := 'N';
--
--       Mandatory field check and content
--      
--       Business Action Reference
         IF p1.lbpa_ban_reference IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',743);
            --'Business Action Reference must be supplied'
         ELSE
            l_chk := NULL;

            OPEN chk_ban_reference(p1.lbpa_ban_reference);
            FETCH chk_ban_reference INTO l_chk;
            CLOSE chk_ban_reference;

            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',744);
               --'Business Action Reference supplied is invalid'               
            END IF;
         END IF;
--         
--       Start Date
         IF p1.lbpa_start_date IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',745);
            --'Start Date must be supplied'
         END IF;   
--         
--       Object Type
         IF p1.lbpa_object_type IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',746);
            --'Object Type must be supplied'
         END IF;
--         
--       Object Reference
         IF p1.lbpa_object_ref IS NULL
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',747);
            --'Object Reference must be supplied'
         END IF;
--               
--       Main Party Ind
         IF NVL(p1.lbpa_main_party_ind,'N') NOT IN ('Y','N')
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',748);
            --'Main Party Indicator supplied is invalid'
         ELSIF p1.lbpa_main_party_ind = 'Y' AND p1.lbpa_ban_reference IS NOT NULL 
         THEN   
            l_main_cnt := 0;
               
            OPEN c_parties_with_main(p1.lbpa_dlb_batch_id
                                    ,p1.lbpa_dl_seqno
                                    ,p1.lbpa_ban_reference);
            FETCH c_parties_with_main INTO l_main_cnt;
            CLOSE c_parties_with_main;

            IF l_main_cnt > 0 THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',749);
               --'Only one party for the business action may have the Main Party Indicator set to Y'
            END IF;                    
         END IF; 

--       Role Code
         IF p1.lbpa_hrv_bac_code IS NOT NULL
         THEN
            l_chk := NULL;

            OPEN chk_bap_role(p1.lbpa_hrv_bac_code);
            FETCH chk_bap_role INTO l_chk;
            CLOSE chk_bap_role;

            IF l_chk IS NULL THEN
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',750);
               --'Business Action Role Code supplied is invalid'               
            END IF;
         END IF;
--                  
         l_par_refno := NULL;
         l_tcy_refno := NULL;
         l_ipp_refno := NULL;
         l_app_refno := NULL;         
         l_peg_code  := NULL;
         l_cos_code  := NULL;
         l_chk := NULL;
         
         IF p1.lbpa_object_type IS NOT NULL AND p1.lbpa_object_ref IS NOT NULL
         THEN
            IF p1.lbpa_object_type = 'PAR'
            THEN

               OPEN chk_par_refno(p1.lbpa_object_ref);
               FETCH chk_par_refno INTO l_chk;
               CLOSE chk_par_refno;

               IF l_chk IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',751);
                  --'Party Reference Number supplied is invalid'               
               ELSE
                  l_par_refno := p1.lbpa_object_ref;
               END IF;
                           
            ELSIF p1.lbpa_object_type = 'ALT_PAR'
            THEN

               OPEN get_par_refno(p1.lbpa_object_ref);
               FETCH get_par_refno INTO l_par_refno;
               CLOSE get_par_refno;

               IF l_par_refno IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',751);
                  --'Party Reference Number supplied is invalid'               
               END IF;  
                           
            ELSIF p1.lbpa_object_type = 'TCY'
            THEN

               OPEN chk_tcy_refno(p1.lbpa_object_ref);
               FETCH chk_tcy_refno INTO l_chk;
               CLOSE chk_tcy_refno;

               IF l_chk IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',752);
                  --'Tenancy Reference Number supplied is invalid'               
               ELSE
                  l_tcy_refno := p1.lbpa_object_ref;
               END IF;  
                           
            ELSIF p1.lbpa_object_type = 'ALT_TCY'
            THEN

               OPEN get_tcy_refno(p1.lbpa_object_ref);
               FETCH get_tcy_refno INTO l_tcy_refno;
               CLOSE get_tcy_refno;

               IF l_tcy_refno IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',752);
                  --'Tenancy Reference Number supplied is invalid'               
               END IF;  

            ELSIF p1.lbpa_object_type = 'IPP'
            THEN

               OPEN chk_ipp_refno(p1.lbpa_object_ref);
               FETCH chk_ipp_refno INTO l_chk;
               CLOSE chk_ipp_refno;

               IF l_chk IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',753);
                  --'Interested Party Reference Number supplied is invalid'               
               ELSE
                  l_ipp_refno := p1.lbpa_object_ref;
               END IF;

            ELSIF p1.lbpa_object_type = 'APP'
            THEN

               OPEN chk_app_refno(p1.lbpa_object_ref);
               FETCH chk_app_refno INTO l_chk;
               CLOSE chk_app_refno;

               IF l_chk IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',754);
                  --'Application Reference Number supplied is invalid'               
               ELSE
                  l_app_refno := p1.lbpa_object_ref;
               END IF;

            ELSIF p1.lbpa_object_type = 'LEGACY_APP'
            THEN

               OPEN get_app_refno(p1.lbpa_object_ref);
               FETCH get_app_refno INTO l_app_refno;
               CLOSE get_app_refno;

               IF l_app_refno IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',754);
                  --'Application Reference Number supplied is invalid'               
               END IF;

            ELSIF p1.lbpa_object_type = 'PEG'
            THEN

               OPEN chk_peg_code(p1.lbpa_object_ref);
               FETCH chk_peg_code INTO l_chk;
               CLOSE chk_peg_code;

               IF l_chk IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',755);
                  --'People Group Code supplied is invalid'               
               ELSE
                  l_peg_code := p1.lbpa_object_ref;
               END IF;

            ELSIF p1.lbpa_object_type = 'COS'
            THEN

               OPEN chk_cos_code(p1.lbpa_object_ref);
               FETCH chk_cos_code INTO l_chk;
               CLOSE chk_cos_code;

               IF l_chk IS NULL THEN
                  l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',756);
                  --'Contractor Site code supplied is invalid'               
               ELSE
                  l_cos_code := p1.lbpa_object_ref;
               END IF;
                                                                                                                                        
            ELSE
               l_errors := s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',757);
               --'Object Type supplied is invalid'        
            END IF;
         END IF;
--                    
--       Check the End Date if supplied
         IF p1.lbpa_end_date IS NOT NULL
         THEN
            IF p1.lbpa_end_date < p1.lbpa_start_date
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',758);
               --'The End Date cannot be less than Start Date'
            END IF;
         END IF;  
              
         --chk_overlapping dates   
         IF p1.lbpa_start_date IS NOT NULL AND
            p1.lbpa_ban_reference IS NOT NULL AND
            (l_par_refno IS NOT NULL OR
             l_tcy_refno IS NOT NULL OR
             l_ipp_refno IS NOT NULL OR
             l_app_refno IS NOT NULL OR
             l_peg_code  IS NOT NULL OR
             l_cos_code  IS NOT NULL)             
         THEN   
            l_chk := NULL;
                                 
            OPEN chk_overlap (p1.lbpa_ban_reference
                             ,l_par_refno
                             ,l_tcy_refno
                             ,l_ipp_refno
                             ,l_app_refno
                             ,l_peg_code
                             ,l_cos_code
                             ,p1.lbpa_start_date                  
                             ,p1.lbpa_end_date);
            FETCH chk_overlap INTO l_chk;
            CLOSE chk_overlap ;

            IF l_chk IS NOT NULL
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',759);
               -- 'Dates overlap with an existing record'
            END IF;            
 
         END IF;
                  
         --Check data load batch for duplicates         
         IF p1.lbpa_ban_reference IS NOT NULL AND 
            p1.lbpa_start_date IS NOT NULL AND  
            p1.lbpa_object_type IS NOT NULL AND
            p1.lbpa_object_ref IS NOT NULL
         THEN   
            l_chk_dup_dl := 0;
                                 
            OPEN  chk_dup_dl(p1.lbpa_dlb_batch_id
                            ,p1.lbpa_dl_seqno
                            ,p1.lbpa_ban_reference
                            ,p1.lbpa_start_date
                            ,p1.lbpa_object_type
                            ,p1.lbpa_object_ref
                            ,p1.lbpa_end_date);

            FETCH chk_dup_dl INTO l_chk_dup_dl;
            CLOSE chk_dup_dl;

            IF l_chk_dup_dl > 0
            THEN
               l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',760);
               --'Record is a duplicate in the data load batch'
            END IF; 
         END IF;  
                           
         --Check the Created date if supplied
         IF (NVl(p1.lbpa_created_date,TRUNC(SYSDATE)) > TRUNC(SYSDATE))
         THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD3',761);
            --'The Created date cannot be later than today (truncated sysdate)'
         END IF;

--       *****************************************
--       Now UPDATE the record count and error code
         IF l_errors = 'F' THEN
            l_error_ind := 'Y';
         ELSE
            l_error_ind := 'N';
         END IF;
--
--       keep a count of the rows processed and commit after every 1000
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
   END LOOP;
--
--
   fsc_utils.proc_end;
   COMMIT;
--
EXCEPTION
   WHEN OTHERS THEN
      s_dl_utils.update_process_summary(cb,cp,cd,'FAILED');
END dataload_validate;
--
--
-- ***********************************************************************
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2
                          ,p_date            IN DATE    ) IS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       lbpa_dl_seqno,
       lbpa_refno
FROM   dl_hcs_business_act_parties
WHERE  lbpa_dlb_batch_id   = p_batch_id
AND    lbpa_dl_load_status = 'C';
--
--
-- *******************************
-- Constants FOR process_summary
--
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'dl_hcs_business_act_parties';
cs       INTEGER;
ce       VARCHAR2(200);
--
-- ***********************************************************************
--
-- Other variables
--
--
i        INTEGER := 0;
l_id     ROWID;
l_an_tab VARCHAR2(1);
--
BEGIN
--
    fsc_utils.proc_start('s_'||ct||'.dataload_delete');
    fsc_utils.debug_message('s_'||ct||'.dataload_delete',3 );
--
    cb := p_batch_id;
    cd := p_date;
--
    s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
--
--
    FOR p1 in c1 LOOP
--
      BEGIN
--
          cs   := p1.lbpa_dl_seqno;
          l_id := p1.rec_rowid;
          i    := i +1;
--
-- Delete from BUSINESS_ACTION_PARTIES table
--
          DELETE
            FROM business_action_parties
           WHERE bpa_refno = p1.lbpa_refno;
--
--
-- ***********************************************************************
--
-- Now UPDATE the record status and process count
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          IF mod(i,5000) = 0 THEN
           COMMIT;
          END IF;
--
          EXCEPTION
               WHEN OTHERS THEN
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
                  set_record_status_flag(l_id,'C');
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
      END;
--
    END LOOP;
--
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUSINESS_ACTION_PARTIES');
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

END s_dl_hcs_business_act_parties;
/

show errors
