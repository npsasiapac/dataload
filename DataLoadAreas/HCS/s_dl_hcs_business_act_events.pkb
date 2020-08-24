CREATE OR REPLACE PACKAGE BODY s_dl_hcs_business_act_events
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--
--      1.0  5.16.1    VS   27-SEP-2009  Initial Version
--
--      1.1  5.16.1    VS   28-OCT-2009  Error Code change 621 to 628
--      
-- ***********************************************************************   
--  
--  declare package variables AND constants
--
--
PROCEDURE set_record_status_flag(p_rowid  IN ROWID,
                                 p_status IN VARCHAR2)
AS
--
BEGIN
--
  UPDATE dl_hcs_business_act_events
     SET lbae_dl_load_status = p_status
   WHERE rowid               = p_rowid;
--
  EXCEPTION
       WHEN OTHERS THEN
          dbms_output.put_line('Error updating status of dl_hcs_business_act_events');
          RAISE;
--
END set_record_status_flag;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_create(p_batch_id          IN VARCHAR2,
                          p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LBAE_DLB_BATCH_ID,
       LBAE_DL_SEQNO,
       LBAE_DL_LOAD_STATUS,
       LBAE_BAN_ALT_REF,
       LBAE_SEQUENCE,
       LBAE_TYPE,
       LBAE_AET_CODE,
       LBAE_BAA_BAP_CODE,
       LBAE_BAA_START_DATE,
       LBAE_SCO_CODE,
       LBAE_STATUS_DATE,
       NVL(LBAE_CREATED_BY,'DATALOAD') LBAE_CREATED_BY,
       NVL(LBAE_CREATED_DATE, SYSDATE) LBAE_CREATED_DATE,
       LBAE_INSERTED_DATETIME,
       LBAE_USR_USERNAME,
       LBAE_ACTUAL_DATE,
       LBAE_TARGET_DATE,
       LBAE_EXPIRY_DATE,
       LBAE_COMMENTS,
       LBAE_PREV_SCO_CODE,
       NVL(LBAE_LINKED_EVENT_IND, 'N') LBAE_LINKED_EVENT_IND,
       LBAE_LINKED_BAN_REF,
       LBAE_LINKED_EVENT_SEQUENCE
  FROM dl_hcs_business_act_events
 WHERE lbae_dlb_batch_id   = p_batch_id
   AND lbae_dl_load_status = 'V';
--
-- ************************************************************************************
--
CURSOR get_bct_refno(p_aet_code	VARCHAR2,
                     p_bap_code	VARCHAR2)
IS
SELECT b.bct_refno
  FROM business_action_pth_events a, 
       business_action_pth_steps  b
 WHERE bev_aet_code  = p_aet_code
   AND bct_bap_code  = p_bap_code
   AND bev_bct_refno = bct_refno;
--
-- ************************************************************************************
--
CURSOR chk_bap_exists(p_bae_ban_reference NUMBER,
                      p_baa_bap_code      VARCHAR2,
                      p_baa_start_date    DATE) 
IS
SELECT 'X'
  FROM business_action_pth_assigns
 WHERE baa_ban_reference  = p_bae_ban_reference
   AND baa_bap_code       = p_baa_bap_code
   AND baa_start_datetime = p_baa_start_date;
--
-- ************************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'CREATE';
ct       		VARCHAR2(30) := 'DL_HCS_BUSINESS_ACT_EVENTS';
cs       		INTEGER;
ce	   		VARCHAR2(200);
l_id            	ROWID;
l_an_tab 		VARCHAR2(1);
--
-- Other variables
--
i	              	INTEGER := 0;
--
--
l_ban_reference         NUMBER(10);
l_bae_bev_bct_refno     NUMBER(10);
l_bap_exists            VARCHAR2(1);
--
--
BEGIN
--
    execute immediate 'alter trigger BAE_BR_I disable';
    execute immediate 'alter trigger BAA_BR_I disable';
--
    fsc_utils.proc_start('s_dl_hcs_business_act_events.dataload_create');
    fsc_utils.debug_message( 's_dl_hcs_business_act_events.dataload_create',3);
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
          cs := p1.lbae_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
--
          l_bae_bev_bct_refno := NULL;
--
          IF (p1.LBAE_TYPE = 'A') THEN
--
            OPEN get_bct_refno (p1.LBAE_AET_CODE, p1.LBAE_BAA_BAP_CODE);
           FETCH get_bct_refno INTO l_bae_bev_bct_refno;
           CLOSE get_bct_refno;
--
          END IF;
--
--
-- Only insert into business action pth assigns table if record
-- doesn't already exists for ban_reference/baa_bap_code/baa_start_date combination
--
          IF (    p1.LBAE_TYPE = 'A' 
              AND p1.LBAE_LINKED_EVENT_IND = 'N') THEN
--
           l_bap_exists := NULL;
--
            OPEN chk_bap_exists(P1.LBAE_BAN_ALT_REF, p1.LBAE_BAA_BAP_CODE, p1.LBAE_BAA_START_DATE);
           FETCH chk_bap_exists INTO l_bap_exists;
           CLOSE chk_bap_exists;
--
          END IF;
--
--
          IF (    p1.LBAE_TYPE = 'A'
              AND l_bap_exists IS NULL) THEN
--
           INSERT INTO business_action_pth_assigns(BAA_BAN_REFERENCE,
                                                   BAA_BAP_CODE,
                                                   BAA_START_DATETIME,
                                                   BAA_CREATED_BY,
                                                   BAA_CREATED_DATE,
                                                   BAA_END_DATETIME
                                                  )
--
                                           VALUES (p1.LBAE_BAN_ALT_REF,
                                                   p1.LBAE_BAA_BAP_CODE,
                                                   p1.LBAE_BAA_START_DATE,
                                                   p1.LBAE_CREATED_BY,
                                                   p1.LBAE_CREATED_DATE,
                                                   NULL);
--
          END IF;
--
--
          INSERT INTO BUSINESS_ACTION_EVENTS(BAE_BAN_REFERENCE,
                                             BAE_SEQUENCE,
                                             BAE_TYPE,
                                             BAE_AET_CODE,
                                             BAE_SCO_CODE,
                                             BAE_STATUS_DATE,
                                             BAE_REUSABLE_REFNO,
                                             BAE_CREATED_BY,
                                             BAE_CREATED_DATE,
                                             BAE_BAA_BAP_CODE,
                                             BAE_BAA_START_DATE,
                                             BAE_BEV_BCT_REFNO,
                                             BAE_INSERTED_DATETIME,
                                             BAE_USR_USERNAME,
                                             BAE_ACTUAL_DATE,
                                             BAE_TARGET_DATE,
                                             BAE_EXPIRY_DATE,
                                             BAE_COMMENTS,
                                             BAE_PREV_SCO_CODE,
                                             BAE_LINKED_EVENT_IND,
                                             BAE_LINKED_BAN_REF,
                                             BAE_LINKED_EVENT_SEQUENCE
                                            )
--
                                     VALUES (p1.LBAE_BAN_ALT_REF,
                                             p1.LBAE_SEQUENCE,
                                             p1.LBAE_TYPE,
                                             p1.LBAE_AET_CODE,
                                             p1.LBAE_SCO_CODE,
                                             p1.LBAE_STATUS_DATE,
                                             reusable_refno_seq.NEXTVAL,
                                             p1.LBAE_CREATED_BY,
                                             p1.LBAE_CREATED_DATE,
                                             p1.LBAE_BAA_BAP_CODE,
                                             p1.LBAE_BAA_START_DATE,
                                             l_bae_bev_bct_refno,
                                             p1.LBAE_INSERTED_DATETIME,
                                             p1.LBAE_USR_USERNAME,
                                             p1.LBAE_ACTUAL_DATE,
                                             p1.LBAE_TARGET_DATE,
                                             p1.LBAE_EXPIRY_DATE,
                                             p1.LBAE_COMMENTS,
                                             p1.LBAE_PREV_SCO_CODE,
                                             p1.LBAE_LINKED_EVENT_IND,
                                             p1.LBAE_LINKED_BAN_REF,
                                             p1.LBAE_LINKED_EVENT_SEQUENCE
                                            );
--
--
-- keep a count of the rows processed and commit after every 5000
--
          i := i+1; 
--
          IF MOD(i,5000)=0 THEN 
           COMMIT; 
          END If;
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
    COMMIT;
--
--
-- Section to analyze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUSINESS_ACTION_EVENTS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUSINESS_ACTION_PTH_ASSIGNS');
--
    fsc_utils.proc_end;
--
    EXCEPTION
         WHEN OTHERS THEN
            s_dl_process_summary.update_summary(cb,cp,cd,'FAILED');
            RAISE;
--
    execute immediate 'alter trigger BAE_BR_I enable';
    execute immediate 'alter trigger BAA_BR_I enable';
--
END dataload_create;
--
-- ************************************************************************************
--
--
PROCEDURE dataload_validate(p_batch_id          IN VARCHAR2,
                            p_date              IN DATE)
AS
--
CURSOR c1 
IS
SELECT rowid rec_rowid,
       LBAE_DLB_BATCH_ID,
       LBAE_DL_SEQNO,
       LBAE_DL_LOAD_STATUS,
       LBAE_BAN_ALT_REF,
       LBAE_SEQUENCE,
       LBAE_TYPE,
       LBAE_AET_CODE,
       LBAE_BAA_BAP_CODE,
       LBAE_BAA_START_DATE,
       LBAE_SCO_CODE,
       LBAE_STATUS_DATE,
       nvl(LBAE_CREATED_BY,'DATALOAD') LBAE_CREATED_BY,
       NVL(LBAE_CREATED_DATE, SYSDATE) LBAE_CREATED_DATE,
       LBAE_INSERTED_DATETIME,
       LBAE_USR_USERNAME,
       LBAE_ACTUAL_DATE,
       LBAE_TARGET_DATE,
       LBAE_EXPIRY_DATE,
       LBAE_COMMENTS,
       LBAE_PREV_SCO_CODE,
       NVL(LBAE_LINKED_EVENT_IND, 'N') LBAE_LINKED_EVENT_IND,
       LBAE_LINKED_BAN_REF,
       LBAE_LINKED_EVENT_SEQUENCE
  FROM dl_hcs_business_act_events
 WHERE lbae_dlb_batch_id   = p_batch_id
   AND lbae_dl_load_status in ('L','F','O');
--
-- ************************************************************************************
--
CURSOR chk_aet_code(p_aet_code VARCHAR2) 
IS
SELECT 'X', aet_general_evt_classif_ind
  FROM action_event_types
 WHERE aet_code = p_aet_code;
--
-- ************************************************************************************
--
CURSOR chk_bap_code(p_bap_code VARCHAR2) 
IS
SELECT 'X'
  FROM business_action_paths
 WHERE bap_code = p_bap_code;
--
-- ************************************************************************************
--
CURSOR chk_ban_exists(p_ban_alt_ref VARCHAR2) 
IS
SELECT 'X'
  FROM business_actions
 WHERE ban_reference = p_ban_alt_ref;
--
-- ************************************************************************************
--
CURSOR chk_sco_code(p_sco_code VARCHAR2) 
IS
SELECT 'X'
  FROM status_codes
 WHERE sco_code = p_sco_code;
--
-- ************************************************************************************
--
CURSOR chk_bae_exists(p_bae_ban_reference NUMBER,
                      p_bae_aet_code      VARCHAR2,
                      p_bae_sequence      NUMBER) 
IS
SELECT 'X'
  FROM business_action_events
 WHERE bae_ban_reference = p_bae_ban_reference
   AND bae_aet_code      = p_bae_aet_code
   AND bae_sequence      = p_bae_sequence;
--
-- ************************************************************************************
--
CURSOR chk_bap_exists(p_bae_ban_reference NUMBER,
                      p_baa_bap_code      VARCHAR2,
                      p_baa_start_date    DATE) 
IS
SELECT 'X'
  FROM business_action_pth_assigns
 WHERE baa_ban_reference  = p_bae_ban_reference
   AND baa_bap_code       = p_baa_bap_code
   AND baa_start_datetime = p_baa_start_date;
--
-- ************************************************************************************
--
-- Constants for process_summary
--
cb       		VARCHAR2(30);
cd      		DATE;
cp       		VARCHAR2(30) := 'VALIDATE';
ct       		VARCHAR2(30) := 'DL_HCS_BUSINESS_ACT_EVENTS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id            	ROWID;
--
-- Other Constants
--
l_aet_exists      	VARCHAR2(1);
l_aet_ind        	VARCHAR2(1);
l_bap_code_exists      	VARCHAR2(1);
l_bap_exists      	VARCHAR2(1);
l_sco_exists      	VARCHAR2(1);
l_prev_sco_exists      	VARCHAR2(1);
l_bae_exists      	VARCHAR2(1);
--
l_ban_exists            VARCHAR2(1);
--
l_errors         	VARCHAR2(10);
l_error_ind      	VARCHAR2(10);
i                	INTEGER :=0;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hcs_business_act_events.dataload_validate');
    fsc_utils.debug_message('s_dl_hcs_business_act_events.dataload_validate',3);
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
          cs := p1.lbae_dl_seqno;
          l_id := p1.rec_rowid;
--
          l_errors    := 'V';
          l_error_ind := 'N';
--
--
-- ************************************************************************************
--
-- CHECK THE EVENT TYPE IS VALID.  A - Automatic Business Action Event, 
--                                 M - Manual Business Action Event
--
--
          IF (p1.LBAE_TYPE IS NOT NULL) THEN
--
           IF (p1.LBAE_TYPE NOT IN ('A','M','L')) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',608);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE BUSINESS ACTION EVENT CODE IS VALID
--
          l_aet_exists := NULL;
          l_aet_ind    := NULL;
--
          IF (p1.LBAE_AET_CODE IS NOT NULL) THEN
--
            OPEN chk_aet_code(p1.LBAE_AET_CODE);
           FETCH chk_aet_code INTO l_aet_exists, l_aet_ind;
           CLOSE chk_aet_code;
--
           IF (l_aet_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',609);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE BUSINESS ACTION PATH CODE IS VALID
--
          l_bap_code_exists := NULL;
--
          IF (p1.LBAE_BAA_BAP_CODE IS NOT NULL) THEN
--
            OPEN chk_bap_code(p1.LBAE_BAA_BAP_CODE);
           FETCH chk_bap_code INTO l_bap_code_exists;
           CLOSE chk_bap_code;
--
           IF (l_bap_code_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',610);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE STATUS CODE IS VALID
--
          l_sco_exists := NULL;
--
          IF (p1.LBAE_SCO_CODE IS NOT NULL) THEN
--
            OPEN chk_sco_code(p1.LBAE_SCO_CODE);
           FETCH chk_sco_code INTO l_sco_exists;
           CLOSE chk_sco_code;
--
           IF (   l_sco_exists IS NULL
               OR p1.LBAE_SCO_CODE NOT IN ('CUR','COM','HLD','CAN','TRG')) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',611);
--
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE PREVIOUS STATUS CODE IS VALID
--
          l_prev_sco_exists := NULL;
--
          IF (p1.LBAE_PREV_SCO_CODE IS NOT NULL) THEN
--
            OPEN chk_sco_code(p1.LBAE_PREV_SCO_CODE);
           FETCH chk_sco_code INTO l_prev_sco_exists;
           CLOSE chk_sco_code;
--
           IF (l_prev_sco_exists IS NULL) THEN
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',612);
           END IF;
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE EXPIRY_DATE is supplied only for 'COURT/NOTICE' Events
--
--
          IF (    l_aet_ind NOT IN ('C','N','E')
              AND p1.LBAE_EXPIRY_DATE IS NOT NULL ) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',613);
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE INSERTED_DATETIME is supplied if EVENT TYPE is 'M'anual and not supplied if
-- EVENT TYPE is 'A'utomatic
--
--
          IF (    p1.LBAE_TYPE = 'M'
              AND p1.LBAE_INSERTED_DATETIME IS NULL ) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',614);
--
          ELSIF (    p1.LBAE_TYPE = 'A'
                 AND p1.LBAE_INSERTED_DATETIME IS NOT NULL ) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',615);
--
          END IF;
--
-- ************************************************************************************
--
-- Action Path must be supplied for 'A'utomatic Event Types
--
--
          IF (     p1.LBAE_TYPE = 'A'
              AND (p1.LBAE_BAA_BAP_CODE IS NULL OR p1.LBAE_BAA_START_DATE IS NULL)) THEN
--
            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',616);
--
          ELSIF (     p1.LBAE_TYPE = 'M'
                 AND (p1.LBAE_BAA_BAP_CODE IS NOT NULL OR p1.LBAE_BAA_START_DATE IS NOT NULL)) THEN
--
              l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',617);
--
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE Business Action Events record doesn't already exists.
--
          l_bae_exists := NULL;
--
           OPEN chk_bae_exists(P1.LBAE_BAN_ALT_REF, p1.LBAE_AET_CODE, p1.LBAE_SEQUENCE);
          FETCH chk_bae_exists INTO l_bae_exists;
          CLOSE chk_bae_exists;
--
          IF (l_bae_exists IS NOT NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',619);
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE Business Action Reference exists.
--
          l_ban_exists := NULL;
--
           OPEN chk_ban_exists(P1.LBAE_BAN_ALT_REF);
          FETCH chk_ban_exists INTO l_ban_exists;
          CLOSE chk_ban_exists;
--
          IF (l_ban_exists IS NULL) THEN
           l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',618);
          END IF;
--
-- ************************************************************************************
--
-- CHECK THE Business Action Path Assign record exists for 'A'utomatic Action Types.
--
--          IF (    p1.lbae_type = 'A' 
--              AND p1.LBAE_LINKED_EVENT_IND = 'N') THEN
--
--           l_bap_exists := NULL;
--
--            OPEN chk_bap_exists(P1.LBAE_BAN_ALT_REF, p1.LBAE_BAA_BAP_CODE, p1.LBAE_BAA_START_DATE);
--           FETCH chk_bap_exists INTO l_bap_exists;
--           CLOSE chk_bap_exists;
--
--           IF (l_bap_exists IS NOT NULL) THEN
--            l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HD1',628);
--           END IF;
--
--          END IF;
--
--
-- ************************************************************************************
--
-- Now UPDATE the record count AND error code
--
          IF l_errors = 'F' THEN
           l_error_ind := 'Y';
          ELSE
             l_error_ind := 'N';
          END IF;
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,l_error_ind);
          set_record_status_flag(l_id,l_errors);
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
          EXCEPTION
               WHEN OTHERS THEN
                  ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
                  s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
                  set_record_status_flag(l_id,'O');
--
      END;
--
    END LOOP;
--
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
-- ************************************************************************************
--
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 IS
SELECT rowid rec_rowid,
       LBAE_DLB_BATCH_ID,
       LBAE_DL_SEQNO,
       LBAE_DL_LOAD_STATUS,
       LBAE_BAN_ALT_REF,
       LBAE_SEQUENCE,
       LBAE_TYPE,
       LBAE_AET_CODE,
       LBAE_BAA_BAP_CODE,
       LBAE_BAA_START_DATE
  FROM dl_hcs_business_act_events
 WHERE lbae_dlb_batch_id   = p_batch_id
   AND lbae_dl_load_status = 'C';
--
-- ************************************************************************************
--
-- Constants for process_summary
cb       		VARCHAR2(30);
cd       		DATE;
cp       		VARCHAR2(30) := 'DELETE';
ct       		VARCHAR2(30) := 'DL_HCS_BUSINESS_ACT_EVENTS';
cs       		INTEGER;
ce       		VARCHAR2(200);
l_id                 	ROWID;
l_an_tab 		VARCHAR2(1);
--
-- Other Variables
--
i                 	INTEGER := 0;
--
BEGIN
--
    fsc_utils.proc_start('s_dl_hcs_business_act_events.dataload_delete');
    fsc_utils.debug_message( 's_dl_hcs_business_act_events.dataload_delete',3 );
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
          cs := p1.lbae_dl_seqno;
          l_id := p1.rec_rowid;
--
          SAVEPOINT SP1;
--
--
          IF (p1.LBAE_TYPE = 'A') THEN
--
           DELETE 
             FROM business_action_pth_assigns
            WHERE baa_ban_reference  = p1.lbae_ban_alt_ref
              AND baa_bap_code       = p1.lbae_baa_bap_code
              AND baa_start_datetime = p1.lbae_baa_start_date;
--
          END IF;
--
          DELETE 
            FROM business_action_events
           WHERE bae_ban_reference = p1.lbae_ban_alt_ref
             AND bae_aet_code      = p1.lbae_aet_code
             AND bae_sequence      = p1.lbae_sequence;
--
--
-- keep a count of the rows processed and commit after every 1000
--
--
          s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
          set_record_status_flag(l_id,'V');
--
          i := i+1; 
--
          IF MOD(i,1000)=0 THEN 
           COMMIT; 
          END IF;
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
    COMMIT;
--
-- Section to anayze the table(s) populated by this dataload
--
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUSINESS_ACTION_PTH_ASSIGNS');
    l_an_tab:=s_dl_hem_utils.dl_comp_stats('BUSINESS_ACTION_EVENTS');
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
END s_dl_hcs_business_act_events;
/
