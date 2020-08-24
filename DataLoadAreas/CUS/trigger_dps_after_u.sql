CREATE OR REPLACE TRIGGER DPS_AFTER_U
AFTER UPDATE ON DL_PROCESS_SUMMARY
FOR EACH ROW
DECLARE
 l_job number;
BEGIN
   IF :NEW.dps_status LIKE 'COM%'
   THEN
     dbms_job.submit(job=>l_job,
                     what=>'begin s_dl_cus_dataload_batches.run_next( '''|| :NEW.dps_status||''','''||:NEW.dps_process||''','''|| :NEW.DPS_DLB_BATCH_ID||''' ); end;',
                     next_date=>SYSDATE + dbms_random.value(2,10)/24/60/60 );     
  END IF;   
END DPS_BR_U;
/