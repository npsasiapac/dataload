/* Formatted on 14/10/2013 3:15:22 PM (QP5 v5.227.12220.39754) */
CREATE OR REPLACE PACKAGE BODY HOU.s_dl_hra_rds_authorities
AS
   -- ***********************************************************************
   --  DESCRIPTION:
   --
   --  CHANGE CONTROL
   --
   --  VERSION DB Vers   WHO  WHEN         WHY
   --
   --  1.0     5.15.0    PH   15-JAN-2009  Initial Creation.
   --
   --  2.0     5.15.0    VS   15-FEB-2009  Defect Id 3545. Cannot assign 'NEW' to
   --                                      the pending action status for RDS Authorities
   --                                      with the status of 'Pending'
   --
   --  3.0     5.15.0    VS   12-APR-2010  Defect Id 4180. New column 
   --                                      needs to be populated. By default for HNSW ONLY 'CLK'
   --
   --  4.0     5.15.0    VS   28-JUN-2010  Defect Id 5159. Addition of
   --                                      Modified By/Date
   --  5.0     6.8.0     MM   14-Oct-2013  ALM 1247. Amend c_get_par to check par_per_alt_ref
   --                                      and resolve the par_refno from the par_per_alt_ref
   --
   --  5.1     6.16  MOK/TG   06-Feb-2018  Added LRDSA_HRV_BSRC_CODE The unique code 
   --                                      for the source of the benefit from which 
   --                                      the deduction will be made.
   --
   --  5.2     6.18	    VRS   17-DEC-2018  added LRDSA_DVA_UIN
   --
   --  declare package variables AND constants
   --
   -- ***********************************************************************
   --
   --
   PROCEDURE set_record_status_flag (p_rowid IN ROWID, p_status IN VARCHAR2)
   AS
   --
   BEGIN
      --
      UPDATE dl_hra_rds_authorities
         SET lrdsa_dl_load_status = p_status
       WHERE ROWID = p_rowid;
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (
            'Error updating status of dl_hra_rds_authorities');
         RAISE;
   --
   END set_record_status_flag;

   --
   -- ***********************************************************************
   --
   PROCEDURE dataload_create (p_batch_id IN VARCHAR2, p_date IN DATE)
   AS
      --
      CURSOR c1
      IS
         SELECT ROWID rec_rowid,
                lrdsa_dlb_batch_id,
                lrdsa_dl_seqno,
                lrdsa_dl_load_status,
                lrdsa_refno,
                lrdsa_ha_reference,
                lrdsa_par_per_alt_ref,
                lrdsa_hrv_rpag_code,
                lrdsa_pay_agency_crn,
                lrdsa_start_date,
                lrdsa_status_date,
                lrdsa_sco_code,
                NVL (lrdsa_created_by, 'DATALOAD') lrdsa_created_by,
                NVL (lrdsa_created_date, SYSDATE) lrdsa_created_date,
                lrdsa_pending_sco_code,
                lrdsa_end_date,
                lrdsa_suspend_from_date,
                lrdsa_suspend_to_date,
                lrdsa_action_sent_datetime,
                lrdsa_hrv_susr_code,
                lrdsa_hrv_terr_code,
                lrdsa_modified_by,
                lrdsa_modified_date,
                lrdsa_hrv_bsrc_code,
                lrdsa_dva_uin
           FROM dl_hra_rds_authorities
          WHERE     lrdsa_dlb_batch_id = p_batch_id
                AND lrdsa_dl_load_status = 'V';

      --
      --
      -- ***********************************************************************
      --
      -- Additional Cursors
      --
      --
      CURSOR c2 (p_lrdsa_par_per_alt_ref VARCHAR2)
      IS
         SELECT par_refno
           FROM parties
          WHERE par_per_alt_ref = p_lrdsa_par_per_alt_ref;

      --
      -- ***********************************************************************
      --
      -- Constants for process_summary
      --
      cb                VARCHAR2 (30);
      cd                DATE;
      cp                VARCHAR2 (30) := 'CREATE';
      ct                VARCHAR2 (30) := 'DL_HRA_RDS_AUTHORITIES';
      cs                INTEGER;
      ce                VARCHAR2 (200);
      l_id              ROWID;
      l_an_tab          VARCHAR2 (1);
      lrdsa_par_refno   NUMBER (10);
      --
      --
      -- ***********************************************************************
      --
      -- Other variables
      --
      i                 INTEGER := 0;
      l_exists          VARCHAR2 (1);
   --
   --
   -- ***********************************************************************
   --
   BEGIN
      --
      fsc_utils.proc_start ('s_dl_hra_rds_authorities.dataload_create');
      fsc_utils.debug_message ('s_dl_hra_rds_authorities.dataload_create', 3);
      --
      cb := p_batch_id;
      cd := p_date;
      s_dl_utils.update_process_summary (cb,
                                         cp,
                                         cd,
                                         'RUNNING');

      --
      FOR p1 IN c1
      LOOP
         --
         BEGIN
            --
            cs := p1.lrdsa_dl_seqno;
            l_id := p1.rec_rowid;
            --
            SAVEPOINT SP1;
            --
            -- Main processing
            --
            -- Open any cursors
            --
            lrdsa_par_refno := NULL;

            --
            --

            --
            OPEN c2 (p1.lrdsa_par_per_alt_ref);

            FETCH c2 INTO lrdsa_par_refno;

            CLOSE c2;

            --
            -- Insert into
            --
            INSERT                                               /* +APPEND */
                  INTO  rds_authorities (rdsa_refno,
                                         rdsa_ha_reference,
                                         rdsa_par_refno,
                                         rdsa_hrv_rpag_code,
                                         rdsa_pay_agency_crn,
                                         rdsa_start_date,
                                         rdsa_status_date,
                                         rdsa_current_sco_code,
                                         rdsa_created_by,
                                         rdsa_created_date,
                                         rdsa_pending_sco_code,
                                         rdsa_end_date,
                                         rdsa_suspended_from_date,
                                         rdsa_suspended_to_date,
                                         rdsa_action_sent_datetime,
                                         rdsa_hrv_susr_code,
                                         rdsa_hrv_terr_code,
                                         rdsa_modified_by,
                                         rdsa_modified_date,
                                         rdsa_hrv_bsrc_code,
                                         rdsa_dva_uin
                                         )
                 VALUES (p1.lrdsa_refno,
                         p1.lrdsa_ha_reference,
                         lrdsa_par_refno,
                         p1.lrdsa_hrv_rpag_code,
                         p1.lrdsa_pay_agency_crn,
                         p1.lrdsa_start_date,
                         p1.lrdsa_status_date,
                         p1.lrdsa_sco_code,
                         p1.lrdsa_created_by,
                         p1.lrdsa_created_date,
                         p1.lrdsa_pending_sco_code,
                         p1.lrdsa_end_date,
                         p1.lrdsa_suspend_from_date,
                         p1.lrdsa_suspend_to_date,
                         p1.lrdsa_action_sent_datetime,
                         p1.lrdsa_hrv_susr_code,
                         p1.lrdsa_hrv_terr_code,
                         p1.lrdsa_modified_by,
                         p1.lrdsa_modified_date,
                         p1.lrdsa_hrv_bsrc_code,
                         p1.lrdsa_dva_uin);

            --
            --
            -- ***********************************************************************
            --
            -- Now UPDATE the record status and process count
            --
            i := i + 1;

            --
            IF MOD (i, 500000) = 0
            THEN
               COMMIT;
            END IF;

            --
            s_dl_process_summary.update_processed_count (cb,
                                                         cp,
                                                         cd,
                                                         'N');
            set_record_status_flag (l_id, 'C');
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               ROLLBACK TO SP1;
               ce :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'ORA',
                                            SQLCODE,
                                            SQLERRM);
               set_record_status_flag (l_id, 'O');
               s_dl_process_summary.update_processed_count (cb,
                                                            cp,
                                                            cd,
                                                            'Y');
         --
         END;
      --
      END LOOP;

      --
      COMMIT;
      --
      -- ***********************************************************************
      --
      -- Section to anayze the table(s) populated by this dataload
      --
      l_an_tab := s_dl_hem_utils.dl_comp_stats ('RDS_AUTHORITIES');
      --
      fsc_utils.proc_END;
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         s_dl_process_summary.UPDATE_summary (cb,
                                              cp,
                                              cd,
                                              'FAILED');
         RAISE;
   --
   END dataload_create;

   --
   --
   -- ***********************************************************************
   --
   --
   PROCEDURE dataload_validate (p_batch_id IN VARCHAR2, p_date IN DATE)
   AS
      --
      CURSOR c1
      IS
         SELECT ROWID rec_rowid,
                lrdsa_dlb_batch_id,
                lrdsa_dl_seqno,
                lrdsa_dl_load_status,
                lrdsa_ha_reference,
                lrdsa_par_per_alt_ref,
                lrdsa_hrv_rpag_code,
                lrdsa_pay_agency_crn,
                lrdsa_start_date,
                lrdsa_status_date,
                lrdsa_sco_code,
                NVL (lrdsa_created_by, 'DATALOAD') lrdsa_created_by,
                NVL (lrdsa_created_date, SYSDATE) lrdsa_created_date,
                lrdsa_pending_sco_code,
                lrdsa_end_date,
                lrdsa_suspend_from_date,
                lrdsa_suspend_to_date,
                lrdsa_action_sent_datetime,
                lrdsa_hrv_susr_code,
                lrdsa_hrv_terr_code,
                lrdsa_modified_by,
                lrdsa_modified_date,
				lrdsa_hrv_bsrc_code,
                lrdsa_dva_uin
           FROM dl_hra_rds_authorities
          WHERE     lrdsa_dlb_batch_id = p_batch_id
                AND lrdsa_dl_load_status IN ('L', 'F', 'O');

      --
      -- ***********************************************************************
      --
      -- Additional Cursors
      --
      CURSOR c_get_par (p_alt_ref VARCHAR2)
      IS
         SELECT 'X'
           FROM parties
          WHERE par_per_alt_ref = p_alt_ref;          --changed from par_refno

      --
      CURSOR c_get_ha_ref (p_ha_reference VARCHAR2)
      IS
         SELECT 'X'
           FROM rds_authorities
          WHERE rdsa_ha_reference = p_ha_reference;

      --
      --
      -- ***********************************************************************
      --
      -- Constants for process_summary
      --
      cb            VARCHAR2 (30);
      cd            DATE;
      cp            VARCHAR2 (30) := 'VALIDATE';
      ct            VARCHAR2 (30) := 'DL_HRA_RDS_AUTHORITIES';
      cs            INTEGER;
      ce            VARCHAR2 (200);
      l_id          ROWID;
      --
      --
      -- ***********************************************************************
      --
      -- Other variables
      --
      l_exists      VARCHAR2 (1);
      l_pro_refno   NUMBER (10);
      l_errors      VARCHAR2 (10);
      l_error_ind   VARCHAR2 (10);
      i             INTEGER := 0;
   --
   -- ***********************************************************************
   --
   --
   BEGIN
      --
      fsc_utils.proc_start ('s_dl_hra_rds_authorities.dataload_validate');
      fsc_utils.debug_message ('s_dl_hra_rds_authorities.dataload_validate',
                               3);
      --
      cb := p_batch_id;
      cd := p_DATE;
      --
      s_dl_utils.update_process_summary (cb,
                                         cp,
                                         cd,
                                         'RUNNING');

      --
      FOR p1 IN c1
      LOOP
         --
         BEGIN
            --
            cs := p1.lrdsa_dl_seqno;
            l_id := p1.rec_rowid;
            --
            l_errors := 'V';
            l_error_ind := 'N';

            --
            -- Validation checks required
            --
            -- Check the Person exists
            --
            OPEN c_get_par (p1.lrdsa_par_per_alt_ref);

            FETCH c_get_par INTO l_exists;

            IF c_get_par%NOTFOUND
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HDL',
                                            868);
            END IF;

            CLOSE c_get_par;

            --
            -- Check the Authority Ref is Unique
            --
            OPEN c_get_ha_ref (p1.lrdsa_ha_reference);

            FETCH c_get_ha_ref INTO l_exists;

            IF c_get_ha_ref%FOUND
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            90);
            END IF;

            CLOSE c_get_ha_ref;

            --
            -- Check Status Code
            --
            IF NVL (p1.lrdsa_sco_code, '^~#') NOT IN
                  ('PND', 'CON', 'ACT', 'ERR', 'SUS', 'TRM', 'CAN')
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            14);
            END IF;

            --
            -- Pending Status Code
            --
            IF p1.lrdsa_pending_sco_code IS NOT NULL
            THEN
               IF p1.lrdsa_pending_sco_code NOT IN
                     ('VAR', 'SUS', 'TRM', 'NEW')
               THEN
                  l_errors :=
                     s_dl_errors.record_error (cb,
                                               cp,
                                               cd,
                                               ct,
                                               cs,
                                               'HD2',
                                               91);
               END IF;
            END IF;

            --
            -- End Date
            --
            IF p1.lrdsa_end_date IS NOT NULL
            THEN
               IF p1.lrdsa_end_date <
                     NVL (p1.lrdsa_start_date, p1.lrdsa_end_date)
               THEN
                  l_errors :=
                     s_dl_errors.record_error (cb,
                                               cp,
                                               cd,
                                               ct,
                                               cs,
                                               'HD2',
                                               20);
               END IF;
            END IF;

            --
            -- Suspension End Date
            --
            IF p1.lrdsa_suspend_to_date IS NOT NULL
            THEN
               IF p1.lrdsa_suspend_to_date <
                     NVL (p1.lrdsa_suspend_from_date,
                          p1.lrdsa_suspend_to_date)
               THEN
                  l_errors :=
                     s_dl_errors.record_error (cb,
                                               cp,
                                               cd,
                                               ct,
                                               cs,
                                               'HD2',
                                               92);
               END IF;
            END IF;

            --
            -- Suspension Start Date
            --
            IF p1.lrdsa_suspend_from_date IS NOT NULL
            THEN
               IF p1.lrdsa_suspend_from_date <
                     NVL (p1.lrdsa_start_date, p1.lrdsa_suspend_from_date)
               THEN
                  l_errors :=
                     s_dl_errors.record_error (cb,
                                               cp,
                                               cd,
                                               ct,
                                               cs,
                                               'HD2',
                                               93);
               END IF;
            END IF;

            --
            -- Reference Values
            --
            -- Agency Code
            --
            IF (NOT s_dl_hem_utils.exists_frv ('RDS_PAY_AGENCY',
                                               p1.lrdsa_hrv_rpag_code,
                                               'N'))
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            94);
            END IF;

            --
            -- Suspension Reasons
            --
            IF (NOT s_dl_hem_utils.exists_frv ('RDS_SUS_RSN',
                                               p1.lrdsa_hrv_susr_code,
                                               'Y'))
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            95);
            END IF;

            --
            -- Termination Reason
            --
            IF (NOT s_dl_hem_utils.exists_frv ('RDS_TERM_RSN',
                                               p1.lrdsa_hrv_terr_code,
                                               'Y'))
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            96);
            END IF;

            --
            -- Check Benefit Source code 
            --
            IF (NOT s_dl_hem_utils.exists_frv('RDS_BEN_SOURCE',
			                                  p1.LRDSA_HRV_BSRC_CODE,
											  'N'))
            THEN
               l_errors:=
			      s_dl_errors.record_error(cb,
			                               cp,
										   cd,
										   ct,
										   cs,
										   'HD2',
										   896);
            END IF;
			
            --
            -- Mandatory Fields not already checked
            --
            -- Authority Ref
            --
            IF p1.lrdsa_ha_reference IS NULL
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            97);
            END IF;

            --
            -- Agency/Person Code
            --
            IF p1.lrdsa_pay_agency_crn IS NULL
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            98);
            END IF;

            --
            -- Start Date
            --
            IF p1.lrdsa_start_date IS NULL
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            12);
            END IF;

            --
            -- Status Date
            --
            IF p1.lrdsa_status_date IS NULL
            THEN
               l_errors :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'HD2',
                                            16);
            END IF;

            --
            --
            --
            -- ***********************************************************************
            --
            -- Now UPDATE the record status and process count
            --
            IF (l_errors = 'F')
            THEN
               l_error_ind := 'Y';
            ELSE
               l_error_ind := 'N';
            END IF;

            --
            s_dl_process_summary.update_processed_count (cb,
                                                         cp,
                                                         cd,
                                                         l_error_ind);
            set_record_status_flag (l_id, l_errors);
            --
            -- keep a count of the rows processed and commit after every 1000
            --
            i := i + 1;

            --
            IF MOD (i, 1000) = 0
            THEN
               COMMIT;
            END IF;
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               ce :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'ORA',
                                            SQLCODE,
                                            SQLERRM);
               s_dl_process_summary.update_processed_count (cb,
                                                            cp,
                                                            cd,
                                                            'Y');
               set_record_status_flag (l_id, 'O');
         --
         END;
      --
      END LOOP;

      --
      fsc_utils.proc_END;
      --
      COMMIT;
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         s_dl_utils.update_process_summary (cb,
                                            cp,
                                            cd,
                                            'FAILED');
   --
   END dataload_validate;

   --
   --
   -- ***********************************************************************
   --
   PROCEDURE dataload_delete (p_batch_id IN VARCHAR2, p_date IN DATE)
   IS
      --
      CURSOR c1
      IS
         SELECT ROWID rec_rowid,
                lrdsa_dlb_batch_id,
                lrdsa_dl_seqno,
                lrdsa_dl_load_status,
                lrdsa_refno
           FROM dl_hra_rds_authorities
          WHERE     lrdsa_dlb_batch_id = p_batch_id
                AND lrdsa_dl_load_status = 'C';

      --
      -- ***********************************************************************
      --
      -- Additional Cursors
      --
      --
      --
      -- ***********************************************************************
      --
      -- Constants FOR process_summary
      --
      cb            VARCHAR2 (30);
      cd            DATE;
      cp            VARCHAR2 (30) := 'DELETE';
      ct            VARCHAR2 (30) := 'DL_HRA_RDS_AUTHORITIES';
      cs            INTEGER;
      ce            VARCHAR2 (200);
      l_id          ROWID;
      l_an_tab      VARCHAR2 (1);
      --
      --
      -- ***********************************************************************
      --
      -- Other variables
      --
      l_exists      VARCHAR2 (1);
      l_pro_refno   NUMBER (10);
      i             INTEGER := 0;
   --
   -- ***********************************************************************
   --
   --
   BEGIN
      --
      fsc_utils.proc_start ('s_dl_hra_rds_authorities.dataload_delete');
      fsc_utils.debug_message ('s_dl_hra_rds_authorities.dataload_delete', 3);
      --
      cb := p_batch_id;
      cd := p_date;
      --
      s_dl_utils.update_process_summary (cb,
                                         cp,
                                         cd,
                                         'RUNNING');

      --
      --
      FOR p1 IN c1
      LOOP
         --
         BEGIN
            --
            cs := p1.lrdsa_dl_seqno;
            l_id := p1.rec_rowid;
            i := i + 1;

            --
            -- Delete from rds_authorities
            --
            DELETE FROM rds_authorities
                  WHERE rdsa_refno = p1.lrdsa_refno;

            --
            --
            -- ***********************************************************************
            --
            -- Now UPDATE the record status and process count
            --
            --
            s_dl_process_summary.update_processed_count (cb,
                                                         cp,
                                                         cd,
                                                         'N');
            set_record_status_flag (l_id, 'V');

            --
            IF MOD (i, 5000) = 0
            THEN
               COMMIT;
            END IF;
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               ce :=
                  s_dl_errors.record_error (cb,
                                            cp,
                                            cd,
                                            ct,
                                            cs,
                                            'ORA',
                                            SQLCODE);
               set_record_status_flag (l_id, 'C');
               s_dl_process_summary.update_processed_count (cb,
                                                            cp,
                                                            cd,
                                                            'Y');
         --
         END;
      --
      END LOOP;

      --
      --
      -- Section to anayze the table(s) populated by this dataload
      --
      l_an_tab := s_dl_hem_utils.dl_comp_stats ('RDS_AUTHORITIES');
      --
      fsc_utils.proc_end;
      COMMIT;
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         s_dl_utils.update_process_summary (cb,
                                            cp,
                                            cd,
                                            'FAILED');
         RAISE;
   --
   END dataload_delete;
--
END s_dl_hra_rds_authorities;
/