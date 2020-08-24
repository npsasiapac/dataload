SET SERVEROUTPUT ON
SPOOL encrypt_users.lis

BEGIN

   FOR c_user IN
   (SELECT *
    FROM   users
    WHERE  SUBSTR(usr_username,1,3) NOT IN ('HOU','FDW')
    AND    usr_username NOT IN ('TEST.ISDSUPP1','TEST.ISDSUPP2','TEST.ISDSUPP3') -- ISD admin users
    AND    usr_current_ind = 'Y'
   )
   LOOP -- encrypt all passwords and remove admin option from all users

      IF NVL(c_user.usr_encrypt_pw,'N') = 'N' -- Password Not Encrypted
         OR  s_users.test_user_role(c_user.usr_username,'ADMIN_ROLE') -- Has Admin Role
      THEN
         dbms_output.put_line('Encrypting password and removing admin access for '||c_user.usr_username);

         a_hou_users.process_update_user
         (p_usr_username             => c_user.usr_username
         ,p_usr_surname              => c_user.usr_surname
         ,p_usr_firstname            => c_user.usr_firstname
         ,p_usr_initials             => c_user.usr_initials
         ,p_usr_title                => c_user.usr_title
         ,p_usr_telephone            => c_user.usr_telephone
         ,p_usr_corr_name            => c_user.usr_corr_name
         ,p_usr_profile_name         => c_user.usr_profile_name
         ,p_usr_encrypt_pw           => 'Y' -- Set to N will allow user to sqlplus with same password
         ,p_usr_spr_printer_name     => c_user.usr_spr_printer_name
         ,p_usr_email_address        => c_user.usr_email_address
         ,p_usr_gpi_notification_ind => c_user.usr_gpi_notification_ind
         ,p_usr_force_pw_change      => c_user.usr_force_pw_change
         ,p_usr_current_ind          => c_user.usr_current_ind
         ,p_usr_hty_code             => c_user.usr_hty_code
         ,p_usr_par_refno            => c_user.usr_par_refno
         ,p_usr_reusable_refno       => c_user.usr_reusable_refno
         ,p_d_default_tablespace     => 'TABLES'
         ,p_d_temporary_tablespace   => 'TEMP'
         ,p_d_user_type              => 'HOU'
         ,p_existing_user_type       => 'HOU'
         ,p_d_admin_role_ind         => 'N' -- Set to Y gives database ADMIN_ROLE and allows user to create users.
         ,p_commit                   => FALSE);

      END IF;

   END LOOP;

   FOR c_user IN
   (SELECT *
    FROM   users
    WHERE  usr_username IN ('CAROLINE_KIWARA','RASHEL_RAHMAN') -- system admin
    OR     SUBSTR(usr_username,1,3)||usr_current_ind = 'NPSY' -- Current NPS Users
   )
   LOOP -- encrypt passwords and grant admin option for all users

      IF NVL(c_user.usr_encrypt_pw,'N') = 'N' -- Password Not Encrypted
         OR  NOT s_users.test_user_role(c_user.usr_username,'ADMIN_ROLE') -- Does not have Admin Role
      THEN
         dbms_output.put_line('Encrypting password and granting admin option for '||c_user.usr_username);

         a_hou_users.process_update_user
         (p_usr_username             => c_user.usr_username
         ,p_usr_surname              => c_user.usr_surname
         ,p_usr_firstname            => c_user.usr_firstname
         ,p_usr_initials             => c_user.usr_initials
         ,p_usr_title                => c_user.usr_title
         ,p_usr_telephone            => c_user.usr_telephone
         ,p_usr_corr_name            => c_user.usr_corr_name
         ,p_usr_profile_name         => c_user.usr_profile_name
         ,p_usr_encrypt_pw           => 'Y' -- Set to N will allow user to sqlplus with same password
         ,p_usr_spr_printer_name     => c_user.usr_spr_printer_name
         ,p_usr_email_address        => c_user.usr_email_address
         ,p_usr_gpi_notification_ind => c_user.usr_gpi_notification_ind
         ,p_usr_force_pw_change      => c_user.usr_force_pw_change
         ,p_usr_current_ind          => c_user.usr_current_ind
         ,p_usr_hty_code             => c_user.usr_hty_code
         ,p_usr_par_refno            => c_user.usr_par_refno
         ,p_usr_reusable_refno       => c_user.usr_reusable_refno
         ,p_d_default_tablespace     => 'TABLES'
         ,p_d_temporary_tablespace   => 'TEMP'
         ,p_d_user_type              => 'HOU'
         ,p_existing_user_type       => 'HOU'
         ,p_d_admin_role_ind         => 'Y' -- Set to Y gives database ADMIN_ROLE and allows user to create users.
         ,p_commit                   => FALSE);

      END IF;

   END LOOP;

EXCEPTION WHEN OTHERS
THEN
   dbms_output.put_line('Exception raised.');
   fsc_utils.handle_exception;
END;
/

SPOOL OFF

