CREATE OR REPLACE PACKAGE s_dl_utils AS

   --  DESCRIPTION:
   --
   --  CHANGE CONTROL
   --  VERSION     WHO  WHEN     WHY
   --  1.0 Phil Naughton 15-May-2000 Initial creation
   --  2.0 peter davies  30-jun-2003  added extra parameters to set record 
   --                                         status flag

   --

  l_data_tab t_dl_data_structure;
  --
  FUNCTION get_batch_col(p_table_name IN VARCHAR2)
  RETURN VARCHAR2;
  --
  FUNCTION get_seqno_col(p_table_name IN VARCHAR2)
  RETURN VARCHAR2;
  --
  FUNCTION get_load_status_col(p_table_name IN VARCHAR2)
  RETURN VARCHAR2;
  --
  FUNCTION get_col_Data
  RETURN T_DL_DATA_STRUCTURE;
  --
  PROCEDURE prepare_col_data (p_table_name IN VARCHAR2,
                         p_batch_id IN VARCHAR2,
						 p_seqno IN NUMBER);
  --
  FUNCTION get_all_col_data (p_table_name IN VARCHAR2,
                             p_batch_id IN VARCHAR2,
                             p_seqno IN NUMBER)
  RETURN T_DL_DATA_STRUCTURE;
  --
  PROCEDURE run_Process(
      p_sys_var1 IN VARCHAR2 DEFAULT NULL, -- p_app_area      IN VARCHAR2
      p_sys_var2 IN VARCHAR2 DEFAULT NULL, -- p_dataload_area IN VARCHAR2
      p_sys_var3 IN VARCHAR2 DEFAULT NULL, -- p_process   IN VARCHAR2
      p_sys_var4 IN VARCHAR2 DEFAULT NULL, -- p_date   IN DATE
      p_sys_var5 IN VARCHAR2 DEFAULT NULL, --
      p_sys_var6 IN VARCHAR2 DEFAULT NULL, --
      p_sys_var7 IN VARCHAR2 DEFAULT NULL,
      p_sys_var8 IN VARCHAR2 DEFAULT NULL,
      p_sys_var9 IN VARCHAR2 DEFAULT NULL,
      p_sys_var10 IN VARCHAR2 DEFAULT NULL,
      p_sys_var11 IN VARCHAR2 DEFAULT NULL,
      p_sys_var12 IN VARCHAR2 DEFAULT NULL,
      p_sys_var13 IN VARCHAR2 DEFAULT NULL,
      p_sys_var14 IN VARCHAR2 DEFAULT NULL,
      p_sys_var15 IN VARCHAR2 DEFAULT NULL,
      p_sys_var16 IN VARCHAR2 DEFAULT NULL,
      p_sys_var17 IN VARCHAR2 DEFAULT NULL,
      p_sys_var18 IN VARCHAR2 DEFAULT NULL, -- P_batch IN VARCHAR2
      p_session_id IN NUMBER,
      p_has_complex_parameters IN VARCHAR2,
      p_username IN VARCHAR2 DEFAULT NULL );
  --
  PROCEDURE set_record_status_flag(p_table_name IN VARCHAR2,
                                   p_batch_id IN VARCHAR2,
                                   p_seqno IN NUMBER,
                                   p_status IN VARCHAR2,
                                   p_seqno_col IN VARCHAR2 DEFAULT NULL, 
                                   p_batch_col IN VARCHAR2 DEFAULT NULL, 
                                   p_status_col IN VARCHAR2 DEFAULT NULL);
  --
  PROCEDURE update_process_summary(p_batch_id IN VARCHAR2,
                                   p_process IN VARCHAR2,
                                   p_date IN DATE,
                                   p_status IN VARCHAR2);
  --
  PROCEDURE remove_batch(p_batch_id IN VARCHAR2);
  --
  PROCEDURE queue_process(p_batch_id IN VARCHAR2,
	 p_prod_area IN VARCHAR2,
	 p_dload_area IN VARCHAR2,
	 p_process IN VARCHAR2,
	 p_batch_mode IN VARCHAR2,
	 p_session_marker IN NUMBER,
	 p_debug_mode IN VARCHAR2);

  PROCEDURE update_col_value (p_table_name IN VARCHAR2,
                              p_column_name IN VARCHAR2,
							  p_new_val IN VARCHAR2,
                              p_batch_id IN VARCHAR2,
							  p_seqno IN NUMBER,
							  p_Date IN BOOLEAN);

  FUNCTION get_data_and_lock(p_table_name IN VARCHAR2,
                             p_col_name IN VARCHAR2,
	 						 p_batch_id IN VARCHAR2,
							 P_seqno IN VARCHAR2)
  RETURN VARCHAR2;
  PROCEDURE delete_loaded_data(p_table_name IN VARCHAR2,
                               p_batch_id IN VARCHAR2);

  FUNCTION get_package_name(
    p_product_area IN VARCHAR2,
    p_dataload_area IN VARCHAR2 )
  RETURN VARCHAR2;

  FUNCTION f_length_long
	(p_table_name IN VARCHAR2,
    p_column_name   IN VARCHAR2 )
  RETURN VARCHAR2;

  FUNCTION check_column_domain
   (p_table_name  IN VARCHAR2,
    p_column_name IN VARCHAR2,
    p_value       IN VARCHAR2)
  RETURN BOOLEAN;

  FUNCTION transfer_messages 
  (p_batch_id     IN VARCHAR2,
     p_process 	  IN VARCHAR2,
 	 p_date 	  IN DATE,
     p_table_name IN VARCHAR2,
  	 p_dl_seqno   IN VARCHAR2,
  	 p_err_field  IN VARCHAR2,
	 p_last_msg   IN OUT NUMBER)
  RETURN VARCHAR2;
--

END s_dl_utils;
/


