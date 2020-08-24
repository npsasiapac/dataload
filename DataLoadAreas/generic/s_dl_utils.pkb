CREATE OR REPLACE PACKAGE BODY s_dl_utils
IS
   -- *************************************************************************
   --  CHANGE CONTROL
   --  VERSION  WHO             WHEN          WHY
   --  1.01     PN              09-May-00     created
   --  1.8      Nathan Morgan   05-July-2001  unstub gpi process call
   --  2.0      PD              10-Jan-2002   Removed Commit from Set Record Status Flag
   --                                         Added Commit to Run Process
   --  2.1      Peter Davies    30-Jun-2003   added extra parameters to set record 
   --                                         status flag.
   --  1.11     K Shannon       26-May-2005   Fix Exec Immed within run_process
   --                                         and added debug messages
   --  1.12     I Amah          11-Sep-2006   WMS 52455 Removed reference to USER
   -- *************************************************************************
   
  -----------------------------------------------------------------------------
  --  Returns the name of the column that holds the batch id for the required table
  -----------------------------------------------------------------------------
  FUNCTION get_batch_col(p_table_name IN VARCHAR2)
  RETURN VARCHAR2
  IS
    CURSOR c_get_Col (cp_table_name IN VARCHAR2)
    IS
    SELECT column_name
      FROM all_tab_columns
     WHERE column_name like '%BATCH_ID%'
       AND table_name = p_table_name;
    l_col_name VARCHAR2(30);
  BEGIN
    fsc_utils.proc_start('s_dl_utils.get_batch_col');
    fsc_utils.debug_message( 's_dl_utils.get_batch_col',3 );

    OPEN c_get_Col(p_table_name);
    FETCH c_get_Col INTO l_col_name;
    CLOSE c_get_Col;

    fsc_utils.proc_end;
    RETURN l_col_name;

  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END get_batch_col;

  -----------------------------------------------------------------------------
  --  Returns the name of the column that holds the load status for the required table
  -----------------------------------------------------------------------------
  FUNCTION get_load_status_col(p_table_name IN VARCHAR2)
  RETURN VARCHAR2
  IS
    CURSOR c_get_col (cp_table_name IN VARCHAR2)
    IS
    SELECT column_name
      FROM all_tab_columns
     WHERE column_name LIKE '%LOAD_STATUS%'
       AND table_name = p_table_name;
    l_col_name VARCHAR2(30);
  BEGIN
    fsc_utils.proc_start('s_dl_utils.get_load_status_col');
    fsc_utils.debug_message( 's_dl_utils.get_load_status_col',3 );

    OPEN c_get_col(p_table_name);
    FETCH c_get_col INTO l_col_name;
    CLOSE c_get_col;

    fsc_utils.proc_end;
    RETURN l_col_name;

  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END get_load_status_col;

  -----------------------------------------------------------------------------
  --  Returns the name of the column that holds the seqno id for the required table
  -----------------------------------------------------------------------------
  FUNCTION get_seqno_col(p_table_name IN VARCHAR2)
  RETURN VARCHAR2
  IS
    CURSOR c_get_Col (cp_table_name IN VARCHAR2)
    IS
    SELECT column_name
      FROM all_tab_columns
     WHERE column_name like '%DL_SEQNO%'
       AND table_name = p_table_name;
    l_col_name VARCHAR2(30);
  BEGIN
    fsc_utils.proc_start('s_dl_utils.get_seqno_col');
    fsc_utils.debug_message( 's_dl_utils.get_seqno_col',3 );

    OPEN c_get_Col(p_table_name);
    FETCH c_get_Col INTO l_col_name;
    CLOSE c_get_Col;

    fsc_utils.proc_end;
    RETURN l_col_name;

  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END get_seqno_col;

  -----------------------------------------------------------------------------
  --  Returns the actual data for a given table, column and pk list
  -- Private function get data reads the actual data from the database
  -----------------------------------------------------------------------------
  FUNCTION get_data
    (p_table_name IN VARCHAR2,
	 p_column_name IN VARCHAR2,
     p_batch_id IN VARCHAR2,
     p_seqno IN NUMBER,
     p_datatype IN VARCHAR2,
     p_seqno_col IN VARCHAR2)
  RETURN VARCHAR2
  IS
    TYPE t_ref_Cursor IS REF CURSOR;
    l_rc t_ref_cursor;
    l_sql VARCHAR2(32000);
    l_col_value VARCHAR2(32000);
  BEGIN
    fsc_utils.proc_start('s_dl_utils.get_data');
    fsc_utils.debug_message( 's_dl_utils.get_data',3 );
    -- Start to build the sql to select the value
    l_sql := 'SELECT ';
    -- If the column is a date field, put a to_char on it.
    IF p_datatype = 'DATE'
    THEN
      l_sql := l_sql || 'TO_CHAR('||p_column_name||',''DD-MON-YYYY'' ) ';
    ELSE
      l_sql := l_sql || p_column_name||' ';
    END IF;
    -- add the from , table and where clauses.
    l_sql := l_sql ||' FROM '||p_table_name||' WHERE '||p_seqno_col||' = :SEQNO'||
      ' AND '||get_batch_col(p_table_name)||' = :BATCHID ';
    -- Retrieve the value.
    OPEN l_rc FOR l_sql USING p_seqno, p_batch_id;
    FETCH l_rc INTO l_col_value;
    CLOSE l_rc;

    fsc_utils.proc_end;

    RETURN l_Col_value;

  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;

  END get_data;

  -----------------------------------------------------------------------------
  --  Prepares the list of column data so that it can be selected later.
  -----------------------------------------------------------------------------
  PROCEDURE prepare_col_data (
    p_table_name IN VARCHAR2,
    p_batch_id IN VARCHAR2,
    p_seqno IN NUMBER)
  IS
     CURSOR c_table_cols(cp_table_name IN VARCHAR2,
                     cp_seqno_col IN VARCHAR2)
     IS
     SELECT column_name, data_type
       FROM all_tab_columns
      WHERE table_name = cp_table_name
        AND column_name != cp_seqno_col
      ORDER BY column_id;

     l_ptr NUMBER := 1;
     l_col_data VARCHAR2(32000);
     l_seqno_col VARCHAR2(30);
  BEGIN
     fsc_utils.proc_start('s_dl_utils.get_col_Data');
     fsc_utils.debug_message( 's_dl_utils.get_Col_data',3 );
     -- Empty/initialise the data table.
     l_data_tab := T_DL_DATA_STRUCTURE();
     -- Find the column name for the lookup column.
     l_seqno_col := get_seqno_col(p_table_name);
     -- Get the names of the columns in the required table.
     FOR l_loop IN c_table_cols(p_table_name, l_seqno_col)
	 LOOP
       l_data_tab.EXTEND;
       -- Now get the data for that column.
       l_col_data := get_data(p_table_name, l_loop.column_name, p_batch_id,p_seqno, l_loop.data_type,l_seqno_col);
       l_data_tab(l_ptr) := tp_dl_data_structure(l_loop.column_name, l_col_data);
       l_ptr := l_ptr + 1;

     END LOOP;
     fsc_utils.proc_end;

  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END prepare_Col_data;

  -----------------------------------------------------------------------------
  --  Returns the data stored in prepare_col_data
  -----------------------------------------------------------------------------
  FUNCTION get_col_data
  RETURN T_DL_DATA_STRUCTURE
  IS
  BEGIN
    fsc_utils.proc_start('s_dl_utils.get_col_Data');
    fsc_utils.debug_message( 's_dl_utils.get_Col_data',3 );
    fsc_utils.proc_end;

    RETURN l_data_tab;

  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;

  END get_col_data;

  -----------------------------------------------------------------------------
  --  Updates the status of the dataload table row to monitor its success/completion etc
  -----------------------------------------------------------------------------
 PROCEDURE set_record_status_flag(
    p_table_name IN VARCHAR2,
    p_batch_id IN VARCHAR2,
    p_seqno IN NUMBER,
    p_status IN VARCHAR2,
    p_seqno_col IN VARCHAR2 DEFAULT NULL,
    p_batch_col IN VARCHAR2 DEFAULT NULL,
    p_status_col IN VARCHAR2 DEFAULT NULL)
  IS
    -- PRAGMA AUTONOMOUS_TRANSACTION   ; -- Can lead to inconsistancies, so commented out
    l_seqno_col  VARCHAR2(30);
    l_batch_col  VARCHAR2(30);
    l_status_col VARCHAR2(30);
  BEGIN
    fsc_utils.proc_start('dataload_process.set_record_status_flag');
    fsc_utils.debug_message( 'dataload_process.set_record_status_flag',3 );


    IF p_seqno_col IS NULL
    THEN
      l_seqno_col := get_seqno_col(p_table_name);
    ELSE l_seqno_col := p_seqno_col;
    END IF;
    --
    IF p_batch_col IS NULL
    THEN
      l_batch_col := get_batch_col(p_table_name);
    ELSE l_batch_col := p_batch_col;
    END IF;
    --
    IF p_status_col IS NULL
    THEN
      l_status_col    := get_load_status_col(p_table_name);
    ELSE l_status_col := p_status_col;
    END IF;
    -- build an update statement for the selected table and row and execute
    -- Using bind variables will improve performance by reducing parse time.
    EXECUTE IMMEDIATE 'UPDATE '||p_table_name||
      ' SET '||l_status_col||' = :NEW_STATUS '||
      ' WHERE '||l_seqno_col||' = :SEQNO '||
      '   AND '||l_batch_col||' = :BATCHID '
      USING p_status, p_seqno, p_batch_id;
    -- COMMIT; -- commented out now that this is not autonomous
    fsc_utils.proc_end;
  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END set_record_status_flag;


  -----------------------------------------------------------------------------
  --  Updates the process summary
  -----------------------------------------------------------------------------
  PROCEDURE update_process_summary(
    p_batch_id IN VARCHAR2,
    p_process IN VARCHAR2,
    p_date IN DATE,
    p_status IN VARCHAR2)
  IS
  BEGIN  -- module_function
    fsc_utils.proc_start('dataload_process.update_process_summary');
    fsc_utils.debug_message( 'dataload_process.update_process_summary',3 );

    -- Pass the parameters to the process package.
    s_dl_process_summary.update_summary(p_batch_id, p_process, p_date, p_status);

    fsc_utils.proc_end;
  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END update_process_summary;

  -----------------------------------------------------------------------------
  --  Remove the selected batch from the system.
  -----------------------------------------------------------------------------
  PROCEDURE remove_batch( p_batch_id IN VARCHAR2 )
  IS
  BEGIN  -- module_function
    fsc_utils.proc_start('dataload_process.remove_batch');
    fsc_utils.debug_message( 'dataload_process.remove_batch',3 );

    s_dl_batches.delete_batch_process (p_batch_id);

    fsc_utils.proc_end;
  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END remove_batch;

  -----------------------------------------------------------------------------
  --  Returns a data structure that is built up of the columns of the table
  --  and the column contents for a given row.
  -----------------------------------------------------------------------------
  FUNCTION get_all_col_data (
    p_table_name IN VARCHAR2,
    p_batch_id IN VARCHAR2,
    p_seqno IN NUMBER)
  RETURN T_DL_DATA_STRUCTURE
  IS
    CURSOR c_table_cols(
	  cp_table_name IN VARCHAR2,
      cp_seqno_col IN VARCHAR2)
    IS
      SELECT column_name, data_type
        FROM all_tab_columns
       WHERE table_name = cp_table_name
         AND column_name != cp_seqno_col
         AND column_name NOT LIKE '%DLB_BATCH_ID'
         AND column_name NOT LIKE '%DL_LOAD_STATUS'
       ORDER BY column_id;

     l_ptr NUMBER := 1;
     l_col_data VARCHAR2(32000);
     l_seqno_Col VARCHAR2(30);

   BEGIN
     fsc_utils.proc_start('s_dl_utils.get_col_Data');
     fsc_utils.debug_message( 's_dl_utils.get_Col_data',3 );
     -- Empty/initialise the collection
     l_data_tab := T_DL_DATA_STRUCTURE();
     l_seqno_col := get_seqno_col(p_table_name);
     -- Get the names of the columns in the required table.
     FOR l_loop IN c_table_cols(p_table_name, l_seqno_col)
	 LOOP
       l_data_tab.EXTEND;
       -- Now get the data for that column.
       l_col_data := get_data(p_table_name, l_loop.column_name,p_batch_id, p_seqno, l_loop.data_type,l_seqno_col);
       l_data_tab(l_ptr) := tp_dl_data_structure(l_loop.column_name, l_col_data);
       l_ptr := l_ptr + 1;

     END LOOP;
     fsc_utils.proc_end;
     RETURN l_data_tab;
  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END get_all_col_data;

   ----------------------------------------------------------------------------
   --  Manages the task of starting jobs and processes, calls the create summary
   --  routine and sends back the finished signal when relevant.
   ----------------------------------------------------------------------------
   PROCEDURE run_process(
      p_sys_var1   IN VARCHAR2 DEFAULT NULL, -- p_app_area      IN VARCHAR2
      p_sys_var2   IN VARCHAR2 DEFAULT NULL, -- p_dataload_area IN VARCHAR2
      p_sys_var3   IN VARCHAR2 DEFAULT NULL, -- p_process       IN VARCHAR2
      p_sys_var4   IN VARCHAR2 DEFAULT NULL, -- p_date          IN DATE
      p_sys_var5   IN VARCHAR2 DEFAULT NULL,
      p_sys_var6   IN VARCHAR2 DEFAULT NULL,
      p_sys_var7   IN VARCHAR2 DEFAULT NULL,
      p_sys_var8   IN VARCHAR2 DEFAULT NULL,
      p_sys_var9   IN VARCHAR2 DEFAULT NULL,
      p_sys_var10  IN VARCHAR2 DEFAULT NULL,
      p_sys_var11  IN VARCHAR2 DEFAULT NULL,
      p_sys_var12  IN VARCHAR2 DEFAULT NULL,
      p_sys_var13  IN VARCHAR2 DEFAULT NULL,
      p_sys_var14  IN VARCHAR2 DEFAULT NULL,
      p_sys_var15  IN VARCHAR2 DEFAULT NULL,
      p_sys_var16  IN VARCHAR2 DEFAULT NULL,
      p_sys_var17  IN VARCHAR2 DEFAULT NULL,
      p_sys_var18  IN VARCHAR2 DEFAULT NULL, -- p_batch IN VARCHAR2
      p_session_id IN NUMBER,
      p_has_complex_parameters IN VARCHAR2,
      p_username   IN VARCHAR2 DEFAULT NULL)
   AS
      -- Variables
      l_answer VARCHAR2(100);
      
      -- Assign generic parameters to variables
      l_batch_id      VARCHAR2(30) := p_sys_var1;
      l_app_area      VARCHAR2(10) := p_sys_var2;
      l_dataload_area VARCHAR2(30) := p_sys_var3;
      l_process       VARCHAR2(30) := p_sys_var4;
      l_date          DATE;
      l_batch_mode    CHAR := p_sys_var18;
      l_table_name    VARCHAR2(30) := 'DL_'||l_app_area||'_'||l_dataload_area;
      l_package_name  VARCHAR2(30);

   BEGIN
      fsc_utils.proc_start('s_dl_utils.run_process '||l_process||' '||l_batch_id);
      fsc_utils.debug_message( 's_dl_utils.run_process'||l_process||' '||l_batch_id,3 );

      -- Retrieve the package name associated with the Product/Area
      l_package_name := s_dl_utils.get_package_name( l_app_area, l_dataload_area );
      fsc_utils.debug_message('l_package_name'||l_package_name,3 );      

    -- To run this process in batch, send the fin straight back to client, rest continues
    IF l_batch_mode = 'Y'
    THEN    
      -- Send the finished "question" back to the form so that it can resume.
      l_answer := s_question_manager.ask_question
        (p_question_id => 1,
         p_question_area => 'FIN',
         p_default => 'N',
         p_cancel => NULL,
         p_parameter1 => NULL,
         p_parameter2 => NULL,
         p_parameter3 => NULL,
         p_session_id => p_session_id);
    END IF;

    -- Create the process summary
    l_date := s_dl_process_summary.create_summary(l_batch_id, l_process, l_table_name );
    fsc_utils.debug_message('l_date'||l_date,3 );
        
    -- Now call the process passing batch, process and date
    fsc_utils.debug_message(
      'BEGIN s_dl_'||l_app_area||'_'||l_package_name||'.DATALOAD_'||l_process||'( '''||L_BATCH_ID||''' , '||
      ' TO_DATE( '''||TO_CHAR(l_date,'DD-MON-YYYY:HH24.MI.SS')||''' , ''DD-MON-YYYY:HH24.MI.SS'')); END; '
      , 4);

    -- nathan morgan 05-jul-2001 - unstub gpi
    -- only execute non load process as a dataload package on the server
    -- load process will fire a gpi job from the client trigger t_gpi_parameters
    fsc_utils.debug_message('l_process'||l_process,3 );    
    IF l_process != 'LOAD'
    THEN
      fsc_utils.debug_message('In before EXECUTE IMMEDIATE',3 );
          
      EXECUTE IMMEDIATE 'BEGIN s_dl_'||l_app_area||'_'||l_package_name||'.DATALOAD_'||
         l_process||'( '''||L_BATCH_ID||''', '||' TO_DATE( '''||TO_CHAR(l_date,'DD-MON-YYYY:HH24.MI.SS')||''' , ''DD-MON-YYYY:HH24.MI.SS'')); END; ';

      -- Set the process status to complete
      s_dl_process_summary.UPDATE_SUMMARY(l_batch_id, l_process, l_date, 'COMPLETE');

    END IF;

    -- If the end hasnt already been sent, send it now.
    IF l_batch_mode != 'Y'
    THEN
      -- Just in case of exceptions
      l_batch_mode := 'Y';
      
      -- Send the finished "question" back to the form so that it can resume.
      l_answer := s_question_manager.ask_question
        (p_question_id => 1,
        p_question_area => 'FIN',
        p_default => 'N',
        p_cancel => NULL,
        p_parameter1 => NULL,
        p_parameter2 => NULL,
        p_parameter3 => NULL,
        p_session_id => p_session_id);
    END IF;
    
    fsc_utils.debug_message('Run Process: END',3 );
    fsc_utils.proc_end;
   EXCEPTION
      WHEN OTHERS
      THEN         
         IF l_batch_mode != 'Y'
         THEN
            -- Send the finished "question" back to the form so that it can resume.
            l_answer := s_question_manager.ask_question(p_question_id => 1,
                                                        p_question_area => 'FIN',
                                                        p_default => 'N',
                                                        p_cancel => NULL,
                                                        p_parameter1 => NULL,
                                                        p_parameter2 => NULL,
                                                        p_parameter3 => NULL,
                                                        p_session_id => p_session_id);
         END IF;
         -- Set the process to failed
         s_dl_process_summary.UPDATE_SUMMARY(l_batch_id, l_process, l_date, 'FAILED','Y');
         fsc_utils.handle_exception;
         COMMIT;
   END run_process;

   -----------------------------------------------------------------------------
   --  Procedure to put the run_process task onto the PLSQL queue - allowing
   --  batch mode processes to be run.
   -----------------------------------------------------------------------------
   PROCEDURE queue_process(p_batch_id       IN VARCHAR2,
                           p_prod_area      IN VARCHAR2,
                           p_dload_area     IN VARCHAR2,
                           p_process        IN VARCHAR2,
                           p_batch_mode     IN VARCHAR2,
                           p_session_marker IN NUMBER,
                           p_debug_mode     IN VARCHAR2)
   IS
   BEGIN
      fsc_utils.proc_start('s_dl_utils.queue_process');
      fsc_utils.debug_message( 's_dl_utils.queue_process'||
         'p_batch_id = '||p_batch_id||
         ', p_prod_area = '||p_prod_area||
         ', p_dload_area = '||p_dload_area||
         ', p_process = '||p_process||
         ', p_batch_mode = '||p_batch_mode||
         ', p_session = '||p_session_marker,3);
            
      -- Pass all the parameters into the queue_manager routine.
      s_queue_manager.nq(p_type => 'PLSQL',
                         p_text => 'Call',
                         p_username => NVL(fsc_variables.username,USER),
                         p_datetime => (SYSDATE - 10),
                         p_session_marker => p_session_marker,
                         p_immediate => TRUE,
                         p_last_debug_text => p_debug_mode,
                         p_sys_var1 => p_batch_id,
                         p_sys_var2 => p_prod_area,
                         p_sys_var3 => p_dload_area,
                         p_sys_var4 => p_process,
                         p_sys_var18 => NVL(p_batch_mode,'N'),
                         p_extra1 => 's_dl_utils.RUN_PROCESS',
                         p_extra2 => 'N');

      fsc_utils.debug_message('s_dl_utils.queue_process : END',3 );
      fsc_utils.proc_end;
   EXCEPTION
      WHEN OTHERS
      THEN
         fsc_utils.handle_Exception;
   END queue_process;

  -----------------------------------------------------------------------------
  --  Updates the value stored for the given table, column and key value
  -----------------------------------------------------------------------------
  PROCEDURE update_col_value (
    p_table_name IN VARCHAR2,
    p_column_name IN VARCHAR2,
    p_new_val IN VARCHAR2,
    p_batch_id IN VARCHAR2,
    p_seqno IN NUMBER,
    p_date IN BOOLEAN)
  IS
    l_update_statement VARCHAR2(1000);
  BEGIN
    fsc_utils.proc_start('s_dl_utils.update_col_value');
    fsc_utils.debug_message( 's_dl_utils.update_Col_value',3 );
    -- Build update statement for the value
    l_update_statement := 'UPDATE '||p_table_name||' SET '||p_column_name ;
    -- Have to ensure dates are formatted correctly.
    IF p_date
    THEN
      l_update_statement := l_update_statement||' = TO_DATE(:newval,''DD-MON-YYYY'') ';
    ELSE
      l_update_statement := l_update_statement||' = :newval ';
    END IF;
    l_update_statement := l_update_statement||
      ' WHERE '||get_Seqno_coL(p_table_name)||' = :seqno '||
      '   AND '||get_batch_col(p_table_name)||' = :Batchid';
    -- Execute, passing in the parameters for this procedure.
    EXECUTE IMMEDIATE l_update_statement USING p_new_val, p_seqno, p_batch_id;

    fsc_utils.proc_end;
  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_Exception;
  END update_col_value;

  -----------------------------------------------------------------------------
  --  Used to ensure integrity - returns current value of column and locks the row
  --  this will fail if another process is updating - mod package should fail if values dont match
  -----------------------------------------------------------------------------
  FUNCTION get_data_and_lock(
    p_table_name IN VARCHAR2,
    p_col_name IN VARCHAR2,
    p_batch_id IN VARCHAR2,
    P_seqno IN VARCHAR2)
  RETURN VARCHAR2
  IS
    l_return VARCHAR2(400);
    TYPE T_RC IS REF CURSOR;
    RC T_RC;
    l_sql VARCHAR2(1000);
   BEGIN
    fsc_utils.proc_start('s_dl_utils.get_data_and_lock',
      p_table_name||' '||p_col_name||' '||p_batch_id||' '||p_seqno);
    fsc_utils.debug_message( 's_dl_utils.update_Col_value'||
      p_table_name||' '||p_col_name||' '||p_batch_id||' '||p_seqno,3 );
    -- Dynamic cursor creates the lock. NOWAIT prevents delay if row is currently locked
    l_sql :=  'SELECT '||p_col_name||
      '  FROM '||p_table_name||
      ' WHERE '||get_seqno_col(p_table_name)||' = '||p_seqno  ||
      '   AND '||get_batch_col(p_table_name)||' = '''||p_Batch_id ||''' ' ||
      '   FOR UPDATE OF '||p_col_name||' NOWAIT ';
    fsc_utils.debug_message('Opening SQL '||l_sql,4);
    OPEN rc FOR l_sql;
    FETCH rc INTO l_return ;
    CLOSE rc;
    fsc_utils.proc_end;
    RETURN l_return;

  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_Exception;
  END get_data_and_lock;

  -----------------------------------------------------------------------------
  --  Used to ensure integrity - returns current value of column and locks the row
  --  this will fail if another process is updating - mod package should fail
  --  if values dont match
  -----------------------------------------------------------------------------
  PROCEDURE delete_loaded_data(
    p_table_name IN VARCHAR2,
    p_batch_id IN VARCHAR2)
  IS
    l_return VARCHAR2(400);
    l_count NUMBER := 1; -- This is used to make sure the whole thing doesnt lock up
  BEGIN
    fsc_utils.proc_start('s_dl_utils.delete_loaded_area');
    fsc_utils.debug_message( 's_dl_utils.delete_loaded_area',3 );

    LOOP
      l_count := l_count + 1;
      EXIT WHEN l_count > 150;
      BEGIN
        fsc_utils.debug_message('DELETE FROM '||p_table_name||
          ' WHERE '||get_batch_col(p_table_name)||' = :BATCHID',4);
        EXECUTE IMMEDIATE 'DELETE FROM '||p_table_name||
          ' WHERE '||get_batch_col(p_table_name)||' = :BATCHID'
          USING p_batch_id;
        EXIT WHEN SQL%ROWCOUNT=0;
      EXCEPTION
      WHEN OTHERS THEN
        -- This should only occur with out of rollback type errors.
        -- In which case the delete is executed again.
        COMMIT;
      END;
    END LOOP;
    fsc_utils.proc_end;
  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_Exception;
  END delete_loaded_data;

  -----------------------------------------------------------------------------
  --  Returns the name of the package associated with a Load Product/Area.
  --  Required because of package name length limitation
  -----------------------------------------------------------------------------
  FUNCTION get_package_name(
    p_product_area IN VARCHAR2,
    p_dataload_area IN VARCHAR2 )
  RETURN VARCHAR2
  IS
    CURSOR c_get_package_name
    IS
      SELECT NVL( dla_package_name, dla_dataload_area)
        FROM dl_load_areas
       WHERE dla_product_area  = p_product_area
         AND dla_dataload_area = p_dataload_area;
    l_package_name VARCHAR2(30);
  BEGIN
    fsc_utils.proc_start('s_dl_utils.get_package_name');
    fsc_utils.debug_message( 's_dl_utils.get_package_name',3 );

    OPEN c_get_package_name;
    FETCH c_get_package_name INTO l_package_name;
    CLOSE c_get_package_name;

    fsc_utils.proc_end;
    RETURN l_package_name;

  EXCEPTION
    WHEN OTHERS THEN
      fsc_utils.handle_exception;
  END get_package_name;

  -----------------------------------------------------------------------------
  --  Returns a query in a string, for use in validation against a column domain
                --

  -----------------------------------------------------------------------------
  FUNCTION f_length_long (
    p_table_name IN VARCHAR2,
    p_column_name   IN VARCHAR2 ) RETURN VARCHAR2
  IS
    l_cursor_id   INTEGER;
    l_select_statement   VARCHAR2(500);
    l_dummy       INTEGER;
    l_dummy2      NUMBER;
    l_total_length  NUMBER(20) := 0;
    l_blocksize     NUMBER(10) := 32767;
    l_start_pos     NUMBER(10) := 0;
    l_output_length NUMBER(10) := 0;
    l_length        VARCHAR2(32767) := 'ABC';
    l_quote varchar2(1) := chr(39);
    l_check NUMBER;
    BEGIN
    l_cursor_id := dbms_sql.open_cursor;

    -- The following line defines the SELECT of the LONG column.
    -- Add a WHERE clause to narrow down the particular row.
    l_select_statement :=
      'SELECT search_condition from all_constraints a, all_cons_columns b WHERE a.constraint_name = b.constraint_name AN
D a.constraint_type = ''C'' '||

      ' AND b.column_name =  '||l_quote||p_column_name||l_quote||'  AND a.table_name = '||l_quote||p_table_name||l_quote
||

      ' AND a.owner = b.owner AND a.owner IN (SELECT ''FSC'' FROM dual UNION SELECT owner FROM all_objects WHERE object_
name = '||l_quote||p_table_name||l_quote||' AND owner NOT IN (''SYSTEM'',''SYS'')) AND ROWNUM = 1';

    dbms_sql.parse(l_cursor_id, l_select_statement, dbms_sql.native);

    dbms_sql.define_column_long(l_cursor_id, 1);
    l_dummy := dbms_sql.execute(l_cursor_id);

    LOOP
      l_dummy2 := dbms_sql.fetch_rows(l_cursor_id);
      --
      IF l_dummy2 = 0
      THEN
        dbms_sql.close_CURSOR(l_cursor_id);
        EXIT;
      END IF;

      dbms_sql.column_value_long(l_cursor_id, 1, l_blocksize, l_start_pos, l_length,l_output_length);
      l_check := INSTR(l_length,'IS NOT NULL');
      IF l_check = 0
      THEN
        dbms_sql.close_CURSOR(l_cursor_id);
        EXIT;
      END IF;

    END LOOP;
    RETURN (l_length);

  END f_length_long;

  -----------------------------------------------------------------------------
  --  Generic routine to ensure the supplied value satisfies the columns' domain.
  -----------------------------------------------------------------------------
  FUNCTION check_column_domain
    (p_table_name  IN VARCHAR2,
     p_column_name IN VARCHAR2,
     p_value       IN VARCHAR2)
  RETURN BOOLEAN
  IS
    l_value     VARCHAR2(32767);
    l_exists    NUMBER ;
    l_result    BOOLEAN := FALSE;
    l_SELECT    VARCHAR2(32767);
    l_quote varchar2(1) := chr(39);
  BEGIN
    --
    l_value :=   f_length_long(p_table_name, p_column_name ) ;
    l_SELECT := 'SELECT 1 FROM DUAL WHERE '||
         REPLACE(l_value, p_column_name, l_quote||p_value||l_quote);
    --
   BEGIN
     EXECUTE IMMEDIATE l_select INTO l_exists;
     l_result := TRUE;
   EXCEPTION
     WHEN no_data_found
     THEN
       l_result := FALSE;
   END;

   RETURN (l_result );

  EXCEPTION
    WHEN Others THEN
      fsc_utils.handle_exception;
  END check_column_domain;


  --  DESCRIPTION:
  --  Transfers any error messages from MESSAGES to DL_ERRORS.
  --  This is to allow use of API calls that queue messages using standard
  --  error handling, rather than S_DL_ERRORS.
  FUNCTION transfer_messages (
    p_batch_id        IN VARCHAR2,
    P_process         IN VARCHAR2,
    p_date            IN DATE,
    p_table_name      IN VARCHAR2,
    p_dl_seqno        IN VARCHAR2,
    p_err_field       IN VARCHAR2,
    p_last_msg        IN OUT NUMBER)
 RETURN VARCHAR2
 IS
   PRAGMA AUTONOMOUS_TRANSACTION;
   CURSOR C1 (p_sess_id IN NUMBER)
   IS
    SELECT msg_no, area_code, error_no, text FROM messages
     WHERE SESSION_MARKER = p_sess_id
       AND error_no IS NOT NULL
       AND (p_last_msg IS NULL OR p_last_msg < msg_no)
       FOR UPDATE;
   l_dummy varchar2(1);
   l_last_error messages.msg_no%type;
   l_err_field VARCHAR2(200);
   l_err_seq NUMBER;

  BEGIN
    fsc_utils.proc_start('dl_utils.transfer_messages');
    fsc_utils.debug_message( 'dl_utils.transfer_messages',3 );
    FOR p1 IN c1(fsc_variables.session_id())
    LOOP
      l_last_error := p1.msg_no;
      IF p_err_field IS NOT NULL THEN
        l_err_field := p1.text || ' (' || p_err_field||')';
      ELSE
        l_err_field := p1.text;
      END IF;
      -- use DLE_ERR_REFNO as a sequence no rather than error no to get
      -- around unique constraint on DL_ERRORS.
     SELECT NVL(MAX(dle_err_refno),1)
	   INTO l_err_seq
	   FROM dl_errors er
      WHERE er.DLE_DPS_DLB_BATCH_ID = p_batch_id
        AND er.DLE_DPS_PROCESS   = p_process
        AND er.DLE_DPS_DATE   = p_date
        AND er.DLE_TABLE_NAME   = p_table_name
        AND er.DLE_DL_SEQNO   = p_dl_seqno;

     l_dummy:=s_dl_errors.record_error
       (p_batch_id
       ,p_process
       ,p_date
       ,p_table_name
       ,p_dl_seqno
       ,p1.area_code
       ,l_err_seq
       ,l_err_field);

     DELETE FROM messages WHERE CURRENT of c1;
     COMMIT; -- because s_dl_errors.record_error does an autonomous commit to dl_errors.
   END LOOP;
   fsc_utils.proc_end;
   RETURN 'F';
  EXCEPTION
   WHEN OTHERS THEN
     fsc_utils.debug_message( SQLERRM,3 );
     fsc_utils.handle_exception;
     -- Return marker for Oracle Error.
     RETURN 'O';
  END transfer_messages;

END s_dl_utils;
/

