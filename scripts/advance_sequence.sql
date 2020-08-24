SET SERVEROUTPUT ON
-- Parameters
--  1 sequence name
--  2 Table it populates
--  3 Primary key column
--  4 Value to increment to

DECLARE

   v_new_seq     NUMBER;
   v_cur_max_seq NUMBER;
   cur           INTEGER := DBMS_SQL.OPEN_CURSOR;
   fdbk          INTEGER;
   v_tmp         NUMBER;
   v_org_val     NUMBER;
   v_seq_inc_to  NUMBER;

   TYPE tr_curs IS REF CURSOR;
   r_curs tr_curs;

BEGIN

   dbms_output.put_line('sequence name = '||'&1');
   dbms_output.put_line('Table it populates = '||'&2');
   dbms_output.put_line('Primary key colum = '||'&3');
   dbms_output.put_line('Value to increment to = '||'&4');

   -- see what sequence is currently set to
   OPEN  r_curs FOR 'SELECT &1..NEXTVAL FROM DUAL';
   FETCH r_curs INTO v_tmp;
   CLOSE r_curs;
   
   OPEN  r_curs FOR 'SELECT &4 FROM DUAL';
   FETCH r_curs INTO v_seq_inc_to;
   CLOSE r_curs;

   IF v_tmp != 1 -- if 1 means sequence has never been used, can alter sequence with a zero
   THEN
      -- take 1 off and turn into a negative number. Need to take one off otherwise
      -- the sequence will get reset to zero

      dbms_output.put_line('sequence nextval = '||v_tmp);

      v_tmp := (v_tmp - 1) *-1;

      dbms_output.put_line('v_tmp = '||v_tmp);

      -- now alter the sequence back to 1. v_tmp is set to the current sequence value
      -- less 1 multiplied by -1 so the sequence goes backwards.
      -- Cannot use START WITH on an alter command and dont want to drop sequence
      -- else will lose grants and synonyms.
      DBMS_SQL.PARSE (cur,
                     'ALTER SEQUENCE &1 INCREMENT BY '||v_tmp,
                     DBMS_SQL.native);
      fdbk := DBMS_SQL.EXECUTE (cur);

      -- now take the next value from the sequence to reset back to 1.
      OPEN  r_curs FOR 'SELECT &1..NEXTVAL FROM DUAL';
      FETCH r_curs INTO v_tmp;
      CLOSE r_curs;
   END IF;

   -- now see what the maximum value is on the table
   OPEN  r_curs FOR 'SELECT NVL(MAX(TO_NUMBER(&3)),0) FROM &2 WHERE rtrim(&3,''1234567890'') IS NULL';
   FETCH r_curs INTO v_cur_max_seq;
   CLOSE r_curs;
   
   OPEN  r_curs FOR 'SELECT TO_NUMBER(&4) FROM DUAL';
   FETCH r_curs INTO v_seq_inc_to;
   CLOSE r_curs;

   dbms_output.put_line('v_cur_max_seq = '||v_cur_max_seq);

   IF v_cur_max_seq > 0 -- No records in table would return a 0
   THEN
      IF v_seq_inc_to > 0
      THEN
         v_new_seq := v_seq_inc_to - 1;
      ELSE
         v_new_seq := v_cur_max_seq - 1;
      END IF;
      
      v_tmp     := 0;

      dbms_output.put_line('v_new_seq = '||v_new_seq);

      -- alter sequence and advance to maximum value plus 1
      DBMS_SQL.PARSE (cur,
                     'ALTER SEQUENCE &1 INCREMENT BY '||v_new_seq,
                     DBMS_SQL.native);
      fdbk := DBMS_SQL.EXECUTE (cur);

      -- select next value to advance sequence
      OPEN  r_curs FOR 'SELECT &1..NEXTVAL FROM DUAL';
      FETCH r_curs INTO v_tmp;
      CLOSE r_curs;

      dbms_output.put_line('v_tmp = '||v_tmp);
   ELSIF v_seq_inc_to > 0
   THEN
      v_new_seq := v_seq_inc_to - 1;
      v_tmp     := 0;

      dbms_output.put_line('v_new_seq = '||v_new_seq);

      -- alter sequence and advance to maximum value plus 1
      DBMS_SQL.PARSE (cur,
                     'ALTER SEQUENCE &1 INCREMENT BY '|| v_new_seq,
                     DBMS_SQL.native);
      fdbk := DBMS_SQL.EXECUTE (cur);

      -- select next value to advance sequence
      OPEN  r_curs FOR 'SELECT &1..NEXTVAL FROM DUAL';
      FETCH r_curs INTO v_tmp;
      CLOSE r_curs;

      dbms_output.put_line('v_tmp = '||v_tmp);
   ELSE
      dbms_output.put_line('No records in table '||'&2');
   END IF;

   v_org_val := 1; 

   -- reset sequence back to normal increment of 1
   DBMS_SQL.PARSE (cur,
                   'ALTER SEQUENCE &1 INCREMENT BY '||v_org_val,
                   DBMS_SQL.native);
   fdbk := DBMS_SQL.EXECUTE (cur);

   -- reset cache back to nocache
   DBMS_SQL.PARSE (cur,
                   'ALTER SEQUENCE &1 NOCACHE',
                   DBMS_SQL.native);
   fdbk := DBMS_SQL.EXECUTE (cur);

END;
/

