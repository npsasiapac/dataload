set serveroutput on 
declare
tname varchar2(100);
ct number :=0;

begin
     for i in (select owner||'.'||object_name object_name,object_type from all_objects
     where SUBSTR(object_name,1,3) = 'DL_'
     and object_type NOT IN ('SYNONYM','INDEXES')
     AND object_name NOT IN ('DL_BATCHES','DL_ERRORS','DL_LOAD_AREAS','DL_PROCESS_SUMMARY','DL_UTILS')
     and object_type != 'PACKAGE BODY'
     order by object_type,owner||'.'||object_name)
--
     loop
        execute immediate 'drop '||i.object_type||' '|| i.object_name;
        tname := i.object_name;
        ct := ct + 1;
     dbms_output.put_line('drop '||i.object_type||' '|| i.object_name);
     end loop;
  dbms_output.put_line('Objects dropped: '|| ct);

     for i in (select object_name object_name,owner,object_type from all_objects
     where SUBSTR(object_name,1,3) = 'DL_'
     and object_type = 'SYNONYM'
     AND object_name NOT IN ('DL_BATCHES','DL_ERRORS','DL_LOAD_AREAS','DL_PROCESS_SUMMARY','DL_UTILS')
     and object_type != 'PACKAGE BODY'
     order by object_type,owner||'.'||object_name)
--
     loop
        execute immediate 'drop '||i.owner||' '||i.object_type||' '||i.object_name;
        tname := i.object_name;
        ct := ct + 1;
     dbms_output.put_line('drop '||i.owner||' '||i.object_type||' '||i.object_name);
     end loop;
  dbms_output.put_line('Synonyms dropped: '|| ct);

     for i in (select owner||'.'||object_name object_name,object_type from all_objects
     where SUBSTR(object_name,1,5) = 'S_DL_'
     and object_type NOT IN ('SYNONYM','INDEXES')
     AND object_name NOT IN ('S_DL_BATCHES','S_DL_ERRORS','S_DL_PROCESS_SUMMARY','S_DL_UTILS')
     and object_type != 'PACKAGE BODY'
     order by object_type,owner||'.'||object_name)
--
     loop
        execute immediate 'drop '||i.object_type||' '|| i.object_name;
        tname := i.object_name;
        ct := ct + 1;
     dbms_output.put_line('drop '||i.object_type||' '|| i.object_name);
     end loop;
  dbms_output.put_line('Packages dropped: '|| ct);

     for i in (select object_name object_name,owner,object_type from all_objects
     where SUBSTR(object_name,1,5) = 'S_DL_'
     and object_type = 'SYNONYM'
     AND object_name NOT IN ('S_DL_BATCHES','S_DL_ERRORS','S_DL_PROCESS_SUMMARY','S_DL_UTILS')
     and object_type != 'PACKAGE BODY'
     order by object_type,owner||'.'||object_name)
--
     loop
        execute immediate 'drop '||i.owner||' '||i.object_type||' '||i.object_name;
        tname := i.object_name;
        ct := ct + 1;
     dbms_output.put_line('drop '||i.owner||' '||i.object_type||' '||i.object_name);
     end loop;
  dbms_output.put_line('Synonyms dropped: '|| ct);

--
exception when others then
  dbms_output.put_line('Error on object :  ' || tname || sqlerrm || sqlcode);
end;
/
