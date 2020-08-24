CREATE OR REPLACE PACKAGE BODY s_dl_hem_admin_groupings
AS
-- ***********************************************************************

  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER  DB Ver WHO  WHEN       WHY
  --  1.0         MTR  23/11/00   Dataload
  --  1.1  5.1.4  PJD  27/02/02   Added extra exeption handler as
  --                              a temporary measure.
  --  2.0  5.2.0  PJD  04/11/02   Correction to cb variable in
  --                              delete procedure
  --  2.1         SB   14/11/02   Correction to error codes
  --  3.0  5.5.0  MH   09/03/04   Added loop to create that makes
  --                              sure all links are in place
  --                              when diamond structures are
  --                              present in au hierarchy
  --  3.1  5.5.0  PJD  05/05/04   Removed extra declaration of i that had
  --                              somehow appeared
  --  3.2  5.5.0  PH   22/07/04   Amended exception Handlers
  --  3.3  5.8.0  PJD  18/01/06   Changed create proc to use own code rather
  --                              than std product procs
  --  3.4  5.8.0  PJD  24/06/06   Changed to remove intermittant oracle error
  --                              in create procedure
  --  3.4  5.10.0 PJD  20/11/06   Added e_agr_exception into the create process
  --                              Improved the delete process to be more 
  --                              exact about which records are deleted.
  --  4.0  5.13.0 PH   06-FEB-2008 Now includes its own 
  --                               set_record_status_flag procedure.
  --
  --
  --  declare package variables AND constants
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
  UPDATE dl_hem_admin_groupings
  SET lagr_dl_load_status = p_status
  WHERE rowid = p_rowid;
  --
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line('Error updating status of dl_hem_admin_groupings');
     RAISE;
  --
END set_record_status_flag;
--
-- ***********************************************************************
--
--
PROCEDURE dataload_create
(p_batch_id          IN VARCHAR2,
 p_date              IN DATE)
AS
--
CURSOR c1(p_batch_id VARCHAR2) is
SELECT
  rowid rec_rowid,
  lagr_dlb_batch_id,
  lagr_dl_seqno,
  lagr_dl_load_status
  ,lagr_aun_code_parent
  ,lagr_aun_code_child
FROM dl_hem_admin_groupings
WHERE lagr_dlb_batch_id    = p_batch_id
AND   lagr_dl_load_status = 'V'
;
--
--
CURSOR c_par (p_child VARCHAR2) IS
SELECT
DISTINCT
agr_aun_code_parent
,agr_auy_code_parent
FROM admin_groupings
WHERE agr_aun_code_child in
               (SELECT a2.agr_aun_code_parent
                  FROM admin_groupings a2
                 WHERE a2.agr_aun_code_child
                          = p_child)
;
--
CURSOR c_child (p_child VARCHAR2) is
SELECT
 agr_aun_code_parent
,agr_aun_code_child
,agr_auy_code_parent
,agr_auy_code_child
,rowid agr_rowid
FROM   admin_groupings
WHERE  agr_aun_code_parent = p_child
;
--
CURSOR c_check_agr(p_aun_code  VARCHAR2) IS
SELECT 'X'
FROM   dual
WHERE  EXISTS (SELECT null
              FROM   admin_groupings                  
              WHERE  agr_aun_code_child  = p_aun_code  
              GROUP BY agr_auy_code_parent
              HAVING count(*) > 1);
--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'CREATE';
ct       VARCHAR2(30) := 'DL_HEM_ADMIN_GROUPINGS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
-- Other variables
--
l_an_tab VARCHAR2(1);
i        INTEGER :=0;
l_exists    VARCHAR2(1);
--
e_dup_agr   EXCEPTION;
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hem_admin_groupings.dataload_create');
  fsc_utils.debug_message( 's_dl_hem_admin_groupings.dataload_create',3);
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1(p_batch_id) LOOP
    --
    BEGIN
    --
    cs := p1.lagr_dl_seqno;
    l_id := p1.rec_rowid;
    --
    SAVEPOINT SP1;
    --
    INSERT INTO
    admin_groupings(agr_aun_code_parent
                    ,agr_aun_code_child
                    ,agr_direct_link
                    ,agr_auy_code_parent
                    ,agr_auy_code_child
                    ,agr_created_by
                    ,agr_created_date
                    )
    VALUES          (
                    p1.lagr_aun_code_parent
                    ,p1.lagr_aun_code_child
                    ,'Y'
                    ,s_admin_units.get_aun_auy_code(p1.lagr_aun_code_parent)
                    ,s_admin_units.get_aun_auy_code(p1.lagr_aun_code_child)
                    ,'DATALOAD'
                    ,trunc(sysdate)
                    );
    --
    DELETE FROM admin_groupings
    WHERE agr_aun_code_child  = p1.lagr_aun_code_child
      AND agr_direct_link     = 'N';
    --
    FOR p3 IN c_par (p1.lagr_aun_code_child) LOOP
      --
      -- dbms_output.put_line('inserting '||p3.agr_aun_code_parent||
      --                 ' '||p1.agr_aun_code_child);
      --
      DELETE FROM admin_groupings
      WHERE agr_aun_code_child  = p1.lagr_aun_code_child
        AND agr_aun_code_parent = p3.agr_aun_code_parent;
      --
      INSERT INTO
      admin_groupings(agr_aun_code_parent
                      ,agr_aun_code_child
                      ,agr_direct_link
                      ,agr_auy_code_parent
                      ,agr_auy_code_child
                      ,agr_created_by
                      ,agr_created_date
                      )
      VALUES          (
                       p3.agr_aun_code_parent
                      ,p1.lagr_aun_code_child
                      ,'N'
                      ,p3.agr_auy_code_parent
                      ,s_admin_units.get_aun_auy_code(p1.lagr_aun_code_child)
                      ,'DATALOAD'
                      ,trunc(sysdate)
                      );
      --
    END LOOP;
    --
    -- delete any direct links that are no longer needed
    --
    -- DELETE FROM admin_groupings ag1
    -- WHERE ag1.agr_aun_code_child  = p1.lagr_aun_code_child
    --  AND ag1.agr_direct_link     = 'Y'
    --  AND EXISTS (SELECT NULL FROM admin_groupings ag2
    --              WHERE  ag2.agr_aun_code_child  = ag1.agr_aun_code_child
    --                AND  ag2.agr_aun_code_parent = ag1.agr_aun_code_parent
    --                AND  ag2.agr_direct_link = 'N');
    --
    -- dbms_output.put_line('Deleting parent child relationships for '||
    --                p2.agr_aun_code_parent||' '||p2.agr_aun_code_child);
    --
    --
    --
    FOR p2 IN c_child(p1.lagr_aun_code_child) LOOP
      --
      -- delete any links that are no longer needed
      --
      --
      DELETE FROM admin_groupings ag1
      WHERE ag1.agr_aun_code_child  = p2.agr_aun_code_child
        AND EXISTS (SELECT NULL FROM admin_groupings ag2
                    WHERE  ag2.agr_aun_code_child  = p2.agr_aun_code_parent
                      AND  ag2.agr_aun_code_parent = ag1.agr_aun_code_parent
                   );
      --
      --
      INSERT INTO
      admin_groupings(agr_aun_code_parent
                      ,agr_aun_code_child
                      ,agr_direct_link
                      ,agr_auy_code_parent
                      ,agr_auy_code_child
                      ,agr_created_by
                      ,agr_created_date
                      )
      SELECT DISTINCT
                       agr_aun_code_parent
                      ,p2.agr_aun_code_child
                      ,'N'
                      ,agr_auy_code_parent
                      ,p2.agr_auy_code_child
                      ,'DATALOAD'
                      ,trunc(sysdate)
                      FROM admin_groupings
                      WHERE agr_aun_code_child = p2.agr_aun_code_parent;
                      --        (SELECT a2.agr_aun_code_parent
                      --         FROM admin_groupings a2
                      --         WHERE a2.agr_aun_code_child
                      --                       = p2.agr_aun_code_parent);

    END LOOP; -- p2
    --
    -- keep a count of the rows processed and commit after every 1000
    --
    --
    -- add a bit of code to check if this new adin units is now linked to more than
    -- one admin unit of any type.
    --
    l_exists := NULL;
    --
    OPEN   c_check_agr(p1.lagr_aun_code_child);   
    FETCH  c_check_agr INTO l_exists;
    CLOSE  c_check_agr;
    --
    IF   l_exists IS NOT NULL
    THEN 
      ROLLBACK TO SP1;
      RAISE e_dup_agr;
    END IF;
    --
    i := i+1; IF MOD(i,1000)=0 THEN COMMIT; END IF;
    --
    s_dl_process_summary.update_processed_count(cb,cp,cd,'N');
    set_record_status_flag(l_id,'C');
    --
    EXCEPTION
    WHEN e_dup_agr THEN
   ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA','20000','DUPLICATE VALUE IN ADMIN GROUPINGS VIEW');
   set_record_status_flag(l_id,'O');
   s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
   --
   WHEN OTHERS THEN
      ROLLBACK TO SP1;
      ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
      set_record_status_flag(l_id,'O');
      s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
    END;
  END LOOP;
  commit;
  --
  --
  -- Section to anayze the table(s) populated by this dataload
  --
  l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADMIN_GROUPINGS');
  --
  fsc_utils.proc_end;
  commit;
  --
EXCEPTION
  WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    set_record_status_flag(l_id,'O');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END dataload_create;
--
--
PROCEDURE dataload_validate
     (p_batch_id          IN VARCHAR2,
      p_date              IN DATE)
AS
--
CURSOR c1 is
SELECT
  rowid rec_rowid,
  lagr_dlb_batch_id,
  lagr_dl_seqno,
  lagr_dl_load_status
  ,lagr_aun_code_parent
  ,lagr_aun_code_child
FROM dl_hem_admin_groupings
WHERE lagr_dlb_batch_id      = p_batch_id
AND   lagr_dl_load_status       in ('L','F','O');
--
CURSOR c_link1
  (p_lagr_aun_code_parent IN dl_hem_admin_groupings.lagr_aun_code_parent%TYPE,
   p_lagr_aun_code_child IN dl_hem_admin_groupings.lagr_aun_code_child%TYPE )
IS
 SELECT 'X'
   FROM admin_groupings
  WHERE agr_aun_code_parent = p_lagr_aun_code_parent
    AND agr_aun_code_child  = p_lagr_aun_code_child;
--  UNION
-- SELECT 'X'
--   FROM dl_hem_admin_groupings
--  WHERE lagr_aun_code_parent = p_lagr_aun_code_parent
--    AND lagr_aun_code_child  = p_lagr_aun_code_child
--    AND lagr_dl_load_status  = 'V';
--
CURSOR c_link2
  (p_lagr_aun_code_parent IN dl_hem_admin_groupings.lagr_aun_code_parent%TYPE,
   p_lagr_aun_code_child IN dl_hem_admin_groupings.lagr_aun_code_child%TYPE )
IS
 SELECT 'X'
   FROM admin_groupings
  WHERE agr_aun_code_parent = p_lagr_aun_code_child
    AND agr_aun_code_child  = p_lagr_aun_code_parent;
--  UNION
-- SELECT 'X'
--   FROM dl_hem_admin_groupings
--  WHERE lagr_aun_code_parent = p_lagr_aun_code_child
--    AND lagr_aun_code_child  = p_lagr_aun_code_parent
--    AND lagr_dl_load_status  = 'V';
--
CURSOR c_parent_type
  (p_lagr_aun_code_parent IN dl_hem_admin_groupings.lagr_aun_code_parent%TYPE,
   p_lagr_aun_code_child IN dl_hem_admin_groupings.lagr_aun_code_child%TYPE )
IS
  SELECT 'x'
    FROM admin_groupings
   WHERE agr_aun_code_child  = p_lagr_aun_code_child
     AND agr_auy_code_parent =  (SELECT aun_auy_code
                                   FROM admin_units
                                  WHERE aun_code = p_lagr_aun_code_parent);
--   UNION
--  SELECT 'x'
--    FROM dl_hem_admin_groupings ,admin_units
--   WHERE lagr_aun_code_parent = aun_code
--     AND lagr_aun_code_child = p_lagr_aun_code_child
--     AND lagr_dl_load_status ='V'
--     AND aun_auy_code =  (SELECT aun_auy_code
--                            FROM admin_units
--                           WHERE aun_code = p_lagr_aun_code_parent);
--
CURSOR c_grandchild
  (p_lagr_aun_code_parent IN dl_hem_admin_groupings.lagr_aun_code_parent%TYPE,
   p_lagr_aun_code_child IN dl_hem_admin_groupings.lagr_aun_code_child%TYPE )
IS
  SELECT 'x'
    FROM admin_groupings
   WHERE agr_aun_code_parent = p_lagr_aun_code_parent
     AND  agr_aun_code_child in (SELECT agr_aun_code_parent
                                 FROM admin_groupings
                                 WHERE agr_aun_code_child =
                                                   p_lagr_aun_code_child);
--   UNION
--  SELECT 'x'
--    FROM dl_hem_admin_groupings
--   WHERE lagr_aun_code_parent = p_lagr_aun_code_parent
--     AND lagr_aun_code_child in (SELECT lagr_aun_code_parent
--                                 FROM dl_hem_admin_groupings
--                                 WHERE lagr_aun_code_child =
--                                                     p_lagr_aun_code_child
--                                 AND lagr_dl_load_status ='V');

--
-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'VALIDATE';
ct       VARCHAR2(30) := 'DL_HEM_ADMIN_GROUPINGS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
l_errors         VARCHAR2(10);
l_error_ind      VARCHAR2(10);
i                INTEGER :=0;
l_link1          VARCHAR2(1);
l_link2          VARCHAR2(1);
l_parent_type    VARCHAR2(1);
l_grandchild     VARCHAR2(1);
--
-- Other variables
--
--
BEGIN
  --
  fsc_utils.proc_start('s_dl_hem_admin_groupings.dataload_validate');
  fsc_utils.debug_message( 's_dl_hem_admin_groupings.dataload_validate',3);
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
    cs := p1.lagr_dl_seqno;
    l_id := p1.rec_rowid;
    --
    l_errors := 'V';
    l_error_ind := 'N';

    -- Check Parent Admin Unit exists
    IF NOT( s_admin_units.is_current_admin_unit( p1.lagr_aun_code_parent ) )
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',847);
    END IF;

    -- Check Child Admin Unit exists
    IF NOT( s_admin_units.is_admin_unit( p1.lagr_aun_code_child ) )
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',848);
    ELSE
      -- Check Parent/Child link does not already exist
      l_link1:= NULL;
      OPEN  c_link1(p1.lagr_aun_code_parent, p1.lagr_aun_code_child);
      FETCH c_link1 INTO l_link1;
      IF l_link1 IS NOT NULL
      THEN
        l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',849);
      END IF;
      CLOSE c_link1;
    END IF;
    -- Check Child/Parent link does not already exist
    l_link2:= NULL;
    OPEN  c_link2(p1.lagr_aun_code_parent, p1.lagr_aun_code_child);
    FETCH c_link2 INTO l_link2;
    IF l_link2 IS NOT NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',849);
    END IF;
    CLOSE c_link2;

    -- An Admin Unit may not link to itself
    IF p1.lagr_aun_code_parent = p1.lagr_aun_code_child
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',850);
    END IF;

    -- An Admin Unit may not be a child of an Admin Unit of the same Admin Unit Type Code
    l_parent_type := NULL;
    OPEN  c_parent_type(p1.lagr_aun_code_parent, p1.lagr_aun_code_child);
    FETCH c_parent_type INTO l_parent_type;
    IF l_parent_type IS NOT NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',851);
    END IF;
    CLOSE c_parent_type;

    -- Check potential link is not already a Grandchild
    l_grandchild := NULL;
    OPEN  c_grandchild(p1.lagr_aun_code_parent, p1.lagr_aun_code_child);
    FETCH c_grandchild INTO l_grandchild;
    IF l_grandchild IS NOT NULL
    THEN
      l_errors:=s_dl_errors.record_error(cb,cp,cd,ct,cs,'HDL',852);
    END IF;
    CLOSE c_grandchild;

    --
-- Now UPDATE the record count and error code
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
--
COMMIT;
--
fsc_utils.proc_END;
--
   EXCEPTION
      WHEN OTHERS THEN
    ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE,SQLERRM);
    s_dl_utils.set_record_status_flag(ct,cb,cs,'O');
    s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
END dataload_validate;
--
PROCEDURE dataload_delete (p_batch_id        IN VARCHAR2,
                           p_date            IN DATE) IS
--
CURSOR c1 is
SELECT
  rowid rec_rowid
  ,lagr_dlb_batch_id
  ,lagr_dl_seqno
  ,lagr_aun_code_parent
  ,lagr_aun_code_child
FROM  dl_hem_admin_groupings
WHERE lagr_dlb_batch_id   = p_batch_id
AND   lagr_dl_load_status = 'C'
;

-- Constants for process_summary
cb       VARCHAR2(30);
cd       DATE;
cp       VARCHAR2(30) := 'DELETE';
ct       VARCHAR2(30) := 'DL_HEM_ADMIN_GROUPINGS';
cs       INTEGER;
ce       VARCHAR2(200);
l_id     ROWID;
--
i        INTEGER := 0;
l_an_tab VARCHAR2(1);

BEGIN
  --
  fsc_utils.proc_start('s_dl_hem_admin_groupings.dataload_delete');
  fsc_utils.debug_message( 's_dl_hem_admin_groupings.dataload_delete',3 );
  --
  cb := p_batch_id;
  cd := p_date;
  s_dl_utils.update_process_summary(cb,cp,cd,'RUNNING');
  --
  FOR p1 IN c1 LOOP
    --
    BEGIN
    --
    cs := p1.lagr_dl_seqno;
    i := i +1;
    l_id := p1.rec_rowid;
    --
    UPDATE admin_groupings a1
    SET agr_direct_link = 'Y'
    WHERE agr_direct_link = 'N'
    AND   a1.agr_aun_code_parent = p1.lagr_aun_code_parent 
    AND   a1.agr_aun_code_child in 
                            (select a2.agr_aun_code_child from admin_groupings a2
                             where a2.agr_aun_code_parent = p1.lagr_aun_code_child
                               and a2.agr_direct_link = 'Y'
                               and not exists              
                                   (select null from admin_groupings a3, admin_groupings a4
                                     where a3.agr_aun_code_parent = p1.lagr_aun_code_parent
                                       and a4.agr_aun_code_child  = a2.agr_aun_code_child
                                       and a3.agr_aun_code_child  = a4.agr_aun_code_parent
                                       and a3.agr_aun_code_child  != p1.lagr_aun_code_child
                                   )
                            );
    --
    UPDATE admin_groupings_self a1
    SET agr_direct_link = 'Y'
    WHERE agr_direct_link = 'N'
    AND   a1.agr_aun_code_parent = p1.lagr_aun_code_parent 
    AND   a1.agr_aun_code_child in 
                            (select a2.agr_aun_code_child from admin_groupings_self a2
                             where a2.agr_aun_code_parent = p1.lagr_aun_code_child
                               and a2.agr_direct_link = 'Y'
                               and not exists              
                                   (select null from admin_groupings_self a3, admin_groupings_self a4
                                     where a3.agr_aun_code_parent = p1.lagr_aun_code_parent
                                       and a4.agr_aun_code_child  = a2.agr_aun_code_child
                                       and a3.agr_aun_code_child  = a4.agr_aun_code_parent
                                       and a3.agr_aun_code_child  != p1.lagr_aun_code_child
                                   )
                            );
    --
  
    DELETE FROM ADMIN_GROUPINGS
    WHERE agr_aun_code_child    = p1.lagr_aun_code_child
      and agr_aun_code_parent   = p1.lagr_aun_code_parent;
    --
    DELETE FROM admin_groupings a1
    WHERE agr_aun_code_child    = p1.lagr_aun_code_child
      AND agr_direct_link = 'N'
      AND NOT EXISTS              
          (select null from admin_groupings_self a3, admin_groupings_self a4
            where a3.agr_aun_code_parent = a1.agr_aun_code_parent
              and a3.agr_aun_code_child  = a4.agr_aun_code_parent
              and a4.agr_aun_code_child  = p1.lagr_aun_code_child
              and a4.agr_direct_link = 'Y' 
          );
    --
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
     ce := s_dl_errors.record_error(cb,cp,cd,ct,cs,'ORA',SQLCODE);
     set_record_status_flag(l_id,'C');
     s_dl_process_summary.update_processed_count(cb,cp,cd,'Y');
--
  END;
--
END LOOP;
--
-- Section to anayze the table(s) populated by this dataload
--
l_an_tab:=s_dl_hem_utils.dl_comp_stats('ADMIN_GROUPINGS');
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
--
--
END s_dl_hem_admin_groupings;
/

show errors