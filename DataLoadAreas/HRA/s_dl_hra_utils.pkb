CREATE OR REPLACE PACKAGE BODY s_dl_hra_utils
AS
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION  DB VER  WHO  WHEN       WHY
--      1.0          PJD  26/11/02   Product Dataload
--
-- ***********************************************************************
--
FUNCTION overlapping_contract
         (p_rac_accno  IN NUMBER
         ,p_par_refno  IN NUMBER
         ,p_start_date IN DATE
         ,p_end_date   IN DATE) RETURN BOOLEAN
IS
--
CURSOR c_exists
IS
SELECT 'X'
FROM   payment_contracts
WHERE  pct_rac_accno = p_rac_accno
  AND  pct_par_refno = p_par_refno
  AND  (   pct_start_date              between p_start_date   and nvl(p_end_date,sysdate)
        OR nvl(pct_end_date,sysdate)   between p_start_date   and nvl(p_end_date,sysdate)
        OR p_start_date                between pct_start_date and nvl(pct_end_date,sysdate)
        OR nvl(p_end_date,sysdate)     between pct_start_date and nvl(pct_end_date,sysdate)
       );
--
   l_exists VARCHAR2(1) := NULL;
   l_result BOOLEAN := FALSE;
BEGIN
--     
    OPEN  c_exists;
    FETCH c_exists INTO l_exists;
    IF c_exists%FOUND THEN
       l_result := TRUE;
    END IF;
    CLOSE c_exists;
--   
    RETURN( l_result );
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
END overlapping_contract;
--
 FUNCTION pct_refno
         (p_rac_accno  IN NUMBER
         ,p_par_refno  IN NUMBER
         ,p_start_date IN DATE
         ,p_end_date   IN DATE) RETURN INTEGER
IS
--
CURSOR c_par_refno
IS
SELECT pct_refno
FROM   payment_contracts
WHERE  pct_rac_accno                 = p_rac_accno
  AND  pct_par_refno                 = p_par_refno
  AND  pct_start_date               <= p_start_date   
  AND  NVL(pct_end_date,p_end_date) >= p_end_date;
--
   l_refno  INTEGER := NULL;
--
BEGIN
--    
    OPEN  c_par_refno;
    FETCH c_par_refno INTO l_refno;
    CLOSE c_par_refno;
--  
    RETURN( l_refno);
  EXCEPTION
   WHEN   Others THEN
     fsc_utils.handle_exception;
END pct_refno;
--
--
FUNCTION f_bru_run_no
(
 p_bru_aun_code VARCHAR2
,p_effective_date DATE
)
RETURN NUMBER IS
--
l_bru_run_no number default null;
--
CURSOR c_bru is
  select bru_run_no
  from   batch_runs
  where  bru_mod_name  = 'HRA069'
  and    bru_aun_code  = p_bru_aun_code
  and    p_effective_date  between bru_period_start_date
  and     bru_period_end_date;
--
BEGIN
--
open  c_bru;
fetch c_bru
into  l_bru_run_no;
close c_bru;
--
return (l_bru_run_no);
--
END f_bru_run_no;
--
-- Create Bank Details for VALID Bank/Account information
--
PROCEDURE insert_bank_details
    (p_bde_bank_name         IN varchar2,
     p_bde_branch_name       IN varchar2,
     p_bad_account_no        IN varchar2,
     p_bad_account_name      IN varchar2,
     p_bad_sort_code         IN varchar2,
     p_bad_start_date        IN DATE,
     p_bde_refno             OUT integer,
     p_bad_refno             OUT integer
     )
IS
--
CURSOR c_bank_details IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name = p_bde_bank_name;
--
CURSOR c_branch_details IS
SELECT bde_refno
  FROM bank_details
 WHERE bde_bank_name = p_bde_bank_name
   AND bde_branch_name = p_bde_branch_name;
--
--
l_bank_exists     number(10);
l_branch_exists   number(10);
l_bde_refno       number(10);
l_bad_refno       number(10);
quit_insert exception;
--
BEGIN
  -- Check Parent AND Child details supplied
  IF p_bde_bank_name IS NULL
  THEN
    raise quit_insert;
  ELSE
    OPEN  c_bank_details;
    FETCH c_bank_details INTO l_bank_exists;
    CLOSE c_bank_details;
    --
    IF p_bde_branch_name IS NOT NULL
    THEN
      OPEN  c_branch_details;
      FETCH c_branch_details INTO l_branch_exists;
      CLOSE c_branch_details;
    END IF;
  END IF;
  --
  -- IF Bank Details do NOT exist CREATE record.
  IF (l_bank_exists IS NULL
  OR  l_branch_exists IS NULL)
  THEN
    -- Get Bank Details refno
    SELECT bde_refno_seq.nextval INTO l_bde_refno FROM dual;
    p_bde_refno := l_bde_refno;
    --
    INSERT into bank_details(
      bde_refno,
      bde_bank_name,
      bde_created_by,
      bde_created_date,
      bde_branch_name )
    VALUES(
      l_bde_refno,
      p_bde_bank_name,
      user,
      TRUNC(sysdate),
      p_bde_branch_name );
  END IF;
 --
 -- IF Bank Account Details do NOT exist CREATE record.
 IF p_bad_account_no IS NOT NULL
 THEN
   IF p_bad_sort_code IS NOT NULL
   AND p_bad_account_name IS NOT NULL
   AND p_bad_start_date IS NOT NULL
   THEN
     -- Get Bank Account Details refno
     SELECT bad_refno_seq.nextval INTO l_bad_refno FROM dual;
     p_bad_refno := l_bad_refno;
     --
     INSERT into bank_account_details(
       bad_refno,
       bad_type,
       bad_sort_code,
       bad_bde_refno,
       bad_account_no,
       bad_account_name,
       bad_start_date,
       bad_created_by,
       bad_created_date )
     VALUES(
       l_bad_refno,
       'ORG',
       p_bad_sort_code,
       l_bde_refno,
       p_bad_account_no,
       p_bad_account_name,
       p_bad_start_date,
       user,
       TRUNC(sysdate) );
   ELSE
       raise quit_insert;
   END IF;
 END IF;
--
EXCEPTION
 WHEN quit_insert THEN
   NULL;
 WHEN   Others THEN
   fsc_utils.handle_exception;
--
END insert_bank_details;
END s_dl_hra_utils;

/

