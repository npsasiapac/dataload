CREATE OR REPLACE PACKAGE s_dl_hrm_budgets
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VERSION     WHO  WHEN       WHY
  --      1.0     PH   19-JUN-02  Bespoke Dataload
  --
  --
  --  declare package variables AND constants
  --***********************************************************************
  --  DESCRIPTION
  --
  --  1:  ...
  --  2:  ...
  --  REFERENCES FUNCTION
  --
 --
PROCEDURE dataload_create       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_validate     (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
PROCEDURE dataload_delete       (p_batch_id        IN VARCHAR2,
                                 p_date            IN DATE);
--
END  s_dl_hrm_budgets;
--
/
