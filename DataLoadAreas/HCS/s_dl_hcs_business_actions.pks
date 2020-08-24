CREATE OR REPLACE PACKAGE s_dl_hcs_business_actions
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER  DB Ver   WHO  WHEN       WHY
  --  1.0  5.16.0   VRS  24/09/09   INITIAL Version
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
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
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
END s_dl_hcs_business_actions;
--
/
