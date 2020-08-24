CREATE OR REPLACE PACKAGE s_dl_mad_other_field_values
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER  DB Ver   WHO  WHEN       WHY
  --  1.0  5.9.0    VRS  07/07/06   New Version
  --  2.0  5.16.0   VRS  21/09/09   add set_record_status_flag
  --  3.0  6.11     AJ   05/03/2015 Amended product Area from FSC (System)
  --                                to MAD(Multi Area)
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
END  s_dl_mad_other_field_values;
--
/

