CREATE OR REPLACE PACKAGE s_dl_mad_notepads
AS
  --  DESCRIPTION:
  --
  --  CHANGE CONTROL
  --  VER  DB Ver   WHO  WHEN       WHY
  --  1.0  5.16.0   VRS  17/10/2009   INITIAL Version
  --  2.0  6.11     AJ   05/03/2015   Changed Area from HEM (Estates) to MAD(Multi Area Dataload)
  --  2.1  6.11     AJ   01/06/2015 Added show errors at the bottom
  --  2.2  6.14     AJ   27/03/2017 Added a couple of blank lines at the bottom
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
END s_dl_mad_notepads;
--
/
show errors

