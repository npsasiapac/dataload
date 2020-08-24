CREATE OR REPLACE PACKAGE s_dl_hco_products
AS
--*****************************************************************************
-- System      : Constractors
-- Sub-System  : Products
-- Author      : Karen Shannon
-- Date        : 23 May 2005
-- Description : Dataload Script for products
--*****************************************************************************
-- Change Control
-- Version  Who        Date         Description
-- 1.0      K Shannon  23-May-2005  Initial Creation.
-- 
-- 2.0      PJD        15-June-2006	Remove BULK COLLECT Method of processing.
--
--  VERSION DB Vers   WHO  WHEN         WHY
--  2.1     5.13.0    PH   06-FEB-2008  Now includes its own set
--                                      record status procedure
--                              
--
--  declare package variables AND constants
--
--
--***********************************************************************
--
PROCEDURE set_record_status_flag(p_rowid           IN ROWID,
                                 p_status          IN VARCHAR2);
--
PROCEDURE dataload_create (p_batch_id          IN VARCHAR2,
      			   p_date              IN DATE);

PROCEDURE dataload_validate (p_batch_id        IN VARCHAR2,
      			     p_date            IN DATE);

PROCEDURE dataload_delete (p_batch_id          IN VARCHAR2,
                           p_date              IN DATE);

END s_dl_hco_products;
/
