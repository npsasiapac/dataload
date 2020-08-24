CREATE OR REPLACE PACKAGE s_dl_hrm_con_site_job_roles
AS
--***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--  VERSION     WHO  WHEN         WHY
--      1.0     AJ   08-JUL-2016  Created new data load to add to Contractors
--                                to load job Role object rows also in main
--                                Contractors data load but can only use that
--                                when initially creating a Contractor and
--                                Contractor Site
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
END  s_dl_hrm_con_site_job_roles;
--
/
show errors
