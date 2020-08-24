-----------------------------------------
-- This is done so that the dataload 
-- can add things to existing applications
------------------------------------------

UPDATE applications
SET app_legacy_ref = app_refno
WHERE app_legacy_ref is null;

COMMIT;
