DECLARE

BEGIN

s_dl_hem_admin_properties.dataload_DELETE
(p_batch_id => 'ADMIN_PROPERTIES_INITIAL'
,p_date     => TO_DATE('28-MAR-2017 11:53:49','DD-MON-YYYY HH24:MI:SS'));

s_dl_hem_admin_groupings.dataload_DELETE
(p_batch_id => 'ADMIN_GROUPINGS_INITIAL'
,p_date     => TO_DATE('28-MAR-2017 11:52:56','DD-MON-YYYY HH24:MI:SS'));


END;
/
