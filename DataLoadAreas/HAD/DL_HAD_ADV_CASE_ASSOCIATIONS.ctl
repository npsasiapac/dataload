--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.14      MJK  14-NOV-2017  Initial Creation.
--  1.1     6.18      PLL  12-SEP-2019  Fixed funcitons
--***********************************************************************
--
load data
APPEND
into table DL_HAD_ADV_CASE_ASSOCIATIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LACAN_DLB_BATCH_ID         CONSTANT "$BATCH_NO"
,LACAN_DL_SEQNO             RECNUM
,LACAN_DL_LOAD_STATUS       CONSTANT "L"
,LACAN_ACAS_ALTERNATE_REF   CHAR "rtrim(UPPER(:LACAN_ACAS_ALTERNATE_REF))"
,LACAN_APP_LEGACY_REF       CHAR "rtrim(UPPER(:LACAN_APP_LEGACY_REF))"
,LACAN_TCY_ALT_REF          CHAR "rtrim(UPPER(:LACAN_TCY_ALT_REF))"
,LACAN_HRV_ACAR_CODE        CHAR "rtrim(UPPER(:LACAN_HRV_ACAR_CODE))"
,LACAN_CREATED_BY           CHAR "rtrim(UPPER(:LACAN_CREATED_BY))"
,LACAN_CREATED_DATE         DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACAN_CREATED_DATE=blanks
,LACAN_COMMENTS             CHAR "rtrim(UPPER(:LACAN_COMMENTS))"
,LACAN_MODIFIED_BY	    CHAR "rtrim(UPPER(:LACAN_MODIFIED_BY))"
,LACAN_MODIFIED_DATE	    DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LACAN_MODIFIED_DATE=blanks
)
