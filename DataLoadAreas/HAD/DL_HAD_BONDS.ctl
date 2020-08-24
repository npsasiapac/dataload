--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     5.15.0    VS   13-JAN-2009  Initial Creation.
--  1.1     6.10      AJ   14-DEC-2015  Control comments section added
--                                      also added LBND_CLAIM_AMOUNT to ctl
--                                      to match doc and pkb
--
--
--
--***********************************************************************
--
load data
APPEND
into table DL_HAD_BONDS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LBND_DLB_BATCH_ID              CONSTANT "$BATCH_NO"
,LBND_DL_SEQNO                  RECNUM
,LBND_DL_LOAD_STATUS            CONSTANT "L"
,LBND_ACHO_LEGACY_REF           CHAR "rtrim(UPPER(:LBND_ACHO_LEGACY_REF))"
,LBND_SCO_CODE                  CHAR "rtrim(UPPER(:LBND_SCO_CODE))"
,LBND_STATUS_DATE               DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBND_STATUS_DATE=blanks
,LBND_AUN_CODE                  CHAR "rtrim(UPPER(:LBND_AUN_CODE))"
,LBND_CREATED_BY                CHAR "rtrim(UPPER(:LBND_CREATED_BY))"
,LBND_CREATED_DATE              DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBND_CREATED_DATE=blanks
,LBND_REFERENCE                 CHAR "rtrim(UPPER(:LBND_REFERENCE))"
,LBND_BOND_LODGEMENT_NUMBER     CHAR "rtrim(UPPER(:LBND_BOND_LODGEMENT_NUMBER))"
,LBND_BOND_AMOUNT               CHAR "rtrim(UPPER(:LBND_BOND_AMOUNT))"
,LBND_CONTRIBUTION_AMOUNT       CHAR "rtrim(UPPER(:LBND_CONTRIBUTION_AMOUNT))"
,LBND_HRV_BOVR_CODE             CHAR "rtrim(UPPER(:LBND_HRV_BOVR_CODE))"
,LBND_OVERRIDE_AMOUNT           CHAR "rtrim(UPPER(:LBND_OVERRIDE_AMOUNT))"
,LBND_OVERRIDE_DATE             DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LBND_OVERRIDE_DATE=blanks
,LBND_OVERRIDE_USERNAME         CHAR "rtrim(UPPER(:LBND_OVERRIDE_USERNAME))"
,LBND_REFUND_AMOUNT             CHAR "rtrim(UPPER(:LBND_REFUND_AMOUNT))"
,LBND_IPP_SHORTNAME             CHAR "rtrim(UPPER(:LBND_IPP_SHORTNAME))"
,LBND_HRV_BCR_CODE              CHAR "rtrim(UPPER(:LBND_HRV_BCR_CODE))"
,LBND_CLAIM_AMOUNT              CHAR "rtrim(UPPER(:LBND_CLAIM_AMOUNT))"
,LBND_REFNO                     INTEGER EXTERNAL "bnd_refno_seq.nextval"
)



