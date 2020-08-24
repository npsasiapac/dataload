--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0     6.14      AJ   13-OCT-2017 Initial creation for GNB Migrate Project
--  1.1     6.14      AJ   18-OCT-2017 Extract field notes comments updated
--  1.2     6.14      AJ   15-JAN-2018 Fields amended to match those now required
--                                     datetime from created and modified only
--  1.3     6.14      AJ   16-JAN-2018 Field iph_ipa_legacy_ref added
--
--         Extract field notes
--         *******************
--         LIPH_APP_LEGACY_REF  = IPH_APP_REFNO
--         LIPH_PAR_PER_ALT_REF = PAR_PER_ALT_REF from parties ELSE IPH_PAR_REFNO
--                                in s_ex_hem_people extract par_per_alt_ref is
--                                extracted as par_refno
--         LIPH_IPA_START_DATE = From involved_parties table used to get ipa_refno
--                               using the unique(uk) combination on the table
--         LIPH_IPA_LEGACY_REF = legacy ref on involved parties table extract to
--                               hold ipa_refno to be added as direct link to find
--                               associated involved party record (other option)
--
--***********************************************************************
--
load data
infile $gri_datafile
APPEND
into table DL_HAT_INVOLVED_PARTY_HIST
fields terminated by "," optionally enclosed by '"'
trailing nullcols       
( LIPH_DLB_BATCH_ID            CONSTANT "$batch_no",             
  LIPH_DL_SEQNO                RECNUM,  
  LIPH_DL_LOAD_STATUS          CONSTANT "L",
  LIPH_APP_LEGACY_REF          CHAR "rtrim(:LIPH_APP_LEGACY_REF)",
  LIPH_PAR_PER_ALT_REF         CHAR "rtrim(:LIPH_PAR_PER_ALT_REF)",
  LIPH_IPA_START_DATE          DATE "DD-MON-YYYY" NULLIF LIPH_IPA_START_DATE=blanks,
  LIPH_IPA_LEGACY_REF          CHAR "rtrim(:LIPH_IPA_LEGACY_REF)",  
  LIPH_MODIFIED_DATE           DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIPH_MODIFIED_DATE=blanks,  
  LIPH_MODIFIED_BY             CHAR "rtrim(:LIPH_MODIFIED_BY)",  
  LIPH_ACTION_IND              CHAR "rtrim(:LIPH_ACTION_IND)",
  LIPH_JOINT_APPL_IND          CHAR "rtrim(:LIPH_JOINT_APPL_IND)",
  LIPH_LIVING_APART_IND        CHAR "rtrim(:LIPH_LIVING_APART_IND)",
  LIPH_REHOUSE_IND             CHAR "rtrim(:LIPH_REHOUSE_IND)",
  LIPH_MAIN_APPLICANT_IND      CHAR "rtrim(:LIPH_MAIN_APPLICANT_IND)",
  LIPH_CREATED_DATE            DATE "DD-MON-YYYY HH24:MI:SS" NULLIF LIPH_CREATED_DATE=blanks,
  LIPH_CREATED_BY              CHAR "rtrim(:LIPH_CREATED_BY)",
  LIPH_START_DATE              DATE "DD-MON-YYYY" NULLIF LIPH_START_DATE=blanks,
  LIPH_END_DATE                DATE "DD-MON-YYYY" NULLIF LIPH_END_DATE=blanks,
  LIPH_GROUPNO                 CHAR "rtrim(:LIPH_GROUPNO)",
  LIPH_ACT_ROOMNO              CHAR "rtrim(:LIPH_ACT_ROOMNO)",
  LIPH_FRV_END_REASON          CHAR "rtrim(:LIPH_FRV_END_REASON)",
  LIPH_FRV_RELATION            CHAR "rtrim(:LIPH_FRV_RELATION)"
)

