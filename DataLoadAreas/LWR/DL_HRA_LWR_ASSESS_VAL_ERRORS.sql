--------------------------------------------------------
--  File created - Wednesday-September-26-2018   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table DL_HRA_LWR_ASSESS_VAL_ERRORS
--------------------------------------------------------

  CREATE TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" 
   (	"LASVE_DLB_BATCH_ID" VARCHAR2(30 BYTE), 
	"LASVE_DL_SEQNO" NUMBER(10,0), 
	"LASVE_DL_LOAD_STATUS" VARCHAR2(1 BYTE), 
	"LASVE_LWRB_REFNO" NUMBER(10,0), 
	"LASVE_WRA_CURR_ASSESSMENT_REF" VARCHAR2(20 BYTE), 
	"LWRA_RATE_PERIOD_START_DATE" DATE, 
	"LWRA_RATE_PERIOD_END_DATE" DATE, 
	"LASVE_FLVE_CODE" VARCHAR2(10 BYTE), 
	"LASVE_CREATED_BY" VARCHAR2(30 BYTE), 
	"LASVE_CREATED_DATE" DATE
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "TABLES" ;
--------------------------------------------------------
--  DDL for Index PK_DL_LWR_ASSESS_VAL_ERRORS
--------------------------------------------------------

  CREATE UNIQUE INDEX "HOU"."PK_DL_LWR_ASSESS_VAL_ERRORS" ON "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" ("LASVE_DLB_BATCH_ID", "LASVE_DL_SEQNO") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "INDEXES" ;
--------------------------------------------------------
--  Constraints for Table DL_HRA_LWR_ASSESS_VAL_ERRORS
--------------------------------------------------------

  ALTER TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" MODIFY ("LASVE_FLVE_CODE" NOT NULL ENABLE);
  ALTER TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" MODIFY ("LWRA_RATE_PERIOD_END_DATE" NOT NULL ENABLE);
  ALTER TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" MODIFY ("LWRA_RATE_PERIOD_START_DATE" NOT NULL ENABLE);
  ALTER TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" MODIFY ("LASVE_WRA_CURR_ASSESSMENT_REF" NOT NULL ENABLE);
  ALTER TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" MODIFY ("LASVE_LWRB_REFNO" NOT NULL ENABLE);
  ALTER TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" MODIFY ("LASVE_DL_LOAD_STATUS" NOT NULL ENABLE);
  ALTER TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" MODIFY ("LASVE_DL_SEQNO" NOT NULL ENABLE);
  ALTER TABLE "HOU"."DL_HRA_LWR_ASSESS_VAL_ERRORS" MODIFY ("LASVE_DLB_BATCH_ID" NOT NULL ENABLE);
