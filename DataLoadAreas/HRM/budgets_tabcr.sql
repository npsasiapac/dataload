--Bespoke table create script for Birmingham CC
--
--  
--  CHANGE CONTROL
--  VERSION  DB VER    WHO  WHEN         WHY
--
--      1.0  5.5.0     IR   26-JUL-04    Create dl_hrm_budgets table
--      1.1  6.12.0    DLB  21-AUG-15    Change public grant for HOU_FULL.
--      1.2  6.12.0    AJ   21-AUG-15    Added grant for FSC_FULL as per DLB's advice.
--
drop table dl_hrm_budgets CASCADE CONSTRAINTS;
--
CREATE TABLE DL_HRM_BUDGETS
(LBUD_DLB_BATCH_ID                     VARCHAR2(30)   NOT NULL
,LBUD_DL_SEQNO                         NUMBER         NOT NULL
,LBUD_DL_LOAD_STATUS                   VARCHAR2(1)    NOT NULL
,LBUD_BHE_CODE                         VARCHAR2(30)   NOT NULL
,LBUD_BHE_DESCRIPTION                  VARCHAR2(40)
,LBUD_BHE_CREATED_BY                   VARCHAR2(30)
,LBUD_BHE_CREATED_DATE                 DATE
,LBUD_BCA_YEAR                         NUMBER(4)
,LBUD_TYPE                             VARCHAR2(1)
,LBUD_AUN_CODE                         VARCHAR2(20)
,LBUD_AMOUNT                           NUMBER(11,2)
,LBUD_ALLOW_NEGATIVE_IND               VARCHAR2(1)
,LBUD_REPEAT_WARNING_IND               VARCHAR2(1)
,LBUD_WARNING_ISSUED_IND               VARCHAR2(1)
,LBUD_SCO_CODE                         VARCHAR2(3)
,LBUD_CREATED_BY                       VARCHAR2(30)
,LBUD_CREATED_DATE                     DATE
,LBUD_BUD_BHE_CODE                     VARCHAR2(30)
,LBUD_BPCA_BPR_CODE                    VARCHAR2(8)
,LBUD_WARNING_PERCENT                  NUMBER(3)
,LBUD_COMMENTS                         VARCHAR2(240)
,LBUD_COMMITTED                        NUMBER(10,2)
,LBUD_ACCRUED                          NUMBER(10,2)
,LBUD_INVOICED                         NUMBER(10,2)
,LBUD_EXPENDED                         NUMBER(10,2)
,LBUD_CREDITED                         NUMBER(10,2)
,LBUD_TAX_COMMITTED                    NUMBER(10,2)
,LBUD_TAX_ACCRUED                      NUMBER(10,2)
,LBUD_TAX_INVOICED                     NUMBER(10,2)
,LBUD_TAX_EXPENDED                     NUMBER(10,2)
,LBUD_TAX_CREDITED                     NUMBER(10,2)
,LBUD_ARC_CODE                         VARCHAR2(8) )
 TABLESPACE &&table_tablespace PCTUSED 40 PCTFREE 10
 STORAGE(INITIAL 1024000 NEXT 1024000 PCTINCREASE 0 )
 PARALLEL (DEGREE 1 INSTANCES 1) NOCACHE;

 CREATE INDEX DL_HRM_BUDGETS ON 
  DL_HRM_BUDGETS (LBUD_DLB_BATCH_ID, LBUD_DL_SEQNO)
  TABLESPACE &&index_tablespace PCTFREE 10
  STORAGE(INITIAL 102400 NEXT 12288 PCTINCREASE 0 ) ;

GRANT ALL ON DL_HRM_BUDGETS TO HOU_FULL;
create or replace public synonym dl_hrm_budgets for dl_hrm_budgets;

GRANT ALL ON DL_HRM_BUDGETS TO FSC_FULL;
create or replace public synonym dl_hrm_budgets for dl_hrm_budgets;
