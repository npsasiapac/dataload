--
-- ***********************************************************************
--  DESCRIPTION:
--
--  CHANGE CONTROL
--
--  VERSION DB Vers   WHO  WHEN         WHY
--
--  1.0               VS   2008         Initial Creation
--  1.0     6.11      AJ   22-Dec-2015  Added new functionality
--                                      released at v611 income liabilities
--
--
--
--***********************************************************************
--
-- Indexes for Income Dataloads NonICS_v611
--
--
CREATE UNIQUE INDEX PK_DL_HEM_INCOME_HEADERS ON
  DL_HEM_INCOME_HEADERS(LINH_DLB_BATCH_ID, LINH_DL_SEQNO) ;
--
CREATE UNIQUE INDEX PK_DL_HEM_INCOME_HEADER_USAGES ON
  DL_HEM_INCOME_HEADER_USAGES(LIHU_DLB_BATCH_ID, LIHU_DL_SEQNO) ;
--
CREATE UNIQUE INDEX PK_DL_HEM_INCOME_DETAILS ON
  DL_HEM_INCOME_DETAILS(LINDT_DLB_BATCH_ID, LINDT_DL_SEQNO) ;
--
CREATE UNIQUE INDEX PK_DL_HEM_ASSETS ON
  DL_HEM_ASSETS(LASSE_DLB_BATCH_ID, LASSE_DL_SEQNO) ;
--
CREATE UNIQUE INDEX PK_DL_HEM_INC_DET_DEDUCTIONS ON
  DL_HEM_INC_DET_DEDUCTIONS(LINDD_DLB_BATCH_ID, LINDD_DL_SEQNO) ;
  
CREATE UNIQUE INDEX PK_DL_HEM_INCOME_LIABILITIES ON
  DL_HEM_INCOME_LIABILITIES(LINLI_DLB_BATCH_ID, LINLI_DL_SEQNO) ;
  

