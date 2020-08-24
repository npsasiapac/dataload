--
-- Date          Ver  DB Ver    Name  Amendment(s)
-- ----          ---  ------    ----  ------------
-- 27-MAR-2017   1.0  6.14/15   AJ    Initial Creation with control drop index added
--
-------------------------------------------------------------------------------
----Needed In addition to the standard PKs indexes that are created
--
DROP INDEX DL_OTHFLDVAL_PERF1;
DROP INDEX DL_OTHFLDVAL_PERF2;
--
CREATE INDEX DL_OTHFLDVAL_PERF1 ON
       DL_MAD_OTHER_FIELD_VALUES(LPVA_LEGACY_REF, LPVA_PDU_POB_TABLE_NAME, LPVA_BM_GRP_SEQ,
                                 LPVA_LEBE_REFNO, LPVA_LEBE_REUSABLE_REFNO);
--
CREATE INDEX DL_OTHFLDVAL_PERF2 ON
       PREVENTION_PAYMENTS(PPYT_ALTERNATIVE_REFERENCE);
--
/
