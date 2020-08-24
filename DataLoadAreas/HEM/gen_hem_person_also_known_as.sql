--
-- gen_hem_person_also_known_as.sql
--
--------------------------------- Comment -------------------------------------
--
-- Script to install the Person Also Known As Dataload Code
--
-------------------------------------------------------------------------------
--
--
-- Date          Ver  DB Ver   Name  Amendment(s)
-- ----          ---  ------   ----  ------------
-- 23-JAN-2019   1.0  6.18     JT    Initial Created for SAHT
--
--
-------------------------------------------------------------------------------
--
@./dl_hem_person_also_known_as_tab_new.sql
@./s_dl_hem_person_also_known_as.pks
@./s_dl_hem_person_also_known_as.pkb
@./dlas_in_hem_person_also_known_as.sql

