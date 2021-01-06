-- Script to advance sequences to the maximum dataloaded value
--                            Sequence Name          Table Name                        Primary Key          Increment by
@advance_sequence.sql         que_refno_seq          questions                         que_refno            0
@advance_sequence.sql         qpr_refno_seq          question_permitted_responses      qpr_refno            0
@advance_sequence.sql         accg_refno_seq         applic_cat_rule_conditions        accg_refno           0
@advance_sequence.sql         dqr_refno_seq          dflt_question_restricts           dqr_refno            0
@advance_sequence.sql         mru_refno_seq          matching_rules                    mru_refno            0
@advance_sequence.sql         dqh_refno_seq          derived_question_headers          dqh_refno            0
@advance_sequence.sql         gque_refno_seq         generic_questions                 gque_reference       0
@advance_sequence.sql         gqgu_refno_seq         generic_question_grp_usages       gqgu_refno           0
@advance_sequence.sql         gdqu_refno_seq         generic_derived_question_usage    gdqu_refno           0