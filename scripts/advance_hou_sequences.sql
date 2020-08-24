-- Script to advance sequences to the maximum dataloaded value
--                                                Sequence Name          Table Name             Primary Key          Increment by
@advance_sequence.sql                             ban_reference_seq      business_actions       ban_reference        0
@advance_sequence.sql                             lta_refno_seq          land_title_assignments lta_refno            0
@advance_sequence.sql                             ltl_refno_seq          land_titles            ltl_refno            0
@advance_sequence.sql                             plpr_refno_seq         plc_property_requests  plpr_refno           1000000
@advance_sequence.sql                             pdai_refno_seq         plc_data_items         pdai_refno           100000
@advance_sequence.sql                             prta_refno_seq         plc_request_type_actions prta_refno         0
@advance_sequence.sql                             lwrb_seq               lwr_batches            lwrb_refno           0
@advance_sequence.sql                             pge_refno_seq          people_group_members   pge_refno            0
@advance_sequence.sql                             app_refno_seq          applications           app_refno            0
@advance_sequence.sql                             rdsa_seq               rds_authorities        rdsa_refno           2000000
@advance_sequence.sql                             rac_accno_seq          revenue_accounts       rac_accno            2000000
