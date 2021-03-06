load data
infile $gri_datafile
APPEND
into table DL_HPP_PP_APPLICATIONS
fields terminated by "," optionally enclosed by '"'
trailing nullcols
(LPAPP_DLB_BATCH_ID                CONSTANT "$batch_no"
,LPAPP_DL_SEQNO                    RECNUM
,LPAPP_DL_LOAD_STATUS              CONSTANT "L"
,LPAPP_PRO_PROPREF                 CHAR "rtrim(:LPAPP_PRO_PROPREF)"
,LPAPP_DISPLAYED_REFERENCE         CHAR "rtrim(:LPAPP_DISPLAYED_REFERENCE)"
,LPAPP_TCY_ALT_REF                 CHAR "rtrim(:LPAPP_TCY_ALT_REF)"
,LPAPP_PATY_CODE                   CHAR "rtrim(:LPAPP_PATY_CODE)"
,LPAPP_PAST_CODE                   CHAR "rtrim(:LPAPP_PAST_CODE)"
,LPAPP_LANDLORD_IPP_SHORTNAME      CHAR "rtrim(:LPAPP_LANDLORD_IPP_SHORTNAME)"
,LPAPP_APPLICATION_DATE            DATE "DD-MON-YYYY" NULLIF LPAPP_APPLICATION_DATE=blanks
,LPAPP_CORR_NAME                   CHAR "rtrim(:LPAPP_CORR_NAME)"
,LPAPP_COMMENTS                    CHAR(2000) "rtrim(:LPAPP_COMMENTS)"
,LPAPP_PAPP_REFNO                  CHAR "rtrim(:LPAPP_PAPP_REFNO)"
,LPAPP_COST_FLOOR_AMOUNT           CHAR "rtrim(:LPAPP_COST_FLOOR_AMOUNT)"
,LPAPP_DISCOUNTED_PRICE            CHAR "rtrim(:LPAPP_DISCOUNTED_PRICE)"
,LPAPP_CALCULATED_DISCOUNT         CHAR "rtrim(:LPAPP_CALCULATED_DISCOUNT)"
,LPAPP_PAPR_CODE                   CHAR "rtrim(:LPAPP_PAPR_CODE)"
,LPAPP_SCO_CODE                    CHAR "rtrim(:LPAPP_SCO_CODE)"
,LPAPP_SCO_CHANGED_DATE            DATE "DD-MON-YYYY" NULLIF LPAPP_SCO_CHANGED_DATE=blanks
,LPAPP_HELD_DATE                   DATE "DD-MON-YYYY" NULLIF LPAPP_HELD_DATE=blanks
,LPAPP_ADMITTED_DATE               DATE "DD-MON-YYYY" NULLIF LPAPP_ADMITTED_DATE=blanks
,LPAPP_COMPLETED_DATE              DATE "DD-MON-YYYY" NULLIF LPAPP_COMPLETED_DATE=blanks
,LPAPP_FORMER_STATUS_CODE          CHAR "rtrim(:LPAPP_FORMER_STATUS_CODE)"
,LPAPP_RTM_HB_ENTITLED_IND         CHAR "rtrim(:LPAPP_RTM_HB_ENTITLED_IND)"
,LPAPP_RTM_HB_WITHDRAWN_IND        CHAR "rtrim(:LPAPP_RTM_HB_WITHDRAWN_IND)"
,LPAPP_RTM_EQUITY                  CHAR "rtrim(:LPAPP_RTM_EQUITY)"
,LPAPP_USTAT_CODE                  CHAR "rtrim(:LPAPP_USTAT_CODE)"
,LPAPP_MORTGAGE_ACCNO              CHAR "rtrim(:LPAPP_MORTGAGE_ACCNO)"
,LPAPP_LEGAL_COMMENTS              CHAR "rtrim(:LPAPP_LEGAL_COMMENTS)"
,LPAPP_LENDER_IPP_SHORTNAME        CHAR "rtrim(:LPAPP_LENDER_IPP_SHORTNAME)"
,LPAPP_SOLICITOR_IPP_SHORTNAME     CHAR "rtrim(:LPAPP_SOLICITOR_IPP_SHORTNAME)"
,LPAPP_SLT_CODE                    CHAR "rtrim(:LPAPP_SLT_CODE)"
,LPAPP_PALT_CODE                   CHAR "rtrim(:LPAPP_PALT_CODE)"
,LPAPP_TITLE_REF                   CHAR "rtrim(:LPAPP_TITLE_REF)"
,LPAPP_LAND_REG_COMMENTS           CHAR "rtrim(:LPAPP_LAND_REG_COMMENTS)"
,LPAPP_INSURER_IPP_SHORTNAME       CHAR "rtrim(:LPAPP_INSURER_IPP_SHORTNAME)"
,LPAPP_INSURANCE_NO                CHAR "rtrim(:LPAPP_INSURANCE_NO)"
,LPAPP_INSURANCE_VALUATION         CHAR "rtrim(:LPAPP_INSURANCE_VALUATION)"
,LPAPP_MORTGAGE_AMOUNT             CHAR "rtrim(:LPAPP_MORTGAGE_AMOUNT)"
,LPAPP_LEASE_YEARS                 CHAR "rtrim(:LPAPP_LEASE_YEARS)"
,LPAPP_YEARS_AS_TENANT             CHAR "rtrim(:LPAPP_YEARS_AS_TENANT)"
,LPAPP_PERIOD_START                DATE "DD-MON-YYYY" NULLIF LPAPP_PERIOD_START=blanks
,LPAPP_REF_PERIOD_END              DATE "DD-MON-YYYY" NULLIF LPAPP_REF_PERIOD_END=blanks
,LPAPP_RTM_WEEKLY_RENT             CHAR "rtrim(:LPAPP_RTM_WEEKLY_RENT)"
,LPAPP_RTM_MULTIPLIER              CHAR "rtrim(:LPAPP_RTM_MULTIPLIER)"
,LPAPP_RTM_MIN_INIT_AMOUNT         CHAR "rtrim(:LPAPP_RTM_MIN_INIT_AMOUNT)"
,LPAPP_RTM_MAX_INIT_AMOUNT         CHAR "rtrim(:LPAPP_RTM_MAX_INIT_AMOUNT)"
,LPAPP_RTM_INIT_DISCOUNT           CHAR "rtrim(:LPAPP_RTM_INIT_DISCOUNT)"
,LPAPP_RTM_LANDLORD_SHARE          CHAR "rtrim(:LPAPP_RTM_LANDLORD_SHARE)"
,LPAPP_CURRENT_LANDLORD_IND        CHAR "rtrim(:LPAPP_CURRENT_LANDLORD_IND)"
,LPAPP_DISCOUNT_AMOUNT             CHAR "rtrim(:LPAPP_DISCOUNT_AMOUNT)"
,LPAPP_PREV_DISCOUNT               CHAR "rtrim(:LPAPP_PREV_DISCOUNT)"
,LPAPP_RTM_CALCULATE_DISCOUNT      CHAR "rtrim(:LPAPP_RTM_CALCULATE_DISCOUNT)"
,LPAPP_RTM_CALC_DISC_PERCENT       CHAR "rtrim(:LPAPP_RTM_CALC_DISC_PERCENT)"
,LPAPP_RTM_PRICE_AFTER_DISCOUNT    CHAR "rtrim(:LPAPP_RTM_PRICE_AFTER_DISCOUNT)"
,LPAPP_RTM_MIN_ICP_AMOUNT          CHAR "rtrim(:LPAPP_RTM_MIN_ICP_AMOUNT)"
,LPAPP_RTM_DFC_AT_PURCHASE         CHAR "rtrim(:LPAPP_RTM_DFC_AT_PURCHASE)"
,LPAPP_RTM_DFC_PERCENT             CHAR "rtrim(:LPAPP_RTM_DFC_PERCENT)"
,LPAPP_RTM_ADJ_WEEKLY_RENT         CHAR "rtrim(:LPAPP_RTM_ADJ_WEEKLY_RENT)"
,LPAPP_RTM_ACT_ICP_AMOUNT          CHAR "rtrim(:LPAPP_RTM_ACT_ICP_AMOUNT)"
,LPAPP_RTM_LOAN_PERIOD             CHAR "rtrim(:LPAPP_RTM_LOAN_PERIOD)"
,LPAPP_RTM_NUM_RENT_WEEKS          CHAR "rtrim(:LPAPP_RTM_NUM_RENT_WEEKS)"
,LPAPP_DSE_CODE                    CHAR "rtrim(:LPAPP_DSE_CODE)"
,LPAPP_MAX_DISCOUNT_AMOUNT         CHAR "rtrim(:LPAPP_MAX_DISCOUNT_AMOUNT)"
)

