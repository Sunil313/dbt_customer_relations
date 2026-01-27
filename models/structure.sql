dbt_banking/
│
├── dbt_project.yml
├── packages.yml
├── README.md
│
├── macros/
│   ├── dates/
│   │   ├── get_business_date.sql
│   │   └── is_month_end.sql
│   │
│   ├── incremental/
│   │   ├── incremental_lookback.sql
│   │   └── merge_strategy.sql
│   │
│   ├── risk/
│   │   └── risk_rating.sql
│   │
│   └── utils/
│       └── surrogate_key.sql
│
├── models/
│   ├── sources/
│   │   ├── core_banking.yml
│   │   ├── payments.yml
│   │   ├── crm.yml
│   │   └── reference.yml
│   │
│   ├── staging/
│   │   ├── core_banking/
│   │   │   ├── stg_accounts.sql
│   │   │   ├── stg_transactions.sql
│   │   │   ├── stg_balances.sql
│   │   │   └── core_banking.yml
│   │   │
│   │   ├── payments/
│   │   │   ├── stg_card_transactions.sql
│   │   │   └── payments.yml
│   │   │
│   │   ├── crm/
│   │   │   ├── stg_customers.sql
│   │   │   └── crm.yml
│   │
│   ├── snapshots/
│   │   ├── customer_snapshot.sql
│   │   ├── account_snapshot.sql
│   │   └── product_snapshot.sql
│   │
│   ├── dimensions/
│   │   ├── dim_customer_360.sql
│   │   ├── dim_account.sql
│   │   ├── dim_product.sql
│   │   └── dim_calendar.sql
│   │
│   ├── facts/
│   │   ├── fct_transactions.sql
│   │   ├── fct_account_daily_balance.sql
│   │   ├── fct_credit_exposure.sql
│   │   ├── fct_insolvency.sql
│   │   └── fct_suspicious_activity.sql
│   │
│   ├── marts/
│   │   ├── finance/
│   │   │   ├── mart_profitability.sql
│   │   │   └── mart_interest_income.sql
│   │   │
│   │   ├── risk/
│   │   │   ├── mart_credit_risk.sql
│   │   │   └── mart_aml.sql
│   │   │
│   │   └── operations/
│   │       └── mart_customer_activity.sql
│   │
│   ├── reporting/
│   │   ├── rpt_fca_returns.sql
│   │   ├── rpt_ifrs_balances.sql
│   │   └── rpt_pra_exposures.sql
│   │
│   └── intermediate/
│       ├── int_customer_accounts.sql
│       ├── int_transaction_enriched.sql
│       └── int_balance_movements.sql
│
├── tests/
│   ├── generic/
│   │   ├── assert_positive_balance.sql
│   │   └── assert_valid_dates.sql
│   │
│   └── singular/
│       └── no_future_transactions.sql
│
└── seeds/
    ├── country_risk.csv
    ├── insolvency_codes.csv
    └── product_mapping.csv
