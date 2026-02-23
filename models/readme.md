Fact: 
- fct_claim_reserve_snapshots (your current model; keep it as a fact)
- fct_account_daily_balance.sqldaily balances, transactions, amounts 


Related Dimensions:
- dim_account: relatively static attributes of the account (owner, product, currency, branch, status, open/close dates, etc.)
- dim_account_balance_scd2: account_id + effective period (when the closing_balance value was valid). We’ll compress consecutive dates with the same balance into one row.
- dim_claim — attributes of the claim (status, cause, loss date, etc.)
 -  dim_policy — SCD2 based policy attributes 
 -  dim_product — product attributes, 1:many to policy
 -  dim_customer — customer attributes (person or organization)
 -  dim_reserve_type — small, static domain (e.g., case, expense, IBNR)
 -  dim_date — calendar dimension 
