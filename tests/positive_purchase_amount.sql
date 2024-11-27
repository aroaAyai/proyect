-- tests/positive_purchase_amount.sql

SELECT
    transaction_id,
    amount
FROM {{ ref('transaction') }}
WHERE transaction_type = 'compra' AND amount <= 0
