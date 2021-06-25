-- select for the stocks csv

SELECT
    ticker,
    incomeType,
    name,
    price,
    dividend,
    notes

FROM stock_view

ORDER BY ticker;
