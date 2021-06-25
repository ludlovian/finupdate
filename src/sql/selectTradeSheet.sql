-- Selects the data for the trade sheet

SELECT
    person,
    account,
    ticker,
    date,
    qty,
    cost,
    gain

FROM trade_view

ORDER BY
    person,
    account,
    ticker,
    tradeId;

