-- select the data for the positions sheet

SELECT
    p.ticker    AS ticker,
    p.person    AS person,
    p.account   AS account,
    p.qty       AS qty,
    p.value     AS value,
    p.income    AS income,
    s.price     AS price,
    s.dividend  AS dividend,
    s.yield     AS yield

FROM    position_view p,
        stock_view s

WHERE   p.ticker = s.ticker

ORDER BY    ticker,
            person,
            account;
