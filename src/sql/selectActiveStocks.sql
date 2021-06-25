-- Returns the tickers of all stocks where
-- we want prices

-- These are all stocks where we have a
-- non-zero position, and all stocks
-- where we have entered dividends

SELECT ticker from stock
WHERE stockId IN (
    SELECT stockId
      FROM stock_dividend
    UNION
    SELECT stockId
      FROM position
      WHERE qty != 0
);
