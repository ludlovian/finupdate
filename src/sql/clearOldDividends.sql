-- removes old dividends

-- params
--  1:  number of days

DELETE FROM stock_dividend
  WHERE updated < datetime('now', '-' || ? || ' days');
