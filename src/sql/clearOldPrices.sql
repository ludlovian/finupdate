-- removes old prices

-- params
--  1:  number of days

DELETE FROM stock_price
  WHERE updated < datetime('now', '-' || ? || ' days');
