const sql = {}

const ddl = `
  CREATE TABLE IF NOT EXISTS stock(
    ticker TEXT PRIMARY KEY,
    name TEXT,
    incomeType TEXT,
    notes TEXT,
    dividend TEXT,
    price TEXT,
    priceSource TEXT,
    priceUpdated TEXT
  );

  CREATE TABLE IF NOT EXISTS position(
    who TEXT,
    account TEXT,
    ticker TEXT,
    qty TEXT,
    PRIMARY KEY (who, account, ticker)
  );

  CREATE TABLE IF NOT EXISTS trade(
    who TEXT,
    account TEXT,
    ticker TEXT,
    seq INTEGER,
    date TEXT,
    cost TEXT,
    qty TEXT,
    gain TEXT,
    notes TEXT,
    PRIMARY KEY (who, account, ticker, seq)
  );
`

sql.insertStock = `
  INSERT INTO stock
    (ticker, name, incomeType, notes)
  VALUES
    ($ticker, $name, $incomeType, $notes)
  ON CONFLICT DO UPDATE
    SET name       = excluded.name,
        incomeType = excluded.incomeType,
        notes      = excluded.notes
`

sql.clearAllDividends = `
  UPDATE stock
    SET dividend = NULL
`

sql.insertDividend = `
  INSERT INTO stock
    (ticker, dividend)
  VALUES
    ($ticker, $dividend)
  ON CONFLICT DO UPDATE
    SET dividend = excluded.dividend
`

sql.clearAllPositions = `
  UPDATE position
    SET qty = NULL
`

sql.insertPosition = `
  INSERT INTO position
    (who, account, ticker, qty)
  VALUES
    ($who, $account, $ticker, $qty)
  ON CONFLICT DO UPDATE
    SET qty = excluded.qty
`

sql.deleteOldPositions = `
  DELETE FROM position
    WHERE qty IS NULL
`

sql.clearAllTrades = `
  UPDATE trade
    SET qty  = NULL,
        cost = NULL
`

sql.insertTrade = `
  INSERT INTO trade
    (who, account, ticker, seq, date, qty, cost, gain, notes)
  VALUES
    ($who, $account, $ticker, $seq, $date, $qty, $cost, $gain, $notes)
  ON CONFLICT DO UPDATE
    SET date  = excluded.date,
        qty   = excluded.qty,
        cost  = excluded.cost,
        gain  = excluded.gain,
        notes = excluded.notes
`

sql.deleteOldTrades = `
  DELETE FROM trade
    WHERE qty  IS NULL
      AND cost IS NULL
`

sql.updateStockName = `
  UPDATE stock
    SET name = $name
  WHERE ticker = $ticker
    AND name IS NULL
`

sql.clearAllPrices = `
  UPDATE stock
    SET price        = NULL,
        priceSource  = NULL,
        priceUpdated = NULL
    WHERE dividend IS NULL
      OR ticker NOT IN (
        SELECT ticker
        FROM   position
      )
`

sql.updatePrice = `
  UPDATE stock
    SET  price        = $price,
         priceSource  = $priceSource,
         priceUpdated = $priceUpdated
    WHERE ticker = $ticker
`

sql.selectActiveTickers = `
  SELECT  ticker
    FROM  stock
    WHERE dividend IS NOT NULL
  UNION
  SELECT  ticker
    FROM  position
`

sql.selectPositions = `
  SELECT p.ticker as ticker, who, account, qty, price, dividend
    FROM  position p, stock s
    WHERE p.ticker = s.ticker
    ORDER BY ticker, who, account
`

sql.selectTrades = `
  SELECT who, account, ticker, seq, date, qty, cost, gain
    FROM trade
    ORDER BY who, account, ticker, seq
`

sql.selectStocks = `
  SELECT ticker, incomeType, name, price, dividend, notes
    FROM stock
    ORDER BY ticker
`
export { sql, ddl }
