import { homedir } from 'os'
import { resolve } from 'path'

import SQLite from 'better-sqlite3'

import decimal from 'decimal'

const DB_NAME = 'findb.sqlite'

const t = s =>
  s[0]
    .split(/\n/)
    .map(s => s.trim())
    .join(' ')

const maybeDecimal = x => (x ? decimal(x) : undefined)

const db = new SQLite(resolve(homedir(), '.databases', DB_NAME))
db.pragma('journal_mode=WAL')
db.exec(t`
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
`)

const insertStock = db.prepare(t`
  INSERT INTO stock (
      ticker,
      name,
      incomeType,
      notes
    )
    VALUES (
      $ticker,
      $name,
      $incomeType,
      $notes
    )
    ON CONFLICT DO
    UPDATE SET
      name = excluded.name,
      incomeType = excluded.incomeType,
      notes = excluded.notes
`)

const clearDividends = db.prepare(t`
  UPDATE stock
  SET dividend = NULL
`)

const insertDividend = db.prepare(t`
  INSERT INTO stock (ticker, dividend)
    VALUES ($ticker, $dividend)
    ON CONFLICT DO UPDATE
    SET dividend = excluded.dividend
`)

const clearPositions = db.prepare(t`
  UPDATE position
  SET qty = NULL
`)

const insertPosition = db.prepare(t`
  INSERT INTO position (who, account, ticker, qty)
    VALUES ($who, $account, $ticker, $qty)
    ON CONFLICT DO UPDATE
    SET qty = excluded.qty
`)

const deletePositions = db.prepare(t`
  DELETE FROM position
  WHERE qty IS NULL
`)

const clearTrades = db.prepare(t`
  UPDATE trade
    SET qty = NULL,
        cost = NULL
`)

const insertTrade = db.prepare(t`
  INSERT INTO trade (who, account, ticker, seq, date, qty, cost, gain, notes)
    VALUES ($who, $account, $ticker, $seq, $date, $qty, $cost, $gain, $notes)
  ON CONFLICT DO UPDATE
    SET date = excluded.date,
        qty = excluded.qty,
        cost = excluded.cost,
        gain = excluded.gain,
        notes = excluded.notes
`)

const deleteTrades = db.prepare(t`
  DELETE FROM trade
  WHERE qty IS NULL
  AND   cost IS NULL
`)

const updateStockName = db.prepare(t`
  UPDATE stock
  SET   name = $name
  WHERE ticker = $ticker
    AND name IS NULL
`)

const clearPrices = db.prepare(t`
  UPDATE stock
  SET   price = NULL,
        priceSource = NULL,
        priceUpdated = NULL
`)

const updatePrice = db.prepare(t`
  UPDATE stock
  SET   price = $price,
        priceSource = $priceSource,
        priceUpdated = $priceUpdated
  WHERE ticker = $ticker
`)

const selectActiveTickers = db.prepare(t`
  SELECT ticker
    FROM stock
   WHERE dividend IS NOT NULL
  UNION
  SELECT ticker
    FROM position
`)

const selectPositions = db.prepare(t`
  SELECT p.ticker as ticker,
      who,
      account,
      qty,
      price,
      dividend
  FROM  position p
  INNER JOIN stock s
  WHERE p.ticker = s.ticker
  ORDER BY ticker, who, account
`)

const selectTrades = db.prepare(t`
  SELECT who,
        account,
        ticker,
        seq,
        date,
        qty,
        cost,
        gain
  FROM trade
  ORDER BY who, account, ticker, seq
`)

const selectStocks = db.prepare(t`
  SELECT ticker,
        incomeType,
        name,
        price,
        dividend,
        notes
  FROM stock
  ORDER BY ticker
`)

export const activeStockTickers = () => selectActiveTickers.pluck().all()

export const getPositions = () =>
  selectPositions
    .all()
    .map(({ ticker, who, account, qty, price, dividend }) => {
      let _yield
      let value
      let income
      qty = maybeDecimal(qty)
      price = maybeDecimal(price)
      dividend = maybeDecimal(dividend)
      if (price && dividend) {
        _yield = dividend
          .withPrecision(9)
          .div(price)
          .withPrecision(3)
      }
      if (qty && price) {
        value = price.mul(qty).withPrecision(2)
      }
      if (qty && dividend) {
        income = dividend.mul(qty).withPrecision(2)
      }
      return {
        ticker,
        who,
        account,
        qty,
        price,
        dividend,
        yield: _yield,
        value,
        income
      }
    })

export const getTrades = () =>
  selectTrades.all().map(row => {
    row.qty = maybeDecimal(row.qty)
    row.cost = maybeDecimal(row.cost)
    row.gain = maybeDecimal(row.gain)
    return row
  })

export const getStocks = () =>
  selectStocks.all().map(row => ({
    ...row,
    price: maybeDecimal(row.price),
    dividend: maybeDecimal(row.dividend)
  }))

export const updateStockDetails = db.transaction(stocks =>
  stocks.forEach(({ ticker, name, incomeType, notes }) =>
    insertStock.run({
      ticker,
      name: name || null,
      incomeType: incomeType || null,
      notes: notes || null
    })
  )
)

export const updateDividends = db.transaction(divis => {
  clearDividends.run()
  divis.forEach(({ ticker, dividend }) =>
    insertDividend.run({ ticker, dividend: dividend.toString() })
  )
})

export const updatePositions = db.transaction(positions => {
  clearPositions.run()
  positions.forEach(({ who, account, ticker, qty }) =>
    insertPosition.run({ who, account, ticker, qty: qty.toString() })
  )
  deletePositions.run()
})

export const updateTrades = db.transaction(trades => {
  clearTrades.run()
  trades.forEach(
    ({ who, account, ticker, seq, date, cost, qty, gain, notes }) =>
      insertTrade.run({
        who,
        account,
        ticker,
        seq,
        date,
        cost: cost ? cost.toString() : null,
        qty: qty ? qty.toString() : null,
        gain: gain ? gain.toString() : null,
        notes: notes || null
      })
  )
  deleteTrades.run()
})

export const updatePrices = db.transaction(prices => {
  clearPrices.run()
  prices.forEach(({ ticker, name, price, priceSource, priceUpdated }) => {
    updateStockName.run({ ticker, name })
    updatePrice.run({
      ticker,
      price: price.toString(),
      priceSource,
      priceUpdated
    })
  })
})
