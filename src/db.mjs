import { homedir } from 'os'
import { resolve, join } from 'path'
import SQLite from 'better-sqlite3'
import decimal from 'decimal'
import { sql, ddl } from './sql.mjs'

const DB_NAME = process.env.DB_NAME || 'findb.sqlite'
const DB_DIR = process.env.DB_DIR || resolve(homedir(), '.databases')

const db = new SQLite(join(DB_DIR, DB_NAME))

db.pragma('journal_mode = WAL')
db.exec(tidy(ddl))
for (const k in sql) sql[k] = db.prepare(tidy(sql[k]))

export function activeStockTickers () {
  return sql.selectActiveTickers.pluck().all()
}

export function getStocks () {
  return sql.selectStocks.all().map(row =>
    adjust(row, {
      price: maybeDecimal,
      dividend: maybeDecimal
    })
  )
}

export function getPositions () {
  return sql.selectPositions.all().map(row => {
    row = adjust(row, {
      qty: maybeDecimal,
      price: maybeDecimal,
      dividend: maybeDecimal
    })
    return calcDerived(row)
  })
}

function calcDerived (row) {
  const { qty, price, dividend } = row
  if (price && dividend) {
    row.yield = dividend
      .withPrecision(9)
      .div(price)
      .withPrecision(3)
  }
  if (qty && price) {
    row.value = price.mul(qty).withPrecision(2)
  }
  if (qty && dividend) {
    row.income = dividend.mul(qty).withPrecision(2)
  }
  return row
}

export function getTrades () {
  return sql.selectTrades.all().map(row =>
    adjust(row, {
      qty: maybeDecimal,
      cost: maybeDecimal,
      gain: maybeDecimal
    })
  )
}

export const updateStockDetails = db.transaction(stocks =>
  stocks.forEach(({ ticker, name, incomeType, notes }) =>
    sql.insertStock.run({
      ticker,
      name: name || null,
      incomeType: incomeType || null,
      notes: notes || null
    })
  )
)

export const updateDividends = db.transaction(divis => {
  sql.clearAllDividends.run()
  divis.forEach(({ ticker, dividend }) =>
    sql.insertDividend.run({ ticker, dividend: dividend.toString() })
  )
})

export const updatePositions = db.transaction(positions => {
  sql.clearAllPositions.run()
  positions.forEach(({ who, account, ticker, qty }) =>
    sql.insertPosition.run({ who, account, ticker, qty: qty.toString() })
  )
  sql.deleteOldPositions.run()
})

export const updateTrades = db.transaction(trades => {
  sql.clearAllTrades.run()
  trades.forEach(
    ({ who, account, ticker, seq, date, cost, qty, gain, notes }) =>
      sql.insertTrade.run({
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
  sql.deleteOldTrades.run()
})

export const updatePrices = db.transaction(prices => {
  sql.clearAllPrices.run()
  prices.forEach(({ ticker, name, price, priceSource, priceUpdated }) => {
    sql.updateStockName.run({ ticker, name })
    sql.updatePrice.run({
      ticker,
      price: price.toString(),
      priceSource,
      priceUpdated
    })
  })
})

function adjust (obj, fns) {
  const ret = { ...obj }
  for (const k in fns) {
    ret[k] = fns[k].call(ret, ret[k])
  }
  return ret
}

function tidy (sql) {
  return sql
    .split('\n')
    .map(s => s.trim())
    .join(' ')
}

function maybeDecimal (x) {
  return x ? decimal(x) : undefined
}
