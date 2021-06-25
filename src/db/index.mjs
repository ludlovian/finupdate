import { homedir } from 'os'
import { join } from 'path'

import SQLite from 'better-sqlite3'

import SQL from '../lib/sql.mjs'
import * as sql from './sql.mjs'

let opened

const DB_VERSION = 2

export function open () {
  if (opened) return
  opened = true

  const dbFile = process.env.DB || join(homedir(), '.databases', 'findb.sqlite')
  const db = new SQLite(dbFile)
  SQL.attach(db)
  sql.ddl.run()
  const version = db
    .prepare('select version from dbversion')
    .pluck()
    .get()
  if (version !== DB_VERSION) {
    throw new Error('Wrong version of db: ' + dbFile)
  }
}

export const insertStocks = SQL.transaction(stocks => {
  for (const { ticker, name, incomeType, notes, source } of stocks) {
    sql.insertStock.run({
      ticker,
      name,
      incomeType: incomeType || null,
      notes: notes || null,
      source
    })
  }
})

export const insertPrices = SQL.transaction(prices => {
  for (const { ticker, name, price, source } of prices) {
    sql.insertPrice.run({
      ticker,
      name,
      price: price.digits,
      priceFactor: price.factor,
      source
    })
  }
  sql.clearOldPrices.run(7)
})

export const insertDividends = SQL.transaction(divs => {
  for (const { ticker, dividend, source } of divs) {
    sql.insertDividend.run({
      ticker,
      dividend: dividend ? dividend.digits : null,
      dividendFactor: dividend ? dividend.factor : null,
      source
    })
  }
  sql.clearOldDividends.run(7)
})

export const insertPositions = SQL.transaction(positions => {
  sql.clearPositions.run()
  for (const { ticker, person, account, qty, source } of positions) {
    sql.insertPosition.run({
      ticker,
      person,
      account,
      qty: qty ? qty.digits : 0,
      qtyFactor: qty ? qty.factor : 1,
      source
    })
  }
})

export const insertTrades = SQL.transaction((account, trades) => {
  sql.clearTrades.run(account)
  for (const trade of trades) {
    const {
      ticker,
      account,
      person,
      seq,
      date,
      qty,
      cost,
      gain,
      notes,
      source
    } = trade
    sql.insertTrade.run({
      ticker,
      account,
      person,
      seq,
      date,
      qty: qty ? qty.digits : null,
      qtyFactor: qty ? qty.factor : null,
      cost: cost ? cost.digits : null,
      costFactor: cost ? cost.factor : null,
      gain: gain ? gain.digits : null,
      gainFactor: gain ? gain.factor : null,
      notes: notes || null,
      source
    })
  }
  sql.deleteTrades.run()
})

export function selectActiveStocks () {
  return sql.selectActiveStocks.pluck().all()
}

export function selectPositionSheet () {
  return sql.selectPositionSheet.all()
}

export function selectTradeSheet () {
  return sql.selectTradeSheet.all()
}

export function selectStockSheet () {
  return sql.selectStockSheet.all()
}
