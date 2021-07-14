import log from 'logjs'

import { overwriteSheetData } from '../sheets.mjs'
import { sql } from '../db/index.mjs'
import { exportDate } from './util.mjs'

const debug = log
  .prefix('export:trades:')
  .colour()
  .level(2)

const trades = { name: 'Positions', range: 'Trades!A2:G' }

export default async function exportTrades (opts) {
  const data = selectTradeSheet().map(makeTradeRow)

  await overwriteSheetData(trades.name, trades.range, data)
  debug('trades sheet updated')
}

function makeTradeRow (t) {
  return [
    t.person,
    t.account,
    t.ticker,
    exportDate(t.date),
    t.qty || 0,
    t.cost || 0,
    t.gain || 0
  ]
}

const selectTradeSheet = sql(`
  SELECT  person, account, ticker, date, qty, cost, gain
  FROM    trade_view
  ORDER BY person, account, ticker, tradeId
`).all
