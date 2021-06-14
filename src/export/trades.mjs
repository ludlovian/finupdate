import log from 'logjs'

import { overwriteSheetData } from '../sheets.mjs'
import { getTrades } from '../db.mjs'
import { exportDecimal, exportDate } from './util.mjs'

const debug = log
  .prefix('export:trades:')
  .colour()
  .level(2)

const trades = { name: 'Positions', range: 'Trades!A2:G' }

export default async function exportTrades (opts) {
  const data = getTrades().map(makeTradeRow)

  await overwriteSheetData(trades.name, trades.range, data)
  debug('trades sheet updated')
}

function makeTradeRow (t) {
  return [
    t.who,
    t.account,
    t.ticker,
    exportDate(t.date),
    exportDecimal(t.qty),
    exportDecimal(t.cost),
    exportDecimal(t.gain)
  ]
}
