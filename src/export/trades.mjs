import log from 'logjs'
import sortBy from 'sortby'

import { get } from '../db.mjs'
import { overwriteSheetData } from '../sheets.mjs'
import { exportDecimal, exportDate } from './util.mjs'

const debug = log
  .prefix('export:trades:')
  .colour()
  .level(2)

const trades = { name: 'Positions', range: 'Trades!A2:G' }

export default async function exportTrades (opts) {
  const data = await getTradesSheet(opts)

  await overwriteSheetData(trades.name, trades.range, data)
  debug('trades sheet updated')
}

async function getTradesSheet (opts) {
  const sortFn = sortBy('who')
    .thenBy('account')
    .thenBy('ticker')
    .thenBy('seq')

  const trades = await get('/trade', opts)

  return trades.sort(sortFn).map(makeTradeRow)
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
