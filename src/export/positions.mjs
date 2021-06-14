import log from 'logjs'

import { getPositions } from '../db.mjs'
import { overwriteSheetData, putSheetData } from '../sheets.mjs'
import { exportDecimal } from './util.mjs'

const debug = log
  .prefix('export:positions:')
  .colour()
  .level(2)

const positions = { name: 'Positions', range: 'Positions!A2:I' }
const timestamp = { name: 'Positions', range: 'Positions!K1' }

export default async function exportPositions (opts) {
  const data = getPositions().map(makePositionRow)
  await overwriteSheetData(positions.name, positions.range, data)
  await putSheetData(timestamp.name, timestamp.range, [[new Date()]])
  debug('position sheet updated')
}

function makePositionRow ({
  ticker,
  who,
  account,
  qty,
  price,
  dividend,
  yield: _yield,
  value,
  income
}) {
  return [
    ticker,
    who,
    account,
    exportDecimal(qty),
    exportDecimal(price),
    exportDecimal(dividend),
    exportDecimal(_yield),
    exportDecimal(value),
    exportDecimal(income)
  ]
}
