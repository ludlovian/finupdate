import log from 'logjs'

import { selectPositionSheet } from '../db/index.mjs'
import { overwriteSheetData, putSheetData } from '../sheets.mjs'

const debug = log
  .prefix('export:positions:')
  .colour()
  .level(2)

const positions = { name: 'Positions', range: 'Positions!A2:I' }
const timestamp = { name: 'Positions', range: 'Positions!K1' }

export default async function exportPositions (opts) {
  const data = selectPositionSheet().map(makePositionRow)
  await overwriteSheetData(positions.name, positions.range, data)
  await putSheetData(timestamp.name, timestamp.range, [[new Date()]])
  debug('position sheet updated')
}

function makePositionRow ({
  ticker,
  person,
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
    person,
    account,
    qty || 0,
    price || 0,
    dividend || 0,
    _yield || 0,
    value || 0,
    income || 0
  ]
}
