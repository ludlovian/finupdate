import log from 'logjs'
import sortBy from 'sortby'

import { overwriteSheetData, putSheetData } from '../sheets.mjs'
import { get } from '../db.mjs'
import { exportDecimal } from './util.mjs'

const debug = log
  .prefix('export:positions:')
  .colour()
  .level(2)

const positions = { name: 'Positions', range: 'Positions!A2:I' }
const timestamp = { name: 'Positions', range: 'Positions!K1' }

export default async function exportPositions (opts) {
  const data = await getPositionsSheet(opts)

  await overwriteSheetData(positions.name, positions.range, data)
  await putSheetData(timestamp.name, timestamp.range, [[new Date()]])

  debug('position sheet updated')
}

async function getPositionsSheet (opts) {
  const sortFn = sortBy('ticker')
    .thenBy('who')
    .thenBy('account')

  const positions = await get('/position', opts)
  const stocks = await get('/stock', opts)

  return positions
    .filter(pos => pos.qty && pos.qty.cmp(0n) > 0)
    .sort(sortFn)
    .map(addStock(stocks))
    .map(addDerived)
    .map(makePositionRow)
}

function addStock (stocks) {
  return position => ({
    position,
    stock: stocks.find(stock => stock.ticker === position.ticker)
  })
}

function addDerived (data) {
  const { position: p, stock: s } = data
  if (s.price && s.dividend) {
    data.yield = s.dividend
      .withPrecision(6)
      .div(s.price)
      .withPrecision(3)
  }
  if (p.qty && s.price) {
    data.value = s.price.mul(p.qty).withPrecision(2)
  }
  if (p.qty && s.dividend) {
    data.income = s.dividend.mul(p.qty).withPrecision(2)
  }
  return data
}

function makePositionRow (data) {
  const { position: p, stock: s } = data
  return [
    p.ticker,
    p.who,
    p.account,
    exportDecimal(p.qty),
    exportDecimal(s.price),
    exportDecimal(s.dividend),
    exportDecimal(data.yield),
    exportDecimal(data.value),
    exportDecimal(data.income)
  ]
}
